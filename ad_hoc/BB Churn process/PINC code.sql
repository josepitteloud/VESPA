
SELECT 
      c.subs_week_of_year AS event_wk
    , d.subs_week_of_year AS Next_status_wk
    , end_date 
    , a.* 
	,  CASE 		WHEN BB_Enter_SysCan + BB_Enter_CusCan + BB_Enter_HM + BB_Enter_3rd_Party > 1 THEN 'MULTI'		--- UPDATED next
					WHEN BB_Enter_SysCan > 0 			THEN 'SysCan' 
					WHEN BB_Enter_CusCan > 0 		THEN 'CusCan'
					WHEN BB_Enter_HM	  > 0 		THEN 'HM'
					WHEN BB_Enter_3rd_Party > 0 	THEN '3rd Party' 
					ELSE NULL END 							AS Churn_type 
    , x.DTV_Active
    ,x.BB_Active
    ,x.Package_Desc
    ,x.DTV_First_Act_Date
    ,x.DTV_Latest_Act_Date
    ,x.Curr_Offer_Start_Date
    ,x.Curr_Offer_End_Date
    ,x.TA_Call_Count
    ,x.TA_Call_Flag
    ,x.Churn_Type AS TV_churn_type
    ,x.PL_Entry_Type
    ,x.Prev_offer_end_date_BB
    ,x.Prev_offer_start_date_BB
    ,x.Prev_offer_end_Date_intended_BB
    ,x.Curr_Offer_end_Date_BB
    ,x.Curr_Offer_start_Date_BB
    ,x.curr_offer_end_date_Intended_BB
    ,x.affluence_bands
    ,x.BB_Churn_Next7d
    ,x.Triple_Play_Churn
    ,x.age
    ,x.BB_Enter_SysCan
    ,x.BB_Enter_CusCan
    ,x.BB_Enter_HM
    ,x.BB_Enter_3rd_Party
    ,x.BB_Status_Code
    , CAST(NULL AS VARCHAR(50)) BB_package
    , CAST(NULL AS VARCHAR(50)) AS Simple_Segment
    , CAST(NULL AS VARCHAR(50)) AS RTM
    , CAST(NULL AS INT) AS Talk_tenure_raw
    , CAST(NULL AS INT) AS my_sky_login_3m_raw
    , CAST(NULL AS INT) AS BB_all_calls_1m_raw
    , CAST(NULL AS INT) AS BB_offer_rem_and_end_raw
    , CAST(NULL AS VARCHAR(50)) AS home_owner_status
    , CAST(NULL AS INT) AS BB_tenure_raw
    , CAST(NULL AS VARCHAR(50)) AS talk_type
    , CAST(NULL AS VARCHAR(50)) AS CQM_Segment
    , CAST(NULL AS INT) DTV_TA_calls_1m_raw
	, ABS(DATEDIFF (day, end_date, a.event_dt)) 						AS diff 
    , rank() OVER (PARTITION BY a.account_number ORDER BY diff ) 			AS rankk
	, CAST (NULL AS INT ) AS node_sa 
	, CAST (NULL AS VARCHAR(30)) AS segment_sa 
	, CAST (NULL AS VARCHAR(30)) AS group_sa 
	, CAST (NULL AS INT ) AS node_tp
	, CAST (NULL AS VARCHAR(30)) AS segment_tp
	, CAST (NULL AS VARCHAR(30)) AS group_tp
	
INTO PINC_analysis_2
from citeam.Broadband_Comms_Pipeline AS a 
JOIN sky_calendar AS c ON a.event_dt = c.calendar_date 
JOIN sky_calendar AS d ON d.calendar_date = COALESCE(PC_effective_to_dt , AB_effective_to_dt , BCRQ_effective_to_dt)
LEFT JOIN cust_fcast_weekly_base AS x ON x.account_number = a.account_number and diff BETWEEN 0 AND 7 
WHERE event_dt >= '2016-12-07' 

        
		DELETE FROM PINC_analysis_2 WHERE rankk >1 
		CREATE HG INDEX id1 on PINC_analysis_2(account_number) 
		CREATE DATE INDEX id1 on PINC_analysis_2(event_dt) 
	GO 	
SELECT csh.account_number 
		,effective_from_dt as churn_date --add this into table also
		,csh.status_code 
		,RANK() OVER (PARTITION BY  csh.account_number ORDER BY  csh.effective_from_dt ASC,csh.cb_row_id) AS rankk --Rank to get the first event
		, churn_type
INTO #all_churn_records
FROM cust_subs_hist as csh
JOIN PINC_analysis_2 AS b ON csh.account_number = b.account_number AND csh.effective_from_dt >= b.event_dt 
 WHERE 
		subscription_sub_type ='Broadband DSL Line' 	--DTV stack
   and csh.status_code in ('PO','SC','CN')              	--CUSCAN and SYSCAN status codes
   AND (b.AB_next_status_code in ('BCRQ')     OR b.PC_next_status_code in ('BCRQ') )
   and status_code_changed = 'Y'
   and effective_from_dt != effective_to_dt

commit
   
DELETE FROM #all_churn_records WHERE rankk >1 
CREATE HG INDEX id1 on #all_churn_records(account_number) 

UPDATE PINC_analysis_2
SET PC_next_status_code = CASE WHEN b.churn_type IN ('CusCan','HM','3rd Party' ) 	THEN b.status_code ELSE PC_next_status_code END 
	,AB_next_status_code = CASE WHEN b.churn_type IN ('SysCan' ) 					THEN b.status_code ELSE AB_next_status_code END 
	,PC_effective_to_dt = CASE WHEN b.churn_type IN ('CusCan','HM','3rd Party' ) 	THEN b.churn_date ELSE PC_effective_to_dt END 
	,AB_effective_to_dt = CASE WHEN b.churn_type IN ('SysCan') 					THEN b.churn_date ELSE AB_effective_to_dt END 
FROM PINC_analysis_2 AS a 
JOIN #all_churn_records AS b On a.account_number = b.account_number  AND a.event_dt >= churn_date

	COMMIT 
	GO
	
	----------------------=======================-*******************************************
SELECT CSH.account_number
		,end_date
		,MAX(CSH.effective_from_dt) AS DT_last_TALK
		,DATEDIFF(day, DT_last_TALK, end_date)  AS tenure
INTO #talk_tenure
FROM cust_subs_hist 			AS CSH
JOIN PINC_analysis_2 			AS BASE   ON BASE.account_number = CSH.account_number
WHERE csh.subscription_sub_type = 'SKY TALK SELECT'
	AND   csh.status_code IN  ('A', 'AC')
	AND   csh.prev_status_code NOT IN ('PC','FBP','BCRQ')
	AND   CSH.status_code_changed = 'Y'
	AND   end_date >= effective_from_dt
GROUP BY  CSH.account_number, end_date

COMMIT 

UPDATE PINC_analysis_2
SET talk_tenure_raw = CASE 	WHEN tenure <= 65 	THEN 1 
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

FROM PINC_analysis_2 		AS base
JOIN #talk_tenure 			AS hold ON hold.account_number = base.account_number and BASE.end_date = HOLD.end_date

MESSAGE 'talk_tenure updated: '||@@rowcount type status to client

------------------------------------------------------------------
SELECT COUNT(DISTINCT visit_date) AS visit_days
      , mr.account_number
	  , event_dt
INTO #days_visited_3m
FROM vespa_shared.mysky_daily_usage 	AS mr 
JOIN PINC_analysis_2 	AS base ON BASE.account_number = mr.account_number
WHERE visit_date BETWEEN DATEADD(mm,-3,event_dt) AND event_dt
GROUP BY mr.account_number, event_dt
COMMIT 
CREATE HG INDEX id1 ON #days_visited_3m (account_number) 

UPDATE PINC_analysis_2
SET my_sky_login_3m_raw  = CASE WHEN visit_days > 2 THEN 3 ELSE visit_days END 
FROM PINC_analysis_2 AS base
INNER JOIN #days_visited_3m AS dv ON base.account_number = dv.account_number AND base.event_dt = dv.event_dt

MESSAGE 'my_sky_login_3m updated: '||@@rowcount type status to client

COMMIT 
DROP TABLE #days_visited_3m
GO 
----------------------------------------------------------------


SELECT base.account_number
      , COUNT(1) AS call_count
	  , event_dt
INTO #BBCalls_Temp_1m
FROM cust_inbound_calls					AS temp
JOIN PINC_analysis_2 	AS base 	ON    base.account_number = temp.account_number
WHERE call_date BETWEEN DATEADD(mm,-1,event_dt) AND DATEADD(dd,-1,event_dt)
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
GROUP BY base.account_number, event_dt

COMMIT 
CREATE HG INDEX id1 ON #BBCalls_Temp_1m (account_number) 
COMMIT 

UPDATE PINC_analysis_2
SET BB_all_calls_1m_raw = CASE WHEN call_count = 0 THEN 0 ELSE 1 END 
FROM PINC_analysis_2 	AS base
JOIN #BBCalls_Temp_1m 					AS temp ON base.account_number = temp.account_number AND base.event_dt = temp.event_dt


---------------



UPDATE PINC_analysis_2 
SET CQM_Segment = CAST(COALESCE (CQM_Score,0) AS VARCHAR)
	, hh_key = cb_key_household
FROM PINC_analysis_2 	AS a 
JOIN attach_view_all					AS b ON a.account_number = b.account_number 
											AND CAST(	YEAR(a.end_date)||(RIGHT('00'||MONTH(a.end_date),2)) AS INT )= monthyear

COMMIT 
MESSAGE 'CQM_Segment updated: '||@@rowcount type status to client

GO
---------------

SELECT cv.cb_key_household	
		,CASE WHEN h_tenure_v2 = '0' THEN 'Owner'
            WHEN h_tenure_v2 = '1' THEN 'Private Rent'
            WHEN h_tenure_v2 = '2' THEN 'Council Rent'
            ELSE 'UNKNOWN'
 		  END AS home_owner       
		,rank() over(PARTITION BY cv.cb_key_household ORDER BY cb_row_id DESC) AS rank
INTO #experian 
FROM EXPERIAN_CONSUMERVIEW 		AS CV
JOIN PINC_analysis_2 			AS base	ON cv.cb_key_household = base.hh_key

COMMIT 
CREATE HG INDEX id1 ON #experian (cb_key_household) 
COMMIT 

UPDATE PINC_analysis_2
SET home_owner_status = home_owner              
FROM PINC_analysis_2 AS base
INNER JOIN #experian ON #experian.cb_key_household = base.hh_key 
WHERE rank = 1

MESSAGE 'home_owner_status updated: '||@@rowcount type status to client

COMMIT 
DROP TABLE #experian
GO
---------------



SELECT CSH.account_number
      ,current_product_description
	  ,end_date
      ,rank() over(PARTITION BY base.account_number ORDER BY effective_from_dt ASC, CSH.cb_row_id ASC) AS rank  
INTO #talk_holding
FROM cust_subs_hist 						AS CSH
INNER JOIN PINC_analysis_2 AS BASE  ON BASE.account_number = CSH.account_number
WHERE csh.subscription_sub_type = 'SKY TALK SELECT'
AND   csh.status_code IN ('A','PC','FBP','RI','FBI','BCRQ')
AND   end_date BETWEEN effective_from_dt AND effective_to_dt
AND   effective_to_dt > effective_from_dt

COMMIT 
CREATE HG INDEX id1 ON #talk_holding (account_number) 
COMMIT 

UPDATE PINC_analysis_2
SET talk_type = current_product_description
FROM PINC_analysis_2 	AS base
INNER JOIN #talk_holding 				AS hold	ON hold.account_number = base.account_number AND base.end_date = hold.end_date
WHERE rank = 1

MESSAGE 'talk_type updated: '||@@rowcount type status to client

COMMIT 
DROP TABLE #talk_holding
GO

-----------------



--Current BB Offer Length
SELECT base.account_number
      ,MAX(offer_duration) AS offer_length
      ,MAX(DATEDIFF(DD,end_date,intended_offer_end_dt)) AS length_rem
INTO #current_bb_offer_length      
FROM PINC_analysis_2 AS base
INNER JOIN offer_usage_all AS oua
ON oua.account_number = base.account_number
WHERE subs_type = 'Broadband DSL Line'
AND end_date >= offer_start_dt_actual
AND end_date <  offer_end_dt_actual
AND intended_total_offer_value_yearly IS NOT NULL
GROUP BY base.account_number;

UPDATE PINC_analysis_2
SET BB_current_offer_duration_rem = CASE WHEN length_rem > 2854 THEN 2854
                                          WHEN length_rem < 0    THEN 0
                                          ELSE length_rem 
                                    END 
FROM PINC_analysis_2 AS base
INNER JOIN #current_bb_offer_length AS offer
ON offer.account_number = base.account_number;


--Last BB Offer End Date
SELECT base.account_number
	, offer_end_dt_actual
	, rank() OVER (PARTITION BY base.account_number ORDER BY offer_start_dt_actual DESC) AS latest_offer
INTO #prev_bb_offer_dt
FROM PINC_analysis_2 AS base
INNER JOIN offer_usage_all AS oua ON oua.account_number = base.account_number
WHERE subs_type = 'Broadband DSL Line' 
		AND end_date > offer_start_dt_actual 
		AND end_date >= offer_end_dt_actual 
		AND intended_total_offer_value_yearly IS NOT NULL;

UPDATE PINC_analysis_2
SET BB_time_since_last_offer_end = DATEDIFF(DD, offer_end_dt_actual, end_date)
FROM PINC_analysis_2 AS base
INNER JOIN #prev_bb_offer_dt AS offer ON offer.account_number = base.account_number
WHERE latest_offer = 1;

--Combined BB Offer Start and End
UPDATE PINC_analysis_2
SET BB_offer_rem_and_end_raw = CASE WHEN BB_current_offer_duration_rem > 0 THEN BB_current_offer_duration_rem 
								WHEN BB_current_offer_duration_rem = 0 AND BB_time_since_last_offer_end <> - 9999 THEN (0 - BB_time_since_last_offer_end) 
								ELSE - 9999 END;
								
								
--========================================================================================
UPDATE PINC_analysis_2 
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





-----------------------------		


SELECT 	csh.Account_number
		, end_date
		, MAX(effective_from_dt) max_bb_act
		, BB_tenure_days = DATEDIFF(dd,max_bb_act,end_date)
INTO    #Act
FROM    cust_subs_hist 						AS csh
JOIN	PINC_analysis_2	AS acc ON  csh.account_number = acc.account_number 
													AND end_date BETWEEN effective_from_dt AND effective_to_dt
WHERE  	csh.status_code = 'AC'
        AND csh.Prev_status_code NOT IN ('AB','AC','PC')
        AND csh.subscription_sub_type ='Broadband DSL Line'
        AND csh.status_code_changed = 'Y'
		AND csh.effective_to_dt > effective_from_dt
GROUP BY 	csh.Account_number, end_date

COMMIT 
CREATE HG INDEX id1 ON #Act (account_number) 
COMMIT 

UPDATE PINC_analysis_2
SET BB_tenure_raw = CASE 	WHEN BB_tenure_days <= 118 				 THEN 1
						WHEN BB_tenure_days BETWEEN 119 AND 231  THEN 2
						WHEN BB_tenure_days BETWEEN 231 AND 329  THEN 3
						WHEN BB_tenure_days BETWEEN 329 AND 391  THEN 4
						WHEN BB_tenure_days BETWEEN 392 AND 499  THEN 5
						WHEN BB_tenure_days BETWEEN 499 AND 641  THEN 6
						WHEN BB_tenure_days BETWEEN 641 AND 1593 THEN 7
						WHEN BB_tenure_days > 1593 				 THEN 8	
						ELSE -1 END 
FROM PINC_analysis_2 AS a 
JOIN #Act 			AS b ON a.account_number = b.account_number AND a.end_date = b.end_date 

COMMIT 
MESSAGE 'BB_tenure updated: '||@@rowcount type status to client

DROP TABLE #Act

GO
-----------------------------------------
UPDATE PINC_analysis_2
SET Simple_Segment = SS.segment
FROM PINC_analysis_2 AS base
INNER JOIN simple_segments_history  AS SS ON SS.account_number = base.account_number
WHERE SS.Observation_date = '2016-12-30'
	
	AND segment IS NOT NULL
	AND (Simple_Segment = 'UNKNOWN' OR  Simple_Segment IS NULL ) 




	
	
	
	
	 
SELECT BASE.account_number
		, event_dt
      ,RANK() over(PARTITION BY bbo.account_number,event_dt ORDER BY order_dt DESC) AS rank
      ,rtm_level_1
INTO #RTM
FROM PINC_analysis_2 	AS base
INNER JOIN citeam.DM_BROADBAND_ORDERS 	AS bbo	ON bbo.account_number = base.account_number 
WHERE CHANGE_TYPE NOT IN ('A','C','M','R') 
AND bbo.order_dt <= event_dt

COMMIT

UPDATE PINC_analysis_2
SET RTM    = rtm_level_1
FROM PINC_analysis_2 AS base
JOIN #RTM ON #RTM.account_number = base.account_number AND base.event_dt = #RTM.event_dt
WHERE rank = 1

MESSAGE 'talk_tenure updated: '||@@rowcount type status to client
COMMIT 
DROP TABLE #RTM
GO
	
	
	------------------------------------====================================
	
	

SELECT base.account_number
		,base.event_dt
		,SUM(ta_c.total_calls) AS Num_past1m_TA
INTO #ta_previous
FROM PINC_analysis_2 	AS base
JOIN view_cust_calls_hist 				AS ta_c		ON base.account_number = ta_c.account_number 
												AND ta_c.event_dt BETWEEN DATEADD(mm,-1,base.event_dt) AND DATEADD(dd,-1,base.event_dt) 
												AND ta_c.DTV = 1 
												AND ta_c.typeofevent IN ('TA') 
GROUP BY base.account_number, base.event_dt

COMMIT

UPDATE PINC_analysis_2
SET DTV_TA_calls_1m_raw  = CASE WHEN Num_past1m_TA > 0 THEN 1 ELSE 0 END 
FROM PINC_analysis_2 AS base
JOIN #ta_previous AS t1 ON base.account_number = t1.account_number AND base.event_dt = t1.event_dt

MESSAGE 'DTV_TA_calls_1m updated: '||@@rowcount type status to client
COMMIT 
DROP TABLE #ta_previous
GO
	
	-------------------------------------
	
	
	
	
SElect 






UPDATE PINC_analysis_2
SET node_sa			 = CASE WHEN BB_offer_rem_and_end_raw IN (-9999,-3) AND BB_tenure_raw  = 1                                                                                                       	THEN 9
							WHEN BB_offer_rem_and_end_raw IN (-9999,-3) AND BB_tenure_raw  IN (2,-1,0, 3)   				                                                                            THEN 10
							WHEN BB_offer_rem_and_end_raw IN (-9999,-3) AND BB_tenure_raw  IN (4,5) 				                                                                                	THEN 11	-- = 499	
							WHEN BB_offer_rem_and_end_raw IN (-9999,-3) AND BB_tenure_raw  = 6		                                                                                         			THEN 12	--<= 641
							WHEN BB_offer_rem_and_end_raw IN (-9999,-3) AND BB_tenure_raw  = 7 AND my_sky_login_3m_raw = 0 AND home_owner_status IN ('Council Rent','Private Rent','UNKNOWN')            	THEN 60	/* <= 1593 */ 
							WHEN BB_offer_rem_and_end_raw IN (-9999,-3) AND BB_tenure_raw  = 7 AND my_sky_login_3m_raw = 0 AND home_owner_status IN ('Owner')                                            	THEN 61 /* <= 1593 */ 
							WHEN BB_offer_rem_and_end_raw IN (-9999,-3) AND BB_tenure_raw  = 7 AND my_sky_login_3m_raw > 0 AND home_owner_status IN ('Council Rent','Owner')                             	THEN 62 /* <= 1593 */ 
							WHEN BB_offer_rem_and_end_raw IN (-9999,-3) AND BB_tenure_raw  = 7 AND my_sky_login_3m_raw > 0 AND home_owner_status IN ('Private Rent','UNKNOWN')                           	THEN 63 /* <= 1593 */ 
							WHEN BB_offer_rem_and_end_raw IN (-9999,-3) AND BB_tenure_raw  = 8 AND my_sky_login_3m_raw = 0 AND talk_type NOT IN ('Sky Pay As You Talk','Sky Talk Evenings and Weekends') 	THEN 64 /* > 1593 */ 
							WHEN BB_offer_rem_and_end_raw IN (-9999,-3) AND BB_tenure_raw  = 8 AND my_sky_login_3m_raw = 0 AND talk_type IN ('Sky Pay As You Talk','Sky Talk Evenings and Weekends')     	THEN 65 /* > 1593 */ 
							WHEN BB_offer_rem_and_end_raw IN (-9999,-3) AND BB_tenure_raw  = 8 AND my_sky_login_3m_raw > 0                                                                               	THEN 35 /* > 1593 */ 
							WHEN BB_offer_rem_and_end_raw = -2  AND my_sky_login_3m_raw = 0 AND talk_type 	  IN ('Sky Pay As You Talk','NONE','Sky Talk Freetime','Sky Talk Unlimited')         			THEN 36 
							WHEN BB_offer_rem_and_end_raw = -2  AND my_sky_login_3m_raw = 0 AND talk_type NOT IN ('Sky Pay As You Talk','NONE','Sky Talk Freetime','Sky Talk Unlimited') AND home_owner_status IN ('Council Rent','Private Rent','UNKNOWN')   THEN 66
							WHEN BB_offer_rem_and_end_raw = -2  AND my_sky_login_3m_raw = 0 AND talk_type NOT IN ('Sky Pay As You Talk','NONE','Sky Talk Freetime','Sky Talk Unlimited') AND home_owner_status IN ('Owner')                                   THEN 67
							WHEN BB_offer_rem_and_end_raw = -2  AND my_sky_login_3m_raw > 0 AND talk_type NOT IN ('Sky Talk Anytime','Sky Talk Freetime','Sky Talk Anytime Extra') AND home_owner_status IN ('Council Rent','Owner')                          THEN 68
							WHEN BB_offer_rem_and_end_raw = -2  AND my_sky_login_3m_raw = 1 AND talk_type NOT IN ('Sky Talk Anytime','Sky Talk Freetime','Sky Talk Anytime Extra') AND home_owner_status IN ('Private Rent','UNKNOWN')                        THEN 82
							WHEN BB_offer_rem_and_end_raw = -2  AND my_sky_login_3m_raw > 1 AND talk_type NOT IN ('Sky Talk Anytime','Sky Talk Freetime','Sky Talk Anytime Extra') AND home_owner_status IN ('Private Rent','UNKNOWN')                        THEN 83
							WHEN BB_offer_rem_and_end_raw = -2  AND my_sky_login_3m_raw > 0 AND talk_type IN ('Sky Talk Anytime','Sky Talk Freetime','Sky Talk Anytime Extra')                        	THEN 39
							WHEN BB_offer_rem_and_end_raw = -1  AND my_sky_login_3m_raw = 0 AND talk_type NOT IN ('Sky Talk International Extra','Sky Talk Anytime Extra')                            	THEN 40
							WHEN BB_offer_rem_and_end_raw = -1  AND my_sky_login_3m_raw = 0 AND talk_type IN ('Sky Talk International Extra','Sky Talk Anytime Extra')                                	THEN 41
							WHEN BB_offer_rem_and_end_raw = -1  AND my_sky_login_3m_raw > 0 AND home_owner_status IN ('Council Rent','Owner','UNKNOWN')                                               	THEN 42
							WHEN BB_offer_rem_and_end_raw = -1  AND my_sky_login_3m_raw > 0 AND home_owner_status IN ('Private Rent')                                                                 	THEN 43
							WHEN BB_offer_rem_and_end_raw = 0   AND my_sky_login_3m_raw = 0 AND talk_type IN ('Sky Pay As You Talk','NONE','Sky Talk Freetime','Sky Talk Unlimited')     AND home_owner_status IN ('Council Rent','Private Rent','UNKNOWN')   THEN 70
							WHEN BB_offer_rem_and_end_raw = 0   AND my_sky_login_3m_raw = 0 AND talk_type IN ('Sky Pay As You Talk','NONE','Sky Talk Freetime','Sky Talk Unlimited')     AND home_owner_status IN ('Owner')                                   THEN 71
							WHEN BB_offer_rem_and_end_raw = 0   AND my_sky_login_3m_raw = 0 AND talk_type NOT IN ('Sky Pay As You Talk','NONE','Sky Talk Freetime','Sky Talk Unlimited') AND home_owner_status IN ('Council Rent','Owner')                    THEN 72
							WHEN BB_offer_rem_and_end_raw = 0   AND my_sky_login_3m_raw = 0 AND talk_type NOT IN ('Sky Pay As You Talk','NONE','Sky Talk Freetime','Sky Talk Unlimited') AND home_owner_status IN ('Private Rent','UNKNOWN')                  THEN 73
							WHEN BB_offer_rem_and_end_raw = 0   AND my_sky_login_3m_raw > 0 AND talk_type IN ('Sky Pay As You Talk','NONE','Sky Talk Freetime','Sky Talk Unlimited')                       				THEN 46
							WHEN BB_offer_rem_and_end_raw = 0   AND my_sky_login_3m_raw > 0 AND talk_type NOT IN ('Sky Pay As You Talk','NONE','Sky Talk Freetime','Sky Talk Unlimited')                   				THEN 47
							WHEN BB_offer_rem_and_end_raw = 1   AND talk_type NOT IN ('Sky Pay As You Talk','Sky Talk Evenings and Weekends')                                                          				THEN 21
							WHEN BB_offer_rem_and_end_raw = 1   AND talk_type IN ('Sky Pay As You Talk')                                                                                               				THEN 22
							WHEN BB_offer_rem_and_end_raw = 1	AND talk_type IN ('Sky Talk Evenings and Weekends') AND my_sky_login_3m_raw = 0 AND home_owner_status IN ('Council Rent','Owner')          				THEN 74
							WHEN BB_offer_rem_and_end_raw = 1	AND talk_type IN ('Sky Talk Evenings and Weekends') AND my_sky_login_3m_raw = 0 AND home_owner_status IN ('Private Rent','UNKNOWN')        				THEN 75
							WHEN BB_offer_rem_and_end_raw = 1	AND talk_type IN ('Sky Talk Evenings and Weekends') AND my_sky_login_3m_raw > 0                                                            				THEN 49
							WHEN BB_offer_rem_and_end_raw = 2	AND home_owner_status IN ('Council Rent')                                                                                              				THEN 24
							WHEN BB_offer_rem_and_end_raw = 2	AND home_owner_status IN ('Owner') AND talk_type NOT IN ('Sky Pay As You Talk','Sky Talk Evenings and Weekends')                       				THEN 50
							WHEN BB_offer_rem_and_end_raw = 2	AND home_owner_status IN ('Owner') AND talk_type IN ('Sky Pay As You Talk','Sky Talk Evenings and Weekends')                           				THEN 51
							WHEN BB_offer_rem_and_end_raw = 2	AND home_owner_status IN ('Private Rent','UNKNOWN') AND my_sky_login_3m_raw = 0 AND BB_tenure_raw IN (0,-1,1,2)                                				THEN 76
							WHEN BB_offer_rem_and_end_raw = 2	AND home_owner_status IN ('Private Rent','UNKNOWN') AND my_sky_login_3m_raw = 0 AND BB_tenure_raw IN (3,4,5,6,7,8)                             				THEN 77
							WHEN BB_offer_rem_and_end_raw = 2	AND home_owner_status IN ('Private Rent','UNKNOWN') AND my_sky_login_3m_raw > 0                                                            				THEN 53
							WHEN BB_offer_rem_and_end_raw = 3	AND home_owner_status IN ('Council Rent')                                                                                              				THEN 27
							WHEN BB_offer_rem_and_end_raw = 3	AND home_owner_status IN ('Owner') AND talk_type NOT IN ('Sky Pay As You Talk','Sky Talk Evenings and Weekends')                       				THEN 54
							WHEN BB_offer_rem_and_end_raw = 3	AND home_owner_status IN ('Owner') AND talk_type IN ('Sky Pay As You Talk','Sky Talk Evenings and Weekends')                           				THEN 55
							WHEN BB_offer_rem_and_end_raw = 3	AND home_owner_status IN ('Private Rent','UNKNOWN') AND my_sky_login_3m_raw = 0				                                                            THEN 56
							WHEN BB_offer_rem_and_end_raw = 3	AND home_owner_status IN ('Private Rent','UNKNOWN') AND my_sky_login_3m_raw > 0                                                            				THEN 57
							WHEN BB_offer_rem_and_end_raw = 4	AND BB_all_calls_1m_raw = 0 AND home_owner_status IN ('Council Rent','Private Rent','UNKNOWN') AND BB_tenure_raw IN (0,-1,1,2,3,4) AND my_sky_login_3m_raw = 0 	THEN 84
							WHEN BB_offer_rem_and_end_raw = 4	AND BB_all_calls_1m_raw = 0 AND home_owner_status IN ('Council Rent','Private Rent','UNKNOWN') AND BB_tenure_raw IN (0,-1,1,2,3,4)	AND my_sky_login_3m_raw > 0 THEN 85
							WHEN BB_offer_rem_and_end_raw = 4	AND BB_all_calls_1m_raw = 0 AND home_owner_status IN ('Council Rent','Private Rent','UNKNOWN') AND BB_tenure_raw IN (5,6,7,8)                               THEN 79
							WHEN BB_offer_rem_and_end_raw = 4	AND BB_all_calls_1m_raw = 0 AND home_owner_status IN ('Owner') AND talk_type IN ('Sky Pay As You Talk','NONE','Sky Talk Anytime Extra')                 THEN 80
							WHEN BB_offer_rem_and_end_raw = 4	AND BB_all_calls_1m_raw = 0 AND home_owner_status IN ('Owner') AND talk_type NOT IN ('Sky Pay As You Talk','NONE','Sky Talk Anytime Extra')             THEN 81
							WHEN BB_offer_rem_and_end_raw = 4	AND BB_all_calls_1m_raw > 0                                                                                                                             THEN 31  
							ELSE 0
                         END
		

UPDATE PINC_analysis_2
SET segment_sa    = CASE WHEN node_sa IN ( 22 , 46 , 49 , 70 , 75 , 71) THEN 'A1. End of Offer - Financially Constrained'
                                    WHEN node_sa IN ( 83 , 53 , 43 , 82 , 73 , 57) THEN 'A2. Transient Population'
                                    WHEN node_sa IN ( 63 , 47 , 68 , 42 , 62 , 12 , 39 , 11 , 35) THEN 'A3. Shopping Around'
                                    WHEN node_sa IN ( 21 , 74 , 72) THEN 'B1. End of Offer - Not Financially Constrained'
                                    WHEN node_sa IN ( 40 , 36 , 66 , 60 , 65) THEN 'B2. Out of Offer - Financially Constrained'
                                    WHEN node_sa IN ( 77 , 31 , 84 , 56 , 76) THEN 'B3. On Offer - Financially Constrained'
                                    WHEN node_sa IN ( 10 , 41 , 67) THEN 'B4. Out of Offer - Not Financially Constrained'
                                    WHEN node_sa IN ( 61 , 51 , 64 , 24 , 50) THEN 'C1. Stable Customer'
                                    WHEN node_sa IN ( 27 , 55 , 85 , 81 , 79 , 80 , 54) THEN 'C2. Long Term Offer'
                                    WHEN node_sa IN ( 9) THEN 'C3. New Full Price Customer'
                                    ELSE segment_sa
                               END
,  group_sa = CASE 	WHEN node_sa IN ( 22,  46,  49,  70,  75,  71,  83,  53,  43,  82,  73,  57,  63,  47,  68,  42,  62,  12,  39,  11,  35) THEN 'High Risk'
					WHEN node_sa IN ( 21,  74,  72,  40,  36,  66,  60,  65,  77,  31,  84,  56,  76,  10,  41,  67) THEN 'Medium Risk'
					WHEN node_sa IN ( 61,  51,  64,  24,  50,  27,  55,  85,  81,  79,  80,  54,  9) THEN 'Low Risk'
					ELSE 'No segment'
				END
GO 











SELECT CSH.account_number
      ,current_product_description
	  ,end_date
      ,rank() over(PARTITION BY base.account_number ORDER BY effective_from_dt DESC, CSH.cb_row_id ASC) AS rank1  
INTO #talk_holding
FROM cust_subs_hist 						AS CSH
INNER JOIN PINC_analysis_2 AS BASE  ON BASE.account_number = CSH.account_number
WHERE csh.subscription_sub_type = 'Broadband DSL Line'
AND   csh.status_code IN ('AB','AC','PC','BCRQ')
AND   end_date >= effective_from_dt 
AND   effective_to_dt > effective_from_dt

COMMIT 
DELETE FROM #talk_holding WHERE rank1 <> 1 
CREATE HG INDEX id1 ON #talk_holding (account_number) 
CREATE DATE INDEX id2 ON #talk_holding (end_date) 
COMMIT 

UPDATE PINC_analysis_2
SET BB_PAckage  =  COALESCE(current_product_description 'UNKNOWN') 
                   
FROM PINC_analysis_2 	AS base
INNER JOIN #talk_holding 				AS hold	ON hold.account_number = base.account_number AND base.end_date = hold.end_date




talk_type
CASE 	WHEN talk_type LIKE '%Talk Anytime%' THEN 'Anytime'
		WHEN talk_type LIKE '%Talk Freetime%' THEN 'Freetime'
		WHEN talk_type LIKE '%Talk Unlimited%' THEN 'Unlimited'
		WHEN talk_type LIKE '%Talk International%' THEN 'International'
		WHEN talk_type LIKE '%Talk Evenings%' THEN 'Evenings'
		WHEN talk_type LIKE '%Off Peak%' THEN 'Off Peak'
		
		ELSE talk_type END talk_type_group
		
		------------============================================
		
UPDATE PINC_analysis_2
SET node_tp = CASE WHEN DTV_TA_calls_1m_raw = 0 AND BB_all_calls_1m_raw = 0 AND my_sky_login_3m_raw = 0 AND Simple_Segment IN ('1 Secure','3 Stimulate','2 Stimulate') AND Talk_tenure_raw IN (0,1)			THEN  32
				WHEN DTV_TA_calls_1m_raw = 0 AND BB_all_calls_1m_raw = 0 AND my_sky_login_3m_raw = 0 AND Simple_Segment IN ('1 Secure','3 Stimulate','2 Stimulate') AND Talk_tenure_raw IN (2,3)			THEN  33
				WHEN DTV_TA_calls_1m_raw = 0 AND BB_all_calls_1m_raw = 0 AND my_sky_login_3m_raw = 0 AND Simple_Segment IN ('1 Secure','3 Stimulate','2 Stimulate') AND Talk_tenure_raw IN (3,4,5,6,7)		THEN  34
				WHEN DTV_TA_calls_1m_raw = 0 AND BB_all_calls_1m_raw = 0 AND my_sky_login_3m_raw = 0 AND Simple_Segment IN ('1 Secure','3 Stimulate','2 Stimulate') AND Talk_tenure_raw IN (8,9,10)			THEN  35
				WHEN DTV_TA_calls_1m_raw = 0 AND BB_all_calls_1m_raw = 0 AND my_sky_login_3m_raw = 0 AND Simple_Segment IN ('2 Start','UNKNOWN') AND RTM IN ('UNKNOWN','Digital')                   	THEN  36
				WHEN DTV_TA_calls_1m_raw = 0 AND BB_all_calls_1m_raw = 0 AND my_sky_login_3m_raw = 0 AND Simple_Segment IN ('2 Start','UNKNOWN') AND RTM IN ('Direct')                              	THEN  37
				WHEN DTV_TA_calls_1m_raw = 0 AND BB_all_calls_1m_raw = 0 AND my_sky_login_3m_raw = 0 AND Simple_Segment IN ('2 Start','UNKNOWN') AND RTM IN ('Homes & Indies','Retail')             	THEN  38
				WHEN DTV_TA_calls_1m_raw = 0 AND BB_all_calls_1m_raw = 0 AND my_sky_login_3m_raw = 0 AND Simple_Segment LIKE '%Support%'	AND RTM IN ('UNKNOWN','Digital','Homes & Indies')       	THEN  39
				WHEN DTV_TA_calls_1m_raw = 0 AND BB_all_calls_1m_raw = 0 AND my_sky_login_3m_raw = 0 AND Simple_Segment LIKE '%Support%' 	AND RTM IN ('Direct','Retail')                  			THEN  40
				WHEN DTV_TA_calls_1m_raw = 0 AND BB_all_calls_1m_raw = 0 AND my_sky_login_3m_raw = 0 AND Simple_Segment IN ('5 Stabilise','4 Stabilise','6 Suspense','5 Suspense') AND Talk_tenure_raw IN (0,1)			THEN  41
				WHEN DTV_TA_calls_1m_raw = 0 AND BB_all_calls_1m_raw = 0 AND my_sky_login_3m_raw = 0 AND Simple_Segment IN ('5 Stabilise','4 Stabilise','6 Suspense','5 Suspense') AND Talk_tenure_raw IN (2,3,4,5) 	THEN  42
				WHEN DTV_TA_calls_1m_raw = 0 AND BB_all_calls_1m_raw = 0 AND my_sky_login_3m_raw = 0 AND Simple_Segment IN ('5 Stabilise','4 Stabilise','6 Suspense','5 Suspense') AND Talk_tenure_raw IN (6,7,8,9,10)	THEN  43
				WHEN DTV_TA_calls_1m_raw = 0 AND BB_all_calls_1m_raw = 0 AND my_sky_login_3m_raw = 1 AND RTM IN ('UNKNOWN','Digital')                                                             					THEN  18
				WHEN DTV_TA_calls_1m_raw = 0 AND BB_all_calls_1m_raw = 0 AND my_sky_login_3m_raw = 1 AND RTM IN ('Direct') AND Simple_Segment IN ('1 Secure','3 Stimulate','2 Stimulate','4 Support','3 Support')  	THEN  44
				WHEN DTV_TA_calls_1m_raw = 0 AND BB_all_calls_1m_raw = 0 AND my_sky_login_3m_raw = 1 AND RTM IN ('Direct') AND Simple_Segment IN ('2 Start','5 Stabilise','4 Stabilise','UNKNOWN','6 Suspense','5 Suspense')	THEN  45
				WHEN DTV_TA_calls_1m_raw = 0 AND BB_all_calls_1m_raw = 0 AND my_sky_login_3m_raw = 1 AND RTM IN ('Homes & Indies','Retail')                                                    		THEN  20
				WHEN DTV_TA_calls_1m_raw = 0 AND BB_all_calls_1m_raw = 0 AND my_sky_login_3m_raw = 2 AND Talk_tenure_raw IN (0,1,2)																		THEN  21
				WHEN DTV_TA_calls_1m_raw = 0 AND BB_all_calls_1m_raw = 0 AND my_sky_login_3m_raw = 2 AND Talk_tenure_raw IN (3,4,5,6) AND Simple_Segment IN ('1 Secure','4 Support','3 Support')		THEN  46
				WHEN DTV_TA_calls_1m_raw = 0 AND BB_all_calls_1m_raw = 0 AND my_sky_login_3m_raw = 2 AND Talk_tenure_raw IN (3,4,5,6) AND Simple_Segment NOT IN ('1 Secure','4 Support','3 Support')    THEN  47
				WHEN DTV_TA_calls_1m_raw = 0 AND BB_all_calls_1m_raw = 0 AND my_sky_login_3m_raw = 2 AND Talk_tenure_raw IN (7,8)	 																	THEN  23
				WHEN DTV_TA_calls_1m_raw = 0 AND BB_all_calls_1m_raw = 0 AND my_sky_login_3m_raw = 2 AND Talk_tenure_raw IN (9,10)                                                           			THEN  24
				WHEN DTV_TA_calls_1m_raw = 0 AND BB_all_calls_1m_raw = 0 AND my_sky_login_3m_raw > 2 AND Talk_tenure_raw IN (0,1,2) AND Simple_Segment NOT IN ('5 Stabilise','4 Stabilise')    			THEN  48
				WHEN DTV_TA_calls_1m_raw = 0 AND BB_all_calls_1m_raw = 0 AND my_sky_login_3m_raw > 2 AND Talk_tenure_raw IN (0,1,2) AND Simple_Segment LIKE '%Stabilise%'            					THEN  49
				WHEN DTV_TA_calls_1m_raw = 0 AND BB_all_calls_1m_raw = 0 AND my_sky_login_3m_raw > 2 AND Talk_tenure_raw IN (3,4,5,6) AND Simple_Segment IN ('1 Secure','4 Support','3 Support')   		THEN  50
				WHEN DTV_TA_calls_1m_raw = 0 AND BB_all_calls_1m_raw = 0 AND my_sky_login_3m_raw > 2 AND Talk_tenure_raw IN (3,4,5,6) AND Simple_Segment IN ('2 Start','UNKNOWN')                       THEN  51
				WHEN DTV_TA_calls_1m_raw = 0 AND BB_all_calls_1m_raw = 0 AND my_sky_login_3m_raw > 2 AND Talk_tenure_raw IN (3,4,5,6) AND Simple_Segment IN ('3 Stimulate','2 Stimulate','5 Stabilise','6 Suspense','4 Stabilise','5 Suspense')  THEN  52
				WHEN DTV_TA_calls_1m_raw = 0 AND BB_all_calls_1m_raw = 0 AND my_sky_login_3m_raw > 2 AND Talk_tenure_raw IN (7,8) AND RTM NOT IN ('Direct')                                     		THEN  53
				WHEN DTV_TA_calls_1m_raw = 0 AND BB_all_calls_1m_raw = 0 AND my_sky_login_3m_raw > 2 AND Talk_tenure_raw IN (7,8) AND RTM IN ('Direct')                                              	THEN  54
				WHEN DTV_TA_calls_1m_raw = 0 AND BB_all_calls_1m_raw = 0 AND my_sky_login_3m_raw > 2 AND Talk_tenure_raw = 9																			THEN  28
				WHEN DTV_TA_calls_1m_raw = 0 AND BB_all_calls_1m_raw = 0 AND my_sky_login_3m_raw > 2 AND Talk_tenure_raw = 10																			THEN  29
				WHEN DTV_TA_calls_1m_raw = 0 AND BB_all_calls_1m_raw > 0 AND Talk_tenure_raw IN (0,1)		                                                                                  		THEN  11
				WHEN DTV_TA_calls_1m_raw = 0 AND BB_all_calls_1m_raw > 0 AND Talk_tenure_raw IN (2,3,4)                                                                                     		THEN  12
				WHEN DTV_TA_calls_1m_raw = 0 AND BB_all_calls_1m_raw > 0 AND Talk_tenure_raw >= 5 AND my_sky_login_3m_raw <= 1                                                                          THEN  30
				WHEN DTV_TA_calls_1m_raw = 0 AND BB_all_calls_1m_raw > 0 AND Talk_tenure_raw >= 5 AND my_sky_login_3m_raw > 1                                                                           THEN  31
				WHEN DTV_TA_calls_1m_raw > 0 AND Simple_Segment IN ('1 Secure','2 Start','3 Stimulate','2 Stimulate','UNKNOWN')                              								THEN  5
				WHEN DTV_TA_calls_1m_raw > 0 AND Simple_Segment IN ('4 Support','5 Stabilise','4 Stabilise','6 Suspense','5 Suspense','3 Support')                         					THEN  6
				ELSE 0
			END

UPDATE PINC_analysis_2
SET group_tp = CASE 	WHEN node IN ( 5, 31, 12, 6, 30, 41, 11, 51, 52, 49, 54, 47, 50, 45, 46)	THEN 'High Risk'
						WHEN node IN ( 23, 28, 53, 24, 29, 44, 42, 37, 43, 38, 21, 20, 48, 18)  	THEN 'Medium Risk'
						WHEN node IN ( 40, 39, 36, 32, 33, 34, 35)                       			THEN 'Low Risk'
						ELSE 'No segment' 
					END

		, segment_tp = CASE WHEN node IN ( 5, 31, 12, 6, 30)                                 			THEN 'A1. Prior Contact'
                                    WHEN node IN ( 41, 11)                                                             THEN 'A2. New Talkers'
                                    WHEN node IN ( 51, 52, 49, 54, 47, 50, 45, 46) THEN 'A3. Mid Life Shopping'
                                    WHEN node IN ( 23, 28, 53, 24, 29, 44)                     THEN 'B1. Long Tenure Shopping'
                                    WHEN node IN ( 42, 37, 43, 38)                                         THEN 'B2. Unstable/Unengaged'
                                    WHEN node IN ( 21, 20, 48, 18)                                         THEN 'B3. New Shoppers'
                                    WHEN node IN ( 40, 39, 36)                                                   THEN 'C1. Supported'
                                    WHEN node IN ( 32, 33, 34, 35)                                         THEN 'C2. Stable'
                                    ELSE node
                                END
COMMIT
GO		