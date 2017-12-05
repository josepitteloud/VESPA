


SELECT a.DTV_TA_calls_1m 
        , a.BB_all_calls_1m 
        , c.Simple_Segment 
        , a.my_sky_login_3m
        , a.Talk_tenure 
        , COALESCE(a.RTM, 'UNKNOWN') RTM2
        , count(DISTINCT a.account_number) hits
        , a.end_date
INTO #t1
FROM citeam.DTV_FCAST_WEEKLY_BASE AS a
JOIN citeam.CUST_FCAST_WEEKLY_BASE AS c ON a.account_number = c.account_number AND a.end_date = c.end_date 
WHERE a.bb_active = 1 AND a.DTV_active  =1 and a.end_date BETWEEN	'2016-12-01' AND '2017-05-12' 
GROUP BY a.DTV_TA_calls_1m 
        , a.BB_all_calls_1m 
        , c.Simple_Segment 
        , a.my_sky_login_3m
        , a.Talk_tenure 
        , RTM2
        , a.end_date


COMMIT 
CREATE LF INDEX ID1 ON #t1(DTV_TA_calls_1m) 
CREATE LF INDEX ID2 ON #t1(BB_all_calls_1m) 
CREATE LF INDEX ID3 ON #t1(Simple_Segment) 
CREATE LF INDEX ID4 ON #t1(Talk_tenure) 
CREATE LF INDEX ID5 ON #t1(RTM2) 
CREATE LF INDEX ID6 ON #t1(end_date) 
COMMIT 

SELECT node
        , segment
        , end_date
        , SUM(hits) t_hits
FROM   #t1                                      AS a       
LEFT JOIN BB_TP_Product_Churn_segments_lookup   AS b ON a.DTV_TA_calls_1m = b.DTV_TA_calls_1m
                                                    AND a.BB_all_calls_1m = b.BB_all_calls_1m 
                                                    AND a.Simple_Segment  = b.Simple_Segment
                                                    AND a.my_sky_login_3m = b.my_sky_login_3m
                                                    AND a.Talk_tenure     = b.Talk_tenure
                                                    AND a.RTM2            = b.RTM
GROUP BY node
        , segment
        , end_date
        
        
        
UPDATE jcartwright.DTV_fcast_weekly_base_2        
SET BB_Enter_CusCan = BB_Enter_CusCan - 1 
FROM jcartwright.DTV_fcast_weekly_base_2        AS a
JOIN BB_churn_HM_accounts AS b ON a.account_number = b.account_number And event_dt BETWEEN end_date AND DATEADD( DAY, 6 , end_date )
WHERE a.BB_Enter_HM >=1
AND a.BB_Enter_HM =1 
AND end_date BETWEEN '2016-06-01' AND '2017-03-02'



UPDATE jcartwright.DTV_fcast_weekly_base_2        
SET BB_Enter_HM = 1
FROM jcartwright.DTV_fcast_weekly_base_2        AS a
JOIN BB_churn_HM_accounts AS b ON a.account_number = b.account_number And event_dt BETWEEN end_date AND DATEADD( DAY, 6 , end_date )
WHERE BB_Enter_HM = 0
commit 

----------------------------------------------------



 INSERT INTO jcartwright.CUST_FCAST_WEEKLY_BASE_2 
 (Subs_Week_and_Year,
Subs_Year,
Subs_Week_Of_Year,
Load_Dt,
End_Date,
Account_Number,
CB_Key_Household,
BB_Enter_SysCan,
BB_Enter_CusCan,
BB_Enter_HM,
BB_Enter_3rd_Party,
DTV_Active,
BB_Active,
Simple_Segment,
curr_offer_start_date_bb,
Days_prev_offer_end_BB,
Days_Curr_Offer_End_BB,
ROI,
Churn_Type,
Random_Number,
BB_Status_Code,
DTV_TA_calls_1m_raw,
RTM,
Talk_tenure_raw,
my_sky_login_3m_raw,
BB_all_calls_1m_raw,
BB_offer_rem_and_end_raw,
home_owner_status,
BB_tenure_raw,
talk_type,
BB_Package,
Curr_Offer_end_Date_Intended_BB,
Prev_offer_end_date_BB,
BB_latest_act_dt,
BB_first_act_dt)
SELECT 
 Subs_Week_and_Year,
Subs_Year,
Subs_Week_Of_Year,
Load_Dt,
End_Date,
Account_Number,
CB_Key_Household,
BB_Enter_SysCan,
BB_Enter_CusCan,
BB_Enter_HM,
BB_Enter_3rd_Party,
DTV_Active,
BB_Active,
Simple_Segment,
curr_offer_start_date_bb,
Days_prev_offer_end_BB,
Days_Curr_Offer_End_BB,
ROI,
Churn_Type,
Random_Number,
BB_Status_Code,
DTV_TA_calls_1m_raw,
RTM,
Talk_tenure_raw,
my_sky_login_3m_raw,
BB_all_calls_1m_raw,
BB_offer_rem_and_end_raw,
home_owner_status,
BB_tenure_raw,
talk_type,
BB_Package,
Curr_Offer_end_Date_Intended_BB,
Prev_offer_end_date_BB,
BB_latest_act_dt,
BB_first_act_dt
FROM citeam.CUST_FCAST_WEEKLY_BASE
WHERE dtv_active = 0 AND bb_active = 1 
AND end_date = '2017-03-23'

commit 












INSERT INTO jcartwright.DTV_FCAST_WEEKLY_BASE_2 (Subs_Week_and_Year
,Subs_Year
,Subs_Week_Of_Year
,Load_Dt
,End_Date
,Account_Number
,CB_Key_Household
,Random_Number
,BB_Enter_SysCan
,BB_Enter_CusCan
,BB_Enter_HM
,BB_Enter_3rd_Party
,Churn_Type
,DTV_Active
,BB_Active
,ROI
,Simple_Segment
,DTV_TA_calls_1m 
,RTM
,talk_tenure
,my_sky_login_3m 
,BB_all_calls_1m 
,BB_offer_rem_and_end 
,home_owner_status
,BB_tenure
,talk_type
,node_TP
,segment_TP
,node_SA
, segment_SA
, BB_package
, BB_Status_Code
, Curr_Offer_start_Date_BB
,  BB_Offer_applied
, data_view 
, sample )
    SELECT 

	Subs_Week_and_Year
	,Subs_Year
	,Subs_Week_Of_Year
	,Load_Dt
	,End_Date
	,Account_Number
	,CB_Key_Household
	,Random_Number
	--------- PL Info
	,BB_Enter_SysCan
	,BB_Enter_CusCan
	,BB_Enter_HM
	,BB_Enter_3rd_Party
	,Churn_Type
	--------- Product Info
	,DTV_Active
	,BB_Active
	,ROI
	--------- Binned Segmenting variables 
	,Simple_Segment
	,DTV_TA_calls_1m = CASE WHEN DTV_TA_calls_1m_raw > 0 THEN 1 ELSE 0 END 
	,RTM
	,talk_tenure = CASE 	WHEN Talk_tenure_raw <= 65 	THEN 1 
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
	,my_sky_login_3m = CASE WHEN my_sky_login_3m_raw > 2 THEN 3 ELSE my_sky_login_3m_raw END 
	,BB_all_calls_1m = CASE WHEN BB_all_calls_1m_raw = 0 THEN 0 ELSE 1 END 
	,BB_offer_rem_and_end = CASE 	WHEN BB_offer_rem_and_end_raw BETWEEN -9998 AND -1015 	THEN -3
									WHEN BB_offer_rem_and_end_raw BETWEEN -1015 AND -215 	THEN -2 
									WHEN BB_offer_rem_and_end_raw BETWEEN -215  AND -75  	THEN -1
									WHEN BB_offer_rem_and_end_raw BETWEEN -74  AND -0    	THEN 0
									WHEN BB_offer_rem_and_end_raw BETWEEN 1    AND 62    	THEN 1
									WHEN BB_offer_rem_and_end_raw BETWEEN 63   AND 162   	THEN 2
									WHEN BB_offer_rem_and_end_raw BETWEEN 163  AND 271		THEN 3
									WHEN BB_offer_rem_and_end_raw >271						THEN 4
									ELSE -9999 END 
	,home_owner_status
	,BB_tenure = CASE 	WHEN BB_tenure_raw <= 118 				 THEN 1
						WHEN BB_tenure_raw BETWEEN 119 AND 231  THEN 2
						WHEN BB_tenure_raw BETWEEN 231 AND 329  THEN 3
						WHEN BB_tenure_raw BETWEEN 329 AND 391  THEN 4
						WHEN BB_tenure_raw BETWEEN 392 AND 499  THEN 5
						WHEN BB_tenure_raw BETWEEN 499 AND 641  THEN 6
						WHEN BB_tenure_raw BETWEEN 641 AND 1593 THEN 7
						WHEN BB_tenure_raw > 1593 				 THEN 8	
						ELSE -1 END 
	,talk_type
	--------------------- SEGMENT INFORMATION
	,CAST (0 AS tinyint	) 			AS node_TP				
	,CAST (NULL	AS varchar	(20)) 	AS segment_TP
	,CAST (0 AS tinyint	) 			AS node_SA
	,CAST (NULL	AS varchar	(20)) 	AS segment_SA

	, BB_package
	, BB_Status_Code
	, Curr_Offer_start_Date_BB
	, CASE WHEN DATEDIFF (DAY, Curr_Offer_start_Date_BB , end_date) BETWEEN 0 AND 7  THEN 1 ELSE 0 END  		AS BB_Offer_applied
	, 'Actuals' 		AS data_view 
	, 1 				AS sample 
FROM 	jcartwright.cust_fcast_weekly_base_2
WHERE end_date >= '2017-03-23'
