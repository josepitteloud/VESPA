SELECT DISTINCT account_number, 1 'dummy'
INTO 	#BCRQ
FROM  	CUST_SUBS_HIST
WHERE 	status_code = 'BCRQ' 
	AND	@base_date  BETWEEN effective_from_dt, effective_to_dt
	AND status_code_changed = 'Y'
	AND effective_to_dt > effective_from_dt
COMMIT 
CREATE HG INDEX id1 ON #BCRQ(account_number) 
COMMIT


SELECT a.account_number
	, end_date
	, subs_year
	, 'subs_week_and_year' = convert(INT, subs_week_and_year)
	, subs_week_of_year
	, 'weekid' = (subs_year - 2010) * 52 + subs_week_of_year
	, BB_Status_Code
	, 'BB_Segment' = CASE WHEN BB_Active > 0 THEN 'BB' ELSE 'Non BB' END
	, 'country' = CASE WHEN ROI > 0 THEN 'ROI' ELSE 'UK' END
	, BB_package
	, 'Churn_type' = CASE WHEN BB_Enter_SysCan + BB_Enter_CusCan + BB_Enter_HM + BB_Enter_3rd_Party > 1 THEN 'MULTI' --- UPDATED next
		WHEN BB_Enter_SysCan > 0 THEN 'SysCan' WHEN BB_Enter_CusCan > 0 THEN 'CusCan' WHEN BB_Enter_HM > 0 THEN 'HM' WHEN BB_Enter_3rd_Party > 0 THEN '3rd Party' ELSE NULL END
	, BB_offer_rem_and_end_raw
	, 'BB_offer_rem_and_end' = convert(INT, NULL)
	, BB_tenure_raw
	, 'BB_tenure' = convert(INT, NULL)
	, my_sky_login_3m_raw
	, 'my_sky_login_3m' = convert(INT, NULL)
	, talk_type
	, home_owner_status
	, BB_all_calls_1m_raw
	, 'BB_all_calls_1m' = convert(INT, NULL)
	, 'Simple_Segments' = CASE 	WHEN trim(simple_segment) IN ('1 Secure') THEN '1 Secure' 
								WHEN trim(simple_segment) IN ('2 Start') THEN '2 Start' 
								WHEN trim(simple_segment) IN ('3 Stimulate', '2 Stimulate') THEN '3 Stimulate' 
								WHEN trim(simple_segment) IN ('4 Support', '3 Support') THEN '4 Support' 
								WHEN trim(simple_segment) IN ('5 Stabilise', '4 Stabilise') THEN '5 Stabilise' 
								WHEN trim(simple_segment) IN ('6 Suspense', '5 Suspense') THEN '6 Suspense' 
								ELSE 'Other/Unknown' -- ??? check the simple segment coding here that cleans this up, but generally looks ok
								END
	, 'node_SA' 		= convert(TINYINT, 0)
	, 'segment_SA' 		= convert(VARCHAR(20), NULL)
	, 'PL_Future_Sub_Effective_Dt' 	= convert(DATE, NULL)
	, 'DTV_Activation_Type' 		= convert(VARCHAR(100), NULL)
	, Curr_Offer_start_Date_BB
	, curr_offer_end_date_Intended_BB
	, Prev_offer_end_date_BB
	, 'Future_offer_Start_dt' = convert(DATE, NULL)
	, 'Future_end_Start_dt' = convert(DATE, NULL)
	, BB_latest_act_dt
	, BB_first_act_dt
	, 'rand_sample' = rand(number() * @multiplier)
	, 'sample' = convert(VARCHAR(10), NULL)
	, 'SABB_flag' = CASE WHEN bb_active = 1 AND dtv_active = 0 THEN 1 ELSE 0 END
	, RANK() OVER (PARTITION BY a.account_number ORDER BY end_date DESC ) AS rankk
INTO #BCRQ_details
FROM jcartwright.cust_fcast_weekly_base_2 	AS a
INTO #BCRQ									AS b ON a.account_number =b.account_number 
WHERE end_date >= DATEADD ( MONTH, -6, @base_date) 
	AND BB_latest_act_dt IS NOT NULL 

COMMIT 
CREATE HG index id1 ON #BCRQ_details(account_number)
CREATE LF index id2 ON #BCRQ_details(rankk)
COMMIT

DELETE FROM #BCRQ_details WHERE rankk <> 1 
COMMIT

UPDATE #BCRQ_details
SET 	BB_offer_rem_and_end_raw 	= DATEADD ( DAY , DATEDIFF ( DAY , end_Date, @base_date), BB_offer_rem_and_end_raw )
	, 	BB_tenure_raw 				= DATEADD ( DAY , DATEDIFF ( DAY , end_Date, @base_date), BB_tenure_raw )
COMMIT 

UPDATE #BCRQ_details SET end_date = @base_date
	, bb_status_code = 'BCRQ' 
COMMIT 

SELECT end_date 
		   MAX(subs_year)								AS subs_year
		, MAX(convert(INT, subs_week_and_year))			AS subs_week_and_year
		, MAX(subs_week_of_year) 						AS subs_week_of_year
		, MAX(weekid) 									AS weekid
INTO #t1
FROM FORECAST_Base_Sample
COMMIT

UPDATE #BCRQ_details
SET a.subs_year 			= b.subs_year
	, a.subs_week_and_year 	= b.subs_week_and_year 	
	, a.subs_week_of_year	= b.subs_week_of_year
	, a.weekid 				= b.weekid
FROM #BCRQ_details 	AS a 
JOIN #t1 			AS b ON a.end_date = b.end_date

COMMIT
 
INSERT INTO FORECAST_Base_Sample
SELECT 	account_number,end_date,subs_year,subs_week_and_year,subs_week_of_year,weekid,BB_Status_Code,BB_Segment,country,BB_package
		,churn_type,BB_offer_rem_and_end_raw,BB_offer_rem_and_end,BB_tenure_raw,BB_tenure,my_sky_login_3m_raw
		,my_sky_login_3m,talk_type,home_owner_status,BB_all_calls_1m_raw,BB_all_calls_1m,Simple_Segments,node_SA
		,segment_SA,PL_Future_Sub_Effective_Dt,DTV_Activation_Type,Curr_Offer_start_Date_BB,Curr_offer_end_date_Intended_BB
		,Prev_offer_end_date_BB,Future_offer_Start_dt,Future_end_Start_dt,BB_latest_act_dt,BB_first_act_dt,rand_sample
		,sample,SABB_flag,Sky_plus,nowtv_flag
FROM #BCRQ_details