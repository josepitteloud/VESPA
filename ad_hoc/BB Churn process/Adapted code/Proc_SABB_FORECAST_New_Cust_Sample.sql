CREATE OR REPLACE PROCEDURE SABB_Build_ForeCAST_New_Cust_Sample (IN LV INT)

BEGIN
	DECLARE Obs_Dt DATE;
	DECLARE @multiplier BIGINT;
	SET @multiplier = DATEPART(millisecond, now()) + 738;
	
	MESSAGE cast(now() as timestamp)||' | SABB_Build_ForeCAST_New_Cust_Sample - Build_ForeCAST_New_Cust_Sample -  Begin ' TO CLIENT;
	

	TRUNCATE TABLE FORECAST_New_Cust_Sample;

	SET Obs_Dt = (SELECT max(calendar_date) FROM citeam.subs_calendar(LV / 100 - 1, LV / 100) WHERE Subs_Week_And_Year < LV );
	SET TEMPORARY OPTION Query_Temp_Space_Limit = 0;

	INSERT INTO FORECAST_New_Cust_Sample
	SELECT  
		 end_date
		, subs_year 													AS year
		, subs_week_of_year 											AS week
		, subs_week_and_year 											AS year_week
		, account_number
		, BB_status_code
		, CASE WHEN BB_Active > 0 	THEN 'BB' 	ELSE 'Non BB' END 		AS BB_Segment
		, CASE WHEN ROI > 0 		THEN 'ROI' 	ELSE 'UK' END 			AS country --
		, BB_package
		, CASE 		WHEN BB_Enter_SysCan + BB_Enter_CusCan + BB_Enter_HM + BB_Enter_3rd_Party > 1 THEN 'MULTI'		--- UPDATED next
					WHEN BB_Enter_SysCan > 0 			THEN 'SysCan' 
					WHEN BB_Enter_CusCan > 0 		THEN 'CusCan'
					WHEN BB_Enter_HM	  > 0 		THEN 'HM'
					WHEN BB_Enter_3rd_Party > 0 	THEN '3rd Party' 
					ELSE NULL END 										AS Churn_type 
		, CAST(NULL AS VARCHAR(4)) 										AS BB_Status_Code_EoW
	--- ========================================================================= 	
		, CASE 	WHEN trim(simple_segment) IN ('1 Secure') 					THEN '1 Secure' 
				WHEN trim(simple_segment) IN ('2 Start')  					THEN '2 Start'
				WHEN trim(simple_segment) IN ('3 Stimulate', '2 Stimulate') THEN '3 Stimulate'
				WHEN trim(simple_segment) IN ('4 Support', '3 Support') 	THEN '4 Support' 
				WHEN trim(simple_segment) IN ('5 Stabilise', '4 Stabilise') THEN '5 Stabilise' 
				WHEN trim(simple_segment) IN ('6 Suspense', '5 Suspense') 	THEN '6 Suspense' 
				ELSE 'Other/Unknown' END 								AS Simple_Segments 						
		, BB_offer_rem_and_end_raw  		
		, CAST (NULL AS INT) 											AS BB_offer_rem_and_end
		, BB_tenure_raw  					
		, CAST (NULL AS INT) 											AS BB_tenure 
		, my_sky_login_3m_raw 				
		, CAST (NULL AS INT) 											AS my_sky_login_3m 
		, talk_type
		, home_owner_status
		, BB_all_calls_1m_raw				
		, CAST (NULL AS INT) 											AS BB_all_calls_1m
		, CAST (0 AS TINYINT) 											AS  node_SA
		, CAST (NULL AS VARCHAR(20)) 									AS segment_SA
		

		, Cast(NULL AS DATE) AS PL_Future_Sub_Effective_Dt
		
		, Cast(NULL AS VARCHAR(100)) AS DTV_Activation_Type 				
		
		, Curr_Offer_start_Date_BB 											
		, curr_offer_end_date_Intended_BB 									
		, Prev_offer_end_date_BB 											
		, Cast(NULL AS DATE) AS Future_offer_Start_dt
		, Cast(NULL AS DATE) AS Future_end_Start_dt							
		, BB_latest_act_dt  											
		, BB_first_act_dt  												
		, rand(number(*) * @multiplier) AS rand_sample
		, CAST (NULL AS VARCHAR(10)) AS sample
		, CASE WHEN bb_active = 1 AND dtv_active = 0 THEN 1 ELSE 0 END 		AS SABB_flag
	FROM pitteloudj.cust_fcast_weekly_base_2
	WHERE end_date BETWEEN Obs_Dt - 5 * 7 AND Obs_Dt 
		AND bb_active = 1 AND dtv_active = 0
		AND BB_latest_act_dt BETWEEN (end_date - 6) AND end_date -- New customers
		AND BB_latest_act_dt IS NOT NULL;

	MESSAGE cast(now() as timestamp)||' | SABB_Build_ForeCAST_New_Cust_Sample -  Insert Into FORECAST_New_Cust_Sample completed: '||@@rowcount TO CLIENT;
	COMMIT;
		
	SELECT a.account_number
		, a.end_date 
		, B.subs_year
		, B.subs_week_of_year
		, CASE 		WHEN b.Enter_SysCan > 0 		THEN 'SysCan' 
					WHEN b.Enter_CusCan > 0 		THEN 'CusCan'
					WHEN b.Enter_HM	  > 0 			THEN 'HM'
					WHEN b.Enter_3rd_Party > 0 		THEN '3rd Party' 
					ELSE NULL END 							AS Churn_type 
		, RANK () OVER (PARTITION BY a.account_number, a.end_date ORDER BY b.event_dt DESC) AS week_rnk
	INTO #t1 
	FROM FORECAST_New_Cust_Sample AS a 
	JOIN CITEAM.Broadband_Comms_Pipeline AS b ON a.account_number = b.account_number 
												AND a.year = b.subs_year
												AND a.week = b.subs_week_of_year 
	WHERE a.Churn_type = 'MULTI';
	
	COMMIT;
	DELETE FROM #t1 WHERE week_rnk > 1 ;
	CREATE HG INDEX IO1 ON #t1(account_number);
	CREATE DTTM INDEX IO2 ON #t1(end_date);
	COMMIT;
	
	UPDATE FORECAST_New_Cust_Sample
	SET a.Churn_type = b.Churn_type
	FROM FORECAST_New_Cust_Sample AS a 
	JOIN #t1 AS b ON 	a.account_number = b.account_number 
				AND 	a.end_date = b.end_date;
	
	MESSAGE cast(now() as timestamp)||' | SABB_Build_ForeCAST_New_Cust_Sample -  Churn_type fixed: '||@@rowcount TO CLIENT;
	
	DROP TABLE #t1;
	COMMIT 			;
	
	UPDATE FORECAST_New_Cust_Sample
	SET a.BB_offer_rem_and_end 	= b.BB_offer_rem_and_end
		, a.BB_tenure 			= b.BB_tenure 
		, a.my_sky_login_3m 	= b.my_sky_login_3m 
		, a.BB_all_calls_1m 	= b.BB_all_calls_1m 
		, a.node_SA 			= b.node_SA 
		, a.segment_SA 			= b.segment_SA 
	FROM FORECAST_New_Cust_Sample AS a 
	JOIN pitteloudj.DTV_FCAST_WEEKLY_BASE_2 AS b ON a.account_number = b.account_number 
												AND a.end_date = b.end_date ;
												
	MESSAGE cast(now() as timestamp)||' | SABB_Build_ForeCAST_New_Cust_Sample -  DTV_fcast variables updated: '||@@rowcount TO CLIENT;
	
	UPDATE FORECAST_New_Cust_Sample sample
	SET sample.PL_Future_Sub_Effective_Dt = MoR.PC_Future_Sub_Effective_Dt
	FROM FORECAST_New_Cust_Sample sample
	INNER JOIN CITeam.Broadband_Comms_Pipeline	  AS MoR ON MoR.account_number = sample.account_number 
														AND MoR.PC_Future_Sub_Effective_Dt > sample.end_date 	
														AND MoR.event_dt <= sample.end_date 
														AND (MoR.PC_effective_to_dt > sample.end_date 
															OR MoR.PC_effective_to_dt IS NULL)
	WHERE sample.BB_Status_Code = 'PC';

	UPDATE FORECAST_New_Cust_Sample sample
	SET PL_Future_Sub_Effective_Dt = MoR.AB_Future_Sub_Effective_Dt
	FROM FORECAST_New_Cust_Sample sample
	JOIN CITeam.Broadband_Comms_Pipeline 	AS  MoR ON 	MoR.account_number = sample.account_number 
													AND MoR.AB_Future_Sub_Effective_Dt > sample.end_date 
													AND MoR.event_dt <= sample.end_date 
													AND (MoR.AB_effective_to_dt > sample.end_date OR MoR.AB_effective_to_dt IS NULL)
	WHERE sample.BB_Status_Code = 'AB';

			
	UPDATE FORECAST_New_Cust_Sample sample
	SET PL_Future_Sub_Effective_Dt = MoR.BCRQ_Future_Sub_Effective_Dt
	FROM FORECAST_New_Cust_Sample AS sample
	INNER JOIN CITeam.Broadband_Comms_Pipeline	MoR ON  MoR.account_number = sample.account_number 
													AND MoR.AB_Future_Sub_Effective_Dt > sample.end_date 
													AND MoR.event_dt <= sample.end_date 
													AND (MoR.AB_effective_to_dt > sample.end_date OR MoR.AB_effective_to_dt IS NULL)
	WHERE sample.BB_Status_Code = 'BCRQ';
	
	UPDATE FORECAST_New_Cust_Sample sample
	SET BB_Status_Code = 'AC'
	WHERE 	PL_Future_Sub_Effective_Dt IS NULL; 
	
	MESSAGE cast(now() as timestamp)||' | SABB_Build_ForeCAST_New_Cust_Sample -  COMPLETED'	TO CLIENT;	
END;

GRANT EXECUTE ON SABB_Build_ForeCAST_New_Cust_Sample TO CITeam, vespa_group_low_security;

	/* 
	-- Test it
	Call SABB_Build_ForeCAST_New_Cust_Sample(201601);
	
	CREATE variable LV INT;
	CREATE variable Obs_Dt DATE;

	SET LV = 201601;
	CREATE TABLE FORECAST_New_Cust_Sample
	
	(end_date 							DATETIME DEFAULT NULL
	, year 								INT DEFAULT NULL
	, week 								INT DEFAULT NULL
	, year_week 						INT DEFAULT NULL
	, account_number					VARCHAR(20) DEFAULT NULL
	, BB_status_code 					VARCHAR(4) DEFAULT NULL
	, BB_Segment VARCHAR(30) 			DEFAULT NULL
	, country VARCHAR(3) 				DEFAULT NULL
	, BB_package	 VARCHAR(50) 		DEFAULT NULL
	, churn_type	varchar	(10)		DEFAULT  NULL
	, BB_Status_Code_EoW 					VARCHAR(4) DEFAULT NULL
	, Simple_Segments VARCHAR(13) 		DEFAULT NULL
		, BB_offer_rem_and_end_raw	int		DEFAULT  -9999
		, BB_offer_rem_and_end		int		DEFAULT  -9999
		, BB_tenure_raw				int		DEFAULT 0
		, BB_tenure					int		DEFAULT 0
		, my_sky_login_3m_raw		int		DEFAULT 0
		, my_sky_login_3m			int		DEFAULT 0
		, talk_type			VARCHAR (30)	DEFAULT 'NONE'
		, home_owner_status	VARCHAR (20)	DEFAULT 'UNKNOWN'
		, BB_all_calls_1m_raw		int		DEFAULT 0
		, BB_all_calls_1m			int		DEFAULT 0
		, node_SA				TINYINT		DEFAULT 0
		, segment_SA		varchar	(20)	DEFAULT 'UNKNOWN'
		, PL_Future_Sub_Effective_Dt datetime		DEFAULT  NULL
		, DTV_Activation_Type	varchar	(100)			DEFAULT  NULL
		, Curr_Offer_start_Date_BB		datetime		DEFAULT  NULL
		, Curr_offer_end_date_Intended_BB	datetime	DEFAULT  NULL
		, Prev_offer_end_date_BB		datetime		DEFAULT  NULL
		, Future_offer_Start_dt			datetime		DEFAULT  NULL
		, Future_end_Start_dt			datetime		DEFAULT  NULL
		, BB_latest_act_dt				datetime		DEFAULT  NULL
		, BB_first_act_dt				datetime		DEFAULT  NULL
		, rand_sample					FLOAT			DEFAULT  NULL
		, sample	 					VARCHAR	(10)	DEFAULT  NULL
		, SABB_flag						BIT		 		DEFAULT 0 )
*/