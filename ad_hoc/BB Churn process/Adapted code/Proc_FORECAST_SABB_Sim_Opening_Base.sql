CREATE OR REPLACE PROCEDURE SABB_Forecast_Create_Opening_Base (IN Forecast_Start_Wk INT, IN sample_pct FLOAT)

AS BEGIN
	
	MESSAGE cast(now() as timestamp)||' | Forecast_Create_Opening_Base - Begining - Initialising Environment' TO CLIENT
	IF NOT EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = UPPER('FORECAST_Base_Sample')) 
	BEGIN 
	CREATE TABLE FORECAST_Base_Sample
		( account_number	varchar	(20)	DEFAULT  NULL
		, end_date	datetime				DEFAULT  NULL
		, real_end_dt	datetime			DEFAULT  NULL
		, subs_year				int			DEFAULT  NULL
		, subs_week_and_year	int			DEFAULT  NULL
		, subs_week_of_year	tinyint			DEFAULT  NULL
		, weekid	bigint					DEFAULT  NULL
		, BB_Status_Code	varchar	(4)		DEFAULT  NULL
		, BB_Status_EOW		varchar	(4)		DEFAULT  NULL
		, BB_Segment	varchar	(30)		DEFAULT  NULL
		, country		varchar	(3)			DEFAULT  NULL
		, BB_package	varchar	(50)		DEFAULT  NULL
		, churn_type	varchar	(10)		DEFAULT  NULL
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
		, Simple_Segments	varchar	(13)	DEFAULT 'UNKNOWN'
		, node_SA				TINYINT		DEFAULT 0
		, segment_SA		varchar	(20)	DEFAULT 'UNKNOWN'
		, PL_Future_Sub_Effective_Dt	datetime		DEFAULT  NULL
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
	MESSAGE cast(now() as timestamp)||' | Forecast_Create_Opening_Base - FORECAST_Base_Sample' TO CLIENT
	END 



	DECLARE @base_date DATE
	DECLARE @true_sample_rate FLOAT
	DECLARE @multiplier BIGINT

	SET @multiplier = DATEPART(millisecond, now()) + 738
	SET TEMPORARY OPTION Query_Temp_Space_Limit = 0
	SELECT * INTO #Sky_Calendar FROM Subs_Calendar(Forecast_Start_Wk / 100 - 1, Forecast_Start_Wk / 100)
	SET @base_date = (SELECT max(calendar_date - 7) FROM #sky_calendar WHERE subs_week_and_year = Forecast_Start_Wk)
	SET @multiplier = DATEPART(millisecond, now()) + 1
	MESSAGE cast(now() as timestamp)||' | Forecast_Create_Opening_Base - @base_date: '||@base_date  TO CLIENT
	-- drop table if exists #base_sample
	DELETE FROM FORECAST_Base_Sample

	MESSAGE cast(now() as timestamp)||' | Forecast_Create_Opening_Base - Cleaning FORECAST_Base_Sample ' TO CLIENT
	INSERT INTO FORECAST_Base_Sample
	SELECT
		  account_number --
		, end_date --
		, subs_year
		, cast(subs_week_and_year AS INT) 								AS  subs_week_and_year		--
		, subs_week_of_year 																		--
		, (subs_year - 2010) * 52 + subs_week_of_year 					AS weekid
		, BB_Status_Code --- ??? we want this to hold the status at the time, so may be held in dtv_status_code at the moment - confirm
		, CASE WHEN BB_Active > 0 	THEN 'BB' 	ELSE 'Non BB' END 		AS BB_Segment
		, CASE WHEN ROI > 0 		THEN 'ROI' 	ELSE 'UK' END 			AS country --
		, BB_package
		, CASE 		WHEN BB_Enter_SysCan + BB_Enter_CusCan + BB_Enter_HM + BB_Enter_3rd_Party > 1 THEN 'MULTI'		--- UPDATED next
					WHEN BB_Enter_SysCan > 0 			THEN 'SysCan' 
					WHEN BB_Enter_CusCan > 0 		THEN 'CusCan'
					WHEN BB_Enter_HM	  > 0 		THEN 'HM'
					WHEN BB_Enter_3rd_Party > 0 	THEN '3rd Party' 
					ELSE NULL END 							AS Churn_type 
	--- ========================================================================= --??? add in here the variables required to build the segments
		, BB_offer_rem_and_end_raw  		----??? hold raw - not bucket
		, CAST (NULL AS INT) 											AS BB_offer_rem_and_end
		, BB_tenure_raw  					----??? hold raw - not bucket
		, CAST (NULL AS INT) 											AS BB_tenure 
		, my_sky_login_3m_raw 				---??? doesn't this need to hold value for that week rather than the overall value?  We will generate the flag on th efly!
		, CAST (NULL AS INT) 											AS my_sky_login_3m 
		, talk_type
		, home_owner_status
		, BB_all_calls_1m_raw				----??? hold raw - not bucket
		, CAST (NULL AS INT) 											AS BB_all_calls_1m
		, CASE 	WHEN trim(simple_segment) IN ('1 Secure') 					THEN '1 Secure' 
				WHEN trim(simple_segment) IN ('2 Start')  					THEN '2 Start'
				WHEN trim(simple_segment) IN ('3 Stimulate', '2 Stimulate') THEN '3 Stimulate'
				WHEN trim(simple_segment) IN ('4 Support', '3 Support') 	THEN '4 Support' 
				WHEN trim(simple_segment) IN ('5 Stabilise', '4 Stabilise') THEN '5 Stabilise' 
				WHEN trim(simple_segment) IN ('6 Suspense', '5 Suspense') 	THEN '6 Suspense' 
				ELSE 'Other/Unknown' END 								AS Simple_Segments 						-- ??? check the simple segment coding here that cleans this up, but generally looks ok
		, CAST (0 AS TINYINT) 											AS  node_SA
		, CAST (NULL AS VARCHAR(20)) 									AS segment_SA
		
		, Cast(NULL AS DATE) AS PL_Future_Sub_Effective_Dt	
		
		, Cast(NULL AS VARCHAR(100)) AS DTV_Activation_Type 				---??? this does what?
		--- ??? we will need something that allows the offer ends times to be manipulated
		, Curr_Offer_start_Date_BB 											---??? dont we need something like this?
		, curr_offer_end_date_Intended_BB 									---??? dont we need something like this?
		, Prev_offer_end_date_BB 											---??? dont we need something like this?
		, Cast(NULL AS DATE) AS Future_offer_Start_dt
		, Cast(NULL AS DATE) AS Future_end_Start_dt							
		, BB_latest_act_dt  											--##### BB_TENURE RAW ???? ---??? this does what?	
		, BB_first_act_dt  												
		, rand(number(*) * @multiplier) AS rand_sample
		, CAST (NULL AS VARCHAR(10)) AS sample
		, CASE WHEN bb_active = 1 AND dtv_active = 0 THEN 1 ELSE 0 END 		AS SABB_flag
		
	FROM pitteloudj.cust_fcast_weekly_base_2 
	WHERE end_date = @base_date 
		AND bb_active = 1 AND dtv_active = 0								--??? do we need a sabb flag?
		AND BB_latest_act_dt IS NOT NULL 									--??? do we have this, or a bb_act_date?
		--???? changes to the where clause here?
	
	---------*******************====================================**********************--------------------------
	---------*******************====================================**********************--------------------------
	---------*******************====================================**********************--------------------------
	
	
	SELECT 
	
	
	
	
	
	
	
	
	
	
	---------*******************====================================**********************--------------------------
	---------*******************====================================**********************--------------------------
	---------*******************====================================**********************--------------------------
	
	MESSAGE cast(now() as timestamp)||' | Forecast_Create_Opening_Base - Insert Into FORECAST_Base_Sample completed: '||@@rowcount TO CLIENT	
	COMMIT 
	
	SELECT a.account_number
		, a.end_date 
		, a.subs_year
		, a.subs_week_of_year
		, CASE 		WHEN b.Enter_SysCan > 0 		THEN 'SysCan' 
					WHEN b.Enter_CusCan > 0 		THEN 'CusCan'
					WHEN b.Enter_HM	  > 0 			THEN 'HM'
					WHEN b.Enter_3rd_Party > 0 		THEN '3rd Party' 
					ELSE NULL END 							AS Churn_type 
		, RANK () OVER (PARTITION BY a.account_number, a.end_date ORDER BY b.event_dt DESC, order_id DESC) AS week_rnk
	INTO #t1 
	FROM FORECAST_Base_Sample AS a 
	JOIN CITEAM.Broadband_Comms_Pipeline AS b ON a.account_number = b.account_number 
												AND a.subs_year = b.subs_year
												AND a.subs_week_of_year = b.subs_week_of_year 
	WHERE a.Churn_type = 'MULTI'
	
	COMMIT 
	DELETE FROM #t1 WHERE week_rnk > 1 
	CREATE HG INDEX IO1 ON #t1(account_number)
	CREATE DTTM INDEX IO2 ON #t1(end_date)
	COMMIT 
	
	UPDATE FORECAST_Base_Sample
	SET a.Churn_type = b.Churn_type
	FROM FORECAST_Base_Sample AS a 
	JOIN #t1 AS b ON 	a.account_number = b.account_number 
				AND 	a.end_date = b.end_date
	
	DROP TABLE #t1
	COMMIT 			
	
	
	
	

	UPDATE FORECAST_Base_Sample
	SET a.BB_offer_rem_and_end 	= b.BB_offer_rem_and_end
		, a.BB_tenure 			= b.BB_tenure 
		, a.my_sky_login_3m 	= b.my_sky_login_3m 
		, a.BB_all_calls_1m 	= b.BB_all_calls_1m 
		, a.node_SA 			= b.node_SA 
		, a.segment_SA 			= b.segment_SA 
	FROM FORECAST_Base_Sample AS a 
	JOIN pitteloudj.DTV_FCAST_WEEKLY_BASE_2 AS b ON a.account_number = b.account_number 
												AND a.end_date = b.end_date 
	
	MESSAGE cast(now() as timestamp)||' | Forecast_Create_Opening_Base - First Update FORECAST_Base_Sample completed: '||@@rowcount TO CLIENT	
																			---????update this?
	UPDATE FORECAST_Base_Sample sample
	SET PL_Future_Sub_Effective_Dt = MoR.PC_Future_Sub_Effective_Dt
	FROM FORECAST_Base_Sample AS sample
	INNER JOIN CITeam.Broadband_Comms_Pipeline MoR ON MoR.account_number = sample.account_number 
												AND MoR.PC_Future_Sub_Effective_Dt > sample.end_date 
												AND MoR.event_dt <= sample.end_date 
												AND (MoR.PC_effective_to_dt > sample.end_date OR MoR.PC_effective_to_dt IS NULL)
	WHERE sample.BB_Status_Code = 'PC'

	
	---????update this?
	------------==================================++++++++++++++++++++++++++++++++==========================================---------------
	------------==================================++++++++++++++++++++++++++++++++==========================================---------------
	------------==================================++++++++++++++++++++++++++++++++==========================================---------------
	   MESSAGE cast(now() as timestamp)||' | SABB_Forecast_Create_Opening_Base - UPDATE AB_Future_Sub_Effective_Dt Begin' TO CLIENT
	   
		SELECT a.account_number 
			, b.event_dt
			, a.BB_status_code
			, a.end_date
			, RANK () OVER (PARTITION BY a.account_number ORDER BY b.event_dt DESC) rankk 
		INTO #AB_BCRQ_2
		FROM FORECAST_Base_Sample		AS a 
		JOIN CITeam.Broadband_Comms_Pipeline AS b ON a.account_number = b.account_number 
												AND  b.event_dt <= a.end_date 
		WHERE a.BB_status_code IN ('AB' , 'BCRQ' ) AND PL_Future_Sub_Effective_Dt IS NULL
			AND b.enter_syscan = 1 
			
		DELETE FROM #AB_BCRQ_2 WHERE rankk > 1  			---- LATEST PL
		
		SELECT 
			  a.account_number
			, a.event_dt
			, a.end_date
			, a.BB_status_code
			, b.status_code 			AS next_cancel_status
			, b.effective_from_dt		AS next_cancel_dt 	
			, RANK() OVER (PARTITION BY a.account_number ORDER BY b.effective_from_dt , b.cb_row_id) rankk
		INTO #AB_BCRQ_3
		FROM #AB_BCRQ_2					AS a 
		JOIN cust_subs_hist 			AS b  	ON a.account_number = b.account_number AND a.event_dt <= b.effective_from_dt
		WHERE   b.subscription_sub_type = 'Broadband DSL Line'
			AND b.status_code_changed = 'Y'
			AND b.effective_from_dt != b.effective_to_dt
			AND b.prev_status_code IN ('BCRQ')
			AND b.status_code IN ('CN','SC','PO') 
		
		DELETE FROM #AB_BCRQ_3 WHERE  rankk > 1  -- First cancellation after event_dt
		CREATE HG INDEX id1 ON #AB_BCRQ_3(account_number)
		
		UPDATE FORECAST_Base_Sample
		SET PL_Future_Sub_Effective_Dt = next_cancel_dt
		FROM FORECAST_Base_Sample 		AS a 
		JOIN  #AB_BCRQ_3 				AS b ON a.account_number = b.account_number AND a.end_date = b.end_date 
		
		DROP TABLE #AB_BCRQ_3
		
		MESSAGE cast(now() as timestamp)||' | SABB_Forecast_Create_Opening_Base -  UPDATE AB_Future_Sub_Effective_Dt checkpoint 1/2' TO CLIENT
		----------------------------------------------------------------
		------------- Accounts in the pipeline 
		----------------------------------------------------------------
		
		SELECT a.account_number 
			, a.end_date
			, b.event_dt
			, RAND(CAST(RIGHT (a.account_number ,6) AS INT) * DATEPART(ms, GETDATE())) randx 
			, RANK () OVER (PARTITION BY a.account_number ORDER BY b.event_dt DESC) rankk 
		INTO #AB_2
		FROM FORECAST_Base_Sample		AS a 
		JOIN CITeam.Broadband_Comms_Pipeline AS b 	ON a.account_number = b.account_number 
													AND b.event_dt <= a.end_date 
		WHERE PL_Future_Sub_Effective_Dt IS NULL AND  a.BB_status_code IN ('AB')
				AND b.enter_syscan = 1
		
		DELETE FROM #AB_2 WHERE rankk > 1
		
		SELECT *
			, CASE 	WHEN randx <= 0.25 					THEN DATEADD(DAY, 15, event_dt) 
					WHEN randx BETWEEN 0.25 AND 0.79 	THEN DATEADD(DAY, 60, event_dt) 
					WHEN randx >=    	0.79			THEN DATEADD(DAY, 65, event_dt) 
					ELSE event_dt END 													AS next_cancel_dt
		INTO #AB_3
		FROM #AB_2
		
		
		UPDATE FORECAST_Base_Sample
		SET PL_Future_Sub_Effective_Dt = next_cancel_dt
		FROM FORECAST_Base_Sample 		AS a 
		JOIN  #AB_3 					AS b ON a.account_number = b.account_number AND a.end_date = b.end_date 
		
		
		MESSAGE cast(now() as timestamp)||' | SABB_Forecast_Create_Opening_Base -  UPDATE AB_Future_Sub_Effective_Dt checkpoint 2/2' TO CLIENT
			
	------------==================================++++++++++++++++++++++++++++++++==========================================---------------
	------------==================================++++++++++++++++++++++++++++++++==========================================---------------
	------------==================================++++++++++++++++++++++++++++++++==========================================---------------
	------------==================================++++++++++++++++++++++++++++++++==========================================---------------
	----------------------------------------
	UPDATE FORECAST_Base_Sample sample
	SET PL_Future_Sub_Effective_Dt = MoR.BCRQ_Future_Sub_Effective_Dt
	FROM FORECAST_Base_Sample AS sample
	INNER JOIN CITeam.Broadband_Comms_Pipeline	MoR ON  MoR.account_number = sample.account_number 
													AND MoR.AB_Future_Sub_Effective_Dt > sample.end_date 
													AND MoR.event_dt <= sample.end_date 
													AND (MoR.AB_effective_to_dt > sample.end_date OR MoR.AB_effective_to_dt IS NULL)
	WHERE sample.BB_Status_Code = 'BCRQ'

	
	UPDATE FORECAST_Base_Sample sample
	SET BB_Status_Code = 'AC'
	WHERE PL_Future_Sub_Effective_Dt IS NULL AND BB_Status_Code IN ('PC','AB','BCRQ')

	
	--sample to speed up processing
	UPDATE FORECAST_Base_Sample
	SET sample = CASE WHEN rand_sample < sample_pct THEN 'A' ELSE 'B' END
		-- Select subs_week_and_year, count(*) as n, count(distinct account_number) as d, n-d as dups from Forecast_Loop_Table group by subs_week_and_year
		-- set @true_sample_rate = (select sum(case when sample='A' then cast(1 as float) else 0 end)/count(*) from #base_sample)
END

-- Grant execute rights to the members of CITeam
GRANT EXECUTE ON SABB_Forecast_Create_Opening_Base TO CITeam, vespa_group_low_security


/*
-- Test it
Select top 1000 * from CITeam.Forecast_Create_Opening_Base(201601,0.25)

*/



