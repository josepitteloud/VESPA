CREATE OR REPLACE PROCEDURE TP_Offer_Applied_Duration_Dist 
		(IN Forecast_Start_Wk INT
		, IN Num_Wks INT) 
	result (
		Offer_segment VARCHAR(30)
	, Total_Offer_Duration_Mth INT
	, Weekly_Avg_New_Offers INT
	, Total_New_Offers INT
	, Cum_New_Offers INT
	, Dur_Pctl_Lower_Bound REAL
	, Dur_Pctl_Upper_Bound REAL
	)

BEGIN
	message cast(now() AS TIMESTAMP) || ' | TP_SABB_Offer_Applied_Duration_Dist - Initialization begin ' TO client;

	SELECT * INTO #Sky_Calendar FROM subs_calendar(Forecast_Start_Wk / 100 - 1, Forecast_Start_Wk / 100);
	
	CREATE OR REPLACE VARIABLE @Lw6dt DATE ;
	CREATE OR REPLACE VARIABLE @Hw6dt DATE ;
	SET @Lw6dt = (SELECT max(calendar_date - 7 - Num_Wks * 7 + 1) FROM #sky_calendar WHERE subs_week_and_year = Forecast_Start_Wk ) ;
	SET @Hw6dt = (SELECT max(calendar_date - 7) FROM #sky_calendar WHERE subs_week_and_year = Forecast_Start_Wk )  ;
	
	--------------------------------------------------------------------------------------------------------------------------------------------		
	SELECT DISTINCT account_number , 1 dummy
	INTO #acct
	FROM citeam.CUST_Fcast_Weekly_Base	
	WHERE end_date BETWEEN @Lw6dt AND @Hw6dt
		AND DTV_active = 1 AND bb_active = 1;
	
	/*
	SELECT DISTINCT a.account_number, 1 sky_plus
	INTO #skyplus
	FROM   CUST_SUBS_HIST 		AS a
	JOIN   #acct AS b ON a.account_number = b.account_number 
	WHERE  subscription_sub_type = 'DTV Sky+'
		AND        	a.status_code='AC'
		AND        	first_activation_dt<=today()               
		AND        	a.account_number is not null
		AND        	a.account_number <> '?'
		AND     	@Hw6dt BETWEEN effective_from_dt AND effective_to_dt ;
		
	SELECT DISTINCT a.account_number, 1 nowtv
	INTO 		#nowtv
	FROM        NOW_TV_SUBS_HIST AS csav
	JOIN 		#acct AS a ON a.account_number= csav.account_number
	WHERE       @Hw6dt BETWEEN effective_from_dt AND effective_to_dt ;
	
	DELETE FROM #acct WHERE account_number IN (SELECT account_number FROM #skyplus);
	DELETE FROM #acct WHERE account_number IN (SELECT account_number FROM #nowtv);
	DROP TABLE #skyplus; 
	DROP TABLE #nowtv; 
	*/
	--------------------------------------------------------------------------------------------------------------------------------------------		
	
	SELECT CASE overall_offer_segment 	WHEN '2.(BB)A1.Acquisition/Upgrade' THEN 'Activations' 
										WHEN '2.(BB)B1.TA' THEN 'TA' 
										WHEN '2.(BB)B2.CoE' THEN 'Other' 
										WHEN '2.(BB)B3.PAT' THEN 'Other' 
										WHEN '2.(BB)B4.Pipeline ReInstate' THEN 'Reactivations' 
										WHEN '2.(BB)B5.Other Retention' THEN 'Reactivations' 
										WHEN '2.(BB)C1.BB Package Movement' THEN 'Other' 
										WHEN '2.(BB)C2.Offer On Call' THEN 'Other' 
										WHEN '2.(BB)C4.Other' THEN 'Other' END AS overall_offer_segment
		, Total_Offer_Duration_Mth
		, COUNT() / Num_Wks AS Weekly_Avg_New_Offers
		, Sum(Weekly_Avg_New_Offers) OVER (PARTITION BY overall_offer_segment) AS Total_New_Offers
		, Sum(Weekly_Avg_New_Offers) OVER (PARTITION BY overall_offer_segment ORDER BY Total_Offer_Duration_Mth ASC) AS Cum_New_Offers
		, cast(Cum_New_Offers AS REAL) / Total_New_Offers AS Pctl_New_Offers
		, Row_Number() OVER (PARTITION BY overall_offer_segment ORDER BY Total_Offer_Duration_Mth ASC) AS Dur_Rnk
	INTO #Offer_Dur
	FROM citeam.offer_usage_all AS oua
																				 
									  
	JOIN #acct AS y ON y.account_number = oua.account_number
	WHERE offer_start_dt_Actual BETWEEN @Lw6dt AND @Hw6dt
				 
			AND Total_Offer_Duration_Mth <= 36 
			AND offer_start_dt_Actual = Whole_Offer_Start_Dt_Actual 
			AND Subs_Type = 'Broadband DSL Line' 
			AND lower(offer_dim_description) NOT LIKE '%price protection%' 
			AND oua.overall_offer_segment_grouped_1 <> 'Price Protection'
	GROUP BY overall_offer_segment
		, Total_Offer_Duration_Mth;


	
	message cast(now() AS TIMESTAMP) || ' | TP_SABB_Offer_Applied_Duration_Dist - Offer_Dur table completed: ' || @@rowcount TO client;

	DROP VARIABLE @Lw6dt;
	DROP VARIABLE @Hw6dt;
	DROP TABLE #Sky_Calendar ;
	
	
	SELECT dur1.overall_offer_segment
		, dur1.Total_Offer_Duration_Mth
		, dur1.Weekly_Avg_New_Offers
		, dur1.Total_New_Offers
		, dur1.Cum_New_Offers
		, Coalesce(dur2.Pctl_New_Offers, 0) AS Dur_Pctl_Lower_Bound
		, dur1.Pctl_New_Offers AS Dur_Pctl_Upper_Bound
	FROM #Offer_Dur AS dur1
	LEFT JOIN #Offer_Dur AS dur2 ON dur2.overall_offer_segment = dur1.overall_offer_segment AND dur2.Dur_Rnk = dur1.Dur_Rnk - 1;

	message cast(now() AS TIMESTAMP) || ' | TP_SABB_Offer_Applied_Duration_Dist - COMPLETED' TO client;
						   
					   
END
GO

