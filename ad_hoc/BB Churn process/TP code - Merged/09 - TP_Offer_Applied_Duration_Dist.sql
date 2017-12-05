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
	message cast(now() AS TIMESTAMP) || ' | TP_Offer_Applied_Duration_Dist - Initialization begin ' TO client;

	SELECT * INTO #Sky_Calendar FROM subs_calendar(Forecast_Start_Wk / 100 - 1, Forecast_Start_Wk / 100);

	DECLARE @Lw6dt DATE ;
	DECLARE @Hw6dt DATE ;
	SET @L6w = (SELECT max(calendar_date - 7 - Num_Wks * 7 + 1) FROM #sky_calendar WHERE subs_week_and_year = Forecast_Start_Wk ) ;
	SET @H6w =  (SELECT max(calendar_date - 7) FROM #sky_calendar WHERE subs_week_and_year = Forecast_Start_Wk ) ;
	
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
	JOIN (	SELECT DISTINCT account_number FROM jcartwright.CUST_Fcast_Weekly_Base_2	
		WHERE end_date BETWEEN @L6w AND @H6w
		AND DTV_active = 1 AND bb_active = 1 ) AS y ON y.account_number = mor.account_number				
	WHERE offer_start_dt_Actual BETWEEN @L6w
									AND @H6w
			AND Total_Offer_Duration_Mth <= 36 
			AND offer_start_dt_Actual = Whole_Offer_Start_Dt_Actual 
			AND Subs_Type = 'Broadband DSL Line' 
			AND lower(offer_dim_description) NOT LIKE '%price protection%' 
			AND oua.overall_offer_segment_grouped_1 <> 'Price Protection'
	GROUP BY overall_offer_segment
		, Total_Offer_Duration_Mth;

	message cast(now() AS TIMESTAMP) || ' | TP_Offer_Applied_Duration_Dist - Offer_Dur table completed: ' || @@rowcount TO client;

	SELECT dur1.overall_offer_segment
		, dur1.Total_Offer_Duration_Mth
		, dur1.Weekly_Avg_New_Offers
		, dur1.Total_New_Offers
		, dur1.Cum_New_Offers
		, Coalesce(dur2.Pctl_New_Offers, 0) AS Dur_Pctl_Lower_Bound
		, dur1.Pctl_New_Offers AS Dur_Pctl_Upper_Bound
	FROM #Offer_Dur AS dur1
	LEFT JOIN #Offer_Dur AS dur2 ON dur2.overall_offer_segment = dur1.overall_offer_segment AND dur2.Dur_Rnk = dur1.Dur_Rnk - 1;

	message cast(now() AS TIMESTAMP) || ' | TP_Offer_Applied_Duration_Dist - COMPLETED' TO client;
	DROP TABLE #Sky_Calendar ;
	DROP TABLE #Offer_Dur;
END
GO

