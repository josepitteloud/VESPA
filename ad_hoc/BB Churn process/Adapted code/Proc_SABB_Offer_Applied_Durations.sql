CREATE OR REPLACE PROCEDURE SABB_Offer_Applied_Duration_Dist (IN Forecast_Start_Wk INT , Num_Wks INT) 
	RESULT (
	Offer_segment VARCHAR(30)
	, Total_Offer_Duration_Mth INT
	, Weekly_Avg_New_Offers INT
	, Total_New_Offers INT
	, Cum_New_Offers INT
	, Dur_Pctl_Lower_Bound FLOAT
	, Dur_Pctl_Upper_Bound FLOAT
	)

BEGIN
		MESSAGE CAST(now() as timestamp)||' | SABB_Offer_Applied_Duration_Dist - Initialization begin ' TO CLIENT;
	SELECT *
	INTO #Sky_Calendar
	FROM subs_calendar(Forecast_Start_Wk / 100 - 1, Forecast_Start_Wk / 100);

	SELECT
			CASE overall_offer_segment 
					WHEN '2.(BB)A1.Acquisition/Upgrade' THEN 'Activations' 
					WHEN '2.(BB)B1.TA' THEN 'TA' 
					WHEN '2.(BB)B2.CoE' THEN 'Other' 
					WHEN '2.(BB)B3.PAT' THEN 'Other' 
					WHEN '2.(BB)B4.Pipeline ReInstate' THEN 'Reactivations' 
					WHEN '2.(BB)B5.Other Retention' THEN 'Reactivations' 
					WHEN '2.(BB)C1.BB Package Movement' THEN 'Other' 
					WHEN '2.(BB)C2.Offer On Call' THEN 'Other' 
					WHEN '2.(BB)C4.Other' THEN 'Other' 
				END 																										AS overall_offer_segment
		, Total_Offer_Duration_Mth
		, COUNT(*) / Num_Wks 																								AS Weekly_Avg_New_Offers
		, Sum(Weekly_Avg_New_Offers) OVER (PARTITION BY overall_offer_segment) 												AS Total_New_Offers
		, Sum(Weekly_Avg_New_Offers) OVER (PARTITION BY overall_offer_segment ORDER BY Total_Offer_Duration_Mth) 			AS Cum_New_Offers
		, Cast(Cum_New_Offers AS FLOAT) / Total_New_Offers AS Pctl_New_Offers
		, Row_Number() OVER (PARTITION BY overall_offer_segment ORDER BY Total_Offer_Duration_Mth) Dur_Rnk
	INTO #Offer_Dur
	FROM citeam.offer_usage_all 				AS oua
	WHERE offer_start_dt_Actual BETWEEN (SELECT max(calendar_date - 7 - Num_Wks * 7 + 1) FROM #sky_calendar WHERE subs_week_and_year = Forecast_Start_Wk) 
									AND (SELECT max(calendar_date - 7) FROM #sky_calendar WHERE subs_week_and_year = Forecast_Start_Wk) 
			AND Total_Offer_Duration_Mth <= 36 
			AND offer_start_dt_Actual = Whole_Offer_Start_Dt_Actual 
			AND Subs_Type = 'Broadband DSL Line' 
			AND lower(offer_dim_description) NOT LIKE '%price protection%' 
			AND oua.overall_offer_segment_grouped_1 != 'Price Protection'
	GROUP BY overall_offer_segment
		, Total_Offer_Duration_Mth ;
		
		MESSAGE CAST(now() as timestamp)||' | SABB_Offer_Applied_Duration_Dist - Offer_Dur table completed: '||@@rowcount TO CLIENT;
		
	SELECT
		  dur1.overall_offer_segment
		, dur1.Total_Offer_Duration_Mth
		, dur1.Weekly_Avg_New_Offers
		, dur1.Total_New_Offers
		, dur1.Cum_New_Offers
		, Coalesce(dur2.Pctl_New_Offers, 0) Dur_Pctl_Lower_Bound
		, dur1.Pctl_New_Offers Dur_Pctl_Upper_Bound
	FROM #Offer_Dur dur1
	LEFT JOIN #Offer_Dur dur2 ON dur2.overall_offer_segment = dur1.overall_offer_segment AND dur2.Dur_Rnk = dur1.Dur_Rnk - 1;
	
	MESSAGE CAST(now() as timestamp)||' | SABB_Offer_Applied_Duration_Dist - COMPLETED' TO CLIENT;
END;

GRANT EXECUTE
	ON SABB_Offer_Applied_Duration_Dist TO CITeam;
		
