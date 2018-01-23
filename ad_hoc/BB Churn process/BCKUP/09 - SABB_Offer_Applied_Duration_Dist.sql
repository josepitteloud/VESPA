CREATE OR REPLACE PROCEDURE SABB_Offer_Applied_Duration_Dist 
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
	message cast(now() AS TIMESTAMP) || ' | SABB_Offer_Applied_Duration_Dist - Initialization begin ' TO client;

	SELECT * INTO #Sky_Calendar FROM subs_calendar(Forecast_Start_Wk / 100 - 1, Forecast_Start_Wk / 100);
	
	CREATE OR REPLACE VARIABLE @Lw6dt DATE ;
	CREATE OR REPLACE VARIABLE @Hw6dt DATE ;
	SET @Lw6dt = (SELECT max(calendar_date - 7 - Num_Wks * 7 + 1) FROM #sky_calendar WHERE subs_week_and_year = Forecast_Start_Wk ) ;
	SET @Hw6dt = (SELECT max(calendar_date - 7) FROM #sky_calendar WHERE subs_week_and_year = Forecast_Start_Wk )  ;
	
	--------------------------------------------------------------------------------------------------------------------------------------------		


	SELECT DISTINCT z.account_number , 1 dummy
	INTO #acct
	FROM citeam.Cust_Weekly_Base	AS z
	LEFT JOIN citeam.nowtv_accounts_ents AS c ON z.account_number = c.account_number AND z.End_date BETWEEN period_start_date AND period_end_date
	WHERE z.end_date BETWEEN @Lw6dt AND @Hw6dt
		AND z.DTV_active = 0 
		AND z.bb_active = 1
		AND z.skyplus_active = 0 
		AND c.account_number IS NULL ;
																													   
 --------------------------------------------------------------------------------------------------------------------------------------------		
	
	
	SELECT CASE 
--	overall_offer_segment 	
										when pac like '%CHURN%' then 'Reactivations'
										when interest_source_level_1_description like '%Turn%' then 'TA'
										else 'Other'
--										WHEN '2.(BB)A1.Acquisition/Upgrade' THEN 'Activations' 
--										WHEN '2.(BB)B1.TA' THEN 'TA' 
--										WHEN '2.(BB)B2.CoE' THEN 'Other' 
--										WHEN '2.(BB)B3.PAT' THEN 'Other' 
--										WHEN '2.(BB)B4.Pipeline ReInstate' THEN 'Reactivations' 
--										WHEN '2.(BB)B5.Other Retention' THEN 'Reactivations' 
--										WHEN '2.(BB)C1.BB Package Movement' THEN 'Other' 
--										WHEN '2.(BB)C2.Offer On Call' THEN 'Other' 
--										WHEN '2.(BB)C4.Other' THEN 'Other' 
										END AS overall_offer_segment
		, intended_discount_duration as Total_Offer_Duration_Mth
		, COUNT() / Num_Wks AS Weekly_Avg_New_Offers
		, Sum(Weekly_Avg_New_Offers) OVER (PARTITION BY overall_offer_segment) AS Total_New_Offers
		, Sum(Weekly_Avg_New_Offers) OVER (PARTITION BY overall_offer_segment ORDER BY Total_Offer_Duration_Mth ASC) AS Cum_New_Offers
		, cast(Cum_New_Offers AS REAL) / Total_New_Offers AS Pctl_New_Offers
		, Row_Number() OVER (PARTITION BY overall_offer_segment ORDER BY Total_Offer_Duration_Mth ASC) AS Dur_Rnk
	INTO #Offer_Dur
	from Decisioning.Offers_Software as oua
	JOIN #acct AS y ON y.account_number = oua.account_number   -- offer_start_dt_actual and whole_offer_start_dt_actual total_offer_duration_mth
	WHERE offer_leg_start_dt_actual BETWEEN @Lw6dt AND @Hw6dt
			AND Total_Offer_Duration_Mth <= 36 
			AND offer_leg_start_dt_actual = Whole_Offer_Start_Dt_Actual 
			AND Subs_Type = 'Broadband DSL Line' 
			AND lower(offer_dim_description) NOT LIKE '%price protection%' 
	GROUP BY overall_offer_segment
		, Total_Offer_Duration_Mth;


	
	message cast(now() AS TIMESTAMP) || ' | SABB_Offer_Applied_Duration_Dist - Offer_Dur table completed: ' || @@rowcount TO client;

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

	message cast(now() AS TIMESTAMP) || ' | SABB_Offer_Applied_Duration_Dist - COMPLETED' TO client;
END
GO

