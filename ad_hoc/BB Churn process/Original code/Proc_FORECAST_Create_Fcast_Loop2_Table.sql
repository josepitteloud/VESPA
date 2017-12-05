-----------------------------------------------------------------------------------------------
----PART I: CUSCAN RATES    -------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
CREATE variable Y2W01 INT;

CREATE variable Y3W52 INT;

SET Y2W01 = 201401;
SET Y3W52 = 201552;

-- First you need to impersonate CITeam
SETUSER CITeam;

-- Drop procedure if exists CITeam.Forecast_Create_Forecast_Loop_Table_2;
CREATE PROCEDURE CITeam.Forecast_Create_Forecast_Loop_Table_2 (
	IN Forecast_Start_Wk INT
	, IN Forecast_End_Wk INT
	, IN true_sample_rate FLOAT
	) SQL Security INVOKER

BEGIN
	DECLARE multiplier BIGINT;

	SET multiplier = DATEPART(millisecond, now()) + 1;
	SET TEMPORARY	OPTION Query_Temp_Space_Limit = 0;

	-- update the dates first
	DROP TABLE IF EXISTS #Loop_Sky_Calendar;
		SELECT * 		INTO #Loop_Sky_Calendar FROM CITeam.Subs_Calendar(Forecast_Start_Wk / 100, Forecast_End_Wk / 100);

	UPDATE Forecast_Loop_Table a
	SET subs_week_and_year = sc.subs_week_and_year
		, subs_week_of_year = sc.subs_week_of_year
	FROM Forecast_Loop_Table a
	INNER JOIN #Loop_Sky_Calendar sc ON sc.calendar_date = a.end_date + 7;

	-- update the segments
	UPDATE Forecast_Loop_Table
	SET CusCan_Forecast_Segment = CASE WHEN DTV_status_code IN ('AB', 'PC') THEN DTV_status_code ELSE csl.cuscan_forecast_segment END
	FROM Forecast_Loop_Table flt
	INNER JOIN CITeam.CusCan_Segment_Lookup csl ON csl.dtv_tenure = flt.dtv_tenure 
							AND csl.Time_Since_Last_TA_Call = flt.Time_Since_Last_TA_Call 
							AND csl.Offer_Length_DTV = flt.Offer_Length_DTV 
							AND csl.Time_To_Offer_End_DTV = flt.Time_To_Offer_End_DTV 
							AND csl.package_desc = flt.package_desc 
							AND csl.Country = flt.Country;

	UPDATE Forecast_Loop_Table flt
	SET SysCan_Forecast_Segment = CASE WHEN DTV_status_code IN ('AB', 'PC') THEN DTV_status_code ELSE ssl.SysCan_Forecast_Segment END
	FROM Forecast_Loop_Table flt
	INNER JOIN CITeam.SysCan_Segment_Lookup ssl ON ssl.Time_Since_Last_AB = flt.Time_Since_Last_AB 
									AND ssl.dtv_tenure = flt.dtv_tenure 
									AND ssl.Affluence = flt.Affluence 
									AND ssl.simple_segments = flt.simple_segments 
									AND ssl.Previous_AB_Count = flt.Previous_AB_Count;

	UPDATE Forecast_Loop_Table
	SET rand_action_Cuscan = rand(number(*) * multiplier + 1)
		, rand_action_Syscan = Cast(NULL AS FLOAT)
		, rand_TA_Vol = rand(number(*) * multiplier + 2)
		, rand_WC_Vol = rand(number(*) * multiplier + 3)
		, rand_TA_Save_Vol = rand(number(*) * multiplier + 4)
		, rand_WC_Save_Vol = rand(number(*) * multiplier + 5)
		, rand_TA_DTV_Offer_Applied = rand(number(*) * multiplier + 6)
		, rand_NonTA_DTV_Offer_Applied = rand(number(*) * multiplier + 7)
		, rand_TA_DTV_PC_Vol = rand(number(*) * multiplier + 8)
		, rand_WC_DTV_PC_Vol = rand(number(*) * multiplier + 9)
		, rand_Other_DTV_PC_Vol = rand(number(*) * multiplier + 10)
		, rand_Intrawk_DTV_PC = rand(number(*) * multiplier + 2134)
		, rand_DTV_PC_Duration = rand(number(*) * multiplier + 234)
		, rand_DTV_PC_Status_Change = rand(number(*) * multiplier + 8323)
		, rand_New_Off_Dur = rand(number(*) * multiplier + 3043)
		, rand_Intrawk_DTV_AB = rand(number(*) * multiplier + 3383);

	-- 3.02 Add Random Number and Segment Size for random event allocations later --
	DROP TABLE IF EXISTS Pred_Rates;
		SELECT a.*
			, b.Cuscan * true_sample_rate AS CusCan_Churn
			, c.Syscan * true_sample_rate AS SysCan_Churn
		INTO Pred_Rates
		FROM Forecast_Loop_Table AS a
		/*-------------------------------------------------------------*/-- this can removed once cuscan and syscan are forecasted - ----
		LEFT JOIN (SELECT Cuscan_forecast_segment
						, sum(PO_Pipeline_Cancellations) + sum(Same_Day_Cancels) AS Cuscan
					FROM citeam.DTV_Fcast_Weekly_Base
					WHERE Subs_year = 2015 AND subs_week = (SELECT max(subs_week_of_year) FROM Forecast_Loop_Table)
					GROUP BY Cuscan_forecast_segment
					) AS b ON a.Cuscan_forecast_segment = b.Cuscan_forecast_segment
		LEFT JOIN (SELECT Syscan_forecast_segment
						, sum(SC_Gross_Terminations) AS Syscan
					FROM citeam.DTV_Fcast_Weekly_Base
					WHERE Subs_year = 2015 AND subs_week = (SELECT max(subs_week_of_year) FROM Forecast_Loop_Table )
					GROUP BY Syscan_forecast_segment
					) AS c ON a.syscan_forecast_segment = c.syscan_forecast_segment
			/*--------------------------------------------------------------*/
			;

	-- 3.04 Calculate Proportions for random event allocation and bring in event rates --
	-- we have calculated above the distributions for TA_Calls and WC_Calls
	--     we need to treat somehow the overlapping customers - that go in PC and AB
	-- we calculate first the cuscan and then we exclude the cuscan in order to caluclate the syscan
	-- we set syscan rank as null
	DROP TABLE IF EXISTS Forecast_Loop_Table_2;
		
		SELECT a.* --account_number
			-- ,a.end_date
			, count(*) OVER (PARTITION BY a.Cuscan_forecast_segment) AS Cuscan_segment_count
			, count(*) OVER (PARTITION BY a.Syscan_forecast_segment) AS Syscan_segment_count
			, cast(row_number() OVER (PARTITION BY a.Cuscan_Forecast_segment ORDER BY rand_action_Cuscan) AS FLOAT) AS CusCan_Group_rank
			, Cast(NULL AS FLOAT) AS SysCan_Group_rank
			, CusCan_Group_rank / Cuscan_segment_count AS pct_cuscan_count
			, cast(NULL AS FLOAT) AS pct_syscan_count
			, cast(0 AS TINYINT) AS CusCan
			, cast(0 AS TINYINT) AS SysCan
			-- cuscan
			, Cast(0 AS FLOAT) AS pred_TA_Call_Cust_rate
			, Cast(0 AS FLOAT) AS pred_TA_Call_Cust_YoY_Trend
			, Cast(0 AS FLOAT) AS cum_TA_Call_Cust_rate
			, Cast(0 AS FLOAT) AS pred_Web_Chat_TA_Cust_rate
			, Cast(0 AS FLOAT) AS pred_Web_Chat_TA_Cust_YoY_Trend
			, Cast(0 AS FLOAT) AS cum_Web_Chat_TA_Cust_rate
			, Cast(0 AS FLOAT) AS cum_Web_Chat_TA_Cust_Trend_rate
			-- ,Cast(0 as float) /*b.DTV_Offer_Applied_Rate*/ as pred_DTV_Offer_Applied_Rate
			--syscan
			, Cast(0 AS FLOAT) AS pred_DTV_AB_rate
			, Cast(0 AS FLOAT) AS pred_DTV_YoY_Trend
			, Cast(0 AS FLOAT) AS cum_DTV_AB_rate
			, cast(0 AS FLOAT) AS cum_DTV_AB_Trend_rate
			, Cast(0 AS FLOAT) AS pred_NonTA_DTV_Offer_Applied_rate
			, Cast(0 AS FLOAT) AS pred_NonTA_DTV_Offer_Applied_YoY_Trend
			, Cast(0 AS FLOAT) AS pred_TA_DTV_Offer_Applied_rate
			, Cast(0 AS FLOAT) AS pred_TA_DTV_Offer_Applied_YoY_Trend
			-- calculate actions and use cumulative to keep the relationship
			-- ,cum_dtv_ab_rate + b.DTV_Offer_Applied_Rate as cum_DTV_Offer_Applied_Rate
			, cast(0 AS TINYINT) AS TA_Call_Cust
			, cast(0 AS TINYINT) AS TA_Call_Count
			, cast(0 AS TINYINT) AS TA_Saves
			, cast(0 AS TINYINT) AS TA_Non_Saves
			, cast(0 AS TINYINT) AS WC_Call_Cust
			, cast(0 AS TINYINT) AS WC_Call_Count
			, cast(0 AS TINYINT) AS WC_Saves
			, cast(0 AS TINYINT) AS WC_Non_Saves
			, cast(0 AS TINYINT) AS DTV_AB
			, cast(0 AS TINYINT) AS DTV_Offer_Applied
			-- ,cast(0 as tinyint) as DTV_Offer_Applied
			---------- TA -> PC
			-- rates
			, cast(0 AS FLOAT) AS pred_TA_DTV_PC_rate
			, cast(0 AS FLOAT) AS pred_TA_Sky_Plus_Save_rate
			, Cast(0 AS FLOAT) AS cum_TA_DTV_PC_rate
			, cast(0 AS FLOAT) AS pred_WC_DTV_PC_rate
			, cast(0 AS FLOAT) AS pred_WC_Sky_Plus_Save_rate
			, Cast(0 AS FLOAT) AS cum_WC_DTV_PC_rate
			, Cast(0 AS FLOAT) AS pred_Other_DTV_PC_rate
			----------- PC
			--- flag
			, cast(0 AS TINYINT) AS TA_DTV_PC
			, cast(0 AS TINYINT) AS WC_DTV_PC
			, cast(0 AS TINYINT) AS TA_Sky_Plus_Save
			, cast(0 AS TINYINT) AS WC_Sky_Plus_Save
			, cast(0 AS TINYINT) AS Other_DTV_PC
		INTO Forecast_Loop_Table_2
		FROM Pred_Rates AS a;
END;

-- Grant execute rights to the members of CITeam
GRANT EXECUTE
	ON CITeam.Forecast_Create_Forecast_Loop_Table_2
	TO CITeam;

-- Change back to your account
SETUSER;

-- Test it
Call CITeam.Forecast_Create_Forecast_Loop_Table_2(201601, 201652);
