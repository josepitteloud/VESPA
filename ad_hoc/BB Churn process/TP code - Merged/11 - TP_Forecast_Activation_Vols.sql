CREATE OR REPLACE PROCEDURE TP_Forecast_Activation_Vols (IN Y2W01 INT, IN Y3W52 INT) 
	result (
		Subs_Week_Of_Year SMALLINT
	, Reinstates INT
	, Acquisitions INT
	, New_Customers INT
	)

BEGIN
	SET TEMPORARY OPTION Query_Temp_Space_Limit = 0;

	message cast(now() AS TIMESTAMP) || ' | TP_Forecast_Activation_Vols - Initialization begin ' TO client;

	DROP TABLE IF EXISTS #Sky_Calendar;
		CREATE TABLE #Sky_Calendar (calendar_date DATE NULL, subs_week_of_year INT NULL,);

	CREATE lf INDEX idx_1 ON #Sky_Calendar (calendar_date);

	INSERT INTO #Sky_Calendar
	SELECT calendar_date
		, subs_week_of_year
	FROM CITeam.Subs_Calendar(Y2W01 / 100, Y3W52 / 100)
	WHERE subs_week_and_year BETWEEN Y2W01 AND Y3W52 AND subs_last_day_of_week = 'Y' AND subs_week_of_year <> 53;

	message cast(now() AS TIMESTAMP) || ' | Forecast_Activation_Vols - Calendar setup' TO client;

	SELECT end_date
		, cast(NULL AS INT) AS Subs_Week_Of_Year
		, sum(CASE WHEN BB_latest_act_dt BETWEEN (end_date - 6) AND end_date AND (BB_first_act_dt < BB_latest_act_dt) THEN 1 ELSE 0 END) AS Reinstates
		, sum(CASE WHEN BB_latest_act_dt BETWEEN (end_date - 6) AND end_date AND (BB_first_act_dt = BB_latest_act_dt) THEN 1 ELSE 0 END) AS Acquisitions
		, Reinstates + Acquisitions AS New_Customers
	INTO #Activation_Vols
	FROM jcartwright.cust_fcast_weekly_Base_2 AS base
	WHERE base.end_date = ANY (SELECT calendar_date FROM #Sky_Calendar ) 
		AND DTV_ACTIVE = 1
		AND bb_active = 1
	GROUP BY end_date;

	message cast(now() AS TIMESTAMP) || ' | Forecast_Activation_Vols - Activations table DONE:' || @@rowcount TO client;

	UPDATE #Activation_Vols AS av
	SET subs_week_of_year = sc.subs_week_of_year
	FROM #Activation_Vols AS av
	INNER JOIN #Sky_Calendar AS sc ON sc.calendar_date = av.end_date;

	SELECT Subs_Week_Of_Year
		, Avg(Coalesce(av.Reinstates, 0)) AS Reinstates
		, Avg(Coalesce(av.Acquisitions, 0)) AS Acquisitions
		, Avg(Coalesce(av.New_Customers, 0)) AS New_Customers
	FROM #Activation_Vols AS av
	GROUP BY Subs_Week_Of_Year;

	message cast(now() AS TIMESTAMP) || ' | Forecast_Activation_Vols - Proc completed :' TO client
END
GO


