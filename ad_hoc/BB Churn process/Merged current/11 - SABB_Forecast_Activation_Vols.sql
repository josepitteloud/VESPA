CREATE OR REPLACE PROCEDURE SABB_Forecast_Activation_Vols (IN Y2W01 INT, IN Y3W52 INT) 
	result (
		Subs_Week_Of_Year SMALLINT
	, Reinstates INT
	, Acquisitions INT
	, New_Customers INT
	)

BEGIN
	SET TEMPORARY OPTION Query_Temp_Space_Limit = 0;

	message cast(now() AS TIMESTAMP) || ' | Forecast_Activation_Vols - Initialization begin ' TO client;

	
	DROP TABLE IF EXISTS #Sky_Calendar;
		CREATE TABLE #Sky_Calendar (calendar_date DATE NULL, subs_week_of_year INT NULL,);

	CREATE lf INDEX idx_1 ON #Sky_Calendar (calendar_date);
	DROP VARIABLE IF EXISTS @end_date; 
	DROP VARIABLE IF EXISTS @scaling_factor; 
	DROP VARIABLE IF EXISTS @min_year; 
	CREATE VARIABLE @end_date DATE; 
	CREATE VARIABLE @scaling_factor FLOAT; 
	CREATE VARIABLE @min_year INT; 
	
	INSERT INTO #Sky_Calendar
	SELECT calendar_date
		, subs_week_of_year
	FROM /*CITeam.*/Subs_Calendar (Y2W01 / 100, Y3W52 / 100)
	WHERE subs_week_and_year BETWEEN Y2W01 AND Y3W52 AND subs_last_day_of_week = 'Y' AND subs_week_of_year <> 53;

	message cast(now() AS TIMESTAMP) || ' | Forecast_Activation_Vols - Calendar setup' TO client;

		--------------------------------------------------------------------------------------------------------------------------------------------		
	SET @end_date = (SELECT max(calendar_date) FROM Subs_Calendar (Y2W01 / 100, Y3W52 / 100) WHERE subs_week_and_year = Y3W52);
	
	SELECT DISTINCT a.account_number, 1 sky_plus
	INTO #skyplus
	FROM   CUST_SUBS_HIST 		AS a
		WHERE  subscription_sub_type = 'DTV Sky+'
		AND        	status_code='AC'
		AND        	first_activation_dt<=today()               
		AND        	a.account_number is not null
		AND        	a.account_number <> '?'
		AND     	@end_date BETWEEN effective_from_dt AND effective_to_dt ;
		
	--------------------------------------------------------------------------------------------------------------------------------------------	
	
	SELECT end_date
		, CAST(LEFT(subs_week_and_year,4) AS INT) AS subs_year
		, cast(NULL AS INT) AS Subs_Week_Of_Year
		, sum(CASE WHEN BB_latest_act_dt BETWEEN (end_date - 6) AND end_date AND (BB_first_act_dt < BB_latest_act_dt) THEN 1 ELSE 0 END) AS Reinstates
		, sum(CASE WHEN BB_latest_act_dt BETWEEN (end_date - 6) AND end_date AND (BB_first_act_dt = BB_latest_act_dt) THEN 1 ELSE 0 END) AS Acquisitions
		, Reinstates + Acquisitions AS New_Customers
	INTO #Activation_Vols
	FROM citeam.CUST_Fcast_Weekly_Base AS base
	LEFT JOIN #skyplus 	AS b ON base.account_number = b.account_number 
	LEFT JOIN citeam.nowtv_accounts_ents	AS c ON base.account_number = c.account_number AND base.end_date BETWEEN period_start_date AND period_end_date
	WHERE base.end_date = ANY (SELECT calendar_date FROM #Sky_Calendar ) 
		AND base.DTV_ACTIVE = 0 
		AND base.bb_active = 1
		AND (b.account_number IS NULL OR c.account_number IS NULL )
	GROUP BY end_date, subs_year;

	message cast(now() AS TIMESTAMP) || ' | Forecast_Activation_Vols - Activations table DONE:' || @@rowcount TO client;

	SET @min_year = (SELECT MIN(YEAR(end_date)) FROM #Activation_Vols);
	
	UPDATE #Activation_Vols 
	SET subs_week_of_year = sc.subs_week_of_year
	FROM #Activation_Vols AS av
	INNER JOIN #Sky_Calendar AS sc ON sc.calendar_date = av.end_date;

	delete from    #Activation_Vols    where New_customers = 0;  

	SET  @scaling_factor = (SELECT AVG(New_Customers) FROM #Activation_Vols WHERE YEAR(end_date) = @min_year +1) - (SELECT AVG(New_Customers) FROM #Activation_Vols WHERE YEAR(end_date) = @min_year);
	--SELECT * INTO act FROM #Activation_Vols;

	SELECT Subs_Week_Of_Year
		, Avg(av.Reinstates) + @scaling_factor AS Reinstates
		, Avg(av.Acquisitions) + @scaling_factor AS Acquisitions
		, Avg(av.New_Customers) + @scaling_factor AS New_Customers
	FROM #Activation_Vols AS av
	GROUP BY Subs_Week_Of_Year;

	
	message cast(now() AS TIMESTAMP) || ' | Forecast_Activation_Vols - Proc completed :' TO client
END
GO
