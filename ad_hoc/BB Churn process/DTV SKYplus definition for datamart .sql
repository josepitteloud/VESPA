-------------------------------  DTV Sky+ definition for the cust_fcast_weekly_base datamart
------------ Coded by Jose Pitteloud


SELECT DISTINCT 
		a.account_number
		, end_date 
	INTO #skyplus
	FROM   CUST_SUBS_HIST 		AS a
	JOIN   citeam.CUST_FCAST_WEEKLY_BASE AS b ON a.account_number = b.account_number AND end_date BETWEEN effective_from_dt AND effective_to_dt 
	WHERE  subscription_sub_type = 'DTV Sky+'
		AND        	status_code IN ('AC','AB','PC') 
		AND        	first_activation_dt<=today()               
		AND        	a.account_number is not null
		AND        	a.account_number <> '?'
		
	COMMIT 
	CREATE HG INDEX id1 ON #skyplus (account_number)
	COMMIT 
	
	UPDATE citeam.CUST_FCAST_WEEKLY_BASE 
	SET Sky_plus = 1 
	FROM citeam.CUST_FCAST_WEEKLY_BASE  AS a 
	JOIN #skyplus					AS b ON a.account_number = b.account_number AND a.end_Date = b.end_date 
	
	DROP TABLE #skyplus
