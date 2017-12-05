--	monthly_base
--	BB_CHURN_calls_details_raw_3yr_final

SELECT 	  account_number
		, b.subs_year	
		, b.subs_week_of_year	
		, b.subs_quarter_of_year	
		, MIN(calendar_date)	AS first_date 
		, SUM(total_transfers+ 1 ) 		AS t_transfers
INTO #t1		
FROM  CALLS_DETAILS AS a 
JOIN SKY_Calendar AS b ON DATE(a.call_date) = b.calendar_date
WHERE 
	call_date >= '2013-09-01'
	AND     final_sct_grouping = 'Retention - BBCoE'
	AND account_number IS NOT NULL 
GROUP BY 		
		  account_number
		, b.subs_year	
		, b.subs_week_of_year	
		, b.subs_quarter_of_year	
		
COMMIT 
CREATE		HG INDEX id1 ON #t1(account_number) 
CREATE		LF INDEX id2 ON #t1(subs_year) 
CREATE		LF INDEX id3 ON #t1(subs_week_of_year) 
CREATE	DATE 	INDEX id4 ON #t1(first_date) 
COMMIT 
		
		
UPDATE BB_CHURN_calls_details_raw_3yr_final
SET transfer = t_transfers
FROM BB_CHURN_calls_details_raw_3yr_final AS a 
JOIN #t1 AS b ON a.account_number = b.account_number 
			AND a.subs_year = b.subs_year
			AND DATE(a.first_date) = DATE(b.first_date)
			AND a.subs_week_of_year = b.subs_week_of_year
			
			COMMIt
			DROP TABLE #t1 


