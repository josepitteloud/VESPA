
--------------------------------------------------------
---------------------------- Sky Generated Home Mover
--------------------------------------------------------
SELECT 
		account_number
		, CASE 	
			WHEN home_move_status = 'Pre Home Move' 				THEN 'Pre Home Move' 
			WHEN home_move_status = 'Pending' 						THEN 'Pending Home Move'
			WHEN home_move_status = 'In-Progress'					THEN 'Post Home Move 0-30 days'
			WHEN home_move_status = 'Post Home Move' 	AND DATEDIFF(dd, effective_from_dt, getdate()) BETWEEN 0 AND 30 	THEN 'Post Home Move 0-30 days'
			WHEN home_move_status = 'Post Home Move' 	AND DATEDIFF(dd, effective_from_dt, getdate()) BETWEEN 31 AND 60 	THEN 'Post Home Move 31-60 days'
			WHEN home_move_status = 'Post Home Move' 	AND DATEDIFF(dd, effective_from_dt, getdate()) BETWEEN 61 AND 90 	THEN 'Post Home Move 61-90 days'
			WHEN home_move_status = 'None' 				AND DATEDIFF(dd, effective_from_dt, getdate()) BETWEEN 1 AND 30 	THEN 'Post Home Move 91-120 days'
			WHEN home_move_status = 'None'				AND DATEDIFF(dd, effective_from_dt, getdate()) BETWEEN > 30 		THEN 'None'
			ELSE 'Unknown' END AS home_move_status
INTO #movers
FROM (SELECT *, rank() OVER( PARTITION BY account_number ORDER BY effective_from_dt DESC , dw_lasT_modified_dt DESC ) AS rankk
             FROM  CUST_HOME_MOVE_STATUS_HIST ) as b
WHERE rankk = 1 AND effective_from_dt > DATEADD(dd, -120, GETDATE())
COMMIT 
CREATE HG INDEX id1 ON #movers(account_number)
COMMIT 
UPDATE ####THETABLE#### 							------- Replace by adsmart master table 
		SET SKY_GENERATED_HOME_MOVER = COALESCE (home_move_status, 'Unknown')
FROM ####THETABLE####  as a 						------- Replace by adsmart master table 
JOIN #movers as b ON a.account_number = b.account_number

DROP TABLE #movers
COMMIT 