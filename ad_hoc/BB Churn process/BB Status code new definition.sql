
SELECT CSH.account_number
	  , status_code
	  ,end_date
	  , CASE 	
				WHEN status_code = 'AC' THEN 1
				WHEN status_code = 'BCRQ' THEN 2 
				WHEN status_code = 'PC' THEN 3 
				WHEN status_code = 'AB' THEN 4 
				ELSE 10 END Status_rank 
      ,rank() over(PARTITION BY base.account_number, end_date ORDER BY Status_rank , effective_from_datetime DESC, CSH.cb_row_id ASC) AS rank1  
	  
INTO #BB_status
FROM cust_subs_hist 						AS CSH
INNER JOIN CUST_FCAST_WEEKLY_BASE AS BASE  ON BASE.account_number = CSH.account_number
WHERE csh.subscription_sub_type = 'Broadband DSL Line'
AND   end_date BETWEEN effective_from_dt AND effective_to_dt
AND   effective_to_dt > effective_from_dt


COMMIT 

DELETE FROM  #BB_status  WHERE  rank1 <> 1
CREATE HG INDEX id1 	ON #BB_status (account_number) 
CREATE DATE INDEX id2 	ON #BB_status (end_date) 
COMMIT 

UPDATE CUST_FCAST_WEEKLY_BASE
SET New_BB_Status  =   status_code
FROM CUST_FCAST_WEEKLY_BASE 	AS base
INNER JOIN #BB_status 				AS hold	ON hold.account_number = base.account_number AND base.end_date = hold.end_date
