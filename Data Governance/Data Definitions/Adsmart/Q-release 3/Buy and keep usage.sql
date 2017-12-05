
/***************************************************************************************
 *                                                                                      *
 *                          SECTION 1 - Creating Adsmart dummy TABLE					*
 *                                                                                      *
 ***************************************************************************************/

SELECT account_number
	, COUNT (*) hits  
INTO #uk_buy_keep_accounts
FROM SKY_STORE_TRANSACTIONS
WHERE   product = 'EST'
	AND digital_state = 'COMPLETED' 
	AND order_date >= DATEADD (MONTH, -12, getdate())
GROUP BY account_number 

COMMIT
CREATE HG INDEX id1 ON #uk_buy_keep_accounts (account_number)
CREATE LF INDEX id2 ON #uk_buy_keep_accounts (hits)
COMMIT

UPDATE ADSMART
SET BUY_AND_KEEP_USAGE_OVER_LAST_12_MONTHS = COALESCE(CASE 	WHEN hits = 1 				THEN 'Yes, 1 in last 12 mths'
															WHEN hits BETWEEN 2 AND 4 	THEN 'Yes, 2-4 in last 12 mths'
															WHEN hits BETWEEN 5 AND 7 	THEN 'Yes, 5-7 in last 12 mths'
															WHEN hits  > 7 				THEN 'Yes, 7+ in last 12 mths'
															ELSE 'Never Bought' END 
															, 'Unknown')
FROM ADSMART AS a 
LEFT JOIN #uk_buy_keep_accounts AS b ON a.account_number = b.account_number 

DROP TABLE #uk_buy_keep_accounts
COMMIT

-----------------------------------------------------------------------------------------------------------------
/* `		QA
SELECT BUY_AND_KEEP_USAGE_OVER_LAST_12_MONTHS = COALESCE(CASE 	WHEN hits = 1 THEN 'Yes, 1 in last 12 mths'
															WHEN hits BETWEEN 2 AND 4 THEN 'Yes, 2-4 in last 12 mths'
															WHEN hits BETWEEN 5 AND 7 THEN 'Yes, 5-7 in last 12 mths'
															WHEN hits BETWEEN >7 THEN 'Yes, 7+ in last 12 mths'
															ELSE 'Never Bought' END 
															, 'Unknown')
																,count(*) hit
FROM adsmartables_ROI_Nov_2015 As a																
LEFT JOIN #uk_buy_keep_accounts AS b ON a.account_number = b.account_number
WHERE sky_base_universe LIKE 'Adsmartable with consent%'
GROUP BY BUY_AND_KEEP_USAGE_OVER_LAST_12_MONTHS

BUY_AND_KEEP_USAGE_OVER_LAST_12_MONTHS	hit
Never Bought							445445
Yes, 1 in last 12 mths					14140
Yes, 2-4 in last 12 mths				7344
Yes, 5-7 in last 12 mths				1345
Yes, 7+ in last 12 mths					892

*/