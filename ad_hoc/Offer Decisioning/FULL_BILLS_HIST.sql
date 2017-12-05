/*
	DECLARE @start_dt 	DATE 
	DECLARE @end_dt 	DATE 
	SET @start_dt 	= '2014-02-15'
	SET @end_dt 	= '2017-03-16' 
*/
/* *******************************************************************
                                                FULL_BILLS_HIST
********************************************************************* */
SELECT *
INTO MCKINSEY_FULL_BILLS_HIST
FROM (SELECT *
		, RANK() OVER (PARTITION BY account_number, year_due, month_due ORDER BY sequence_num DESC) AS Billrank
	FROM (SELECT datepart(year, payment_due_dt) AS year_due
				, datepart(month, payment_due_dt) AS month_due
				, cb.account_number
				, sequence_num
				, payment_due_dt
				, cb.total_new_charges AS amount_due
				, count(*) AS n
			FROM cust_bills cb
			INNER JOIN cust_subs_hist csh ON cb.account_number = csh.account_number
			WHERE cb.payment_due_dt >= '2014-02-15'
					AND csh.status_code IN ('AC', 'PC', 'AB', 'A', 'PT', 'CF', 'BCRQ', 'FBI', 'FBP') 
					AND csh.effective_from_dt <= cb.payment_due_dt 
					AND csh.effective_to_dt > cb.payment_due_dt 
					AND csh.subscription_sub_type NOT IN ('CLOUDWIFI', 'MCAFEE') 
					AND csh.account_sub_type IN ('Normal', '?')
			GROUP BY year_due
				, month_due
				, cb.account_number
				, sequence_num
				, payment_due_dt
				, amount_due) 		AS bills
	) bills_ranked
LEFT JOIN sky_calendar b ON bills_ranked.payment_due_dt = b.calendar_date
WHERE billrank = 1



COMMIT 
GRANT SELECT ON MCKINSEY_FULL_BILLS_HIST TO vespa_group_low_security, rko04, citeam		
---643063725 Row(s) affected


GO 

