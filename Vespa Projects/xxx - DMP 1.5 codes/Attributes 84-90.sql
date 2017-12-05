
/*
85	Customer Cancellation timeperiods
86	BB End Date
87	Skyfibre_estimated_enabled_date
88	DTV Package ----------- MAPPING!!
89	Primary Box Description
90	Customer Tenure
*/

----------------------------------------------------------
------------------ 85	Customer Cancellation timeperiod
----------------------------------------------------------
SELECT account_number
	, acct_latest_cancellation_attempt_dt
	, row_number() OVER (
		PARTITION BY account_number ORDER BY acct_latest_cancellation_attempt_dt DESC
		) AS rankk
INTO temp_Customer_Cancellation_timeperiods
FROM Cust_single_account_view
WHERE acct_latest_cancellation_attempt_dt IS NOT NULL

UPDATE ###DMP_TABLE### a
    SET Customer_Cancellation_timeperiods = CASE WHEN datediff(day, acct_latest_cancellation_attempt_dt, getdate()) BETWEEN 0 	AND 30 THEN '0 - 30 days back' 
												WHEN datediff(day, acct_latest_cancellation_attempt_dt, getdate()) BETWEEN 31	AND 60 THEN '31 - 60 days back' 
												WHEN datediff(day, acct_latest_cancellation_attempt_dt, getdate()) BETWEEN 61	AND 90 THEN '61 - 90 days back' 
												WHEN datediff(day, acct_latest_cancellation_attempt_dt, getdate()) BETWEEN 91	AND 120 THEN '91 - 120 days back' 
												WHEN datediff(day, acct_latest_cancellation_attempt_dt, getdate()) BETWEEN 121	AND 180 THEN '121 - 180 days back' 
												WHEN datediff(day, acct_latest_cancellation_attempt_dt, getdate()) BETWEEN 181	AND 270 THEN '181 - 270 days back' 
												WHEN datediff(day, acct_latest_cancellation_attempt_dt, getdate()) BETWEEN 271	AND 365 THEN '271 - 365 days back' 
												WHEN datediff(day, acct_latest_cancellation_attempt_dt, getdate()) > 365 THEN '1 year +' END
FROM temp_Customer_Cancellation_timeperiods b
WHERE a.account_number = b.account_number AND rankk = 1

DROP TABLE temp_Customer_Cancellation_timeperiods


----------------------------------------------------------
------------------ 86	BB End Date
----------------------------------------------------------

SELECT account_number
    , CASE WHEN DATEDIFF(day,max(effective_to_dt), today()) BETWEEN 0 AND 14 THEN '0 - 14 days'
        WHEN DATEDIFF(day,max(effective_to_dt), today()) BETWEEN 15 AND 30 THEN '15 - 30 days'
        WHEN DATEDIFF(day,max(effective_to_dt), today()) BETWEEN 31 AND 60 THEN '31 - 60 days'
        WHEN DATEDIFF(day,max(effective_to_dt), today()) BETWEEN 61 AND 90 THEN '61 - 90 days'
        WHEN DATEDIFF(day,max(effective_to_dt), today()) BETWEEN 91 AND 180 THEN '91 - 180 days'
        WHEN DATEDIFF(day,max(effective_to_dt), today()) BETWEEN 181 AND 334 THEN '181 - 334 days'
        WHEN DATEDIFF(day,max(effective_to_dt), today()) BETWEEN 335 AND 365 THEN '335 - 365 days'
        WHEN DATEDIFF(day,max(effective_to_dt), today()) > 365 THEN '1 year +' END AS BB_END_DT
    ,DATEDIFF(day,max(effective_to_dt), today())
INTO temp_BB_END_DATE
FROM cust_subs_hist AS CSH
INNER JOIN ###DMP_TABLE### b on csh.account_number = b.account_number
WHERE subscription_sub_type = 'Broadband DSL Line'
   AND csh.effective_from_dt <= GETDATE()
   AND csh.effective_to_dt < GETDATE()
   AND effective_from_dt != effective_to_dt
   AND status_code NOT IN ('AC','AB','PC')
GROUP BY account_number

UPDATE ###DMP_TABLE### a
    SET a.BB_END_DT = b.BB_END_DT
FROM temp_BB_END_DATE b
WHERE a.account_number = b.account_number

----------------------------------------------------------
------------------ 87	Skyfibre_estimated_enabled_date
----------------------------------------------------------

SELECT account_number
	, x_skyfibre_enabled_date
	, CASE WHEN datediff(day, x_skyfibre_enabled_date, getdate()) BETWEEN 0	AND - 14 THEN 'Fibre available in next 14 days' 
				WHEN datediff(day, x_skyfibre_enabled_date, getdate()) BETWEEN - 15	AND - 30 THEN 'Fibre available in next 30 days' 
				WHEN datediff(day, x_skyfibre_enabled_date, getdate()) BETWEEN - 31	AND - 60 THEN 'Fibre available in next 60 days   ' 
				WHEN datediff(day, x_skyfibre_enabled_date, getdate()) BETWEEN - 61	AND - 90 THEN 'Fibre available in next 90 days' 
				WHEN datediff(day, x_skyfibre_enabled_date, getdate()) < - 90 THEN 'Fibre planned in future 90 days+' 
				WHEN datediff(day, x_skyfibre_enabled_date, getdate()) > 0 THEN 'Fibre available now' END AS estimated_enabled_date
	, row_number() OVER (PARTITION BY account_number ORDER BY x_skyfibre_enabled_date DESC) AS rankk
INTO temp_BT_FIBRE_POSTCODE
FROM BT_FIBRE_POSTCODE a
JOIN adsmart b ON a.cb_address_postcode = b.cb_address_postcode
WHERE x_skyfibre_enabled_date IS NOT NULL

UPDATE ###DMP_TABLE### a
SET Skyfibre_estimated_enabled_date = estimated_enabled_date
FROM temp_BT_FIBRE_POSTCODE b
WHERE a.account_number = b.account_number
AND rankk = 1

----------------------------------------------------------
------------------ 88	DTV Package
----------------------------------------------------------

-- TO BE DECIDED!!!!!!
--- CUST_ENTITLEMENTS_HISTORY ------- cust_subs_hist

----------------------------------------------------------
------------------ 89	Primary Box Description
----------------------------------------------------------

-- TO BE DECIDED!!!!!!
--- CUST_SET_TOP_BOX

UPDATE ###DMP_TABLE### a
SET PRIMARY_BOX_DESCRIPTION = COALESCE(CASE WHEN x_manufacturer = 'Sky (Amstrad)' THEN 'Sky' ELSE x_manufacturer end || ' ' || CASE WHEN x_box_type = 'Sky+HD' THEN 'HD' else x_box_type end
										, 'Unknown')
FROM CUST_SET_TOP_BOX b
WHERE a.account_number = b.account_number
AND x_manufacturer is not null
AND x_box_type is not null
AND active_box_flag = 'Y'

----------------------------------------------------------
------------------ 90	Customer Tenure
----------------------------------------------------------

SELECT account_number
	, start_dt
	, CASE WHEN datediff(day, acct_latest_cancellation_attempt_dt, getdate()) BETWEEN 0 	AND 14 THEN '0 - 14 days' 
		WHEN datediff(day, acct_latest_cancellation_attempt_dt, getdate()) BETWEEN 15	AND 30 THEN '15 - 30 days' 
		WHEN datediff(day, acct_latest_cancellation_attempt_dt, getdate()) BETWEEN 31	AND 60 THEN '31 - 60 days' 
		WHEN datediff(day, acct_latest_cancellation_attempt_dt, getdate()) BETWEEN 61	AND 90 THEN '61 - 90 days' 
		WHEN datediff(day, acct_latest_cancellation_attempt_dt, getdate()) BETWEEN 91	AND 180 THEN '91 - 180 days' 
		WHEN datediff(day, acct_latest_cancellation_attempt_dt, getdate()) BETWEEN 181	AND 334 THEN '181 - 334 days' 
		WHEN datediff(day, acct_latest_cancellation_attempt_dt, getdate()) BETWEEN 335	AND 364 THEN '335 - 364 days' 
		WHEN datediff(day, acct_latest_cancellation_attempt_dt, getdate()) > 364 THEN '1 year +' end as  Customer_Tenure
    , row_number() over (partition by account_number order by start_dt desc) as rankk
	into temp_cust_contract_agreements
    from cust_contract_agreements
where account_number is not null

UPDATE ###DMP_TABLE### a
SET a.Customer_Tenure = b.Customer_Tenure
FROM temp_cust_contract_agreements b
WHERE a.account_number = b.account_number
AND rankk = 1

DROP TABLE temp_cust_contract_agreements
















