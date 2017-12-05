SELECT 
	  sbo.account_number
	, a.subscriber_id 
  , COUNT(1) as Logs
	, sav.prod_active_broadband_package_desc
	, sav.current_package
  , sav.cb_key_household
INTO CONT_INIT_accounts_with_logs
FROM vespa_analysts.panel_data as a
JOIN vespa_analysts.vespa_single_box_view as sbo ON  a.subscriber_id = sbo.subscriber_id
JOIN  sk_prod.cust_single_account_view AS sav ON sbo.account_number = sav.account_number
WHERE a.data_received = 1
  AND a.panel = 12
	AND a.dt BETWEEN '2013-10-01' and '2013-11-30 23:59:59'
GROUP BY 
	  sbo.account_number
	, a.subscriber_id 
  , sav.prod_active_broadband_package_desc
	, sav.current_package
  , sav.cb_key_household