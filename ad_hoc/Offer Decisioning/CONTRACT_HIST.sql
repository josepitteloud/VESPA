/*
	DECLARE @start_dt 	DATE 
	DECLARE @end_dt 	DATE 
	SET @start_dt 	= '2014-02-15'
	SET @end_dt 	= '2017-03-16' 
*/
/* *******************************************************************
                                                CONTRACT_HIST
********************************************************************* */
CREATE OR REPLACE VIEW MCKINSEY_FULL_CONTRACT_HIST AS 
SELECT DISTINCT 
	  created_dt
	, DW_created_dt
	, dw_last_modified_dt
	, Start_dt
	, end_dt
	, end_dt_calc
	, last_modified_dt
	, min_term_months
	, subscription_id
	, subscription_type
	, account_number
	, agreement_item_type_code
	, created_by_id
	
FROM cust_contract_agreements
WHERE created_dt BETWEEN '2014-02-15' AND '2017-03-16'

COMMIT 
GRANT SELECT ON MCKINSEY_FULL_CONTRACT_HIST TO vespa_group_low_security, rko04, citeam

GO

