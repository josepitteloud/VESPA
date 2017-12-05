/*
	DECLARE @start_dt 	DATE 
	DECLARE @end_dt 	DATE 
	SET @start_dt 	= '2014-02-15'
	SET @end_dt 	= '2017-03-16' 
*/
/* *******************************************************************
                                                FULL_BOX_HIST
********************************************************************* */
CREATE OR REPLACE VIEW MCKINSEY_FULL_BOX_HIST AS 
SELECT account_number
	, created_dt
	, x_model_number
	, x_manufacturer
	, x_box_type
	, box_installed_dt 
	, box_replaced_dt
FROM cust_set_top_box AS a
WHERE box_replaced_dt >= '2014-02-15'
GROUP BY account_number
	, created_dt
	, x_model_number
	, x_manufacturer
	, x_box_type
	, box_installed_dt 
	, box_replaced_dt
	

COMMIT 
GRANT SELECT ON MCKINSEY_FULL_BOX_HIST TO vespa_group_low_security, rko04, citeam		

GO 

