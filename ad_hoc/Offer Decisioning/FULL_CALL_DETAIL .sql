/*
	DECLARE @start_dt 	DATE 
	DECLARE @end_dt 	DATE 
	SET @start_dt 	= '2014-02-15'
	SET @end_dt 	= '2017-03-16' 
*/

/* *******************************************************************
                                                FULL_CALL_DETAIL 
********************************************************************* */
 
CREATE OR REPLACE VIEW MCKINSEY_FULL_CALL_DETAIL AS 
SELECT
   account_number
  ,call_date
  ,initial_sct_grouping
  ,initial_working_location
  ,final_working_location
  ,final_sct_grouping
  ,start_date_time
  ,total_transfers
FROM calls_details
WHERE call_date BETWEEN  '2014-02-15' AND '2017-03-16' 


COMMIT 
GRANT SELECT ON MCKINSEY_FULL_CALL_DETAIL TO vespa_group_low_security, rko04, citeam		

GO

