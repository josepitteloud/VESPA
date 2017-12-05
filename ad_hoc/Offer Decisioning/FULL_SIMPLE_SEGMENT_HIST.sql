/*
	DECLARE @start_dt 	DATE 
	DECLARE @end_dt 	DATE 
	SET @start_dt 	= '2014-02-15'
	SET @end_dt 	= '2017-03-16' 
*/

/* *******************************************************************
                        FULL_SIMPLE_SEGMENT_HIST
********************************************************************* */
CREATE OR REPLACE VIEW MCKINSEY_FULL_SIMPLE_SEGMENT_HIST AS 
SELECT account_number
	, segment
	, segment_lev2
	, observation_date
FROM simple_segments_history
WHERE observation_date >= DATEADD(MONTH, - 3, '2014-02-15')
GROUP BY account_number
	, segment
	, segment_lev2
	, observation_date
	
--181555154 Row(s) affected
COMMIT 
GRANT SELECT ON MCKINSEY_FULL_SIMPLE_SEGMENT_HIST TO vespa_group_low_security, rko04, citeam

GO 

