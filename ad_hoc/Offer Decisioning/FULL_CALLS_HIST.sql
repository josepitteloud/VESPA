/*
	DECLARE @start_dt 	DATE 
	DECLARE @end_dt 	DATE 
	SET @start_dt 	= '2014-02-15'
	SET @end_dt 	= '2017-03-16' 
*/
/* *******************************************************************
					FULL_CALLS_HIST 
********************************************************************* */
CREATE OR REPLACE VIEW MCKINSEY_FULL_CALLS_HIST AS 

SELECT a.account_number
	, cast(created_date AS DATE) AS Event_Date
	, count(*) AS Calls
	, 0 AS saves
	, 'IC' AS TypeOfEvent
--    into MCKINSEY_FULL_CALLS_HIST_NEW_1
FROM cust_contact AS a
WHERE cast(created_date AS DATE) BETWEEN  '2014-02-15' AND '2017-03-16' 
	AND contact_channel = 'I PHONE COMMUNICATION' 
	AND contact_grouping_identity IS NOT NULL
GROUP BY a.account_number
		, event_date
UNION
SELECT a.account_number
	, event_dt AS Event_Date
	, count(*) AS Calls
	, sum(saves) AS saves
	, 'TA' AS TypeOfEvent
FROM view_cust_calls_hist a
WHERE typeofevent = 'TA' 
	AND event_dt BETWEEN  '2014-02-15' AND '2017-03-16' 
GROUP BY a.account_number
	, event_date
UNION
SELECT a.account_number
	, event_dt AS Event_Date
	, count(*) AS Calls
	, sum(saves) AS saves
	, 'PAT' AS TypeOfEvent
FROM view_cust_calls_hist a
WHERE typeofevent = 'PAT' 
	AND event_dt  BETWEEN  '2014-02-15' AND '2017-03-16' 
GROUP BY a.account_number
	, event_date
	
	
COMMIT 
GRANT SELECT ON MCKINSEY_FULL_CALLS_HIST TO vespa_group_low_security, rko04, citeam

GO

