-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
--	Netezza queries for Ethan Vespa Dashboard																														---
--	@http://sp-department.bskyb.com/sites/IQSKY/SIG/Insight%20Collation%20Documents/01%20Analysis%20Requests/V246%20-%20Project%20Ethan/Ethan_Vespa_Dashboard.xlsx	---
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------


---------------------------------------------------------------------------------------------------------------------------------------------------------
--	Calculate volume of accounts, subscribers and devices by DTH viewing event day, taking into account days in which there been no activity (empty logs)
---------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT
		CAL.DAY_PK	AS	DT
	,	CASE	SUBSTR(DEVICE_ID,3,1)
			WHEN	'B'	THEN	'Sky Q Silver'
			WHEN	'C'	THEN	'Sky Q'
			WHEN	'D'	THEN	'Sky Q Mini'
			ELSE				NULL
		END										AS	DEVICE_TYPE
	,	EMP.EMPTY_LOG_DT_FLAG
	,	COUNT(DISTINCT EMP.ACCOUNT_NUMBER)		AS	ACCOUNTS
	,	COUNT(DISTINCT EMP.SCMS_SUBSCRIBER_ID)	AS	SUBSCRIBERS
	,	COUNT(DISTINCT EMP.DEVICE_ID)			AS	DEVICES
	,	SUM(EMP.EVENTS)							AS	EVENTS
FROM
				rocket_prepare..DATE_DIM							AS	CAL
	LEFT JOIN	(	-- Identify empty-log days using this join/subquery (essentially flagging empty-log events that encompass entire days)
					SELECT
							SCMS_SUBSCRIBER_ID
						,	DEVICE_ID
						,	ACCOUNT_NUMBER
						,	DTH_VIEWING_EVENT_DAY
						,	MIN	(
									CASE
										WHEN	(
														EVENT_ACTION			=		'Empty Log'
													AND	DTH_VIEWING_EVENT_DAY	BETWEEN	EVENT_START_DATETIME
																				AND		EVENT_END_DATETIME
												)																	THEN	1
																													ELSE	0
									END
								)								AS	EMPTY_LOG_DT_FLAG
						,	COUNT(1)	AS	EVENTS		-- This actually includes "Empty Log" as an event
					FROM	DIS_REFERENCE..FINAL_DTH_VIEWING_EVENT_HISTORY
					WHERE
							PANEL_ID_REPORTED		=		15
						AND	DEVICE_ID				LIKE	'32%'
						AND	DTH_VIEWING_EVENT_DAY	BETWEEN	DATE(NOW())	-	61
													AND		DATE(NOW())	-	1
					GROUP BY
							SCMS_SUBSCRIBER_ID
						,	DEVICE_ID
						,	ACCOUNT_NUMBER
						,	DTH_VIEWING_EVENT_DAY
--					limit	100
				)													AS	EMP		ON	CAL.DAY_PK	=	EMP.DTH_VIEWING_EVENT_DAY
WHERE CAL.DAY_PK	BETWEEN	DATE(NOW())	-	61
					AND		DATE(NOW())	-	1
GROUP BY
		CAL.DAY_PK
	,	DEVICE_TYPE
	,	EMP.EMPTY_LOG_DT_FLAG
ORDER BY
		CAL.DAY_PK
	,	DEVICE_TYPE
	,	EMP.EMPTY_LOG_DT_FLAG
--LIMIT	1000
;




---------------------------------------------------------------------------------------
--	Calculate total and average (mean and median) consumption per DTH viewing event day
---------------------------------------------------------------------------------------
SELECT
		DT
	,	A.DEVICE_TYPE
	,	EVENT_CATEGORY
	,	EVENT_SUB_CATEGORY
	,	EVENT_SUB_TYPE
	,	COUNT(DISTINCT ACCOUNT_NUMBER)										AS	ACCOUNTS
	,	COUNT(DISTINCT SCMS_SUBSCRIBER_ID)									AS	SUBSCRIBERS
	,	COUNT(1)															AS	DEVICES
	,	SUM(CAPPED_DURATION_HOURS)											AS	TOTAL_DURATION_HOURS
	,	1.0	* TOTAL_DURATION_HOURS / cast(DEVICES as int)					AS	AVG_DURATION_PER_DEVICE_HOURS_WITHIN_GROUP
	,	PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CAPPED_DURATION_HOURS)	AS	MEDIAN_DURATION_PER_DEVICE_HOURS_WITHIN_GROUP
	,	TOTAL_DURATION_HOURS / EMP.VIEWING_DEVICES							AS	AVG_DURATION_PER_DEVICE_HOURS
FROM
				(
					SELECT
							A.ACCOUNT_NUMBER
						,	A.SCMS_SUBSCRIBER_ID
						,	A.DEVICE_ID
						,	CASE	SUBSTR(A.DEVICE_ID,3,1)
								WHEN	'B'	THEN	'Sky Q Silver'
								WHEN	'C'	THEN	'Sky Q'
								WHEN	'D'	THEN	'Sky Q Mini'
								ELSE				NULL
							END										AS	DEVICE_TYPE
						,	A.DTH_VIEWING_EVENT_DAY				AS	DT
						,	A.EVENT_CATEGORY
						,	A.EVENT_SUB_CATEGORY
						,	A.EVENT_SUB_TYPE
						,	SUM(B.EVENT_END_CAPPED_SECONDS)	/ 3600.0	AS	CAPPED_DURATION_HOURS
						,	count(1)
					FROM
									DIS_REFERENCE..FINAL_DTH_VIEWING_EVENT_HISTORY	A
						INNER JOIN	DIS_REFERENCE..FINAL_CAPPED_EVENTS_HISTORY		B		ON	A.DTH_VIEWING_EVENT_ID	=	B.DTH_VIEWING_EVENT_ID
					WHERE
							A.PANEL_ID_REPORTED			=	15
						AND	A.DEVICE_ID					LIKE	'32%'
						AND	A.DTH_VIEWING_EVENT_DAY			BETWEEN	DATE(NOW())	-	61
															AND		DATE(NOW())	-	1
					GROUP BY
							A.ACCOUNT_NUMBER
						,	A.SCMS_SUBSCRIBER_ID
						,	A.DEVICE_ID
						,	DEVICE_TYPE
						,	DT
						,	A.EVENT_CATEGORY
						,	A.EVENT_SUB_CATEGORY
						,	A.EVENT_SUB_TYPE
--					LIMIT	1000
				)	AS	A
	LEFT JOIN	(	--	Count number of viewing devices per device group and day
					SELECT
							DTH_VIEWING_EVENT_DAY
						,	CASE	SUBSTR(DEVICE_ID,3,1)
								WHEN	'B'	THEN	'Sky Q Silver'
								WHEN	'C'	THEN	'Sky Q'
								WHEN	'D'	THEN	'Sky Q Mini'
								ELSE				NULL
							END										AS	DEVICE_TYPE
						,	SUM	(
									CASE	EMPTY_LOG_DT_FLAG
										WHEN	1	THEN	0
										ELSE				1
									END
								)	AS		VIEWING_DEVICES
					FROM
						(
							-- Identify empty-log days using this join/subquery (essentially flagging empty-log events that encompass entire days)
							SELECT
									SCMS_SUBSCRIBER_ID
								,	DEVICE_ID
								,	ACCOUNT_NUMBER
								,	DTH_VIEWING_EVENT_DAY
								,	MIN	(
											CASE
												WHEN	(
																EVENT_ACTION			=		'Empty Log'
															AND	DTH_VIEWING_EVENT_DAY	BETWEEN	EVENT_START_DATETIME
																						AND		EVENT_END_DATETIME
														)																	THEN	1
																															ELSE	0
											END
										)								AS	EMPTY_LOG_DT_FLAG
								,	COUNT(1)	AS	EVENTS		-- This actually includes "Empty Log" as an event
							FROM	DIS_REFERENCE..FINAL_DTH_VIEWING_EVENT_HISTORY
							WHERE
									PANEL_ID_REPORTED		=		15
								AND	DEVICE_ID				LIKE	'32%'
								AND	DTH_VIEWING_EVENT_DAY	BETWEEN	DATE(NOW())	-	61
															AND		DATE(NOW())	-	1
							GROUP BY
									SCMS_SUBSCRIBER_ID
								,	DEVICE_ID
								,	ACCOUNT_NUMBER
								,	DTH_VIEWING_EVENT_DAY
--							limit	1000
						)	AS	A
					GROUP BY
							DTH_VIEWING_EVENT_DAY
						,	DEVICE_TYPE
--					LIMIT	1000
				)													AS	EMP		ON	A.DT			=	EMP.DTH_VIEWING_EVENT_DAY
																				AND	A.DEVICE_TYPE	=	EMP.DEVICE_TYPE
--LIMIT	1000
GROUP BY
		A.DT
	,	A.DEVICE_TYPE
	,	A.EVENT_CATEGORY
	,	A.EVENT_SUB_CATEGORY
	,	A.EVENT_SUB_TYPE
	,	EMP.VIEWING_DEVICES
ORDER BY
		A.DT
	,	A.DEVICE_TYPE
	,	A.EVENT_CATEGORY
	,	A.EVENT_SUB_CATEGORY
	,	A.EVENT_SUB_TYPE
;





/*
-------------------------------------------------------------
--	Get accounts, subscribers and devices by log received day
-------------------------------------------------------------
SELECT
		DATE(LOG_RECEIVED_DATETIME)		AS	LOG_RECEIVED_DATE
	,	ACCOUNT_NUMBER
	,	SCMS_SUBSCRIBER_ID
	,	DEVICE_ID
	,	CASE	SUBSTR(DEVICE_ID,3,1)
			WHEN	'B'	THEN	'Sky Q Silver'
			WHEN	'C'	THEN	'Sky Q'
			WHEN	'D'	THEN	'Sky Q Mini'
			ELSE				NULL
		END										AS	DEVICE_TYPE
FROM	DIS_REFERENCE..FINAL_DTH_VIEWING_EVENT_HISTORY
WHERE
		PANEL_ID_REPORTED		=		15
	AND	DEVICE_ID				LIKE	'32%'
	AND	DTH_VIEWING_EVENT_DAY	BETWEEN	DATE(NOW())	-	31
								AND		DATE(NOW())	-	1
GROUP BY
		LOG_RECEIVED_DATE
	,	ACCOUNT_NUMBER
	,	SCMS_SUBSCRIBER_ID
	,	DEVICE_ID
	,	DEVICE_TYPE
ORDER BY
		LOG_RECEIVED_DATE
	,	ACCOUNT_NUMBER
	,	SCMS_SUBSCRIBER_ID
	,	DEVICE_ID
	,	DEVICE_TYPE
;

*/