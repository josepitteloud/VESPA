/*
V321 - Capping calibration

This is a simple query that calculates the minute-by-minute weighted Sky consumption based on BARB data.

*/

-------------------
-- Create time base
-------------------
DROP TABLE  #MINUTES;
SELECT  ROW_NUM AS  TIME_MIN
INTO    #MINUTES
FROM    SA_ROWGENERATOR(0,1439)
;
CREATE UNIQUE LF INDEX U_LF_IDX_1 ON #MINUTES(TIME_MIN);



DROP TABLE  #UTC;
SELECT
        UTC_DAY_DATE
    ,   DATEADD(MINUTE,MINS.TIME_MIN,CAST(UTC_DAY_DATE AS TIMESTAMP))   AS  UTC_DATEHOURMIN
    ,   DATEADD (       HOUR
                    ,   DATEPART(HOUR,UTC_DATEHOURMIN)
                    ,   CAST(DATE(UTC_DATEHOURMIN) AS TIMESTAMP)
                )                                                       AS  UTC_DATEHOUR
INTO    #UTC
FROM
                sk_prod.VIQ_DATE  AS  CAL
    CROSS JOIN  #MINUTES    AS  MINS
WHERE   CAL.UTC_DAY_DATE    BETWEEN '2013-10-13'
                            AND     '2013-10-15'
GROUP BY
        UTC_DAY_DATE
    ,   UTC_DATEHOURMIN
    ,   UTC_DATEHOURMIN
ORDER BY
        UTC_DAY_DATE
    ,   UTC_DATEHOURMIN
    ,   UTC_DATEHOURMIN
;
CREATE DATE INDEX DATE_IDX_1 ON #UTC(UTC_DAY_DATE);
CREATE DTTM INDEX DTTM_IDX_1 ON #UTC(UTC_DATEHOURMIN);
CREATE DTTM INDEX DTTM_IDX_2 ON #UTC(UTC_DATEHOUR);



--------------------------------
-- BARB analysis by viewing time
--------------------------------
DROP TABLE  #BARB_WEIGHTED_MINS;
SELECT  --TOP 20  *
        UTC.UTC_DATEHOURMIN
    ,   UTC.UTC_DATEHOUR
	,	CASE
			WHEN 	BARB.Session_activity_type	IN	(1,13)			THEN	'LIVE'
			WHEN	BARB.Session_activity_type	IN	(4,5,11,14,15)	THEN	'PLAYBACK'
			ELSE															NULL
		END												AS	STREAM_TYPE
    ,   SUM(BARB.weighted_total_people_viewing)         AS  TOTAL_INDIVIDUAL_WEIGHTED_MINS
    ,   SUM(BARB.Household_Weight)                      AS  TOTAL_HOUSEHOLD_WEIGHED_MINS
INTO    #BARB_WEIGHTED_MINS
FROM
                #UTC                                            AS  UTC -- Create minute-by-minute time base

    -- Join time base onto BARB viewing data
    LEFT JOIN   ripolile.barb_daily_ind_prog_viewed_oct2013     AS  BARB    ON  UTC.UTC_DATEHOURMIN     BETWEEN BARB.Local_BARB_Instance_Start_Date_Time
                                                                                                        AND     BARB.Local_BARB_Instance_End_Date_Time

                                                                            AND BARB.Sky_STB_viewing      	=   'Y'
                                                                            AND BARB.Panel_or_guest_flag   	=   'Panel'
                                                                            AND BARB.Session_activity_type 	IN  (1,13,4,5,11,14,15)
GROUP BY
        UTC.UTC_DATEHOURMIN
    ,   UTC.UTC_DATEHOUR
	,	STREAM_TYPE
ORDER BY
        UTC.UTC_DATEHOURMIN
    ,   UTC.UTC_DATEHOUR
	,	STREAM_TYPE
;
CREATE DTTM INDEX DTTM_IDX_1 ON #BARB_WEIGHTED_MINS(UTC_DATEHOURMIN);




---------------------------------------------------------------
-- Now retain the hourly bins but aggregate by EVENT START HOUR
---------------------------------------------------------------

-- First, calculate the weighted minutes viewed PER BARB VIEWING EVENT
DROP TABLE  #BARB_EVENT_WEIGHTED_MINUTES;
SELECT
        BARB.Household_number
    ,   BARB.Local_TV_Event_Start_Date_Time -- use UTC field instead
	,	BARB.Local_TV_Event_End_Date_Time
	,	STREAM_TYPE
    ,   SUM(TOTAL_WEIGHTED_MINS)        AS  TOTAL_WEIGHTED_MINS
INTO    #BARB_EVENT_WEIGHTED_MINUTES
FROM

    (   -- Calculate the weighted viewing minutes per BARB viewing instance, BUT then aggregating them by their common EVENT start times per household
        SELECT
                Household_number
            ,   Local_TV_Event_Start_Date_Time
			,	Local_TV_Event_End_Date_Time
			,	CASE
					WHEN 	Session_activity_type	IN	(1,13)			THEN	'LIVE'
					WHEN	Session_activity_type	IN	(4,5,11,14,15)	THEN	'PLAYBACK'
					ELSE												NULL
				END															AS	STREAM_TYPE
            ,   DATEDIFF    (
                                    MINUTE
                                ,   Local_BARB_Instance_Start_Date_Time
                                ,   Local_BARB_Instance_End_Date_Time
                            )                                               AS  DT
            ,   SUM(Household_Weight   *   DT)                              AS  TOTAL_WEIGHTED_MINS
        FROM    ripolile.barb_daily_ind_prog_viewed_oct2013
        WHERE
                Sky_STB_viewing         =   'Y'
            AND Panel_or_guest_flag     =   'Panel'
            AND Session_activity_type   IN  (1,13,4,5,11,14,15)
        GROUP BY
                Household_number
            ,   Local_TV_Event_Start_Date_Time
			,	Local_TV_Event_End_Date_Time
			,	STREAM_TYPE
            ,   DT
    )   AS  BARB

GROUP BY
        BARB.Household_number
    ,   BARB.Local_TV_Event_Start_Date_Time
	,	BARB.Local_TV_Event_End_Date_Time
	,	STREAM_TYPE
ORDER BY
        BARB.Household_number
    ,   BARB.Local_TV_Event_Start_Date_Time
	,	BARB.Local_TV_Event_End_Date_Time
	,	STREAM_TYPE
;
CREATE HG INDEX HG_IDX_1 ON #BARB_EVENT_WEIGHTED_MINUTES(Household_number);
CREATE DTTM INDEX DTTM_IDX_1 ON #BARB_EVENT_WEIGHTED_MINUTES(Local_TV_Event_Start_Date_Time);




-- Now, join onto the previously generated time base. First aggregate by start minute.
DROP TABLE  #BARB_WEIGHTED_MINS_BY_EVENT_STARTMIN;
SELECT
        UTC.UTC_DATEHOURMIN
    ,   UTC.UTC_DATEHOUR
	,	BARB.STREAM_TYPE
    ,   SUM(BARB.TOTAL_WEIGHTED_MINS)                   AS  TOTAL_HOUSEHOLD_WEIGHED_MINS_BY_STARTMIN
INTO    #BARB_WEIGHTED_MINS_BY_EVENT_STARTMIN
FROM
                #UTC                                    AS  UTC -- Minute-by-minute time base

    -- Join time base onto BARB viewing data
    LEFT JOIN   #BARB_EVENT_WEIGHTED_MINUTES            AS  BARB       ON      UTC.UTC_DATEHOURMIN =   BARB.Local_TV_Event_Start_Date_Time
GROUP BY
        UTC.UTC_DATEHOURMIN
    ,   UTC.UTC_DATEHOUR
	,	BARB.STREAM_TYPE
ORDER BY
        UTC.UTC_DATEHOURMIN
    ,   UTC.UTC_DATEHOUR
	,	BARB.STREAM_TYPE
;
CREATE DTTM INDEX DTTM_IDX_1 ON #BARB_WEIGHTED_MINS_BY_EVENT_STARTMIN(UTC_DATEHOURMIN);
CREATE DTTM INDEX DTTM_IDX_2 ON #BARB_WEIGHTED_MINS_BY_EVENT_STARTMIN(UTC_DATEHOUR);




-- Join both analyses by viewing time and event start time into a single query output for pivot table-ing
SELECT
		DATEADD(HOUR,-1,UTC.UTC_DATEHOURMIN)			AS	UTC_DATEHOURMIN
	,	DATEADD(HOUR,-1,UTC.UTC_DATEHOUR)				AS	UTC_DATEHOUR
	,	S.STREAM_TYPE
	,	A.TOTAL_HOUSEHOLD_WEIGHED_MINS					AS  BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME
	,	B.TOTAL_HOUSEHOLD_WEIGHED_MINS_BY_STARTMIN		AS  BARB_WEIGHTED_MINUTES_BY_EVENT_START_TIME
FROM
                #UTC									AS  UTC
    CROSS JOIN  (
                    SELECT  CAST('LIVE' AS VARCHAR)			AS  STREAM_TYPE
                    UNION ALL
                    SELECT  CAST('PLAYBACK' AS VARCHAR)		AS  STREAM_TYPE
                )										AS  S
    LEFT JOIN   #BARB_WEIGHTED_MINS						AS	A	ON	UTC.UTC_DATEHOURMIN		=	A.UTC_DATEHOURMIN
                                                                AND UTC.UTC_DATEHOUR		=	A.UTC_DATEHOUR
                                                                AND	S.STREAM_TYPE			=	A.STREAM_TYPE
    LEFT JOIN	#BARB_WEIGHTED_MINS_BY_EVENT_STARTMIN	AS	B	ON	UTC.UTC_DATEHOURMIN		=	B.UTC_DATEHOURMIN
                                                                AND	UTC.UTC_DATEHOUR		=	B.UTC_DATEHOUR
                                                                AND	S.STREAM_TYPE			=	B.STREAM_TYPE
ORDER BY
		UTC.UTC_DATEHOURMIN
    ,	UTC.UTC_DATEHOUR
    ,	S.STREAM_TYPE
;








