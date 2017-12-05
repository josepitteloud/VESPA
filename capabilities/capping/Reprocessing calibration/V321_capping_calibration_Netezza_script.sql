SELECT        
		UTC_TIME.DAY_START
	,	SUM	(
				(
					CASE 
						WHEN	
									A.BARB_END_DATETIME		BETWEEN	DAY_START AND DAY_END 
								AND	A.BARB_START_DATETIME 	BETWEEN DAY_START AND DAY_END 
							THEN 	DATE_PART('HOUR',BARB_END_DATETIME - BARB_START_DATETIME) *60	+	DATE_PART('MINUTE',BARB_END_DATETIME - BARB_START_DATETIME)	+	1 
						WHEN 
									A.BARB_END_DATETIME 	BETWEEN DAY_START AND DAY_END 
								AND	A.BARB_START_DATETIME < DAY_START 
							THEN	DATE_PART('HOUR',BARB_END_DATETIME - DAY_START) *60	+	DATE_PART('MINUTE',BARB_END_DATETIME - DAY_START)	+	1
						WHEN
									A.BARB_END_DATETIME		>	DAY_END 
								AND	A.BARB_START_DATETIME	BETWEEN	DAY_START	AND	DAY_END
							THEN	DATE_PART('HOUR',DAY_END - BARB_START_DATETIME) *60	+	DATE_PART('MINUTE',DAY_END - BARB_START_DATETIME)	+	1
						ELSE		0
					END
				)	*	A.EVENT_WEIGHT
			)	AS	TOTAL_WEIGHTED_VIEWING              
FROM	
				(	-- Create time base
					SELECT 
							UTC_DAY_DATE + CAST('5 HOURS' AS INTERVAL) 			AS	DAY_START	-- Midnight of day
						,	UTC_DAY_DATE + CAST('1739 MINUTES' AS INTERVAL ) 	AS	DAY_END		-- 04:59:00
					FROM	SMI_DW..DATEHOUR_DIM
					WHERE	
							UTC_DAY_DATE 	BETWEEN 	'2014-01-01' 	-- Define desired date range
											AND 		'2014-03-31'
						AND UTC_TIME_HOURS 	= 	0		-- Get midnights
				)	UTC_TIME
		JOIN	(
					SELECT 
							BARB_SDH.UTC_DAY_DATE + BARB_STM.UTC_TIME 		AS	BARB_START_DATETIME
						,	BARB_ENDDH.UTC_DAY_DATE + BARB_ENDTM.UTC_TIME 	AS	BARB_END_DATETIME
						,	VPIF.WEIGHT_SCALED 								AS	EVENT_WEIGHT
					FROM	
									SMI_DW..VIEWING_PROGRAMME_INSTANCE_FACT AS	VPIF
						
						-- BARB START DATE AND TIME
						INNER JOIN	SMI_DW..DATEHOUR_DIM 					AS	BARB_SDH	ON 	VPIF.DK_BARB_MIN_START_DATEHOUR_DIM 	= 	BARB_SDH.PK_DATEHOUR_DIM
						INNER JOIN 	SMI_DW..TIME_DIM 						AS	BARB_STM	ON 	VPIF.DK_BARB_MIN_START_TIME_DIM 		= 	BARB_STM.PK_TIME_DIM
						
						-- BARB END DATE AND TIME
						INNER JOIN 	SMI_DW..DATEHOUR_DIM 					AS	BARB_ENDDH	ON 	VPIF.DK_BARB_MIN_END_DATEHOUR_DIM 		= 	BARB_ENDDH.PK_DATEHOUR_DIM
						INNER JOIN 	SMI_DW..TIME_DIM 						AS	BARB_ENDTM	ON	VPIF.DK_BARB_MIN_END_TIME_DIM 			= 	BARB_ENDTM.PK_TIME_DIM
						
						-- Needed to retrieve LIVE/RECORDED flag
						INNER JOIN 	SMI_DW..PLAYBACK_DIM 					AS	PD			ON 	VPIF.DK_PLAYBACK_DIM 					= 	PD.PK_PLAYBACK_DIM
					WHERE
							(
									VPIF.DK_BARB_MIN_START_DATEHOUR_DIM 	BETWEEN	2014010106 
																			AND 	2014040105
								OR 	VPIF.DK_BARB_MIN_END_DATEHOUR_DIM 		BETWEEN	2014010106 
																			AND 	2014040105
							)
						AND VPIF.WEIGHT_SCALED IS NOT NULL
						AND	PD.LIVE_OR_RECORDED = 'LIVE'
				)	A	
						-- ON	1 = 1	-- join on first column? start datetime?
						ON	UTC_TIME.DAY_START = A.BARB_START_DATETIME
WHERE	(
				A.BARB_END_DATETIME 	BETWEEN DAY_START AND DAY_END
			OR 	A.BARB_START_DATETIME 	BETWEEN DAY_START AND DAY_END
		)
GROUP BY	1
ORDER BY	1
;


/*	Martin's original code
--	Minute by minute summary – can be run over longer periods, but takes a while

SELECT UTC_TIME.UTC_BARB_MINUTE,
        SUM(CASE WHEN A.ACCOUNT_NUMBER IS NULL THEN 0 ELSE 1 END) AS NO_RECORDS ,
        SUM(CASE WHEN A.EVENT_WEIGHT IS NULL THEN 0 ELSE
                                        A.EVENT_WEIGHT END) AS TOTAL_LIVE_WEIGHT_EVENT, -- used in Viewer 360
        sum(CASE WHEN FSHH.WEIGHT_SCALED_VALUE IS NULL THEN 0
                                 ELSE FSHH.WEIGHT_SCALED_VALUE  END ) AS TOTAL_LIVE_WEIGHT_TE -- used in TE
FROM (
                SELECT CAST('2014-06-20' AS DATE) + UTC_TIME AS UTC_BARB_MINUTE
                FROM SMI_DW..TIME_DIM
                WHERE CLOCK_OFFSET_TYPE_ID = 1 AND DATE_PART('Second',UTC_TIME) = 0
                                AND UTC_BARB_MINUTE > '2014-06-20 04:59:00'
                UNION ALL
                SELECT CAST('2014-06-21' AS DATE) + UTC_TIME AS UTC_BARB_MINUTE
                FROM SMI_DW..TIME_DIM
                WHERE CLOCK_OFFSET_TYPE_ID = 1 AND DATE_PART('Second',UTC_TIME) = 0
                                AND UTC_BARB_MINUTE < '2014-06-21 05:00:00'
        ) UTC_TIME
LEFT JOIN
        (
        SELECT  DH1.UTC_DAY_DATE + TD1.UTC_TIME AS BARB_START_DATEHOUR,
                        DH2.UTC_DAY_DATE + TD2.UTC_TIME AS BARB_END_DATEHOUR,
                        WEIGHT_SCALED AS EVENT_WEIGHT,
                        BACD.ACCOUNT_NUMBER
        FROM SMI_DW..VIEWING_PROGRAMME_INSTANCE_FACT VPIF
        -- BARB START DATE AND TIME
        JOIN SMI_DW..DATEHOUR_DIM DH1
                ON VPIF.DK_BARB_MIN_START_DATEHOUR_DIM = DH1.PK_DATEHOUR_DIM
        JOIN SMI_DW..TIME_DIM TD1
                ON VPIF.DK_BARB_MIN_START_TIME_DIM = TD1.PK_TIME_DIM
        -- BARB END DATE AND TIME
        JOIN SMI_DW..DATEHOUR_DIM DH2
                ON VPIF.DK_BARB_MIN_END_DATEHOUR_DIM = DH2.PK_DATEHOUR_DIM
        JOIN SMI_DW..TIME_DIM TD2
                ON VPIF.DK_BARB_MIN_END_TIME_DIM = TD2.PK_TIME_DIM
        -- PICK UP THE ACCOUNT NUMBER
        JOIN MDS..BILLING_CUSTOMER_ACCOUNT_DIM BACD
                ON VPIF.DK_BILLING_CUSTOMER_ACCOUNT_DIM = BACD.PK_BILLING_CUSTOMER_ACCOUNT_DIM
        -- note the use of the playback field to determine whether the viewing is live
        JOIN SMI_DW..PLAYBACK_DIM PD
                ON VPIF.DK_PLAYBACK_DIM = PD.PK_PLAYBACK_DIM
        WHERE (DK_BARB_MIN_START_DATEHOUR_DIM BETWEEN 2014062005 AND 2014062104
                OR DK_BARB_MIN_END_DATEHOUR_DIM BETWEEN 2014062005 AND 2014062104)
                AND WEIGHT_SCALED IS NOT NULL
                AND PD.LIVE_OR_RECORDED = 'LIVE'
        ) A
ON UTC_TIME.UTC_BARB_MINUTE BETWEEN A.BARB_START_DATEHOUR AND A.BARB_END_DATEHOUR
LEFT JOIN DIS_REFERENCE..FINAL_SCALING_HOUSEHOLD_HISTORY FSHH
        ON A.ACCOUNT_NUMBER = FSHH.ACCOUNT_NUMBER
        AND CAST(UTC_TIME.UTC_BARB_MINUTE AS DATE) = FSHH.EVENT_START_DATE
GROUP BY UTC_BARB_MINUTE
ORDER BY 1

*/



------------------------------------------------
-- Create views to easily point to backup tables
------------------------------------------------

CREATE OR REPLACE VIEW V_CAPPING_CALIBRATION_VPIF
AS
	SELECT	*
	FROM
			SMI_DW..VIEWING_PROGRAMME_INSTANCE_FACT									--	Current 
--			SMI_DW..VIEWING_PROGRAMME_INSTANCE_FACT_OCT_TO_NOV11_2013					--	Original
--			SMI_DW..VIEWING_PROGRAMME_INSTANCE_FACT_CAPPING_OCT_2013_NEW_CODE_V2		-- 	EDM code update to include last events
--			SMI_DW..VIEWING_PROGRAMME_INSTANCE_FACT_CAPPING_OCT_2013_NEW_CODE_V3		--	First revision of thresholds in capping meta data table
;

CREATE OR REPLACE VIEW V_CAPPING_CALIBRATION_CAPPED_THRESHOLD_DIM
AS
	SELECT	*
	FROM
		SMI_DW..CAPPED_THRESHOLD_DIM									--	Current 
--		SMI_DW..CAPPED_THRESHOLD_DIM_OCT_TO_NOV11_2013					--	Original
--		SMI_DW..CAPPED_THRESHOLD_DIM_CAPPING_OCT_2013_NEW_CODE_V2		-- 	EDM code update to include last events
--		SMI_DW..CAPPED_THRESHOLD_DIM_CAPPING_OCT_2013_NEW_CODE_V3		--	First revision of thresholds in capping meta data table
;

CREATE OR REPLACE VIEW V_CAPPING_CALIBRATION_CAPPED_EVENTS_HISTORY
AS
	SELECT	*
	FROM
		DIS_REFERENCE..FINAL_CAPPED_EVENTS_HISTORY										--	Current
--		DIS_REFERENCE..FINAL_CAPPED_EVENTS_HISTORY_OCT_TO_NOV11_2013					--	Original
--		DIS_REFERENCE..FINAL_CAPPED_EVENTS_HISTORY_CAPPING_OCT_2013_NEW_CODE_V2			-- 	EDM code update to include last events
--		DIS_REFERENCE..FINAL_CAPPED_EVENTS_HISTORY_CAPPING_OCT_2013_NEW_CODE_V3			--	First revision of thresholds in capping meta data table
;

CREATE OR REPLACE VIEW V_CAPPING_CALIBRATION_FINAL_SCALING_HOUSEHOLD_HISTORY
AS
	SELECT	*
	FROM
		DIS_REFERENCE..FINAL_SCALING_HOUSEHOLD_HISTORY									--	Current
--		DIS_REFERENCE..FINAL_SCALING_HOUSEHOLD_HISTORY_OCT_TO_NOV11_2013				--	Original
--		DIS_REFERENCE..FINAL_SCALING_HOUSEHOLD_HISTORY_CAPPING_OCT_2013_NEW_CODE_V2		-- 	EDM code update to include last events
--		DIS_REFERENCE..FINAL_SCALING_HOUSEHOLD_HISTORY_CAPPING_OCT_2013_NEW_CODE_V3		--	First revision of thresholds in capping meta data table
;



---------------------------------------
-- Create time base from TIME_DIM table
---------------------------------------

SELECT	
		DD.DAY_DATE					
	,	DD.DAY_DATE + TD.UTC_TIME																	AS	DT_DATEHOURMINUTE_LO
	,	DT_DATEHOURMINUTE_LO + CAST('59 SECONDS' AS INTERVAL)										AS	DT_DATEHOURMINUTE_HI
	,	DD.DAY_DATE + TD.UTC_TIME																	AS	DT_DATEHOURMINUTE
	,	DD.DAY_DATE + CAST('''' || DATE_PART('HOUR',DT_DATEHOURMINUTE) || ' HOURS''' AS INTERVAL)	AS	DT_DATEHOUR
	,	DATE(DT_DATEHOURMINUTE - CAST('5 HOURS' AS INTERVAL))										AS	BARB_DATE
INTO	TEMP	UTC
FROM	
				SMI_DW..DATE_DIM	AS	DD
	CROSS JOIN	SMI_DW..TIME_DIM	AS	TD
WHERE	
		DD.DAY_DATE						BETWEEN '2013-10-13' 
										AND 	'2013-10-15'
	AND	TD.CLOCK_OFFSET_TYPE_ID			=		1
	AND	DATE_PART('Second',TD.UTC_TIME)	=		0			--	Filter for the start of each minute
--ORDER BY	2
;



-------------------------------
--	Aggregation by VIEWING time
-------------------------------

SELECT 
		UTC.DT_DATEHOURMINUTE_LO
	,	UTC.DT_DATEHOUR
	,	A.LIVE_OR_RECORDED
	,	SUM	(
				CASE 
					WHEN	A.ACCOUNT_NUMBER	IS NULL	THEN	0 
					ELSE										1 
				END
			)																			AS	NO_RECORDS
	,	SUM	(
				CASE
					WHEN	A.EVENT_WEIGHT	IS NULL THEN	0
					ELSE									A.EVENT_WEIGHT
				END
			)																			AS	TOTAL_LIVE_WEIGHT_EVENT -- used in Viewer 360
--	,	SUM	(	
--				CASE
--					WHEN	FSHH.WEIGHT_SCALED_VALUE	IS NULL	THEN	0
--					ELSE												FSHH.WEIGHT_SCALED_VALUE
--				END
--			)																			AS	TOTAL_LIVE_WEIGHT_TE -- used in TE
	,	COUNT(DISTINCT A.ACCOUNT_NUMBER)												AS	DISTINCT_ACCOUNTS
INTO	TEMP	VESPA_WEIGHTED_VIEWING_BY_VIEWING_TIME
FROM
				UTC															AS	UTC	-- Time base
	LEFT JOIN	(
					SELECT
							VPIF.DTH_VIEWING_EVENT_ID
						,	DH1.UTC_DAY_DATE + TD1.UTC_TIME					AS	BARB_START_DATEHOURMIN
						,	DH2.UTC_DAY_DATE + TD2.UTC_TIME					AS	BARB_END_DATEHOURMIN
						,	VPIF.WEIGHT_SCALED								AS	EVENT_WEIGHT
						,	BACD.ACCOUNT_NUMBER
						,	RANK() OVER	(	
											PARTITION BY 	VPIF.DTH_VIEWING_EVENT_ID 
											ORDER BY 		VPIF.AUDIT_TIMESTAMP_01 DESC	
										)									AS	RNK			-- Dedupe event records in VPIF
						-- ,	DTH.TX_DAY
						,	PD.LIVE_OR_RECORDED
					FROM
						-- View on SMI_DW..VIEWING_PROGRAMME_INSTANCE_FACT
								V_CAPPING_CALIBRATION_VPIF						AS	VPIF
					
						-- BARB START DATE AND TIMES
						JOIN	SMI_DW..DATEHOUR_DIM 							AS	DH1		ON	VPIF.DK_BARB_MIN_START_DATEHOUR_DIM		=	DH1.PK_DATEHOUR_DIM
						JOIN	SMI_DW..TIME_DIM								AS	TD1		ON	VPIF.DK_BARB_MIN_START_TIME_DIM			=	TD1.PK_TIME_DIM
					
						-- BARB END DATE AND TIMES
						JOIN	SMI_DW..DATEHOUR_DIM 							AS	DH2		ON	VPIF.DK_BARB_MIN_END_DATEHOUR_DIM	=	DH2.PK_DATEHOUR_DIM
						JOIN	SMI_DW..TIME_DIM 								AS	TD2		ON	VPIF.DK_BARB_MIN_END_TIME_DIM		=	TD2.PK_TIME_DIM
					
						-- PICK UP THE ACCOUNT NUMBERS
						JOIN	MDS..BILLING_CUSTOMER_ACCOUNT_DIM 				AS	BACD	ON	VPIF.DK_BILLING_CUSTOMER_ACCOUNT_DIM	=	BACD.PK_BILLING_CUSTOMER_ACCOUNT_DIM

						-- note the use of the playback field to determine whether the viewing is live
						JOIN	SMI_DW..PLAYBACK_DIM 							AS	PD		ON	VPIF.DK_PLAYBACK_DIM	=	PD.PK_PLAYBACK_DIM
																							AND	PD.LIVE_OR_RECORDED 	IN	('LIVE','RECORDED')
																				
/*						-- Additional joins that were added to bring in info on the last viewing event
						
						JOIN 	DIS_REFERENCE..FINAL_DTH_VIEWING_EVENT_HISTORY	AS	DTH		ON 	VPIF.DTH_VIEWING_EVENT_ID	=	DTH.DTH_VIEWING_EVENT_ID
--																							AND	DTH.RULE_LAST_EVENT_FLAG 	= 	1	-- Activate this filter to isolate for last events
						
						-- Get capping data using view on SMI_DW..CAPPED_THRESHOLD_DIM
						JOIN 	V_CAPPING_CALIBRATION_CAPPED_THRESHOLD_DIM		AS	CTD		ON	VPIF.DK_CAPPED_THRESHOLD_DIM			=	CTD.PK_CAPPED_THRESHOLD_DIM
--																							AND	CTD.CAPPED_THRESHOLD_MAX_CUTOFF_FLAG	=	1	-- Activate this filter to isolate for last events

						-- Get capping history data - ntiles, segments etc. using view on DIS_REFERENCE..FINAL_CAPPED_EVENTS_HISTORY
						JOIN	V_CAPPING_CALIBRATION_CAPPED_EVENTS_HISTORY		AS	CEH		ON 	VPIF.DTH_VIEWING_EVENT_ID	=	CEH.DTH_VIEWING_EVENT_ID
																							AND	CEH.NTILE_NUMBER IS NOT NULL	-- Required in the re-processing environment to drop duplicated records that have no ntile assigned
*/

					WHERE
							(
									VPIF.DK_BARB_MIN_START_DATEHOUR_DIM	BETWEEN	2013101306	AND	2013101505	--	>>>>>>>CHANGE THIS TARGET DATE IF REQUIRED<<<<<<<<
								OR	VPIF.DK_BARB_MIN_END_DATEHOUR_DIM	BETWEEN	2013101306	AND	2013101505	--	>>>>>>>CHANGE THIS TARGET DATE IF REQUIRED<<<<<<<<
							)
						AND	VPIF.WEIGHT_SCALED IS NOT NULL
					GROUP BY	-- group-by required for deduping duplicate DTH_VIEWING_EVENT_ID records in FINAL_DTH_VIEWING_HISTORY
							VPIF.DTH_VIEWING_EVENT_ID
						,	BARB_START_DATEHOURMIN
						,	BARB_END_DATEHOURMIN
						,	EVENT_WEIGHT
						,	BACD.ACCOUNT_NUMBER
						,	VPIF.AUDIT_TIMESTAMP_01
						-- ,	DTH.TX_DAY
						,	PD.LIVE_OR_RECORDED
				)												AS	A		ON	A.RNK	=	1	-- Dedupe events coming from VPIF
																			AND	(
																						UTC.DT_DATEHOURMINUTE_LO	BETWEEN	A.BARB_START_DATEHOURMIN
																													AND		A.BARB_END_DATEHOURMIN
																				)
/*
	-- View on DIS_REFERENCE..FINAL_SCALING_HOUSEHOLD_HISTORY
	LEFT JOIN	V_CAPPING_CALIBRATION_FINAL_SCALING_HOUSEHOLD_HISTORY		AS	FSHH	ON	A.ACCOUNT_NUMBER 							=	FSHH.ACCOUNT_NUMBER
																						AND	CAST(UTC.DT_DATEHOURMINUTE_LO AS DATE)		=	FSHH.EVENT_START_DATE
*/
GROUP BY
		UTC.DT_DATEHOURMINUTE_LO
	,	UTC.DT_DATEHOUR
	,	A.LIVE_OR_RECORDED
--ORDER BY
--		UTC.DT_DATEHOURMINUTE_LO
--	,	UTC.DT_DATEHOUR
;



-------------------------------------------------------
--	Aggregation by minute (or hour) on EVENT START TIME
-------------------------------------------------------

SELECT 
		UTC.DT_DATEHOURMINUTE_LO
	,	UTC.DT_DATEHOUR
	,	A.LIVE_OR_RECORDED
	,	SUM	(	
				CASE 
					WHEN	A.ACCOUNT_NUMBER	IS NULL	THEN	0 
					ELSE										1 
				END
			)																			AS	NO_RECORDS
	,	SUM	(
				CASE
					WHEN	A.EVENT_WEIGHT	IS NULL THEN	0
--					ELSE									A.EVENT_WEIGHT
					ELSE									A.EVENT_WEIGHT * CAST(EXTRACT(EPOCH FROM (A.CAPPED_EVENT_END_DATEHOURMIN - A.EVENT_START_DATEHOURMIN)) AS DOUBLE) / 60.0
				END
			)																			AS	TOTAL_LIVE_WEIGHT_EVENT -- used in Viewer 360
INTO	TEMP	VESPA_WEIGHTED_VIEWING_BY_EVENT_START_TIME
FROM
				UTC															AS	UTC	-- Time base
	LEFT JOIN	(
					SELECT
							VPIF.DTH_VIEWING_EVENT_ID
--						,	DH1.UTC_DAY_DATE + TD1.UTC_TIME					AS	BARB_START_DATEHOURMIN
--						,	DH2.UTC_DAY_DATE + TD2.UTC_TIME					AS	BARB_END_DATEHOURMIN
						,	VPIF.WEIGHT_SCALED								AS	EVENT_WEIGHT
						,	BACD.ACCOUNT_NUMBER
						,	DH3.UTC_DAY_DATE + TD3.UTC_TIME					AS	EVENT_START_DATEHOURMIN
						,	DH4.UTC_DAY_DATE + TD4.UTC_TIME					AS	CAPPED_EVENT_END_DATEHOURMIN
						,	RANK() OVER	(	
											PARTITION BY 	VPIF.DTH_VIEWING_EVENT_ID 
											ORDER BY 		VPIF.AUDIT_TIMESTAMP_01 DESC	
										)									AS	RNK			-- Dedupe event records in VPIF
--						,	DTH.TX_DAY
--						,	CEH.CAPPED_METADATA_KEY
--						,	CEH.NTILE_NUMBER
--						,	CEH.NTILE_EXISTS_FLAG
--						,	CEH.MAX_CUTOFF_FLAG
--						,	CEH.EVENT_END_CAPPED_MINUTES
						,	PD.LIVE_OR_RECORDED
					FROM
						-- View on SMI_DW..VIEWING_PROGRAMME_INSTANCE_FACT
								V_CAPPING_CALIBRATION_VPIF						AS	VPIF
					
						-- BARB START DATE AND TIMES
						JOIN	SMI_DW..DATEHOUR_DIM 							AS	DH1		ON	VPIF.DK_BARB_MIN_START_DATEHOUR_DIM		=	DH1.PK_DATEHOUR_DIM
						JOIN	SMI_DW..TIME_DIM								AS	TD1		ON	VPIF.DK_BARB_MIN_START_TIME_DIM			=	TD1.PK_TIME_DIM
					
						-- BARB END DATE AND TIMES
						JOIN	SMI_DW..DATEHOUR_DIM 							AS	DH2		ON	VPIF.DK_BARB_MIN_END_DATEHOUR_DIM	=	DH2.PK_DATEHOUR_DIM
						JOIN	SMI_DW..TIME_DIM 								AS	TD2		ON	VPIF.DK_BARB_MIN_END_TIME_DIM		=	TD2.PK_TIME_DIM
					
						-- BARB EVENT START DATE AND TIMES
						JOIN	SMI_DW..DATEHOUR_DIM 							AS	DH3		ON	VPIF.DK_EVENT_START_DATEHOUR_DIM	=	DH3.PK_DATEHOUR_DIM
						JOIN	SMI_DW..TIME_DIM 								AS	TD3		ON	VPIF.DK_EVENT_START_TIME_DIM		=	TD3.PK_TIME_DIM

						-- BARB EVENT END DATE AND TIMES
						JOIN	SMI_DW..DATEHOUR_DIM 							AS	DH4		ON	VPIF.DK_CAPPED_EVENT_END_TIME_DATEHOUR_DIM	=	DH4.PK_DATEHOUR_DIM
						JOIN	SMI_DW..TIME_DIM 								AS	TD4		ON	VPIF.DK_CAPPED_EVENT_END_TIME_DIM			=	TD4.PK_TIME_DIM

						-- PICK UP THE ACCOUNT NUMBERS
						JOIN	MDS..BILLING_CUSTOMER_ACCOUNT_DIM 				AS	BACD	ON	VPIF.DK_BILLING_CUSTOMER_ACCOUNT_DIM	=	BACD.PK_BILLING_CUSTOMER_ACCOUNT_DIM

						-- note the use of the playback field to determine whether the viewing is live
						JOIN	SMI_DW..PLAYBACK_DIM 							AS	PD		ON	VPIF.DK_PLAYBACK_DIM	=	PD.PK_PLAYBACK_DIM
																							AND	PD.LIVE_OR_RECORDED 	IN	('LIVE','RECORDED')
																				
/*						-- Additional joins that were added to bring in info on the last viewing event
						
						JOIN 	DIS_REFERENCE..FINAL_DTH_VIEWING_EVENT_HISTORY	AS	DTH		ON 	VPIF.DTH_VIEWING_EVENT_ID	=	DTH.DTH_VIEWING_EVENT_ID
--																							AND	DTH.RULE_LAST_EVENT_FLAG 	= 	1	-- Activate this filter to isolate for last events
						
						-- Get capping data using view on SMI_DW..CAPPED_THRESHOLD_DIM
						JOIN 	V_CAPPING_CALIBRATION_CAPPED_THRESHOLD_DIM		AS	CTD		ON	VPIF.DK_CAPPED_THRESHOLD_DIM			=	CTD.PK_CAPPED_THRESHOLD_DIM
--																							AND	CTD.CAPPED_THRESHOLD_MAX_CUTOFF_FLAG	=	1	-- Activate this filter to isolate for last events

						-- Get capping history data - ntiles, segments etc. using view on DIS_REFERENCE..FINAL_CAPPED_EVENTS_HISTORY
						JOIN	V_CAPPING_CALIBRATION_CAPPED_EVENTS_HISTORY		AS	CEH		ON 	VPIF.DTH_VIEWING_EVENT_ID	=	CEH.DTH_VIEWING_EVENT_ID
																							AND	CEH.NTILE_NUMBER IS NOT NULL	-- Required in the re-processing environment to drop duplicated records that have no ntile assigned
*/

					WHERE
							(
									VPIF.DK_BARB_MIN_START_DATEHOUR_DIM	BETWEEN	2013101306	AND	2013101505	--	>>>>>>>CHANGE THIS TARGET DATE IF REQUIRED<<<<<<<<
								OR	VPIF.DK_BARB_MIN_END_DATEHOUR_DIM	BETWEEN	2013101306	AND	2013101505	--	>>>>>>>CHANGE THIS TARGET DATE IF REQUIRED<<<<<<<<
							)
						AND	VPIF.WEIGHT_SCALED IS NOT NULL
					GROUP BY	-- group-by required for deduping duplicate DTH_VIEWING_EVENT_ID records in FINAL_DTH_VIEWING_HISTORY
							VPIF.DTH_VIEWING_EVENT_ID
--						,	BARB_START_DATEHOURMIN
--						,	BARB_END_DATEHOURMIN
						,	EVENT_WEIGHT
						,	BACD.ACCOUNT_NUMBER
						,	EVENT_START_DATEHOURMIN
						,	CAPPED_EVENT_END_DATEHOURMIN
						,	VPIF.AUDIT_TIMESTAMP_01
--						,	DTH.TX_DAY
--						,	CEH.CAPPED_METADATA_KEY
--						,	CEH.NTILE_NUMBER
--						,	CEH.NTILE_EXISTS_FLAG
--						,	CEH.MAX_CUTOFF_FLAG
--						,	CEH.EVENT_END_CAPPED_MINUTES
						,	PD.LIVE_OR_RECORDED
				)												AS	A		ON	A.RNK	=	1	-- Dedupe events coming from VPIF
																			AND	(
--																						UTC.DT_DATEHOURMINUTE	BETWEEN	A.BARB_START_DATEHOURMIN
--																												AND		A.BARB_END_DATEHOURMIN
																						A.EVENT_START_DATEHOURMIN	BETWEEN	UTC.DT_DATEHOURMINUTE_LO
																													AND		UTC.DT_DATEHOURMINUTE_HI
																				)
/*
	-- View on DIS_REFERENCE..FINAL_SCALING_HOUSEHOLD_HISTORY
	LEFT JOIN	V_CAPPING_CALIBRATION_FINAL_SCALING_HOUSEHOLD_HISTORY		AS	FSHH	ON	A.ACCOUNT_NUMBER 							=	FSHH.ACCOUNT_NUMBER
																						AND	CAST(UTC.DT_DATEHOURMINUTE_LO AS DATE)		=	FSHH.EVENT_START_DATE
*/
GROUP BY
		UTC.DT_DATEHOURMINUTE_LO
	,	UTC.DT_DATEHOUR
	,	A.LIVE_OR_RECORDED
--ORDER BY
--		UTC.DT_DATEHOURMINUTE_LO
--	,	UTC.DT_DATEHOUR
;



-----------------------------------------------------------------
-- Combine weights from viewing time and event start time results
-----------------------------------------------------------------

SELECT
		A.DT_DATEHOURMINUTE_LO			AS	DT_DATEHOURMINUTE
	,	A.DT_DATEHOUR
	,	LOR.LIVE_OR_RECORDED
	,	A.NO_RECORDS					AS	VESPA_RECORDS_BY_VIEWING_TIME
	,	A.TOTAL_LIVE_WEIGHT_EVENT		AS	VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME
	,	A.DISTINCT_ACCOUNTS				AS	VESPA_ACCOUNTS_BY_VIEWING_TIME
	,	B.NO_RECORDS					AS	VESPA_RECORDS_BY_EVENT_START_TIME
	,	B.TOTAL_LIVE_WEIGHT_EVENT		AS	VESPA_WEIGHTED_MINUTES_BY_EVENT_START_TIME
FROM
				UTC
	CROSS JOIN	(
					SELECT	'LIVE'		AS	LIVE_OR_RECORDED
					UNION ALL
					SELECT	'RECORDED'	AS	LIVE_OR_RECORDED
				)											AS	LOR
	LEFT JOIN	VESPA_WEIGHTED_VIEWING_BY_VIEWING_TIME		AS	A		ON		UTC.DT_DATEHOURMINUTE_LO	=	A.DT_DATEHOURMINUTE_LO
																		AND		UTC.DT_DATEHOUR				=	A.DT_DATEHOUR
																		AND		LOR.LIVE_OR_RECORDED		=	A.LIVE_OR_RECORDED
	LEFT JOIN	VESPA_WEIGHTED_VIEWING_BY_EVENT_START_TIME	AS	B		ON		UTC.DT_DATEHOURMINUTE_LO	=	B.DT_DATEHOURMINUTE_LO
																		AND		UTC.DT_DATEHOUR				=	B.DT_DATEHOUR
																		AND		LOR.LIVE_OR_RECORDED		=	B.LIVE_OR_RECORDED
ORDER BY
		A.DT_DATEHOURMINUTE_LO
	,	A.DT_DATEHOUR
	,	LOR.LIVE_OR_RECORDED
;



---------------------
-- Drop working views
---------------------
DROP VIEW V_CAPPING_CALIBRATION_VPIF;
DROP VIEW V_CAPPING_CALIBRATION_CAPPED_THRESHOLD_DIM;
DROP VIEW V_CAPPING_CALIBRATION_CAPPED_EVENTS_HISTORY;
DROP VIEW V_CAPPING_CALIBRATION_FINAL_SCALING_HOUSEHOLD_HISTORY;



