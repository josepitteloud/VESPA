/*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES

-----------------------------------------------------------------------------------

**Project Name:                         Capping Calibration Automation
**Analysts:                             Leonardo Ripoli  (Leonardo.Ripoli@sky.uk)
                                        Jonathan Green   (Jonathan.Green2@sky.uk)
                                        
**Lead(s):                              Hoi Yu Tang (hoiyu.tang@sky.uk)
**Stakeholder:                          Jose Loureda
**Project Code (Insight Collation):     V306
**SharePoint Folder:                    http://sp-department.bskyb.com/sites/IQSKY/SIG/Insight%20Collation%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FIQSKY%2FSIG%2FInsight%20Collation%20Documents%2F01%20Analysis%20Requests%2FV306%20-%20Foundation%20-%20Platform%20Maintenance%2FPhase%202%2FCapping%20Calibration%20Automation

**Business Brief:

Capping Calibration Automation ultimately aims to deliver improved Vespa viewing consumption through monthly alignment to BARB reference data. 
The Capping algorithm was developed in order to truncate the length of viewing events where there is no actual audience present (e.g. TV set off, but STB on and registering viewing).

Up to this point, the parameters and thresholds that feed into the Capping algorithm have been largely static since the time of the original algorithm development by IQ and ultimate implementation within EDM. 
Although a recent capping calibration exercise addressed exactly the issue realigning Vespa viewing to BARB, this was a highly manual process that required much resource to construct and perform the analyses and assessment. 
Capping Calibration Automation will adopt those calculations and fundamental approach, but delivered as a self-contained and automated process that calculates the best set of capping thresholds and parameters in IQ/Olive 
for ingest within into the EDM production environment

This project will also aim to increase the precision over which the Capping thresholds and parameters operate. For example, the current parameters are defined at the day-part level, 
each of which spans a number of hours in the day. The intention is to explore the possibility of redefining the parameters at the hourly level in order to give greater control over the alignment process against BARB. 
In theory, there should be little to no adjustment required to the actual flow of the Capping algorithm since the thresholds and parameters are contained in an external lookup table rather than being hard-coded in the SQL.




**Module:                               V306_CP2_M06_BARB_Minutes

This is a simple query that calculates the minute-by-minute weighted Sky consumption based on BARB data.

*/

create or replace procedure V306_CP2_M06_BARB_Minutes
as begin

	execute M00_2_output_to_logger '@ M06 : V306_CP2_M06_BARB_Minutes'
	COMMIT
	
	
	--------------------------------
	-- BARB analysis by viewing time
	--------------------------------

	execute M00_2_output_to_logger '@ M06 : BARB consumption by viewing time'
	COMMIT

	-- if exists	(
					-- select	1
					-- from	sysobjects
					-- where
							-- [name]			=	'BARB_WEIGHTED_MINS'
						-- and uid				=	user_id()
						-- and	upper([type])	=	'U'
				-- )
		-- drop table BARB_WEIGHTED_MINS
	-- commit
	
	SELECT  --TOP 20  *
			UTC.UTC_DATEHOURMIN
		,   UTC.UTC_DATEHOUR
		,	CASE
				WHEN    BARB.Session_activity_type      IN      (1,13)              THEN    'LIVE'
				WHEN    BARB.Session_activity_type      IN      (4,5,11,14,15)		THEN    'PLAYBACK'
				ELSE                                                              	NULL
			END												AS      STREAM_TYPE
		,   SUM(BARB.weighted_total_people_viewing)         AS  TOTAL_INDIVIDUAL_WEIGHTED_MINS
		,   SUM(BARB.Household_Weight)                      AS  TOTAL_HOUSEHOLD_WEIGHTED_MINS
	INTO    #BARB_WEIGHTED_MINS
	FROM
					-- Minute-by-minute time base
					UTC												AS	UTC
		
					-- Join time base onto BARB viewing data
		LEFT JOIN   barb_daily_ind_prog_viewed	AS	BARB	ON	UTC.UTC_DATEHOURMIN		BETWEEN BARB.UTC_BARB_Instance_Start_Date_Time
																						AND     BARB.UTC_BARB_Instance_End_Date_Time
															AND BARB.Sky_STB_viewing            =   'Y'
															AND BARB.Panel_or_guest_flag        =   'Panel'
															AND BARB.Session_activity_type      IN  (1,13,4,5,11,14,15)
	GROUP BY
			UTC.UTC_DATEHOURMIN
		,	UTC.UTC_DATEHOUR
		,	STREAM_TYPE
	-- ORDER BY
			-- UTC.UTC_DATEHOURMIN
		-- ,	UTC.UTC_DATEHOUR
		-- ,	STREAM_TYPE
	COMMIT

	CREATE DTTM INDEX DTTM_IDX_1 ON #BARB_WEIGHTED_MINS(UTC_DATEHOURMIN)	COMMIT
	CREATE DTTM INDEX DTTM_IDX_2 ON #BARB_WEIGHTED_MINS(UTC_DATEHOUR)	COMMIT



	---------------------------------------------------------------
	-- Now retain the hourly bins but aggregate by EVENT START HOUR
	---------------------------------------------------------------

	execute M00_2_output_to_logger '@ M06 : BARB consumption by viewing start time'
	COMMIT

	-- if exists	(
					-- select	1
					-- from	sysobjects
					-- where
							-- [name]			=	'BARB_EVENT_WEIGHTED_MINUTES'
						-- and uid				=	user_id()
						-- and	upper([type])	=	'U'
				-- )
		-- drop table BARB_EVENT_WEIGHTED_MINUTES
	-- commit

	-- First, calculate the weighted minutes viewed PER BARB VIEWING EVENT
	SELECT
			BARB.Household_number
		,   BARB.UTC_TV_Event_Start_Date_Time -- use UTC field instead
		,	BARB.UTC_TV_Event_End_Date_Time
		,	STREAM_TYPE
		,	SUM(TOTAL_WEIGHTED_MINS)        AS  TOTAL_WEIGHTED_MINS
	INTO	#BARB_EVENT_WEIGHTED_MINUTES
	FROM
			(   -- Calculate the weighted viewing minutes per BARB viewing instance, BUT then aggregating them by their common EVENT start times per household
				SELECT
						Household_number
					,	UTC_TV_Event_Start_Date_Time
					,	UTC_TV_Event_End_Date_Time
					,	CASE
							WHEN	Session_activity_type IN (1,13)				THEN	'LIVE'
							WHEN	Session_activity_type IN (4,5,11,14,15)		THEN	'PLAYBACK'
							ELSE														NULL
						END																					AS      STREAM_TYPE
					,	DATEDIFF	(
											MINUTE
										,	UTC_BARB_Instance_Start_Date_Time
										,	UTC_BARB_Instance_End_Date_Time
									)																		AS  DT
					,	SUM(Household_Weight * DT)															AS  TOTAL_WEIGHTED_MINS
				FROM	barb_daily_ind_prog_viewed
				WHERE
						Sky_STB_viewing			=	'Y'
					AND	Panel_or_guest_flag		=	'Panel'
					AND	Session_activity_type	IN	(1,13,4,5,11,14,15)
				GROUP BY
						Household_number
					,	UTC_TV_Event_Start_Date_Time
					,	UTC_TV_Event_End_Date_Time
					,	STREAM_TYPE
					,   DT
			)	AS	BARB
	GROUP BY
			BARB.Household_number
		,	BARB.UTC_TV_Event_Start_Date_Time
		,	BARB.UTC_TV_Event_End_Date_Time
		,	STREAM_TYPE
	-- ORDER BY
			-- BARB.Household_number
		-- ,	BARB.UTC_TV_Event_Start_Date_Time
		-- ,	BARB.UTC_TV_Event_End_Date_Time
		-- ,	STREAM_TYPE
	COMMIT

	CREATE HG INDEX HG_IDX_1 ON #BARB_EVENT_WEIGHTED_MINUTES(Household_number)	COMMIT
	CREATE DTTM INDEX DTTM_IDX_1 ON #BARB_EVENT_WEIGHTED_MINUTES(UTC_TV_Event_Start_Date_Time)	COMMIT


	
	-- Now, join onto the previously generated time base. First aggregate by start minute.
	-- if exists	(
					-- select	1
					-- from	sysobjects
					-- where
							-- [name]			=	'BARB_WEIGHTED_MINS_BY_EVENT_STARTMIN'
						-- and uid				=	user_id()
						-- and	upper([type])	=	'U'
				-- )
		-- drop table BARB_WEIGHTED_MINS_BY_EVENT_STARTMIN
	-- commit
	
	SELECT
			UTC.UTC_DATEHOURMIN
		,	UTC.UTC_DATEHOUR
		,	BARB.STREAM_TYPE
		,	SUM(BARB.TOTAL_WEIGHTED_MINS)                   AS  TOTAL_HOUSEHOLD_WEIGHTED_MINS_BY_STARTMIN
	INTO	#BARB_WEIGHTED_MINS_BY_EVENT_STARTMIN
	FROM
					UTC								AS	UTC -- Minute-by-minute time base

		-- Join time base onto BARB viewing data
		LEFT JOIN   #BARB_EVENT_WEIGHTED_MINUTES		AS	BARB	ON	UTC.UTC_DATEHOURMIN	=	BARB.UTC_TV_Event_Start_Date_Time
	GROUP BY
			UTC.UTC_DATEHOURMIN
		,   UTC.UTC_DATEHOUR
		,	BARB.STREAM_TYPE
	-- ORDER BY
			-- UTC.UTC_DATEHOURMIN
		-- ,   UTC.UTC_DATEHOUR
		-- ,	BARB.STREAM_TYPE
	COMMIT

	CREATE DTTM INDEX DTTM_IDX_1 ON #BARB_WEIGHTED_MINS_BY_EVENT_STARTMIN(UTC_DATEHOURMIN)	COMMIT
	CREATE DTTM INDEX DTTM_IDX_2 ON #BARB_WEIGHTED_MINS_BY_EVENT_STARTMIN(UTC_DATEHOUR)	COMMIT

	
	

	----------------------------------------------------------------------------------------------------------------------
	-- Join both analyses by viewing time and event start time into a single query output for pivot table-ing
	----------------------------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M06 : Combine BARB minute-by-minute consumption analyses'
	COMMIT

	execute DROP_LOCAL_TABLE 'BARB_MINUTE_BY_MINUTE_WEIGHTED_VIEWING'
	commit

	SELECT
			DATEADD(HOUR,-1,UTC.UTC_DATEHOURMIN)			AS      UTC_DATEHOURMIN
		,	DATEADD(HOUR,-1,UTC.UTC_DATEHOUR)				AS      UTC_DATEHOUR
		,	S.STREAM_TYPE
		,	A.TOTAL_HOUSEHOLD_WEIGHTED_MINS					AS  BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME
		,	B.TOTAL_HOUSEHOLD_WEIGHTED_MINS_BY_STARTMIN		AS  BARB_WEIGHTED_MINUTES_BY_EVENT_START_TIME
	INTO	BARB_MINUTE_BY_MINUTE_WEIGHTED_VIEWING
	FROM
					UTC										AS  UTC
					
		CROSS JOIN	(	-- Add Live/Playback field base
						SELECT	CAST('LIVE' AS VARCHAR)			AS	STREAM_TYPE
						UNION ALL
						SELECT	CAST('PLAYBACK' AS VARCHAR)		AS	STREAM_TYPE
					)										AS  S
					
		LEFT JOIN   #BARB_WEIGHTED_MINS						AS	A	ON	UTC.UTC_DATEHOURMIN	=	A.UTC_DATEHOURMIN
																	AND	UTC.UTC_DATEHOUR	=	A.UTC_DATEHOUR
																	AND	S.STREAM_TYPE		=	A.STREAM_TYPE
																	
		LEFT JOIN	#BARB_WEIGHTED_MINS_BY_EVENT_STARTMIN	AS	B	ON	UTC.UTC_DATEHOURMIN	=	B.UTC_DATEHOURMIN
																	AND	UTC.UTC_DATEHOUR	=	B.UTC_DATEHOUR
																	AND	S.STREAM_TYPE		=	B.STREAM_TYPE
	-- ORDER BY
			-- UTC.UTC_DATEHOURMIN
		-- ,	UTC.UTC_DATEHOUR
		-- ,	S.STREAM_TYPE
	COMMIT

	CREATE DTTM INDEX DTTM_IDX_1 ON BARB_MINUTE_BY_MINUTE_WEIGHTED_VIEWING(UTC_DATEHOURMIN)	COMMIT
	CREATE DTTM INDEX DTTM_IDX_2 ON BARB_MINUTE_BY_MINUTE_WEIGHTED_VIEWING(UTC_DATEHOUR)	COMMIT

	
end;
commit;

grant execute on V306_CP2_M06_BARB_Minutes to vespa_group_low_security;
commit;








