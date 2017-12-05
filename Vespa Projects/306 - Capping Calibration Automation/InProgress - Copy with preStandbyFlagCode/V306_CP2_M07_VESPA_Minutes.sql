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




**Module:                               V306_CP2_M07_VESPA_Minutes


*/

create or replace procedure V306_CP2_M07_VESPA_Minutes
											@target_date		date	=	NULL     -- Date of daily table caps to cache
										,	@iteration_number	int	=	0     -- current iteration
as begin

        execute M00_2_output_to_logger '@ M07: V306_CP2_M07_VESPA_Minutes start...'
        commit

                --------------------------------
        -- VESPA analysis by viewing time
        --------------------------------


        execute M00_2_output_to_logger '@ M07: VESPA consumption by viewing time'
        COMMIT

        SELECT  --TOP 20  *
                UTC.UTC_DATEHOURMIN
                ,   UTC.UTC_DATEHOUR
                ,       CASE
                                WHEN    VESPA.live=1                 THEN    'LIVE'
                                WHEN    VESPA.live=0                 THEN    'PLAYBACK'
                                ELSE        NULL -- never happens, left for legacy
                        END                                                                                             AS      STREAM_TYPE
                ,   /*SUM(VESPA.weighted_total_people_viewing)*/ cast(null as int)         AS  TOTAL_INDIVIDUAL_WEIGHTED_MINS
                ,   SUM(VESPA.scaling_weighting) AS  TOTAL_HOUSEHOLD_WEIGHED_MINS
        INTO    #VESPA_WEIGHTED_MINS
        FROM
					UTC		AS  UTC -- Create minute-by-minute time base
        LEFT JOIN	(	-- Collapse capped viewing instances into distinct viewing events
						select
								VDA.account_number
							,	subscriber_id
							,	scaling_weighting
							,	adjusted_event_start_time
							,	case
									when	capped_event_end_time is null	then	X_Adjusted_Event_End_Time
									else											capped_event_end_time
							end	as	event_end_time
							,	live
						from
									Vespa_Daily_Augs	VDA
						INNER JOIN	CP2_accounts		ACC		ON 	VDA.account_number	=	ACC.account_number		-- limit analysis to scaled accounts only
																AND	VDA.target_date		=	ACC.reference_date
						where
								iteration_number	=	@iteration_number
							-- and	target_date			=	@target_date
						group by
								VDA.account_number
							,	subscriber_id
							,	scaling_weighting
							,	adjusted_event_start_time
							,	event_end_time
							,	live
					)       AS      VESPA           ON  UTC.UTC_DATEHOURMIN     BETWEEN VESPA.adjusted_event_start_time -- Local_BARB_Instance_Start_Date_Time
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        AND     VESPA.event_end_time --Local_BARB_Instance_End_Date_Time
        GROUP BY
                                        UTC.UTC_DATEHOURMIN
                                        ,       UTC.UTC_DATEHOUR
                                        ,       STREAM_TYPE
        ORDER BY
                                                UTC.UTC_DATEHOURMIN
                                        ,   UTC.UTC_DATEHOUR
                                        ,       STREAM_TYPE
        commit
        CREATE DTTM INDEX VS_DTTM_IDX_1 ON #VESPA_WEIGHTED_MINS(UTC_DATEHOURMIN)
        commit


        ---------------------------------------------------------------
        -- Now retain the hourly bins but aggregate by EVENT START HOUR
        ---------------------------------------------------------------

        -- First, calculate the weighted minutes viewed PER VESPA VIEWING EVENT

        execute M00_2_output_to_logger '@ M07: VESPA consumption by viewing start time'
        COMMIT

        SELECT
                        VESPA.account_number -- Household_number
                ,   VESPA.adjusted_event_start_time--Local_TV_Event_Start_Date_Time
                ,       VESPA.X_Adjusted_Event_End_Time -- Local_TV_Event_End_Date_Time
                ,       STREAM_TYPE
                ,   SUM(TOTAL_WEIGHTED_MINS)        AS  TOTAL_WEIGHTED_MINS
        INTO    #VESPA_EVENT_WEIGHTED_MINUTES
        FROM
        (   -- Calculate the weighted viewing minutes per VESPA viewing instance, BUT then aggregating them by their common EVENT start times per household
                SELECT
							VDA.account_number -- Household_number
                        ,	adjusted_event_start_time --Local_TV_Event_Start_Date_Time
                        ,	X_Adjusted_Event_End_Time -- Local_TV_Event_End_Date_Time
                        ,	CASE
                                        WHEN    live=1                 THEN    'LIVE'
                                        WHEN    live=0                 THEN    'PLAYBACK'
                                        ELSE        NULL -- never happens, left for legacy
                                END AS STREAM_TYPE
                        ,   DATEDIFF    (
												MINUTE
												,   viewing_starts -- Local_BARB_Instance_Start_Date_Time
												,   viewing_stops -- Local_BARB_Instance_End_Date_Time
										) AS  DT
                        ,   SUM(scaling_weighting   *   DT) AS  TOTAL_WEIGHTED_MINS
                FROM
							Vespa_Daily_Augs	VDA
				INNER JOIN	CP2_accounts		ACC		ON 	VDA.account_number	=	ACC.account_number		-- limit analysis to scaled accounts only
														AND	VDA.target_date		=	ACC.reference_date
                GROUP BY
                                VDA.account_number -- Household_number
                        ,       adjusted_event_start_time --Local_TV_Event_Start_Date_Time
                        ,       X_Adjusted_Event_End_Time --Local_TV_Event_End_Date_Time
                        ,       STREAM_TYPE
                        ,       DT
        )   AS  VESPA

        GROUP BY
                        VESPA.account_number -- Household_number
                ,   VESPA.adjusted_event_start_time --Local_TV_Event_Start_Date_Time
                ,       VESPA.X_Adjusted_Event_End_Time --Local_TV_Event_End_Date_Time
                ,       STREAM_TYPE
        ORDER BY
                        VESPA.account_number -- Household_number
                ,   VESPA.adjusted_event_start_time --Local_TV_Event_Start_Date_Time
                ,       VESPA.X_Adjusted_Event_End_Time --Local_TV_Event_End_Date_Time
                ,       STREAM_TYPE
        COMMIT
        CREATE HG INDEX VS_HG_IDX_1 ON #VESPA_EVENT_WEIGHTED_MINUTES(account_number/*Household_number*/)
        CREATE DTTM INDEX VS_DTTM_IDX_1 ON #VESPA_EVENT_WEIGHTED_MINUTES(adjusted_event_start_time/*Local_TV_Event_Start_Date_Time*/)
        COMMIT


        -- Now, join onto the previously generated time base. First aggregate by start minute.

        SELECT
                        UTC.UTC_DATEHOURMIN
                ,       UTC.UTC_DATEHOUR
                ,       VESPA.STREAM_TYPE
                ,       SUM(VESPA.TOTAL_WEIGHTED_MINS) AS  TOTAL_HOUSEHOLD_WEIGHED_MINS_BY_STARTMIN
        INTO #VESPA_WEIGHTED_MINS_BY_EVENT_STARTMIN
        FROM
                UTC AS  UTC -- Minute-by-minute time base
                                        -- Join time base onto VESPA viewing data
                                        LEFT JOIN   #VESPA_EVENT_WEIGHTED_MINUTES AS  VESPA       ON      UTC.UTC_DATEHOURMIN =   VESPA.adjusted_event_start_time --Local_TV_Event_Start_Date_Time
        GROUP BY
                 UTC.UTC_DATEHOURMIN
                ,UTC.UTC_DATEHOUR
                ,VESPA.STREAM_TYPE
        ORDER BY
                UTC.UTC_DATEHOURMIN
                ,UTC.UTC_DATEHOUR
				,VESPA.STREAM_TYPE
        COMMIT

        CREATE DTTM INDEX VS_DTTM_IDX_1 ON #VESPA_WEIGHTED_MINS_BY_EVENT_STARTMIN(UTC_DATEHOURMIN)
        CREATE DTTM INDEX VS_DTTM_IDX_2 ON #VESPA_WEIGHTED_MINS_BY_EVENT_STARTMIN(UTC_DATEHOUR)
        COMMIT


        ----------------------------------------------------------------------------------------------------------------------
        -- Join both analyses by viewing time and event start time into a single query output for pivot table-ing
        ----------------------------------------------------------------------------------------------------------------------

        execute M00_2_output_to_logger '@ M07: Combine VESPA minute-by-minute consumption analyses'
        COMMIT

        execute DROP_LOCAL_TABLE 'VESPA_MINUTE_BY_MINUTE_WEIGHTED_VIEWING'
        commit

        -- Join both analyses by viewing time and event start time into a single query output for pivot table-ing
        SELECT
                        DATEADD(HOUR,-1,UTC.UTC_DATEHOURMIN)                    AS      UTC_DATEHOURMIN
                ,       DATEADD(HOUR,-1,UTC.UTC_DATEHOUR)                               AS      UTC_DATEHOUR
                ,       S.STREAM_TYPE
                ,       A.TOTAL_HOUSEHOLD_WEIGHED_MINS                                  AS  VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME
                ,       B.TOTAL_HOUSEHOLD_WEIGHED_MINS_BY_STARTMIN              AS  VESPA_WEIGHTED_MINUTES_BY_EVENT_START_TIME
        INTO VESPA_MINUTE_BY_MINUTE_WEIGHTED_VIEWING
        FROM
                UTC                                                                    AS  UTC
                CROSS JOIN  (
                                                SELECT  CAST('LIVE' AS VARCHAR)                     AS  STREAM_TYPE
                                                UNION ALL
                                                SELECT  CAST('PLAYBACK' AS VARCHAR)         AS  STREAM_TYPE
                                        )                                                                               AS  S
                LEFT JOIN   #VESPA_WEIGHTED_MINS     AS  A  ON  UTC.UTC_DATEHOURMIN =   A.UTC_DATEHOURMIN
                                                                                                                AND UTC.UTC_DATEHOUR    =       A.UTC_DATEHOUR
                                                                                                                AND S.STREAM_TYPE       =       A.STREAM_TYPE
                                                                                                                
                LEFT JOIN   #VESPA_WEIGHTED_MINS_BY_EVENT_STARTMIN   AS   B     ON UTC.UTC_DATEHOURMIN =        B.UTC_DATEHOURMIN
                                                                                                                                                AND UTC.UTC_DATEHOUR   =        B.UTC_DATEHOUR
                                                                                                                                                AND S.STREAM_TYPE      =        B.STREAM_TYPE
        ORDER BY
                        UTC.UTC_DATEHOURMIN
                ,   UTC.UTC_DATEHOUR
                ,   S.STREAM_TYPE
        COMMIT

                
        execute M00_2_output_to_logger '@ M07: V306_CP2_M07_VESPA_Minutes end'
        commit
        

end;
commit;

grant execute on V306_CP2_M07_VESPA_Minutes to vespa_group_low_security;
commit;





