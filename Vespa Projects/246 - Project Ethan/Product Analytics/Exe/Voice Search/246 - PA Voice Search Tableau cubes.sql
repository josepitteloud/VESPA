/*
#                                            '#                                 
#                                            '#                                 
#                                            '#                                 
#                                            '#                                 
#                                            '#                                 
#                                            '#                                 
#              '##                           '#                                 
#              ###                           '#                                 
#             .###                           '#                                 
#             .###                           '#                                 
#     .:::.   .###       ::         ..       '#       .                   ,:,   
#   ######### .###     #####       ###.      '#      '##  ########`     ########
#  ########## .###    ######+     ####       '#      '##  #########'   ########'
# ;#########  .###   +#######     ###;       '#      '##  ###    ###.  ##       
# ####        .###  '#### ####   '###        '#      '##  ###     ###  ##       
# '####+.     .### ;####  +###:  ###+        '#      '##  ###      ##  ###`     
#  ########+  .###,####    #### .###         '#      '##  ###      ##. ;#####,  
#  `######### .###`####    `########         '#      '##  ###      ##.  `######`
#     :######`.### +###.    #######          '#      '##  ###      ##      .####
#         ###'.###  ####     ######          '#      '##  ###     ;##         ##
#  `'':..+###:.###  .####    ,####`          '#      '##  ###    `##+         ##
#  ########## .###   ####.    ####           '#      '##  ###   +###   ;,    +##
#  #########, .###    ####    ###:           '#      '##  #########    ########+
#  #######;   .##:     ###+  '###            '#      '##  '######      ;######, 
#                            ###'            '#                                 
#                           ;###             '#                                 
#                           ####             '#                                 
#                          :###              '#                                 
#                                            '#                                 
#                                            '#                                 
#                                            '#                                 
#                                            '#                                 
#                                            '#                                 
#                                            '#                                 
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# 246 - PA Voice Search Tableau cubes.sql
# 2017-01-06
# 
# Environment:
# SQL script to be run on Netezza
# 
# 
# Function:
# Cube scripts for Voice Search extracts and attachment from Tableau
# 
# Dependencies:
# None
# 
# ------------------------------------------------------------------------------
# 
*/


-----------------------------------------------------------------------
-- Copy data from daily batch load into final table
-----------------------------------------------------------------------
/* -- Some prerequisite actions...

-- Create final table to house raw Voice Search events
CREATE TABLE ETHAN_PA_PROD..PA_VOICE_SEARCH_RAW    (
														"source/serialNumber"   VARCHAR(17)
													,   "eventLog/action/asrConfidenceLevel" INTEGER
													,   "eventLog/action/error_msg" VARCHAR(512)
													,   "eventLog/action/id"    VARCHAR(128)
													,   "eventLog/action/oldQuery"  VARCHAR(2048)
													,   "eventLog/action/query"  VARCHAR(2048)
													,   "eventLog/action/suggestions"   INTEGER
													,   "eventLog/ref/id"   VARCHAR(2048)
													,   "eventLog/timems"   BIGINT
													,   "eventLog/trigger/id"   VARCHAR(512)
													,   "eventLog/trigger/input"    VARCHAR(512)
													,   "eventLog/trigger/remote/batterylevel"  INTEGER
													,   "eventLog/trigger/remote/conntype"  VARCHAR(512)
													,   "eventLog/trigger/remote/deviceid"  VARCHAR(512)
													,   "eventLog/trigger/remote/hwrev" VARCHAR(512)
													,   "eventLog/trigger/remote/make"  VARCHAR(512)
													,   "eventLog/trigger/remote/model" VARCHAR(512)
													,   "eventLog/trigger/remote/name"  VARCHAR(512)
													,   "eventLog/trigger/remote/swrev" VARCHAR(512)
													,   "auditTimestamp"  TIMESTAMP
												)
;

-- Create transient version of the above table to house each batch
SELECT	*
INTO	ETHAN_PA_PROD..TMP_VOICE_SEARCH_EXTRACT_BATCH
FROM	ETHAN_PA_PROD..PA_VOICE_SEARCH_RAW
WHERE	1 <> 1
;
*/



/*
TRUNCATE TABLE ETHAN_PA_PROD..TMP_VOICE_SEARCH_EXTRACT_BATCH;

INSERT INTO	ETHAN_PA_PROD..PA_VOICE_SEARCH_RAW
SELECT	*
FROM	ETHAN_PA_PROD..TMP_VOICE_SEARCH_EXTRACT_BATCH
;
*/

-----------------------------------------------------------------------
-- Voice Search cubes
-----------------------------------------------------------------------

-- Tableau data source "Sky Q - PA Voice Search Engagement last 12 months"
WITH	T0	AS	(	-- Awkward SQL to combine trialist details with that from entire Sky base
					SELECT
							COALESCE(A.DEVICE_ID,B.DEVICE_ID)									DEVICE_ID
						,	CASE
								WHEN	A.TRIAL_GROUP IS NOT NULL	THEN	A.TRIAL_GROUP
								ELSE										'Staff Trial'
							END																	TRIAL_GROUP
						,	COALESCE(A.ACCOUNT_NUMBER,CAST(B.ACCOUNT_NUMBER AS VARCHAR(128)))	ACCOUNT_NUMBER
						,	COALESCE(A.POSTAL_OUTCODE,B.FIN_POSTCODE_OUTCODE_PAF)				POSTAL_OUTCODE
						,	B.FIN_COUNTY_PAF													COUNTY
						,	B.USER_GROUP
						,	B.BOX_INSTALLED_DT
						,	B.BOX_REPLACED_DT
					FROM
									ETHAN_PA_PROD..PA_D12_TRIALIST_DETAILS	A
						FULL JOIN	ETHAN_PA_PROD..PA_DEVICE_ACCOUNT_LOOKUP	B	ON	A.DEVICE_ID			=	B.DEVICE_ID
																				AND	A.ACCOUNT_NUMBER	=	B.ACCOUNT_NUMBER
				)
	,	T2	AS	(	-- Calculate first and last use of Voice Search features per STB from the last 12 months
					SELECT
							"source/serialNumber"
						,	DATE(DATE('1970-01-01') + CAST(CAST(CAST(MIN("eventLog/timems") AS DOUBLE) / 1e3 AS VARCHAR(512))	|| ' seconds' AS INTERVAL))	AS	VS_FIRST_USE_DATE
						,	DATE(DATE('1970-01-01') + CAST(CAST(CAST(MAX("eventLog/timems") AS DOUBLE) / 1e3 AS VARCHAR(512))	|| ' seconds' AS INTERVAL))	AS	VS_LAST_USE_DATE
					FROM	ETHAN_PA_PROD..PA_VOICE_SEARCH_RAW
					WHERE	DATE('1970-01-01') + CAST(CAST(CAST("eventLog/timems" AS DOUBLE) / 1e3 AS VARCHAR(512))	|| ' seconds' AS INTERVAL)	BETWEEN	DATE(NOW()) - 365
										AND		DATE(NOW())
					GROUP BY	"source/serialNumber"
				)
-- Calculate engagement at household grain
SELECT
		TRIAL_GROUP
	,	USER_GROUP
	,	ACCOUNT_NUMBER
	,	POSTAL_OUTCODE
	,	COUNTY
	,	CASE	MAX(STB_STATUS)
			WHEN	0	THEN	'Churned'
			WHEN	1	THEN	'Active'
			ELSE				NULL
		END													HOUSEHOLD_STATUS
	,	MIN(VS_FIRST_USE_DATE)								VS_FIRST_USE_DATE_HH
	,	MAX(VS_LAST_USE_DATE)								VS_LAST_USE_DATE_HH
	,	EXTRACT(DAYS FROM NOW() - VS_LAST_USE_DATE_HH)		DAYS_SINCE_LAST_VS_HH
	,	VS_LAST_USE_DATE_HH - VS_FIRST_USE_DATE_HH			VS_USAGE_LIFETIME_DAYS_HH
FROM	(	-- STB-grain flags/calculations
			SELECT
					"source/serialNumber"
				,	T0.TRIAL_GROUP
				,	T0.USER_GROUP
				,	T0.ACCOUNT_NUMBER
				,	T0.POSTAL_OUTCODE
				,	T0.COUNTY
				,	T2.VS_FIRST_USE_DATE
				,	T2.VS_LAST_USE_DATE
				,	CASE
						WHEN	T0.BOX_REPLACED_DT	<	NOW()	THEN	0
						ELSE											1
					END														STB_STATUS
				-- ,	EXTRACT(DAYS FROM NOW() - T2.VS_LAST_USE_DATE)			DAYS_SINCE_LAST_VS
				-- ,	T2.VS_LAST_USE_DATE - T2.VS_FIRST_USE_DATE				VS_USAGE_LIFETIME_DAYS
			FROM
							T2
				LEFT JOIN	T0	ON	SUBSTR(T2."source/serialNumber",1,16)	=	T0.DEVICE_ID
		)	A
GROUP BY
		TRIAL_GROUP
	,	USER_GROUP
	,	ACCOUNT_NUMBER
	,	POSTAL_OUTCODE
	,	COUNTY
-- LIMIT	100
;

-----------------------------------------------------------------------
-- Onward journeys 
-- (This now replaces the original standalone cube that is essentially the A0 subquery below )
-----------------------------------------------------------------------

WITH	T0	AS	(	-- Awkward SQL to combine trialist details with that from entire Sky base
					SELECT
							COALESCE(A.DEVICE_ID,B.DEVICE_ID)									DEVICE_ID
						,	CASE
								WHEN	A.TRIAL_GROUP IS NOT NULL	THEN	A.TRIAL_GROUP
								ELSE										'Staff Trial'
							END																	TRIAL_GROUP
						,	COALESCE(A.ACCOUNT_NUMBER,CAST(B.ACCOUNT_NUMBER AS VARCHAR(128)))	ACCOUNT_NUMBER
						,	COALESCE(A.POSTAL_OUTCODE,B.FIN_POSTCODE_OUTCODE_PAF)				POSTAL_OUTCODE
						,	B.FIN_COUNTY_PAF													COUNTY
						,	B.USER_GROUP
						,	B.BOX_INSTALLED_DT
						,	B.BOX_REPLACED_DT
					FROM
									ETHAN_PA_PROD..PA_D12_TRIALIST_DETAILS	A
						FULL JOIN	ETHAN_PA_PROD..PA_DEVICE_ACCOUNT_LOOKUP	B	ON	A.DEVICE_ID			=	B.DEVICE_ID
																				AND	A.ACCOUNT_NUMBER	=	B.ACCOUNT_NUMBER
				)
	,	T1	AS	(	-- Get Voice Search data for the last 60 days
					SELECT
							DATE('1970-01-01') + CAST(CAST(CAST("eventLog/timems" AS DOUBLE) / 1e3 AS VARCHAR(512))	|| ' seconds' AS INTERVAL)	DT
						,	"source/serialNumber"
						,	"eventLog/action/asrConfidenceLevel"
						,	"eventLog/action/error_msg"
						,	"eventLog/action/id"
						,	"eventLog/action/query"
						,	"eventLog/action/suggestions"
						,	"eventLog/timems"
						,	ROW_NUMBER()	OVER	(
														PARTITION BY	"source/serialNumber"
														ORDER BY	"eventLog/timems"	
													)	RNK
					FROM	ETHAN_PA_PROD..PA_VOICE_SEARCH_RAW
					WHERE
							DATE(DT)						BETWEEN	DATE(NOW()) - 60
															AND		DATE(NOW())
						-- AND	"source/serialNumber"			=		'32B05504800100713'
				)
SELECT
		A0.DT
	,	A0."source/serialNumber"
	,	A0."eventLog/action/asrConfidenceLevel"
	,	A0."eventLog/action/error_msg"
	,	A0."eventLog/action/id"
	,	A0."eventLog/action/query"
	,	A0."eventLog/action/suggestions"
	,	A0."eventLog/timems"
	,	CASE
			WHEN	(
							A0."eventLog/action/error_msg"	=	''
						AND	A0."eventLog/action/suggestions"	=	0
					)				THEN	'VALID SEARCH - Zero suggestions'
			WHEN	(
							A0."eventLog/action/error_msg"	=	''
						AND	A0."eventLog/action/suggestions"	>	0
					)				THEN	A2.ACTION_FULL_NAME
			ELSE							A0."eventLog/action/error_msg"
		END		SEARCH_RESULT	
	,	A0.TRIAL_GROUP
	,	A0.ACCOUNT_NUMBER
	,	A0.POSTAL_OUTCODE
	,	A0.COUNTY
	,	A0.USER_GROUP
	,	A2.VS_CONVERSION_FLAG
	,	A2.SECONDS_SINCE_VOICESEARCH
	,	A2.DK_ACTION_ID
	,	A2.ACTION_FULL_NAME
FROM
				(	-- Original standalone cube
					SELECT
							T1.DT
						,	T1."source/serialNumber"
						,	T1."eventLog/action/asrConfidenceLevel"
						,	T1."eventLog/action/error_msg"
						,	T1."eventLog/action/id"
						,	T1."eventLog/action/query"
						,	T1."eventLog/action/suggestions"
						,	T1."eventLog/timems"
						,	T0.TRIAL_GROUP
						,	T0.ACCOUNT_NUMBER
						,	T0.POSTAL_OUTCODE
						,	T0.COUNTY
						,	T0.USER_GROUP
					FROM
									T1
						LEFT JOIN	T0	ON	SUBSTR(T1."source/serialNumber",1,16)	=		T0.DEVICE_ID
											AND	T1.DT								BETWEEN	T0.BOX_INSTALLED_DT
																							AND		T0.BOX_REPLACED_DT
				)	A0
	LEFT JOIN	(	--	Onward journyes - retain only the final conversion/exit action
					SELECT
							DT
						,	"source/serialNumber"
						-- ,	"eventLog/action/asrConfidenceLevel"
						-- ,	"eventLog/action/id"
						-- ,	"eventLog/action/query"
						-- ,	"eventLog/action/suggestions"
						,	"eventLog/timems"
						,	SECONDS_SINCE_VOICESEARCH
						,	DK_ACTION_ID
						,	DK_CURRENT
						,	DK_NEXT
						,	DK_PREVIOUS
						,	DK_REFERRER_ID
						,	DK_TRIGGER_ID
						,	EPG_SECTION
						,	ACTION_FULL_NAME
						,	CASE
								WHEN	ACTION_EXIT_FLAG		=	1		THEN	0
								WHEN	ACTION_CONVERSION_FLAG	=	1		THEN	1
								WHEN	DK_ACTION_ID			=	'01400'	THEN	0	-- catch remaining GLobal Navigation and class as Exits
								ELSE												NULL
							END		VS_CONVERSION_FLAG
					FROM	(
								-- Join onto events fact and action dim, then define the exit/conversion flags
								SELECT
										A.DT
									,	A."source/serialNumber"
									-- ,	A."eventLog/action/asrConfidenceLevel"
									-- ,	A."eventLog/action/id"
									-- ,	A."eventLog/action/query"
									-- ,	A."eventLog/action/suggestions"
									,	A."eventLog/timems"
									,	C.TIMEMS
									,	(C.TIMEMS - A."eventLog/timems") / 1E3				SECONDS_SINCE_VOICESEARCH
									,	C.DK_ACTION_ID
									,	C.DK_CURRENT
									,	C.DK_NEXT
									,	C.DK_PREVIOUS
									,	C.DK_REFERRER_ID
									,	C.DK_TRIGGER_ID
									,	D.EPG_SECTION
									,	D.ACTION_FULL_NAME
									,	CASE	
											WHEN
													C.TIMEMS	>	A."eventLog/timems"
												AND	(
															C.DK_CURRENT	=		'guide://menu/home'				-- Exit back to Home
														OR	C.DK_CURRENT	LIKE	'guide://ondemand/asset/EVOD%'	-- Return to Top Picks
														OR	C.DK_ACTION_ID	IN		(
																							'00100'					-- Dismiss
																						,	'01605'					-- Search
																						,	'04000'					-- App trag open
																						,	'04002'					-- App tray launch
																						,	'00002'					-- Active standby-in
																						,	'01000'					-- Open Mini Guide
																						-- ,	'01400'					-- Global navigation -- ignore this here to avoid prematurely exiting journeys
																					)
													)				THEN	1
											ELSE							NULL
										END													ACTION_EXIT_FLAG
									,	CASE	
											WHEN	(
															C.DK_ACTION_ID	IN	(
																						'00001'	-- Fullscreen
																					,	'02000'	-- Make a standalone booking
																					-- ,	'02002'	-- Ongoing standalone recording start
																					,	'02010' -- Make a series booking
																					,	'02400'	-- Trigger download
																					-- ,	'02420'	-- Start download
																					,	'03000'	-- Playback start
																					,	'03001'	-- Playback stop
																				)
														OR	C.TIMEMS		IS	NULL
													)	THEN	1
											ELSE				NULL
										END													ACTION_CONVERSION_FLAG
									,	MIN	(
												CASE	COALESCE(ACTION_EXIT_FLAG,ACTION_CONVERSION_FLAG)
													WHEN	1	THEN	C.TIMEMS
													ELSE				NULL
												END
											)	OVER	(
															PARTITION BY
																	A."source/serialNumber"
																,	A."eventLog/timems"
														)									EXIT_CONVERSION_TIMEMS
								FROM			T1								A

									LEFT JOIN	T1								B	-- Join on self to use next VS event as further join condition below
																					ON	A."source/serialNumber"					=		B."source/serialNumber"
																					AND	B.RNK									=		A.RNK + 1
																					AND	A."eventLog/action/error_msg"			=		''
																					AND	A."eventLog/action/suggestions"			>		0

									INNER JOIN	ETHAN_PA_PROD..PA_EVENTS_FACT	C	ON	SUBSTR(A."source/serialNumber",1,16)	=		C.DK_SERIAL_NUMBER
																					AND	C.TIMEMS								BETWEEN	A."eventLog/timems"
																																AND		CASE	-- Limit this join with max upper limit of 10-minute UI-wide timeout
																																			WHEN	B."eventLog/timems" < (A."eventLog/timems" + (10*60E3))	THEN	B."eventLog/timems"
																																			ELSE																	A."eventLog/timems" + (10*60E3)
																																		END
																					AND	C.DK_DATE								BETWEEN	CAST(TO_CHAR(DATE(NOW())-60,'YYYYMMDD') AS INT)
																																AND		CAST(TO_CHAR(DATE(NOW()),'YYYYMMDD') AS INT)

									LEFT JOIN	ETHAN_PA_PROD..PA_ACTION_DIM	D	ON	C.DK_ACTION_ID							=		D.PK_ACTION_ID
								WHERE
										A."eventLog/action/error_msg"	=	''
									AND	A."eventLog/action/suggestions"	>	0
							)	A1
					WHERE	A1.TIMEMS	=	A1.EXIT_CONVERSION_TIMEMS
					-- ORDER BY
					-- 		A1."source/serialNumber"
					-- 	,	A1.DT
					-- 	,	A1.TIMEMS
					-- LIMIT 1000
				)	A2	ON	A2."source/serialNumber"	=	A0."source/serialNumber"
						AND	A2."eventLog/timems"		=	A0."eventLog/timems"
;
