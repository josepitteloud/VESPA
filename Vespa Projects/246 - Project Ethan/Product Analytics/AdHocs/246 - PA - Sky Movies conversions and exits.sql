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
#     .:::.   .###       ::         ..       '#
#   ######### .###     #####       ###.      '#
#  ########## .###    ######+     ####       '#
# ;#########  .###   +#######     ###;       '#
# ####        .###  '#### ####   '###        '#
# '####+.     .### ;####  +###:  ###+        '#
#  ########+  .###,####    #### .###         '#
#  `######### .###`####    `########         '#
#     :######`.### +###.    #######          '#
#         ###'.###  ####     ######          '#
#  `'':..+###:.###  .####    ,####`          '#
#  ########## .###   ####.    ####           '#
#  #########, .###    ####    ###:           '#
#  #######;   .##:     ###+  '###            '#
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
# 246 - PA - Sky Movies conversions and exits.sql
# 2017-01-09
# 
# Environment:
# SQL script to be run on Netezza
# 
# 
# Dependencies:
# None
# 
# ------------------------------------------------------------------------------
# 
*/

-----------------------------------------------------------------------
-- Percentile of conversion/exit times
-- Target TABLEAU datasource name: Sky Q PA - Ad Hoc - Sky Cinema Conversion wc 17th Oct - session grain
-----------------------------------------------------------------------
WITH	T0	AS	(	-- Calculate time since menu entry and generate conversion/exit flags per action AND session
					SELECT
							A.INDEX_
						,	A.DT
						,	A.DATE_
						,	A.SS_ELAPSED_NEXT_ACTION
						,	A.DK_SERIAL_NUMBER
						,	A.DK_ACTION_ID
						,	A.ACTION_NAME
						,	A.DK_TRIGGER_ID
						,	A.DK_PREVIOUS
						,	A.DK_CURRENT
						,	A.DK_REFERRER_ID
						,	A.GN_LVL2_SESSION_GRAIN
						,	A.GN_LVL2_SESSION
						,	FIRST_VALUE(A.DT)	OVER	(
															PARTITION BY
																	A.DATE_
																,	A.DK_SERIAL_NUMBER
																,	A.GN_LVL2_SESSION_GRAIN
																,	A.GN_LVL2_SESSION
															ORDER BY	A.DT
														)			DT_MENU_ENTRY
						,	DURATION_SUBTRACT(DT,DT_MENU_ENTRY)	SECONDS_SINCE_MENU_ENTRY
						,	CASE
								WHEN	A.DK_ACTION_ID	IN	(
																	'02400'	-- Trigger download
																-- ,	'02420'	-- Start download
																,	'03000'	-- Playback start
																,	'03001'	-- Playback stop
															)	THEN	1
								ELSE									0
							END										CONVERSION_EXIT_FLAG
						,	MAX(CONVERSION_EXIT_FLAG)	OVER	(
																	PARTITION BY
																			A.DATE_
																		,	A.DK_SERIAL_NUMBER
																		,	A.GN_LVL2_SESSION_GRAIN
																		,	A.GN_LVL2_SESSION
																)	CONVERTED_SESSION_FLAG
					FROM	ETHAN_PA_PROD..Z_PA_EVENTS_FACT	A
					WHERE
							A.DATE_				BETWEEN	'2016-10-17'
												AND		'2016-10-23'
						AND	A.GN_LVL2_SESSION	=		'Sky Movies'
					-- LIMIT	100
				)
-- Ntile the conversion/exit times
SELECT
		T1.DATE_
	,	T1.DK_SERIAL_NUMBER
	,	T1.GN_LVL2_SESSION_GRAIN
	,	T1.GN_LVL2_SESSION
	,	CASE
			WHEN	(
							T1.CONVERTED_SESSION_FLAG	=	1
						AND	T0.DK_ACTION_ID				IN	(
																	'02400'	-- Trigger download
																-- ,	'02420'	-- Start download
																,	'03000'	-- Playback start
																,	'03001'	-- Playback stop
															)
					)	THEN	T0.ACTION_NAME
			ELSE				'OTHER'
		END						ACTION_NAME
	,	T1.CONVERTED_SESSION_FLAG
	,	T1.SECONDS_FOR_FIRST_CONVERSION
	,	T1.SECONDS_UNTIL_EXIT
	,	NTILE(100)	OVER	(
								PARTITION BY
										T1.CONVERTED_SESSION_FLAG
									,	ACTION_NAME
								ORDER BY	T1.SECONDS_FOR_FIRST_CONVERSION
							)	NT_SECONDS_UNTIL_CONVERSION
	,	NTILE(100)	OVER	(
								PARTITION BY	T1.CONVERTED_SESSION_FLAG
								ORDER BY 		T1.SECONDS_UNTIL_EXIT
							)	NT_SECONDS_UNTIL_EXIT
FROM
				(	-- Separate into conversion actions and everything else, then calculate the time until first action and final exit
					SELECT
							DATE_
						,	DK_SERIAL_NUMBER
						,	GN_LVL2_SESSION_GRAIN
						,	GN_LVL2_SESSION
						,	CONVERTED_SESSION_FLAG
						,	MIN	(
									CASE	CONVERSION_EXIT_FLAG
										WHEN	1	THEN	SECONDS_SINCE_MENU_ENTRY
										ELSE				NULL
									END
							
								)							SECONDS_FOR_FIRST_CONVERSION
						,	MAX(SECONDS_SINCE_MENU_ENTRY)	SECONDS_UNTIL_EXIT
						,	MIN	(
									CASE	CONVERSION_EXIT_FLAG
										WHEN	1	THEN	INDEX_
										ELSE				NULL
									END
							
								)							INDEX_FOR_FIRST_CONVERSION
						-- ,	COUNT(DISTINCT INDEX_)			NUMBER_OF_ACTIONS
					FROM	T0
					GROUP BY
							DATE_
						,	DK_SERIAL_NUMBER
						,	GN_LVL2_SESSION_GRAIN
						,	GN_LVL2_SESSION
						,	CONVERTED_SESSION_FLAG
					HAVING	(	-- Ignore conversions/exits faster than 20s
									SECONDS_FOR_FIRST_CONVERSION	>	20
								OR	(		SECONDS_FOR_FIRST_CONVERSION	IS	NULL
										AND	SECONDS_UNTIL_EXIT				>	20
									)
							)
				)	T1
	LEFT JOIN		T0	ON	T1.DATE_						=	T0.DATE_
						AND	T1.DK_SERIAL_NUMBER				=	T0.DK_SERIAL_NUMBER
						AND	T1.GN_LVL2_SESSION_GRAIN		=	T0.GN_LVL2_SESSION_GRAIN
						AND	T1.GN_LVL2_SESSION				=	T0.GN_LVL2_SESSION
						AND	T1.CONVERTED_SESSION_FLAG		=	T0.CONVERTED_SESSION_FLAG
						AND	T1.INDEX_FOR_FIRST_CONVERSION	=	T0.INDEX_
-- LIMIT	1000
;





-----------------------------------------------------------------------
-- Enhance original grain of data with session-wise conversion/exit 
-- times and their time ntile for fastest/slowest analysis
-- Target TABLEAU datasource name: Sky Q PA - Ad Hoc - Sky Cinema Conversion wc 17th Oct - event grain
-----------------------------------------------------------------------
WITH	T0	AS	(	-- Calculate time since menu entry and generate conversion/exit flags per action AND session
					SELECT
							A.INDEX_
						,	A.DT
						,	A.DATE_
						,	A.SS_ELAPSED_NEXT_ACTION
						,	A.DK_SERIAL_NUMBER
						,	A.DK_ACTION_ID
						,	A.ACTION_NAME
						,	A.DK_TRIGGER_ID
						,	A.DK_PREVIOUS
						,	A.DK_CURRENT
						,	A.DK_REFERRER_ID
						,	A.GN_LVL2_SESSION_GRAIN
						,	A.GN_LVL2_SESSION
						,	FIRST_VALUE(A.DT)	OVER	(
															PARTITION BY
																	A.DATE_
																,	A.DK_SERIAL_NUMBER
																,	A.GN_LVL2_SESSION_GRAIN
																,	A.GN_LVL2_SESSION
															ORDER BY	A.DT
														)			DT_MENU_ENTRY
						,	DURATION_SUBTRACT(DT,DT_MENU_ENTRY)	SECONDS_SINCE_MENU_ENTRY
						,	CASE
								WHEN	A.DK_ACTION_ID	IN	(
																	'02400'	-- Trigger download
																-- ,	'02420'	-- Start download
																,	'03000'	-- Playback start
																,	'03001'	-- Playback stop
															)	THEN	1
								ELSE									0
							END										CONVERSION_EXIT_FLAG
						,	MAX(CONVERSION_EXIT_FLAG)	OVER	(
																	PARTITION BY
																			A.DATE_
																		,	A.DK_SERIAL_NUMBER
																		,	A.GN_LVL2_SESSION_GRAIN
																		,	A.GN_LVL2_SESSION
																)	CONVERTED_SESSION_FLAG
					FROM	ETHAN_PA_PROD..Z_PA_EVENTS_FACT	A
					WHERE
							A.DATE_				BETWEEN	'2016-10-17'
												AND		'2016-10-23'
						AND	A.GN_LVL2_SESSION	=		'Sky Movies'
				)
SELECT
		-- T0.*
		T0.INDEX_
	,	T0.DT
	,	T0.DATE_
	,	T0.SS_ELAPSED_NEXT_ACTION
	,	T0.DK_SERIAL_NUMBER
	,	T0.DK_ACTION_ID
	,	T0.ACTION_NAME
	,	T0.DK_TRIGGER_ID
	,	T0.DK_PREVIOUS
	,	T0.DK_CURRENT
	,	T0.DK_REFERRER_ID
	,	T0.GN_LVL2_SESSION_GRAIN
	,	T0.GN_LVL2_SESSION
	,	T0.DT_MENU_ENTRY
	,	T0.SECONDS_SINCE_MENU_ENTRY
	,	T0.CONVERSION_EXIT_FLAG
	,	T0.CONVERTED_SESSION_FLAG
	,	T2.RAND_SESSION
	,	T2.SECONDS_FOR_FIRST_CONVERSION
	,	T2.SECONDS_UNTIL_EXIT
	,	T2.NT_SECONDS_UNTIL_CONVERSION
	,	T2.NT_SECONDS_UNTIL_EXIT
FROM
				T0
	INNER JOIN	(	-- Ntile the conversion/exit times, while (LEFT) joining back onto the event-grain data to get conversion type
					SELECT
							T1.DATE_
						,	T1.DK_SERIAL_NUMBER
						,	T1.GN_LVL2_SESSION_GRAIN
						,	T1.GN_LVL2_SESSION
						,	CASE
								WHEN	(
												T1.CONVERTED_SESSION_FLAG	=	1
											AND	T0.DK_ACTION_ID				IN	(
																						'02400'	-- Trigger download
																					-- ,	'02420'	-- Start download
																					,	'03000'	-- Playback start
																					,	'03001'	-- Playback stop
																				)
										)	THEN	T0.ACTION_NAME
								ELSE				'OTHER'
							END										ACTION_NAME
						,	T1.CONVERTED_SESSION_FLAG
						,	RANDOM()								RAND_SESSION
						,	T1.SECONDS_FOR_FIRST_CONVERSION
						,	T1.SECONDS_UNTIL_EXIT
						,	NTILE(100)	OVER	(
													PARTITION BY
															T1.CONVERTED_SESSION_FLAG
														,	ACTION_NAME
													ORDER BY	T1.SECONDS_FOR_FIRST_CONVERSION
												)					NT_SECONDS_UNTIL_CONVERSION
						,	NTILE(100)	OVER	(
													PARTITION BY	T1.CONVERTED_SESSION_FLAG
													ORDER BY 		T1.SECONDS_UNTIL_EXIT
												)					NT_SECONDS_UNTIL_EXIT
					FROM
									(	-- Separate into conversion actions and everything else, then calculate the time until first action and final exit. Collapse onto session-grain
										SELECT
												DATE_
											,	DK_SERIAL_NUMBER
											,	GN_LVL2_SESSION_GRAIN
											,	GN_LVL2_SESSION
											,	CONVERTED_SESSION_FLAG
											,	MIN	(
														CASE	CONVERSION_EXIT_FLAG
															WHEN	1	THEN	SECONDS_SINCE_MENU_ENTRY
															ELSE				NULL
														END
												
													)							SECONDS_FOR_FIRST_CONVERSION
											,	MAX(SECONDS_SINCE_MENU_ENTRY)	SECONDS_UNTIL_EXIT
											,	MIN	(
														CASE	CONVERSION_EXIT_FLAG
															WHEN	1	THEN	INDEX_
															ELSE				NULL
														END
												
													)							INDEX_FOR_FIRST_CONVERSION
										FROM	T0
										GROUP BY
												DATE_
											,	DK_SERIAL_NUMBER
											,	GN_LVL2_SESSION_GRAIN
											,	GN_LVL2_SESSION
											,	CONVERTED_SESSION_FLAG
										HAVING	(	-- Ignore conversions/exits faster than 20s
														SECONDS_FOR_FIRST_CONVERSION	>	20
													OR	(		SECONDS_FOR_FIRST_CONVERSION	IS	NULL
															AND	SECONDS_UNTIL_EXIT				>	20
														)
												)
									)	T1
						LEFT JOIN		T0	ON	T1.DATE_						=	T0.DATE_
											AND	T1.DK_SERIAL_NUMBER				=	T0.DK_SERIAL_NUMBER
											AND	T1.GN_LVL2_SESSION_GRAIN		=	T0.GN_LVL2_SESSION_GRAIN
											AND	T1.GN_LVL2_SESSION				=	T0.GN_LVL2_SESSION
											AND	T1.CONVERTED_SESSION_FLAG		=	T0.CONVERTED_SESSION_FLAG
											AND	T1.INDEX_FOR_FIRST_CONVERSION	=	T0.INDEX_
				)	T2	ON	T0.DATE_					=	T2.DATE_
						AND	T0.DK_SERIAL_NUMBER			=	T2.DK_SERIAL_NUMBER
						AND	T0.GN_LVL2_SESSION_GRAIN	=	T2.GN_LVL2_SESSION_GRAIN
						AND	T0.GN_LVL2_SESSION			=	T2.GN_LVL2_SESSION
						AND	T0.CONVERTED_SESSION_FLAG	=	T2.CONVERTED_SESSION_FLAG
-- ORDER BY
-- 		T0.DATE_
-- 	,	T0.DK_SERIAL_NUMBER
-- 	,	T0.DT
-- 	,	T0.INDEX_
-- LIMIT	1000
;
