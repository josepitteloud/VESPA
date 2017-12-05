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




**Module:                               M00_Initialise

**      Part A:       Processing tables: things that data goes through
**              A00 - The table of metadata
**              A05 - The box lookup for profiling
**
**      Part C:       Processing tables for calculation & storing caps
**              C01 - The table containing cap decisions
**              C02 - Bucket assignment lookup
**              C03 - Holding pen - last stop before dynamically named tables
**
**      Part D: D01 - Structural placeholder for daily tables
**
**      Part Q:       QA tables: things where we track suitability
**              Q01 - Ongoing QA totals of total viewing before / after caping
**              Q02 - Ongoing totals of BARB minute-by-minute consistency totals
**              Q03 - Historic tracking of total viewing per day
**
*/

create or replace procedure V306_CP2_M00_Initialise
                                                                                @hard_initialise        bit     =       0
as begin

        /****************** PART A00: The table of metadata ******************/

        -- Check for hard reset and drop table if requested
        if      (
						@hard_initialise	=	1
					and	exists			(
											select	1
											from	sysobjects
											where
															[name]			=	'CP2_metadata_table'
													and uid					=	user_id()
													and     upper([type])	=	'U'
										)
                )
			drop table CP2_metadata_table
        commit
        
        
        -- Create and populate CP2_metadata_table
        if not  exists  (
							select	1
										from	sysobjects
										where
														[name]			=	'CP2_metadata_table'
												and uid					=	user_id()
												and     upper([type])	=	'U'
						)
        begin

			execute M00_2_output_to_logger '@ M00 : Creating table CP2_metadata_table...'
			commit

			-- table does not exist, let's create it!!!
			create table CP2_metadata_table (
							row_id                                                                  int                     primary key identity -- primary key
					,	CAPPING_METADATA_KEY                                    int --
					,	START_TIME                                                              time-- Start time that the day part, used when joining to events data to determine which metadata record to use
					,	END_TIME                                                                time-- End time of the day part, used when joining to events data to determine which metadata record to use
					,	DAY_PART_DESCRIPTION                                    varchar(25)
					,	THRESHOLD_NTILE                                                 int
					,	THRESHOLD_NONTILE                                               int
					,	PLAYBACK_NTILE                                                  int                             default 198
					,	BANK_HOLIDAY_WEEKEND                                    int
					,	BOX_SHUT_DOWN                                                   int
					,	HOUR_IN_MINUTES                                                 int
					,	HOUR_24_CLOCK_LAST_HOUR                                 int
					,	MINIMUM_CUT_OFF                                                 int                             default 20
					,	MAXIMUM_CUT_OFF                                                 int                             default 120
					--,     MAXIMUM_ITERATIONS int -- used for scaling
					--,     MINIMUM_HOUSEHOLD_FOR_SCALING int -- used for scaling
					,	SAMPLE_MAX_POP                                                  int                             default 30000
					,	SHORT_DURATION_CAP_THRESHOLD                    int                             default 6
					,	MINIMUM_HOUSEHOLD_FOR_CAPPING                   int                             default null
					,	CURRENT_FLAG                                                    int
					,	EFFECTIVE_FROM                                                  date
					,	EFFECTIVE_TO                                                    date
					/*
					,	RECORDED_NTILE                                                  int                             default 198
					,	VOSDAL_1H_NTILE                                                 int                             default 190
					,	VOSDAL_1H_24H_NTILE                                             int                             default 194
					,	PUSHVOD_NTILE                                                   int                             default 199
					*/
					,	COMMON_PARAMETER_GROUP											varchar(255)					default null
			)
			commit
			
			create lf index lf1 on CP2_metadata_table(COMMON_PARAMETER_GROUP)
			commit


			-- Populate with some workable default values
			insert into CP2_metadata_table  (
													CAPPING_METADATA_KEY
												,	START_TIME
												,	END_TIME
												,	DAY_PART_DESCRIPTION
												,	THRESHOLD_NTILE
												,	THRESHOLD_NONTILE
												,	PLAYBACK_NTILE
												,	BANK_HOLIDAY_WEEKEND
												,	BOX_SHUT_DOWN
												,	HOUR_IN_MINUTES
												,	HOUR_24_CLOCK_LAST_HOUR
												,	MINIMUM_CUT_OFF
												,	MAXIMUM_CUT_OFF
												--,		MAXIMUM_ITERATIONS int -- used for scaling
												--,		MINIMUM_HOUSEHOLD_FOR_SCALING int -- used for scaling
												,	SAMPLE_MAX_POP
												,	SHORT_DURATION_CAP_THRESHOLD
												,	MINIMUM_HOUSEHOLD_FOR_CAPPING
												,	CURRENT_FLAG
												,	EFFECTIVE_FROM
												,	EFFECTIVE_TO
												,	COMMON_PARAMETER_GROUP
											)
					-- values	(	12,	'00:00:00',	'03:59:59',	'Late Evening Weekday',		0,	20,		196,	0,	122,	NULL,	NULL,	20,	120,	10000,	6,	NULL,	1,	'2012-12-25',	'9999-09-09',	'Late Evening'			)
				-- ,	values	(	14,	'00:00:00',	'03:59:59',	'Late Evening Weekend',		0,	20,		196,	1,	122,	NULL,	NULL,	20,	120,	10000,	6,	NULL,	1,	'2012-12-25',	'9999-09-09',	'Late Evening'			)
				-- ,	values	(	1,	'04:00:00',	'05:59:59',	'Early Morning Weekday',	2,	25,		196,	0,	243,	NULL,	NULL,	20,	120,	10000,	6,	NULL,	1,	'2012-12-25',	'9999-09-09',	'Morning to Afternoon'	)
				-- ,	values	(	2,	'04:00:00',	'05:59:59',	'Early Morning Weekend',	2,	25,		196,	1,	243,	NULL,	NULL,	20,	120,	10000,	6,	NULL,	1,	'2012-12-25',	'9999-09-09',	'Morning to Afternoon'	)
				-- ,	values	(	3,	'06:00:00',	'09:59:59',	'Peak Morning Weekday',		2,	25,		196,	0,	243,	NULL,	NULL,	20,	120,	10000,	6,	NULL,	1,	'2012-12-25',	'9999-09-09',	'Morning to Afternoon'	)
				-- ,	values	(	4,	'06:00:00',	'09:59:59',	'Peak Morning Weekend',		2,	25,		196,	1,	243,	NULL,	NULL,	20,	120,	10000,	6,	NULL,	1,	'2012-12-25',	'9999-09-09',	'Morning to Afternoon'	)
				-- ,	values	(	5,	'10:00:00',	'14:59:59',	'Midday Viewing Weekday',	2,	25,		196,	0,	243,	NULL,	NULL,	20,	120,	10000,	6,	NULL,	1,	'2012-12-25',	'9999-09-09',	'Morning to Afternoon'	)
				-- ,	values	(	6,	'10:00:00',	'14:59:59',	'Midday Viewing Weekend',	2,	25,		196,	1,	243,	NULL,	NULL,	20,	120,	10000,	6,	NULL,	1,	'2012-12-25',	'9999-09-09',	'Morning to Afternoon'	)
				-- -- ,	values	(	7,	'15:00:00',	'19:59:59',	'Late Afternoon Weekday',	2,	1,		196,	0,	NULL,	NULL,	NULL,	20,	120,	10000,	6,	NULL,	1,	'2012-12-25',	'9999-09-09',	'Late Afternoon'		)
				-- -- ,	values	(	8,	'15:00:00',	'19:59:59',	'Late Afternoon Weekend',	2,	1,		196,	1,	NULL,	NULL,	NULL,	20,	120,	10000,	6,	NULL,	1,	'2012-12-25',	'9999-09-09',	'Late Afternoon'		)
				-- ,	values	(	7,	'15:00:00',	'19:59:59',	'Late Afternoon Weekday',	2,	NULL,		196,	0,	NULL,	NULL,	NULL,	20,	120,	10000,	6,	NULL,	1,	'2012-12-25',	'9999-09-09',	'Late Afternoon'		)
				-- ,	values	(	8,	'15:00:00',	'19:59:59',	'Late Afternoon Weekend',	2,	NULL,		196,	1,	NULL,	NULL,	NULL,	20,	120,	10000,	6,	NULL,	1,	'2012-12-25',	'9999-09-09',	'Late Afternoon'		)
				-- ,	values	(	9,	'20:00:00',	'20:59:59',	'Prime Time Weekday',		0,	20,		196,	0,	122,	60,		23,		20,	120,	10000,	6,	NULL,	1,	'2012-12-25',	'9999-09-09',	'Prime Time'			)
				-- ,	values	(	10,	'20:00:00',	'20:59:59',	'Prime Time Weekend',		0,	20,		196,	1,	122,	60,		23,		20,	120,	10000,	6,	NULL,	1,	'2012-12-25',	'9999-09-09',	'Prime Time'			)
				-- ,	values	(	15,	'21:00:00',	'21:59:59',	'Prime Time Weekday',		0,	20,		196,	0,	122,	60,		23,		20,	120,	10000,	6,	NULL,	1,	'2012-12-25',	'9999-09-09',	'Prime Time'			)
				-- ,	values	(	17,	'21:00:00',	'21:59:59',	'Prime Time Weekend',		0,	20,		196,	1,	122,	60,		23,		20,	120,	10000,	6,	NULL,	1,	'2012-12-25',	'9999-09-09',	'Prime Time'			)
				-- ,	values	(	16,	'22:00:00',	'22:59:59',	'Prime Time Weekday',		0,	20,		196,	0,	122,	60,		23,		20,	120,	10000,	6,	NULL,	1,	'2012-12-25',	'9999-09-09',	'Prime Time'			)
				-- ,	values	(	18,	'22:00:00',	'22:59:59',	'Prime Time Weekend',		0,	20,		196,	1,	122,	60,		23,		20,	120,	10000,	6,	NULL,	1,	'2012-12-25',	'9999-09-09',	'Prime Time'			)
				-- ,	values	(	11,	'23:00:00',	'23:59:59',	'Late Evening Weekday',		0,	20,		196,	0,	122,	NULL,	NULL,	20,	120,	10000,	6,	NULL,	1,	'2012-12-25',	'9999-09-09',	'Late Evening'			)
				-- ,	values	(	13,	'23:00:00',	'23:59:59',	'Late Evening Weekend',		0,	20,		196,	1,	122,	NULL,	NULL,	20,	120,	10000,	6,	NULL,	1,	'2012-12-25',	'9999-09-09',	'Late Evening'			)

					values	(	1,	'00:00:00',	'00:59:59',	'Late Evening Weekday',		0,	20,		196,	0,	243,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Late Evening'			)
				,	values	(	2,	'00:01:00',	'01:59:59',	'Late Evening Weekday',		0,	20,		196,	0,	243,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Late Evening'			)
				,	values	(	3,	'00:02:00',	'02:59:59',	'Late Evening Weekday',		0,	20,		196,	0,	243,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Late Evening'			)
				,	values	(	4,	'00:03:00',	'03:59:59',	'Late Evening Weekday',		0,	20,		196,	0,	243,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Late Evening'			)
				,	values	(	5,	'00:00:00',	'00:59:59',	'Late Evening Weekend',		0,	20,		196,	1,	243,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Late Evening'			)
				,	values	(	6,	'01:00:00',	'01:59:59',	'Late Evening Weekend',		0,	20,		196,	1,	243,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Late Evening'			)
				,	values	(	7,	'02:00:00',	'02:59:59',	'Late Evening Weekend',		0,	20,		196,	1,	243,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Late Evening'			)
				,	values	(	8,	'03:00:00',	'03:59:59',	'Late Evening Weekend',		0,	20,		196,	1,	243,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Late Evening'			)
				,	values	(	9,	'04:00:00',	'04:59:59',	'Early Morning Weekday',	8,	25,		196,	0,	243,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Morning to Afternoon'	)
				,	values	(	10,	'05:00:00',	'05:59:59',	'Early Morning Weekday',	8,	25,		196,	0,	243,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Morning to Afternoon'	)
				,	values	(	11,	'04:00:00',	'04:59:59',	'Early Morning Weekend',	8,	25,		196,	1,	243,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Morning to Afternoon'	)
				,	values	(	12,	'05:00:00',	'05:59:59',	'Early Morning Weekend',	8,	25,		196,	1,	243,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Morning to Afternoon'	)
				,	values	(	13,	'06:00:00',	'06:59:59',	'Peak Morning Weekday',		0,	25,		196,	0,	243,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Morning to Afternoon'	)
				,	values	(	14,	'07:00:00',	'07:59:59',	'Peak Morning Weekday',		0,	25,		196,	0,	243,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Morning to Afternoon'	)
				,	values	(	15,	'08:00:00',	'08:59:59',	'Peak Morning Weekday',		0,	25,		196,	0,	243,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Morning to Afternoon'	)
				,	values	(	16,	'09:00:00',	'09:59:59',	'Peak Morning Weekday',		0,	25,		196,	0,	243,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Morning to Afternoon'	)
				,	values	(	17,	'06:00:00',	'06:59:59',	'Peak Morning Weekend',		0,	25,		196,	1,	243,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Morning to Afternoon'	)
				,	values	(	18,	'07:00:00',	'07:59:59',	'Peak Morning Weekend',		0,	25,		196,	1,	243,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Morning to Afternoon'	)
				,	values	(	19,	'08:00:00',	'08:59:59',	'Peak Morning Weekend',		0,	25,		196,	1,	243,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Morning to Afternoon'	)
				,	values	(	20,	'09:00:00',	'09:59:59',	'Peak Morning Weekend',		0,	25,		196,	1,	243,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Morning to Afternoon'	)
				,	values	(	21,	'10:00:00',	'10:59:59',	'Midday Viewing Weekday',	0,	25,		196,	0,	243,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Morning to Afternoon'	)
				,	values	(	22,	'11:00:00',	'11:59:59',	'Midday Viewing Weekday',	0,	25,		196,	0,	243,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Morning to Afternoon'	)
				,	values	(	23,	'12:00:00',	'12:59:59',	'Midday Viewing Weekday',	0,	25,		196,	0,	243,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Morning to Afternoon'	)
				,	values	(	24,	'13:00:00',	'13:59:59',	'Midday Viewing Weekday',	0,	25,		196,	0,	243,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Morning to Afternoon'	)
				,	values	(	25,	'14:00:00',	'14:59:59',	'Midday Viewing Weekday',	0,	25,		196,	0,	243,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Morning to Afternoon'	)
				,	values	(	26,	'10:00:00',	'10:59:59',	'Midday Viewing Weekend',	0,	25,		196,	1,	243,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Morning to Afternoon'	)
				,	values	(	27,	'11:00:00',	'11:59:59',	'Midday Viewing Weekend',	0,	25,		196,	1,	243,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Morning to Afternoon'	)
				,	values	(	28,	'12:00:00',	'12:59:59',	'Midday Viewing Weekend',	0,	25,		196,	1,	243,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Morning to Afternoon'	)
				,	values	(	29,	'13:00:00',	'13:59:59',	'Midday Viewing Weekend',	0,	25,		196,	1,	243,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Morning to Afternoon'	)
				,	values	(	30,	'14:00:00',	'14:59:59',	'Midday Viewing Weekend',	0,	25,		196,	1,	243,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Morning to Afternoon'	)
				,	values	(	31,	'15:00:00',	'15:59:59',	'Late Afternoon Weekday',	7,	NULL,	196,	0,	NULL,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Late Afternoon'		)
				,	values	(	32,	'16:00:00',	'16:59:59',	'Late Afternoon Weekday',	7,	NULL,	196,	0,	NULL,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Late Afternoon'		)
				,	values	(	33,	'17:00:00',	'17:59:59',	'Late Afternoon Weekday',	7,	NULL,	196,	0,	NULL,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Late Afternoon'		)
				,	values	(	34,	'18:00:00',	'18:59:59',	'Late Afternoon Weekday',	7,	NULL,	196,	0,	NULL,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Late Afternoon'		)
				,	values	(	35,	'19:00:00',	'19:59:59',	'Late Afternoon Weekday',	7,	NULL,	196,	0,	NULL,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Late Afternoon'		)
				,	values	(	36,	'15:00:00',	'15:59:59',	'Late Afternoon Weekend',	7,	NULL,	196,	1,	NULL,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Late Afternoon'		)
				,	values	(	37,	'16:00:00',	'16:59:59',	'Late Afternoon Weekend',	7,	NULL,	196,	1,	NULL,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Late Afternoon'		)
				,	values	(	38,	'17:00:00',	'17:59:59',	'Late Afternoon Weekend',	7,	NULL,	196,	1,	NULL,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Late Afternoon'		)
				,	values	(	39,	'18:00:00',	'18:59:59',	'Late Afternoon Weekend',	7,	NULL,	196,	1,	NULL,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Late Afternoon'		)
				,	values	(	40,	'19:00:00',	'19:59:59',	'Late Afternoon Weekend',	7,	NULL,	196,	1,	NULL,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Late Afternoon'		)
				,	values	(	41,	'20:00:00',	'20:59:59',	'Prime Time Weekday',		0,	10,		196,	0,	122,	60,		23,		20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Prime Time'			)
				,	values	(	42,	'20:00:00',	'20:59:59',	'Prime Time Weekend',		0,	10,		196,	1,	122,	60,		23,		20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Prime Time'			)
				,	values	(	43,	'21:00:00',	'21:59:59',	'Prime Time Weekday',		0,	6,		196,	0,	122,	60,		23,		20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Prime Time'			)
				,	values	(	44,	'21:00:00',	'21:59:59',	'Prime Time Weekend',		0,	6,		196,	1,	122,	60,		23,		20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Prime Time'			)
				,	values	(	45,	'22:00:00',	'22:59:59',	'Prime Time Weekday',		0,	2,		196,	0,	122,	60,		23,		20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Prime Time'			)
				,	values	(	46,	'22:00:00',	'22:59:59',	'Prime Time Weekend',		0,	2,		196,	1,	122,	60,		23,		20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Prime Time'			)
				,	values	(	47,	'23:00:00',	'23:59:59',	'Late Evening Weekday',		0,	1,		196,	0,	243,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Late Evening'			)
				,	values	(	48,	'23:00:00',	'23:59:59',	'Late Evening Weekend',		0,	1,		196,	1,	243,	NULL,	NULL,	20,	120,	30000,	7,	20000,	1,	'2012-12-25',	'9999-09-09',	'Late Evening'			)
			commit
			

--			-- FOR TEST/DEV PURPOSES
--			insert into CP2_metadata_table  (
--													CAPPING_METADATA_KEY
--												,	START_TIME
--												,	END_TIME
--												,	DAY_PART_DESCRIPTION
--												,	THRESHOLD_NTILE
--												,	THRESHOLD_NONTILE
--												,	PLAYBACK_NTILE
--												,	BANK_HOLIDAY_WEEKEND
--												,	BOX_SHUT_DOWN
--												,	HOUR_IN_MINUTES
--												,	HOUR_24_CLOCK_LAST_HOUR
--												,	MINIMUM_CUT_OFF
--												,	MAXIMUM_CUT_OFF
--												--,		MAXIMUM_ITERATIONS int -- used for scaling
--												--,		MINIMUM_HOUSEHOLD_FOR_SCALING int -- used for scaling
--												,	SAMPLE_MAX_POP
--												,	SHORT_DURATION_CAP_THRESHOLD
--												,	MINIMUM_HOUSEHOLD_FOR_CAPPING
--												,	CURRENT_FLAG
--												,	EFFECTIVE_FROM
--												,	EFFECTIVE_TO
--												,	COMMON_PARAMETER_GROUP
--											)
--			select
--						CAPPING_METADATA_KEY
--					,	START_TIME
--					,	END_TIME
--					,	DAY_PART_DESCRIPTION
--					,	THRESHOLD_NTILE
--					,	THRESHOLD_NONTILE
--					,	PLAYBACK_NTILE
--					,	BANK_HOLIDAY_WEEKEND
--					,	BOX_SHUT_DOWN
--					,	HOUR_IN_MINUTES
--					,	HOUR_24_CLOCK_LAST_HOUR
--					,	MINIMUM_CUT_OFF
--					,	MAXIMUM_CUT_OFF
--					,	SAMPLE_MAX_POP
--					,	SHORT_DURATION_CAP_THRESHOLD
--					,	MINIMUM_HOUSEHOLD_FOR_CAPPING
--					,	CURRENT_FLAG
--					,	EFFECTIVE_FROM
--					,	EFFECTIVE_TO
--					,	COMMON_PARAMETER_GROUP
--			from    tanghoi.CP2_metadata_table_referenceCopy
--			commit

			execute M00_2_output_to_logger '@ M00 : Creating table CP2_metadata_table...DONE'
			COMMIT


        end


        /****************** PART A05: BOX LOOKUP FOR PROFILING ******************/

        -- There are some things we need for account profiling, but we're not going to
        -- denormalise them onto the viewing tables... probably...
        execute M00_2_output_to_logger '@ M00 : Creating table CP2_box_lookup...'
        COMMIT
		
		execute DROP_LOCAL_TABLE 'CP2_box_lookup'
        commit

        -- This table used to be called "all_boxes_info" but this one is slightly better.
        create table CP2_box_lookup (
                                        subscriber_id                       bigint          primary key
                                        ,account_number                     varchar(20)     not null
                                        ,service_instance_id                varchar(50)
                                        ,PS_flag                            varchar(1)      default 'U'
                                        -- What else do we use at box level? At account level even?
        )
        commit
        -- Also note that this guy isn't built in the regular daily cycle; it's built
        -- as it's own thing, and it's refreshed as each 7th day is processed. The
        -- profiling is done as of the beginning of the period, for consistnecy with
        -- Scaling workstream's approach to box segmentations.


        -- Because some of the updates come from the customer database via the service_instance_id link:
        create index service_instance_index on CP2_box_lookup (service_instance_id)
        commit

        execute M00_2_output_to_logger '@ M00 : Creating table CP2_box_lookup...DONE'
        COMMIT



        -- There are also a couple of other collection / processing tables that we need
        -- because they can't be temporary as we populate them dynamically.
        execute M00_2_output_to_logger '@ M00 : Creating table CP2_relevant_boxes...'
        COMMIT
        
		execute DROP_LOCAL_TABLE 'CP2_relevant_boxes'
        commit

        create table CP2_relevant_boxes (
                                        account_number                      varchar(20)
                                        ,subscriber_id                      bigint
                                        ,service_instance_id                varchar(50)
        )
        commit
        --
        execute M00_2_output_to_logger '@ M00 : Creating table CP2_relevant_boxes...DONE'
        COMMIT



        /****************** PART C01: TABLES CONTAINING THE CAPS BY BUCKET ******************/

        -- Week caps is important enough to want to get split out into his own permanent-like
        -- table... This table holds the caps we calculate for each "bucket"
        execute M00_2_output_to_logger '@ M00 : Creating table CP2_calculated_viewing_caps...'
        COMMIT
        
		execute DROP_LOCAL_TABLE 'CP2_calculated_viewing_caps'
		COMMIT

        create table CP2_calculated_viewing_caps (
                                        -- If we ever want to try to roll out some kind of bucket key:
                                        sub_bucket_id                       integer identity primary key
                                        ,bucket_id                          integer         -- We don't use pack_grp or box_subscription in the buckets, this gets picked up based just on event_start_day, event_start_hour, initial_genre and Live

                                        -- The composite PK columns: these define a "bucket"
                                        ,Live                               bit
                                        ,event_start_day                    tinyint
                                        ,event_start_hour                   tinyint
                                        ,box_subscription                   varchar(1)      -- 'P' or 'S' or 'U'
                                        ,pack_grp                           varchar(30)
                                        ,initial_genre                      varchar(25)

                                        -- Important derived columns
                                        ,max_dur_mins                       integer         -- the length of the cap to be applied, in minutes

        )
        commit
        -- That table will hold all the caps for one day, since we're looping to build cap
        -- viewing data one day at a a time.

        
        -- Indices: still not convinced we need all of these, that they all do anything useful...
        create hng index idx1 on CP2_calculated_viewing_caps(event_start_day)   commit
        create hng index idx2 on CP2_calculated_viewing_caps(event_start_hour)  commit
        create hng index idx4 on CP2_calculated_viewing_caps(box_subscription)  commit
        create hng index idx5 on CP2_calculated_viewing_caps(pack_grp)                  commit
        create hng index idx6 on CP2_calculated_viewing_caps(initial_genre)             commit

        -- This one, however, supports the application of caps to viewing data:
        create unique index forcing_uniqueness on CP2_calculated_viewing_caps
                                        (event_start_hour, event_start_day, initial_genre, box_subscription, pack_grp, Live)
        commit
        -- Unique forces the bucketing we're expecting to observe. But this one:
        create index for_the_joining_group on CP2_calculated_viewing_caps
                                        (bucket_id, box_subscription, pack_grp)
        commit
        -- That's the one that actually gets used in joins, since the bucket_ID does
        -- a lot of simplification for the DB.

        -- I dunno why anyone else needs this, but they don't need more than SELECT
        grant select on CP2_calculated_viewing_caps to vespa_group_low_security
        commit
        -- 

        execute M00_2_output_to_logger '@ M00 : Creating table CP2_calculated_viewing_caps...DONE'
        COMMIT



        /****************** PART C02: CAPPING BUCKETS LOOKUP ******************/

        -- This guy is a composite key that summarises event_start_hour, event_start_day,
        -- initial_genre and live into one integer that's easy to use (/index/join). Helps
        -- reduce the number of columns needed in some summaries and joins by 3, so that's
        -- a good thing.
        execute M00_2_output_to_logger '@ M00 : Creating table CP2_capping_buckets...'
        COMMIT
		
		execute DROP_LOCAL_TABLE 'CP2_capping_buckets'
        commit
        
        create table CP2_capping_buckets (
                                        bucket_id                           integer identity primary key
                                        ,event_start_hour                   tinyint not null
                                        ,event_start_day                    tinyint not null
                                        ,initial_genre                      varchar(30) not null
                                        ,live                               bit
        )
        commit

        -- So this table still isn't as wildely used as it could be in the build, it's
        -- implemented in a few places to facilitate a few things, but the big messy
        -- middle bit of the code which makes the caps according to the various rules
        -- doesn't really use it. But stuff there is split up enough to not really
        -- need it. Maybe pushing it back onto the viewing data will need it, but we
        -- are okay so far.

        create unique index for_uniqueness on CP2_capping_buckets
                                        (event_start_hour, event_start_day, initial_genre, live)
        commit
        -- 

        execute M00_2_output_to_logger '@ M00 : Creating table CP2_capping_buckets...DONE'
        COMMIT

        
        

        /****************** PART C03: HOLDING PEN PRIOR TO DYNAMIC TABLE ******************/

        -- This table is where we prepare all the cap details that we want, just before we
        -- chuck it all into the dynamically named daily caps table.
        execute M00_2_output_to_logger '@ M00 : Creating table CP2_capped_data_holding_pen...'
        COMMIT
		
		execute DROP_LOCAL_TABLE 'CP2_capped_data_holding_pen'
        commit
        
        create table CP2_capped_data_holding_pen (
                                        cb_row_id                   bigint              primary key     -- Links to the viewing data daily table of the same day
                                        ,subscriber_id              bigint              not null
                                        ,account_number             varchar(20)         not null
                                        ,scaling_segment_id         bigint                              -- To help with the MBM proc builds....                     -- NYIP!
                                        ,scaling_weighting          numeric(13,6)                               --                                                          -- NYIP!
                                        ,programme_trans_sk         bigint                              -- To make the minute-by-minute stuff real easy
                                        ,viewing_starts             datetime                            -- Capped viewing start time
                                        ,viewing_stops              datetime
                                        ,viewing_duration           bigint                              -- Capped viewing in seconds
                                        ,BARB_minute_start          datetime                            -- Viewing with Capping treatment + BARB minute allocation  -- NYIP!
                                        ,BARB_minute_end            datetime                            -- BARB minutes are pulled back to broadcast time           -- NYIP!
                                        ,timeshifting               varchar(10)                         -- 'LIVE' or 'VOSDAL' (same day as live) or 'PLAYBACK7' (playback within 7 days) or 'PLAYBACK28' (otherwise)
                                        ,capped_flag                tinyint                             -- 0-3 depending on capping treatment, or 11 if there are lingering events that are not yet treated
                                        ,capped_event_end_time      datetime
                                        -- So those are the columns that go into the dynamically named table,
                                        -- but there are a few others used to process those out:
                                        ,adjusted_event_start_time  datetime
                                        ,X_Adjusted_Event_End_Time  datetime
                                        ,x_viewing_start_time       datetime
                                        ,x_viewing_end_time         datetime
                                        -- Other things we only need to maintain our control totals:
                                        ,program_air_date           date
                                        ,live                       tinyint
                                        ,genre                      varchar(50)
        )
        commit

        -- Indices? what else are we doing here?

        execute M00_2_output_to_logger '@ M00 : Creating table CP2_capped_data_holding_pen...DONE'
        COMMIT



        /****************** PART D01: STRUCTURAL PLACEHOLDER FOR DALIES ******************/

        -- So these guys are built dynamically for each day, but the sctucture should be
        -- identical to this for each day:
        /* (Commented out because we don't actually build the things like this)
        create table vespa_analysts.Vespa_daily_augs_YYYYMMDD (
                                        cb_row_id                   bigint              primary key     -- Links to the viewing data daily table of the same day
                                        ,subscriber_id              bigint              not null
                                        ,account_number             varchar(20)         not null
                                        ,programme_trans_sk         bigint                              -- to help out with the minute-by-minute stuff
                                        ,scaling_segment_id         bigint                              -- To help with the MBM proc builds....                         -- NYIP!
                                        ,scaling_weighting          numeric(13,6)                               -- Also assisting with the MBM proc builds                      -- NYIP!
                                        ,viewing_starts             datetime                            -- Capped viewing start time
                                        ,viewing_stops              datetime
                                        ,viewing_duration           bigint                              -- Capped viewing in seconds
                                        ,BARB_minute_start          datetime                            -- Viewing with Capping treatment + BARB minute allocation      -- NYIP!
                                        ,BARB_minute_end            datetime                                                                                            -- NYIP!
                                        ,timeshifting               varchar(10)                         -- 'LIVE' or 'VOSDAL' (same day as live) or 'PLAYBACK7' (playback within 7 days) or 'PLAYBACK28' (otherwise)
                                        ,capped_flag                tinyint                             -- 0-2 depending on capping treatment: 0 -> event not capped, 1 -> event capped but doesn't effect viewing, 2 -> event capped & shortens viewing, 3 -> event capped & excludes viewing (actually 3 doesn't turn up in the table, but that's what it means during processing)
                                        ,capped_event_end_time      datetime                            -- Only populated for capped events
        );

        create index for_MBM            on vespa_analysts.Vespa_daily_augs_YYYYMMDD (scaling_segment_id, viewing_starts, viewing_stops)
        create index for_barb_MBM       on vespa_analysts.Vespa_daily_augs_YYYYMMDD (scaling_segment_id, BARB_minute_start, BARB_minute_end)
        create index subscriber_id      on vespa_analysts.Vespa_daily_augs_YYYYMMDD (subscriber_id);
        create index account_number     on vespa_analysts.Vespa_daily_augs_YYYYMMDD (account_number);

        grant select on Vespa_daily_augs_YYYYMMDD to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg;

        commit;

        */
        -- Initial studies show these tables to be about 800MB per day - on the pre-rampup panel of 210k
        -- boxes returning data (190k accounts). That's quite a bit, means we're guessing at... 150GB of
        -- capping cache stuff to go back to November 2011. Awesome. That's not actually a whole lot in
        -- the scheme of things (though yeah, it's a lot more than the scaling builds.)

        /****************** PART Q01: TABLES TRACKING VIEWING TOTALS ******************/

        -- We're storing the totals of viewing for each major stage of processing, and also for
        -- each capping strand
        execute M00_2_output_to_logger '@ M00 : Creating table CP2_QA_viewing_control_totals...'
        COMMIT
		
		execute DROP_LOCAL_TABLE 'CP2_QA_viewing_control_totals'
        commit

        create table CP2_QA_viewing_control_totals (
                                        build_date                  date                not null -- The date that the caps apply to
                                        ,data_state                 varchar(20)         not null
                                        ,program_air_date           date                not null
                                        ,live                       bit
                                        ,genre                      varchar(25)
                                        ,viewing_records            int
                                        ,total_viewing_in_days      decimal(8,2)        not null
                                        ,primary key (build_date, data_state, program_air_date, live, genre)
        )
        commit

        execute M00_2_output_to_logger '@ M00 : Creating table CP2_QA_viewing_control_totals...DONE'
        COMMIT

        /* What we expect for the data states in the above table (for each build_date):
                                        *. '1.) Collect' should match '2.) Pre-Cap'
                                        *. '4a.) Uncapped' + '4c.) Truncated' should add up to '3.) Capped',
                                        *. '4a.) Uncapped' + '4b.) Excluded' + '4c.) Truncated' + '4d.) T-Margin' should add up to '1.) Collect'
        They should match pretty much exactly, since we've rounded everything to 2dp in hours.
        */

        -- We're also tracking how many viewing events fal into each category of the capping
        execute M00_2_output_to_logger '@ M00 : Creating table CP2_QA_viewing_control_distribs...'
        COMMIT
		
		execute DROP_LOCAL_TABLE 'CP2_QA_viewing_control_distribs'
        commit
        
        create table CP2_QA_viewing_control_distribs (
                                        build_date                  date                not null -- The date that the caps apply to
                                        ,data_state                 varchar(20)         not null -- '1.) Uncapped' or '2.) Capped'
                                        ,duration_interval          int                 not null -- batched into 10s chunks, so 0 means viewing durations between 0s and 10s
                                        ,viewing_events             int                          -- Er... but these are not events, but viewing bits... oh well
                                        ,primary key (build_date, data_state, duration_interval)
        )
        commit

        execute M00_2_output_to_logger '@ M00 : Creating table CP2_QA_viewing_control_distribs...DONE'
        COMMIT


        -- Now also doign the same thing not for viewing items, but for event durations
        -- and with a resolution of 1 minute because these things are much longer.
        execute M00_2_output_to_logger '@ M00 : Creating table CP2_QA_event_control_distribs...'
        COMMIT
		
        execute DROP_LOCAL_TABLE 'CP2_QA_event_control_distribs'
        commit

        create table CP2_QA_event_control_distribs (
                                        build_date                  date                not null -- The date that the caps apply to
                                        ,data_state                 varchar(20)         not null -- '1.) Uncapped' or '2.) Capped'
                                        ,duration_interval          int                 not null -- batched into 1m chunks, so 0 means viewing durations between 0s and 1 minute
                                        ,viewing_events             int
                                        ,primary key (build_date, data_state, duration_interval)
        )
        commit

        grant select on CP2_QA_viewing_control_totals      to vespa_group_low_security  commit
        grant select on CP2_QA_viewing_control_distribs    to vespa_group_low_security  commit
        grant select on CP2_QA_event_control_distribs      to vespa_group_low_security  commit

        -- 
        execute M00_2_output_to_logger '@ M00 : Creating table CP2_QA_event_control_distribs...DONE'
        COMMIT

        
        
        /****************** PART Q02: BARB MINUTE BY MINUTE CONTROL TOTALS ******************/

        -- Tables which track the daily viewing totals before and after the BARB minute batching

        /****************** PART Q03: HISTORICAL TRACKING OF DAILY TOTAL VIEWING ******************/

        -- Tables which track the total viewing for the various stages of processing through both
        -- capping and BARB minute allocation. These averages are just over people who watch *some*
        -- TV at all, so will be higher than the average TV watching since boxes supplying only
        -- logs don't get considered here. Also notice that this is daily viewing *on panel* and
        -- the Sky Base average isn't calculated here (because it depends on Scaling and we're not
        -- sure that we have the appropriate eights prepared when the capping gets done).
        execute M00_2_output_to_logger '@ M00 : Creating table CP2_QA_daily_average_viewing...'
        COMMIT
		
        execute DROP_LOCAL_TABLE 'CP2_QA_daily_average_viewing'
        commit

        create table CP2_QA_daily_average_viewing (
                                        build_date                  date                not null primary key
                                        ,subscriber_count           int                 not null        -- Number of boxes noticed in the build
                                        ,average_uncleansed_viewing int                 default null    -- All the viewing counts are in minutes per box
                                        ,average_uncapped_viewing   int                 default null
                                        ,average_capped_viewing     int                 default null
                                        ,average_BARB_viewing       int                 default null    -- IE the average viewing per box after BARB minute-by-minute processing has been applied (NYIP)
        )
        commit

        grant select on CP2_QA_daily_average_viewing       to vespa_group_low_security  commit

        execute M00_2_output_to_logger '@ M00 : Creating table CP2_QA_daily_average_viewing...DONE'
        COMMIT



        /****************** PART Q04: HISTORICAL TRACKING OF MAGNITUDE OF CALCULATED CAPS ******************/

        -- We want to know how big the various caps are that we're calculating, just
        -- to see how much viewing we think is okay for each case
        execute M00_2_output_to_logger '@ M00 : Creating table CP2_QA_viewing_control_cap_distrib...'
        COMMIT
		
        execute DROP_LOCAL_TABLE 'CP2_QA_viewing_control_cap_distrib'
        commit
        
        create table CP2_QA_viewing_control_cap_distrib (
                                        build_date                  date                not null -- The date that the caps apply to
                                        ,max_dur_mins               int                 not null
                                        ,cap_instances              int                 not null
                                        ,primary key (build_date, max_dur_mins)
        )
        commit

        execute M00_2_output_to_logger '@ M00 : Creating table CP2_QA_viewing_control_cap_distrib...DONE'
        COMMIT



        execute M00_2_output_to_logger '@ M00 : Creating table Vespa_Daily_Augs...'
        COMMIT
		
		execute DROP_LOCAL_TABLE 'Vespa_Daily_Augs'
        commit

        create table Vespa_Daily_Augs (
                Cb_Row_Id                    bigint           --   primary key    -- Links to the viewing data daily table of the same day
				,target_date				 date
				,iteration_number			 int
                ,Account_Number              varchar(20)         not null
                ,Subscriber_Id               bigint              not null
                ,Programme_Trans_Sk          bigint                             -- to help out with the minute-by-minute stuff
                ,Timeshifting                varchar(10)
                ,Viewing_Starts              datetime                           -- Capped viewing start time (UTC time)
                ,Viewing_Stops               datetime
                ,Viewing_Duration            bigint                             -- Capped viewing in seconds
                ,Capped_Flag                 tinyint                            -- 0-2 depending on capping treatment: 0 -> event not capped, 1 -> event capped but does not effect viewing, 2 -> event capped & shortens viewing, 3 -> event capped & excludes viewing (actually 3 will not turn up in the table, but that is what it means during processing)
                ,Capped_Event_End_Time       datetime                           -- Only populated for capped events
                ,Scaling_Segment_Id          bigint                             -- To help with the MBM proc builds.... -- NYIP!
                ,Scaling_Weighting           float                              -- Also assisting with the MBM proc builds -- NYIP!
                ,BARB_Minute_Start           datetime                           -- Viewing with Capping treatment + BARB minute allocation
                ,BARB_Minute_End             datetime                            --
                ,adjusted_event_start_time  datetime
                ,X_Adjusted_Event_End_Time  datetime
                ,x_viewing_start_time       datetime
                ,x_viewing_end_time         datetime
                -- Other things we only need to maintain our control totals:
                ,program_air_date           date
                ,live                       tinyint
                ,genre                      varchar(50)
        )
        commit


        create hg   index idx11 on Vespa_Daily_Augs (Subscriber_Id)      commit
        create hg   index idx12 on Vespa_Daily_Augs (Account_Number)     commit
        create hg   index idx13 on Vespa_Daily_Augs (Programme_Trans_Sk) commit
        create dttm index idx14 on Vespa_Daily_Augs (Viewing_Starts)     commit
        create dttm index idx15 on Vespa_Daily_Augs (Viewing_Stops)      commit

        execute M00_2_output_to_logger '@ M00 : Creating table Vespa_Daily_Augs...DONE'
        COMMIT

        execute DROP_LOCAL_TABLE 'VESPAvsBARB_metrics_table'
		COMMIT

        execute M00_2_output_to_logger '@ M00 : Creating table VESPAvsBARB_metrics_table...'
        COMMIT

		create table    VESPAvsBARB_metrics_table   (
															iteration_number						int
														,	UTC_DATEHOURMIN							timestamp
														,	UTC_DATEHOUR							timestamp
														,	UTC_DAY_OF_INTEREST						date
														,	stream_type								varchar(8)
														,	scaled_account_flag						bit
														,	BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME	double
														,	BARB_WEIGHTED_MINUTES_BY_EVENT_START_TIME	double
														,	VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME	double
														,	VESPA_WEIGHTED_MINUTES_BY_EVENT_START_TIME	double
														,	VESPA_UNWEIGHTED_MINUTES_BY_VIEWING_TIME	double
														,	VESPA_UNWEIGHTED_MINUTES_BY_EVENT_START_TIME	double
														,	percentageDiff_by_minute_stream			float
														,	variance_by_minute_stream				double
														,	percentageDiff_by_minute				float
														,	variance_by_minute						double
														,	percentageDiff_by_hour_stream			float
														,	variance_by_hour_stream					double
														,	percentageDiff_by_hour					float
														,	variance_by_hour						double
														,	percentageDiff_by_day_stream			float
														,	variance_by_day_stream					double
														,	percentageDiff_by_day					float
														,	variance_by_day							double
													)
		commit
        
		execute M00_2_output_to_logger '@ M00 : Creating table VESPAvsBARB_metrics_table...DONE'
        COMMIT

        -- CP2_metadata_iterations_diff
        execute M00_2_output_to_logger '@ M00 : Creating table CP2_metadata_iterations_diff...'
        commit

        if      (
                                @hard_initialise        =       1
                        and     exists  (
                                                        select  1
                                                        from    sysobjects
                                                        where
                                                                        [name]                  =       'CP2_metadata_iterations_diff'
                                                                and uid                         =       user_id()
                                                                and     upper([type])   =       'U'
                                                )
                )
                        drop table CP2_metadata_iterations_diff
        commit

        if not  exists  (
                                        select  1
                                        from    sysobjects
                                        where
                                                        [name]                  =       'CP2_metadata_iterations_diff'
                                                and uid                         =       user_id()
                                                and     upper([type])   =       'U'
                                )
        begin

                                        -- table does not exist, let's create it!!!
        create table CP2_metadata_iterations_diff (
                        primary_row_id                                                  int                     primary key identity -- primary key
                ,       iteration_number                                                int
                ,       grouping_key	                                                tinyint
				,	grouping_key_start_time											time
				,	grouping_key_end_time											time
                ,       BANK_HOLIDAY_WEEKEND                                    		int
                ,       LIVE_PLAYBACK		                                    		varchar(15)
                ,       THRESHOLD_NTILE                                                 int
                ,       THRESHOLD_NONTILE                                               int
                ,       PLAYBACK_NTILE                                                  int
                ,       short_duration_cap_threshold                                                   int
                ,       SUM_BARB                                                        bigint
                ,       SUM_VESPA                                                       bigint
                ,       VARIANCE_DIFF                                                   DOUBLE
                ,       PERCENTAGE_DIFF                                                 DOUBLE
        )
        commit
        create hg   index idx1_METDIF on CP2_metadata_iterations_diff (iteration_number)        commit
                
        execute M00_2_output_to_logger '@ M00 : Creating table CP2_metadata_iterations_diff...DONE'
        COMMIT

		end

        execute M00_2_output_to_logger '@ M00 : Truncating table CP2_metadata_iterations_diff...'
        COMMIT
		truncate table CP2_metadata_iterations_diff
		commit
        execute M00_2_output_to_logger '@ M00 : Truncating table CP2_metadata_iterations_diff...DONE'
        COMMIT
		
        ---------------------------------------------------------------
        -- Historic / persistent tables
        ---------------------------------------------------------------
        
        -- VESPAvsBARB_metrics_historic_table
        execute M00_2_output_to_logger '@ M00 : Creating table VESPAvsBARB_metrics_historic_table...'
        COMMIT

        -- Check for hard reset and drop table if requested
        if      (
                                @hard_initialise        =       1
                        and     exists  (
                                                        select  1
                                                        from    sysobjects
                                                        where
                                                                        [name]                  =       'VESPAvsBARB_metrics_historic_table'
                                                                and uid                         =       user_id()
                                                                and     upper([type])   =       'U'
                                                )
                )
                        drop table VESPAvsBARB_metrics_historic_table
        commit
        
        
        if not  exists  (
                                        select  1
                                        from    sysobjects
                                        where
                                                        [name]                  =       'VESPAvsBARB_metrics_historic_table'
                                                and uid                         =       user_id()
                                                and     upper([type])   =       'U'
                                )
        begin
        
        -- same as the normal one, simply this will store more data
        create table VESPAvsBARB_metrics_historic_table	(
																iteration_number							int
															,	UTC_DATEHOURMIN								timestamp
															,	UTC_DATEHOUR								timestamp
															,	UTC_DAY_OF_INTEREST							date
															,	stream_type									varchar(8)
															,	scaled_account_flag							bit
															,	BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME		double
															,	BARB_WEIGHTED_MINUTES_BY_EVENT_START_TIME	double
															,	VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME		double
															,	VESPA_WEIGHTED_MINUTES_BY_EVENT_START_TIME	double
															,	VESPA_UNWEIGHTED_MINUTES_BY_VIEWING_TIME		double
															,	VESPA_UNWEIGHTED_MINUTES_BY_EVENT_START_TIME	double
															,	percentageDiff_by_minute_stream				float
															,	variance_by_minute_stream					double
															,	percentageDiff_by_minute					float
															,	variance_by_minute							double
															,	percentageDiff_by_hour_stream				float
															,	variance_by_hour_stream						double
															,	percentageDiff_by_hour						float
															,	variance_by_hour							double
															,	percentageDiff_by_day_stream				float
															,	variance_by_day_stream						double
															,	percentageDiff_by_day						float
															,	variance_by_day								double
														)
        commit
        
        create hg   index idx21 on VESPAvsBARB_metrics_historic_table (iteration_number) commit
        create dttm index idx23 on VESPAvsBARB_metrics_historic_table (UTC_DATEHOUR)  commit
        create dttm index idx24 on VESPAvsBARB_metrics_historic_table (UTC_DATEHOURMIN)  commit
        create date index idx22 on VESPAvsBARB_metrics_historic_table (UTC_DAY_OF_INTEREST)     commit

        execute M00_2_output_to_logger '@ M00 : Creating table VESPAvsBARB_metrics_historic_table...DONE'
        COMMIT
        end
        


        -- CP2_metadata_historic_table
        execute M00_2_output_to_logger '@ M00 : Creating table CP2_metadata_historic_table...'
        commit

        -- Check for hard reset and drop table if requested
        if      (
                                @hard_initialise        =       1
                        and     exists  (
                                                        select  1
                                                        from    sysobjects
                                                        where
                                                                        [name]                  =       'CP2_metadata_historic_table'
                                                                and uid                         =       user_id()
                                                                and     upper([type])   =       'U'
                                                )
                )
                        drop table CP2_metadata_historic_table
        commit

        if not  exists  (
                                        select  1
                                        from    sysobjects
                                        where
                                                        [name]                  =       'CP2_metadata_historic_table'
                                                and uid                         =       user_id()
                                                and     upper([type])   =       'U'
                                )
        begin

                                        -- table does not exist, let's create it!!!
        create table CP2_metadata_historic_table	(
															primary_row_id					int				primary key identity -- primary key
														,	iteration_number				int
														,   row_id							int                     -- this refers to the row_id in the current table from which we save the parameters in this historic table
														,   CAPPING_METADATA_KEY			int --
														,   START_TIME						time-- Start time that the day part, used when joining to events data to determine which metadata record to use
														,   END_TIME						time-- End time of the day part, used when joining to events data to determine which metadata record to use
														,   DAY_PART_DESCRIPTION			varchar(25)
														,   THRESHOLD_NTILE					int
														,   THRESHOLD_NONTILE				int
														,   PLAYBACK_NTILE					int
														,   BANK_HOLIDAY_WEEKEND			int
														,   BOX_SHUT_DOWN					int
														,   HOUR_IN_MINUTES					int
														,   HOUR_24_CLOCK_LAST_HOUR			int
														,   MINIMUM_CUT_OFF					int
														,   MAXIMUM_CUT_OFF					int
														--  MAXIMUM_ITERATIONS int -- used for scaling
														--  MINIMUM_HOUSEHOLD_FOR_SCALING int -- used for scaling
														,   SAMPLE_MAX_POP					int
														,   SHORT_DURATION_CAP_THRESHOLD	int
														,   MINIMUM_HOUSEHOLD_FOR_CAPPING	int 			default null
														,   CURRENT_FLAG					int
														,   EFFECTIVE_FROM					date
														,   EFFECTIVE_TO					date
	/*													, 	RECORDED_NTILE					int				default 198
														,	VOSDAL_1H_NTILE					int				default 190
														,	VOSDAL_1H_24H_NTILE				int				default 194
														,	PUSHVOD_NTILE					int				default 199
	*/
														,	COMMON_PARAMETER_GROUP			varchar(255)	default null
													)
        commit
        create hg   index idx31 on CP2_metadata_historic_table (iteration_number)        commit
                
        execute M00_2_output_to_logger '@ M00 : Creating table CP2_metadata_historic_table...DONE'
        COMMIT

        end

        execute M00_2_output_to_logger '@ M00 : Creating table CP2_metadata_iterations_diff_historic_table...'
        commit

       -- Check for hard reset and drop table if requested
        if      (
                                @hard_initialise        =       1
                        and     exists  (
                                                        select  1
                                                        from    sysobjects
                                                        where
                                                                        [name]                  =       'CP2_metadata_iterations_diff_historic_table'
                                                                and uid                         =       user_id()
                                                                and     upper([type])   =       'U'
                                                )
                )
                        drop table CP2_metadata_iterations_diff_historic_table
        commit

        if not  exists  (
                                        select  1
                                        from    sysobjects
                                        where
                                                        [name]                  =       'CP2_metadata_iterations_diff_historic_table'
                                                and uid                         =       user_id()
                                                and     upper([type])   =       'U'
                                )
        begin

                                        -- table does not exist, let's create it!!!
        create table CP2_metadata_iterations_diff_historic_table (
                        primary_row_id                                                  int                     primary key identity -- primary key
                ,       target_date       												date
                ,	analysis_window											    tinyint
                ,       iteration_number                                                int
                ,       grouping_key	                                                tinyint
				,	grouping_key_start_time											time
				,	grouping_key_end_time											time
                ,       BANK_HOLIDAY_WEEKEND                                    		int
                ,       LIVE_PLAYBACK		                                    		varchar(15)
                ,       THRESHOLD_NTILE                                                 int
                ,       THRESHOLD_NONTILE                                               int
                ,       PLAYBACK_NTILE                                                  int
                ,       short_duration_cap_threshold                                                   int
                ,       SUM_BARB                                                        bigint
                ,       SUM_VESPA                                                       bigint
                ,       VARIANCE_DIFF                                                   DOUBLE
                ,       PERCENTAGE_DIFF                                                 DOUBLE
        )
        commit
        create hg   index idx1_METDIF_hist on CP2_metadata_iterations_diff_historic_table (iteration_number)        commit
                
        execute M00_2_output_to_logger '@ M00 : Creating table CP2_metadata_iterations_diff...DONE'
        commit

        end
				


        -- Reset CP2_accounts table
		if exists  (
                                        select  1
                                        from    sysobjects
                                        where
                                                        [name]                  =       'CP2_accounts'
                                                and uid                         =       user_id()
                                                and     upper([type])   =       'U'
                                )
        begin
		
			execute DROP_LOCAL_TABLE 'CP2_accounts'
			commit
		end
		
		create table CP2_accounts (
						account_number    varchar(20)
				,       adsmart_scaling_weight   numeric(13,6)
				,       rand_num      float
				,       reference_date  date
				)
		commit

		create hg   index idx1_CP2_accounts on CP2_accounts (account_number)        commit
		create hg index hgran ON CP2_accounts(rand_num)	commit

		-- truncate table CP2_accounts
		-- commit




	----------------------------------------------------------------------------------------------------------
	-- Define parameter space for optimistaion -- MOVE TO INITIALISATION MODULE ONCE DEVELOPMENT IS COMPLETE
	----------------------------------------------------------------------------------------------------------
	

	-- Define the range of values that our parameters can take

	execute M00_2_output_to_logger '@ M07_8: Initialise parameter space...'
	commit

	execute DROP_LOCAL_TABLE 'CP2_metadata_parameter_space'
	commit
	
	create table CP2_metadata_parameter_space	(
														parameter_name	varchar(255)
													,	min_value		int
													,	max_value		int
													,	step_size		int		default	1
												)
	commit
	
	insert into CP2_metadata_parameter_space
			values	('THRESHOLD_NTILE',		0,	30,		1)
		,	values	('THRESHOLD_NONTILE',	0,	30,		1)
		,	values	('PLAYBACK_NTILE',		1,	200,	1)
	commit
	
	grant select on CP2_metadata_parameter_space to vespa_group_low_security
	commit
	
	create unique lf index lf1 on CP2_metadata_parameter_space(parameter_name)
	commit
	
	



	-- Now create and populate our parameter space with initial randonmised values

	execute M00_2_output_to_logger '@ M07_8: Initialise parameter scores...'
	commit

	execute DROP_LOCAL_TABLE 'CP2_metadata_parameter_scores'
	commit
	
	create table CP2_metadata_parameter_scores	(
														row_id					int				primary key		identity
													,	parameter_name			varchar(255)
													,	parameter_value			int
													,	COMMON_PARAMETER_GROUP	varchar(255)	default null
													,	bank_holiday_weekend	bit
													,	score					double			default	null
												)
	commit
	

	insert into	CP2_metadata_parameter_scores	(
														parameter_name
													,	parameter_value
													,	COMMON_PARAMETER_GROUP
													,	bank_holiday_weekend
												)
	select
			MET.parameter_name
		,   t0.row_num      as  parameter_value
		,	c.COMMON_PARAMETER_GROUP
		,	d.bank_holiday_weekend
	from
					CP2_metadata_parameter_space	MET
		cross join	(
						select	row_num
						from
										(
											select
													min(min_value)		as	min_v
												,	max(max_value)		as	max_v
												,	min(step_size)		as	stepstep
											from	CP2_metadata_parameter_space
										)														a
							cross join  sa_rowgenerator(min_v,max_v,stepstep)    				b
					)								t0
		cross join	(
						select	distinct	COMMON_PARAMETER_GROUP
						from	CP2_metadata_table
					)								c
		cross join	(
						select	cast(row_num as bit)	as	bank_holiday_weekend
						from	sa_rowgenerator(0,1,1)
					)								d
	where	parameter_value	between	MET.min_value
							and		MET.max_value
	commit
	
	
	
	update	CP2_metadata_parameter_scores
	set		score	=	rand(row_id*datepart(us,now()))
	commit



end; -- procedure
commit;

grant execute on V306_CP2_M00_Initialise to vespa_group_low_security;
commit;
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




**Module:                               M00_2_output_to_logger

This module installs a procedure to output debug messages both to logger and to client


*/

create or replace procedure M00_2_output_to_logger              
        @output_msg             varchar(2000)
    ,@CP2_build_ID      bigint = NULL   -- Logger ID (so all builds end up in same queue)
        ,@logger_level          tinyint = 3  -- event level
as begin

        declare @output_with_timestamp varchar(2048)
        commit

        set @output_with_timestamp	=	CAST(NOW() AS TIMESTAMP) || ' | ' || @output_msg
        commit

        if @CP2_build_ID is not null
        begin
                execute logger_add_event @CP2_build_ID, @logger_level, cast(@output_with_timestamp as varchar(200))
                        -- Check that there is data in viewing table
                commit --; --^^ to be removed
        end
        
        MESSAGE @output_with_timestamp TO CLIENT
        commit

-- check if the debug output table exists, if not create it.
        if not  exists  (
							select  1
							from    sysobjects
							where
											[name]                  =       'CP2_debug_output_table'
									and uid                         =       user_id()
									and     upper([type])   =       'U'
						)
        begin
                                        -- table does not exist, let's create it!!!
        create table CP2_debug_output_table (
                        row_id          int                     primary key identity -- primary key
                ,       msg                     varchar(2048)
        )
        commit

        end



        insert into CP2_debug_output_table(msg)
        select @output_with_timestamp
        commit

end

commit;

grant execute on M00_2_output_to_logger to vespa_group_low_security;
commit;
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




**Module:                               M01_Process_Manager

This module runs each of the individual modules within the process


Example execution syntax:
	
	V306_CP2_M01_Process_Manager
			'2015-01-01'    --  @processing_date
		,   1               --  @analysis_window
		,   1               --  @max_calibration_iterations
		,   5               --  @sample_size
		,   1               --  @hard_initialise
	;


*/

create or replace procedure V306_CP2_M01_Process_Manager
										
										-- Processing date - default to date of execution
										@processing_date	date                =	today()
										
										-- Number of days to calibrate Capping over
									,	@analysis_window	tinyint             =	14
									
										-- Upper limit in iterations over Capping parameters
									,	@max_calibration_iterations	tinyint     =	20
									
										-- Sample size selection
									,	@sample_size		tinyint             =	100
									
										-- Hard initialise
                                                                        ,       @hard_initialise        bit                 =   0

                                                                                -- Exclude pre-standby events from ntile setting process                    
                                                                        ,       @exclude_pre_standby_events bit             =     0        -- if =1 then pre_standby events will be excluded from the NTile setting process
as begin
	
	-- Display execution parameters to console
	execute	M00_2_output_to_logger 
			'@ M01 : V306_CP2_M01_Process_Manager, params: @processing_date = '	||	@processing_date
		||	', @analysis_window = '	||	@analysis_window
		||	', @max_calibration_iterations = '	||	@max_calibration_iterations
		||	', @sample_size = '	||	@sample_size
		||	'%,	@hard_initialise = '	|| @hard_initialise
	COMMIT


	-------------------------------------------------------------
	-- Initialise local variables
	-------------------------------------------------------------
	
	-- Flag determining acceptability of Capping results parameters in 
	declare @acceptable bit default 0
	commit
	
	-- Calibration iteration counter
	declare @iter_calibration tinyint = 0
	commit
	
	-- Date iteration counter
	declare @iter_days tinyint = 0
	commit
	
	-- Capping date
	declare @capping_date date
	commit

	-- Source viewing table name
	declare @VESPA_table_name varchar(150)
	commit
	
	-------------------------------------------------------------
	-- Initialise tables (only takes 30s, so we may as well run each time to allow for changing table structure while in development)
	-------------------------------------------------------------
	execute V306_CP2_M00_Initialise	@hard_initialise
	commit
	

	-------------------------------------------------------------
	-- Define analysis/capping dates
	-------------------------------------------------------------
	execute M00_2_output_to_logger '@ M00 : Creating table V306_CAPPING_DATES...'
	COMMIT

	execute DROP_LOCAL_TABLE 'V306_CAPPING_DATES'
	commit

	select
			row_num																	as	iteration_num
		,	date	(
						dateadd	(
										dd
									,	-1 * (@analysis_window - row_num + 1)
									,	@processing_date
								)
					)																as	capping_date
	into	V306_CAPPING_DATES
	from	sa_rowgenerator(1,@analysis_window)
	commit
	
	create unique lf index ulf on V306_CAPPING_DATES(iteration_num)
	commit

	execute M00_2_output_to_logger '@ M00 : Creating table V306_CAPPING_DATES...DONE'
	COMMIT	
	
	-- Iterate over sets of Capping parameters





	-----------------------------------------------------------------------------
	-- Define minute-by-minute time base and calculate reference BARB consumption
	-----------------------------------------------------------------------------
--	Time saver while testing	
	-- Create time base for minute-by-minute analysis spanning the entire range of dates
	execute	V306_CP2_M05_2_Time_Tables
	commit


	-- Calculate BARB minute-by-minute consumption for the analysis dates
	execute	V306_CP2_M06_BARB_Minutes
	commit





	-------------------------------------------------------------
	-- Capping Calibration loop
	-------------------------------------------------------------


	--	
	while	@iter_days	<	@analysis_window
	begin
	
		
		-- Progress day counter
		set	@iter_days	=	@iter_days	+	1
		commit
		
		-- Set the date of interest
		select	@capping_date	=	capping_date
		from	V306_CAPPING_DATES
		where	iteration_num	=	@iter_days
		commit
	
		execute M00_2_output_to_logger '@ M01 : building tables for day : ' ||	cast(@iter_days as varchar)	||	' : ' ||	@capping_date
		COMMIT
		
		-- -- Check data before proceeding - not really needed here...
		-- execute	V306_CP2_M02_Capping_Stage1
								-- @capping_date
							-- ,	@VESPA_table_name output
		-- commit
 	
		
		-- Create sample selections based on scaled acccounts for each capping date (selections will remain static per date)
		execute V306_CP2_M03_Capping_Stage2_phase1
								@capping_date
							,	@sample_size
							,	@VESPA_table_name	-- not actually used here now that we've split out Stage2 into 2 phases
		commit
				
	end	-- while	@iter_days	<	@analysis_window




	
	while
				@acceptable			=	0
		and		@iter_calibration	<	@max_calibration_iterations
	begin

/*
	-- Iterate over days
	set	@iter_days	=	0
	commit

	-- execute V306_CP2_M01_Iter_Initialise	1
	-- commit

	while	@iter_days	<	@analysis_window
	begin
	
		
		-- Progress day counter
		set	@iter_days	=	@iter_days	+	1
		commit
		
		-- Set the date of interest
		select	@capping_date	=	capping_date
		from	V306_CAPPING_DATES
		where	iteration_num	=	@iter_days
		commit
	
		execute M00_2_output_to_logger '@ M01 : building tables for day : ' ||	cast(@iter_days as varchar)	||	' : ' ||	@capping_date
		COMMIT
		
		-- Check data before proceeding
		execute	V306_CP2_M02_Capping_Stage1
								@capping_date
							,	@VESPA_table_name output
		commit
		
		
		-- Create view to apply sample selection on accounts and remove duplicated instances
		execute V306_CP2_M03_Capping_Stage2_phase1
								@capping_date
							,	@sample_size
							,	@VESPA_table_name
		commit
				
	end	-- while	@iter_days	<	@analysis_window
*/

		-- Progress calibration iterator
		set	@iter_calibration	=	@iter_calibration	+	1
		commit
		
		execute M00_2_output_to_logger '@ M01 : Calibration iteration : ' ||	cast(@iter_calibration as varchar)
		COMMIT
		

		-- Save meta-data parameters at each iteration
	    execute V306_CP2_M07_5_Save_metadata_params
								@iter_calibration

		
		-- Begin iterations over days
		set	@iter_days	=	0
		commit
		
		while	@iter_days	<	@analysis_window
		begin
		
			
			-- Progress day counter
			set	@iter_days	=	@iter_days	+	1
			commit
			
			-- Set the date of interest
			select	@capping_date	=	capping_date
			from	V306_CAPPING_DATES
			where	iteration_num	=	@iter_days
			commit
		
			execute M00_2_output_to_logger '@ M01 : Capping day iteration : ' ||	cast(@iter_days as varchar)	||	' : ' ||	@capping_date
			COMMIT
			
			
			
			-- Check data before proceeding
			execute	V306_CP2_M02_Capping_Stage1
									@capping_date
								,	@VESPA_table_name output
			commit
			
			

			-- Create view to apply sample selection on accounts and remove duplicated instances
			execute V306_CP2_M03_Capping_Stage2_phase2
									@capping_date
								,	@sample_size
								,	@VESPA_table_name
			commit
			
			
			-- STB profiling (add primary/secondary flags)
			execute V306_CP2_M04_Profiling
									@capping_date -- dateformat((@capping_date - datepart(weekday,@capping_date))-2, 'YYYY-MM-DD')	-- gets the previous Thursday
			commit
			
			
			-- Apply core Capping algorithm
			execute V306_CP2_M05_Build_Day_Caps
									@capping_date
									, @iter_calibration
                                                                        , @exclude_pre_standby_events     -- if =1 then pre_standby events will be excluded from the NTile setting process
			commit



		end	-- while	@iter_days	<	@analysis_window
		
		
		-- Calculate VESPA post-Capping minute-by-minute consumption across all capping dates
		execute	V306_CP2_M07_VESPA_Minutes
								NULL	-- not used
								, @iter_calibration
		commit


		-- Compare capped Vespa viewing against BARB
		execute	V306_CP2_M07_1_BARB_vs_VESPA
								NULL	-- capping_date not used anymore
							,	@sample_size
							,	@iter_calibration




		-- -------------------------------------------------------------
		-- -- Calculate global rms and differences
		-- -------------------------------------------------------------
		-- -- calculate for normal weekdays
		-- execute V306_CP2_M07_6_calculate_diff_by_time_range_LIVE 
								-- @processing_date
							-- ,	@analysis_window
							-- ,	@iter_calibration
							-- ,	0

		-- -- calculate for weekends
		-- execute V306_CP2_M07_6_calculate_diff_by_time_range_LIVE 
								-- @processing_date
							-- ,	@analysis_window
							-- ,	@iter_calibration
							-- ,	1

		-- -- calculate for playback
		-- execute V306_CP2_M07_6_calculate_diff_by_time_range_PLAYBACK 
								-- @processing_date
							-- ,	@analysis_window
							-- ,	@iter_calibration

							
		-- -------------------------------------------------------------
		-- -- Adjust Capping parameters for next iteration (if appropriate)
		-- -------------------------------------------------------------
		-- if (@iter_calibration	<	@max_calibration_iterations) and (@acceptable =	0)
		-- begin -- only tune params if there is at least another iteration
				-- execute V306_CP2_M07_7_tune_iteration_params
										-- 0 -- workdays

				-- execute V306_CP2_M07_7_tune_iteration_params
										-- 1 -- bank holidays
		-- end

--		execute V306_CP2_M07_8_optimise_capping_parameters
--								@iter_calibration
--		commit

	end	-- while @acceptable = 0 and @iter_calibration < @max_calibration_iterations

	
	-------------------------------------------------------------
	-- Finish and output
	-------------------------------------------------------------

end; -- procedure V306a_CP2_M01_Process_Manager
commit;

grant execute on V306_CP2_M01_Process_Manager to vespa_group_low_security;
commit;


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




**Module:                               V306_CP2_M02_Capping_Stage1

create AUG tables - CUSTOM

*/

create or replace procedure V306_CP2_M02_Capping_Stage1
													@target_date       	date = NULL     -- Date of daily table caps to cache
												,	@VESPA_table_name	varchar(150)	output
as begin

	execute M00_2_output_to_logger '@ M02 : Initial data check : '
	COMMIT
			
	declare @query varchar(1000)
	commit --; --^^ to be removed
	-- declare @VESPA_table_name varchar(150)
	-- commit --; --^^ to be removed

	execute M00_2_output_to_logger 'M02: CP2 checking data availability for date ' || convert(varchar(10),@target_date,123)
	-- Check that there is data in viewing table
	commit --; --^^ to be removed
	declare @cust_subs_hist_IsOK bit
	commit --; --^^ to be removed
	declare @vespa_table_min_check_IsOK bit		
	commit --; --^^ to be removed
	declare @vespa_table_max_check_IsOK bit		
	commit --; --^^ to be removed

	-- set @cust_subs_hist_IsOK=case when (select max(effective_from_dt) from cust_subs_hist)>=@target_date then 1 else 0 end
	-- commit --; --^^ to be removed

	set @VESPA_table_name = 'VESPA_DP_PROG_VIEWED_' || convert(varchar(6),@target_date,112)
	commit --; --^^ to be removed

	execute M00_2_output_to_logger '@ M02 : Source table identified : ' || @VESPA_table_name
	COMMIT

	-- set @query='
	-- set @@vespa_table_min_check_IsOK=case when (select min(dk_event_start_datehour_dim)/100  from ###tableName###) <= cast(@target_date as integer) then 1 else 0 end
	-- commit --; --^^ to be removed
	-- set @@vespa_current_max_IsOK=case when (select max(dk_event_start_datehour_dim)/100  from ###tableName###) >= cast(@target_date as integer) then 1 else 0 end
	-- commit --; --^^ to be removed
	-- '
	-- commit --; --^^ to be removed
	
	-- select @@vespa_table_min_check_IsOK

	-- execute(replace(@query,'###tableName###',@VESPA_table_name))
	-- commit --; --^^ to be removed

	-- set @dateIsOK=case when (@cust_subs_hist_IsOK!=0) and (@vespa_table_min_check_IsOK!=0) and (@vespa_current_max_IsOK!=0) then 1 else 0 end
	-- commit --; --^^ to be removed

	-- execute logger_add_event @CP2_build_ID, 3, 'M02: CP2 check results: cust_subs_hist ' || cast(@cust_subs_hist_IsOK as varchar(1)) || ', ' || @VESPA_table_name || ' min ' || cast(@cust_subs_hist_IsOK as varchar(1)) ||' max ' || cast(@cust_subs_hist_IsOK as varchar(1))

	
end;
commit;

grant execute on V306_CP2_M02_Capping_Stage1 to vespa_group_low_security;
commit;





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




**Module:                               V306_CP2_M03_Capping_Stage2_phase1

create AUG tables - CUSTOM

*/

create or replace procedure V306_CP2_M03_Capping_Stage2_phase1
											@capping_date date
										,	@sample_size  tinyint
										,	@VESPA_table_name	varchar(150)
as begin

	-- ########################################################################
	-- #### Capping State 2 - create AUG tables - CUSTOM                   ####
	-- ########################################################################
	-- Change months according to range of run

	execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase1'
	COMMIT

	--------------------------------------------------------------
	-- Initialise
	--------------------------------------------------------------
	
	declare @QA_catcher   integer	commit
	
	--------------------------------------------------------------
	-- Select sample of accounts
	--------------------------------------------------------------

	execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase1 : Apply sample selection @ '	||	@sample_size
	COMMIT
	
	-- Get Vespa panel accounts for capping date of interest and the scaling weight
	insert into	CP2_accounts
	select
			account_number
		,	adsmart_scaling_weight
		,	rand(number())	as	rand_num
		,	@capping_date
	from	sk_prod.VIQ_viewing_data_scaling
	where	adjusted_event_start_date_vespa	=	@capping_date
	commit
	
	-- Apply sample trimming
	delete from CP2_accounts
	where	rand_num	>	(@sample_size/ 100.0)
	and  reference_date = @capping_date
	commit
end;
commit;

grant execute on V306_CP2_M03_Capping_Stage2_phase1 to vespa_group_low_security;
commit;

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




**Module:                               V306_CP2_M03_Capping_Stage2_phase2

create AUG tables - CUSTOM

*/

create or replace procedure V306_CP2_M03_Capping_Stage2_phase2
											@capping_date date
										,	@sample_size  tinyint
										,	@VESPA_table_name	varchar(150)
as begin

	-- ########################################################################
	-- #### Capping State 2 - create AUG tables - CUSTOM                   ####
	-- ########################################################################
	-- Change months according to range of run

	execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2'
	COMMIT

	--------------------------------------------------------------
	-- Initialise
	--------------------------------------------------------------
	
	declare @QA_catcher   integer	commit

	declare @sql_	varchar(5000)	commit
	
	--------------------------------------------------------------
	-- Select sample of accounts
	--------------------------------------------------------------

	execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2 : take sample selection @ '	||	@sample_size ||	'%'
	COMMIT


	--------------------------------------------------------------
	-- Identify non-duplicated VPIF keys
	--------------------------------------------------------------
	
	execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2 : Identify and retain non-duplicated VPIF keys'
	COMMIT


	-- Clear table if necessary
	execute DROP_LOCAL_TABLE 'CP2_unique_vpif_keys'
	commit	
	
	-- Get non-duplicated VPIF keys for the date of interest
	set	@sql_	=	'
		select
				pk_viewing_prog_instance_fact
			,	account_number
			,	cast(NULL as tinyint)		as	tx_day
                        ,       cast(NULL as bigint)            as      dth_viewing_event_id
                        ,       cast(0 as bit)                  as      pre_standby_event_flag --updated in the next stage
			,	count()                         as	cnt
		into	CP2_unique_vpif_keys
		from
						' || @VESPA_table_name	|| '	as	dpp
		where
                dpp.log_received_start_date_time_utc    between	dateadd(day,-3,''' || @capping_date || ''')
                                                        and     dateadd(hour,30,''' || @capping_date || ''')
			and	panel_id							in	(11,12)
			and	type_of_viewing_event				is not null
			and	type_of_viewing_event				<>	''Non viewing event''
		group by
				pk_viewing_prog_instance_fact
			,	account_number
		having	cnt	=	1
		commit
		'
	commit
	
/*
	TX definition required - (log received date - event start date + 1)? Include TX+0 if there are any.
	TX+1 -> ntiling process
	TX+2/3 -> capping applied -- is this actually needed? how many get scaling weights? - shouldn't expect many
	MbM - still isolate to scaled accounts for reporting/TE
*/
	
	execute(@sql_)
	commit
	
	create unique hg index uhg on CP2_unique_vpif_keys(pk_viewing_prog_instance_fact)
	commit
	create hg index idx1 on CP2_unique_vpif_keys(account_number)
	commit
	create lf index idx2 on CP2_unique_vpif_keys(tx_day)
	commit

	
	execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2 : Non-duplicated VPIF instances identified.'
	COMMIT



	

	
	
	-- Calculate TX day for each instance/event. Do this separately from the initial VPIF deduping so as not to re-introduce duplicates at that level.

	execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2 : Calculate TX Day for each instance'
	COMMIT

	set	@sql_	=	'
		update	CP2_unique_vpif_keys
		set		tx_day	=	datediff	(
												day
											,	dpp.event_start_date_time_utc
											,	dpp.log_received_start_date_time_utc
										)
		from
						CP2_unique_vpif_keys			as	a
			inner join	' || @VESPA_table_name	|| '	as	dpp		on  dpp.pk_viewing_prog_instance_fact		=		a.pk_viewing_prog_instance_fact
																	and	dpp.account_number						=		a.account_number
																	and	dpp.log_received_start_date_time_utc    between	dateadd(day,-3,''' || @capping_date || ''')
																												and     dateadd(hour,30,''' || @capping_date || ''')
																	and	dpp.panel_id							in		(11,12)
																	and	dpp.type_of_viewing_event				is 		not null
																	and	dpp.type_of_viewing_event				<>		''Non viewing event''
		commit
		'
	commit
							
	execute(@sql_)
	commit
	


	-- Retain only those events up to TX+3
	execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2 : Retain only those events up to TX+3'
	COMMIT

	delete from	CP2_unique_vpif_keys
	where	tx_day	not between	0
						and		3
	commit

	
	-- TEMPORARY FILTER
	execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2 : Retain only those events at TX+1'
	COMMIT
	delete from	CP2_unique_vpif_keys
	where	tx_day	<>	1
	commit
	
	

	-----------------------------------------------------------------------------------------------------------------------------------------------------
	-- Now also apply sample trimming. This was previously performed by joining into the above, but we now need to factoring in unscaled accounts as well
	-----------------------------------------------------------------------------------------------------------------------------------------------------

	execute DROP_LOCAL_TABLE 'CP2_unscaled_accounts'
	commit
	
	select
			a.account_number
		,	rand(number())	as	rand_num
	into	CP2_unscaled_accounts
	from
					CP2_unique_vpif_keys	a
		left join	CP2_accounts			b 	on	a.account_number	=	b.account_number
	where	b.account_number	is null
	group by	a.account_number	-- shouldn't need this, but deduping just in case!
	commit
	


	-- Apply sample trimming on unscaled accounts (the scaled version was performed earlier in V306_CP2_M03_Capping_Stage2_phase1)
	-- For now, this sample can change between iterations, whereas the scaled account sample is fixed per capping date.
	delete from CP2_unscaled_accounts
	where	rand_num	>	(@sample_size/ 100.0)
	commit
	

	delete from 	CP2_unique_vpif_keys
	from
					CP2_unique_vpif_keys	a
		left join	(
						select	account_number
						from	CP2_accounts
						union all
						select	account_number
						from	CP2_unscaled_accounts
					)						b	on	a.account_number	=	b.account_number
		where	a.account_number	is null
	commit

	

 ---*********************************************************************************************---
        ---*********************************************************************************************---
        --   THIS SECTION USES AN
        --           E X T E R N A L L Y   C R E A T E D   T A B L E
        --           - - - - - - - - - -                to set the pre-stand-by event flags
        ---*********************************************************************************************---
        ---*********************************************************************************************---

        UPDATE CP2_unique_vpif_keys         uvk
           SET uvk.pre_standby_event_flag = 1 --case when standby.pk_viewing_programme_instance_fact IS NOT NULL then cast(1 as bit) else cast(0 as bit) end
          FROM Capping_NZ_Extract_Standby_dth_viewing_event_id_tmp as standby  --               <--- EXTERNAL (and manually) built table [contains records that only lead to a standby event] change to flag in viewing table when available
         WHERE uvk.pk_viewing_prog_instance_fact = standby.pk_viewing_programme_instance_fact
        commit

	execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2 : PreStandby events updated<'||@@rowcount||'>'
	COMMIT


        ----------------------------------------------------------------------------
	-- Create view to the appropriate monthly viewing table for the capping date
	----------------------------------------------------------------------------
		
	set @sql_	=	'
		create or replace view	Capping2_00_Raw_Uncapped_Events as
		select	dpp.*,
                        keys.pre_standby_event_flag
		from
						' || @VESPA_table_name	|| '			as	dpp
			inner join	CP2_unique_vpif_keys					as	keys		on	dpp.pk_viewing_prog_instance_fact	=	keys.pk_viewing_prog_instance_fact
		where
                dpp.log_received_start_date_time_utc    between	dateadd(day,-3,''' || @capping_date || ''')
                                                        and     dateadd(hour,30,''' || @capping_date || ''')
--				(
--						(
--							dk_event_start_datehour_dim	between	cast(dateformat(''' || @capping_date || ''', ''yyyymmdd00'') as int)
--														and		cast(dateformat(''' || @capping_date || ''', ''yyyymmdd23'') as int)
--						)
--					or	(
--							dk_event_end_datehour_dim	between	cast(dateformat(''' || @capping_date || ''', ''yyyymmdd00'') as int)
--														and		cast(dateformat(''' || @capping_date || ''', ''yyyymmdd23'') as int)
--						)
--				)
			and	panel_id							in	(11,12)
			and	type_of_viewing_event				is not null
			and	type_of_viewing_event				<>	''Non viewing event''
		commit
	'
	commit
	
	execute (@sql_)
	commit
	
	
	execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2 : Working view created : Capping2_00_Raw_Uncapped_Events'
	COMMIT
	

/*
commit								
execute logger_create_run 'Capping2.x CUSTOM', 'Weekly capping run', @varBuildId output

select @QA_catcher = count(1)
from CP2_duplicated_keys

execute logger_add_event @varBuildId, 3, 'Number of duplicated keys: ', coalesce(@QA_catcher, -1)
*/

end;
commit;

grant execute on V306_CP2_M03_Capping_Stage2_phase2 to vespa_group_low_security;
commit;

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




**Module:                               V306_CP2_M04_Profiling

-- We don't need to profile each box on each different day, we're just going to profile
-- once a week (or something like that) at the beginning of the build week and use that
-- for the whole week. This way isn't going to be super robust against race conditions,
-- but the scheduler is fairly robust against two things running the same proc at the
-- same time. Still, we can also throw the build date onto the metadata table to ensure
-- things don't get desynchronised.

*/

-------------------------------------------------------------------------------------------------
-- J - WEEKLY PROFILING BULD OF BOX METADATA
-------------------------------------------------------------------------------------------------

create or replace procedure V306_CP2_M04_Profiling
										@profiling_thursday     date = NULL
as begin

	execute M00_2_output_to_logger '@ M04 : V306_CP2_M04_Profiling'
	COMMIT


	declare @QA_catcher             integer
	commit 

	-- Note that we've started the build:
	execute M00_2_output_to_logger  ' New week: Profiling boxes as of ' || dateformat(@profiling_thursday, 'yyyy-mm-dd') ||'.'
	commit --;-- ^^ originally a commit

	-------------------------------------------------------------------------------------------------
	-- J01) CLEARING OLD STUFF OUT OF THE TABLE, REPOPULATING
	-------------------------------------------------------------------------------------------------
	execute M00_2_output_to_logger '@ M04 : Update CP2_relevant_boxes'
	COMMIT
	
	-- For the dev build we're using '2012-01-26' but now it's a proc we want to be able to fire
	-- in dates of our own choosing.
	--set @profiling_thursday = '2012-01-26'

	truncate table CP2_box_lookup
	commit --
	-- Yeah, the trick now is that no single loop will contain all the boxes we want to
	-- process, because we're only caching one day worth of caps at once. So we need to go
	-- over the daily tables again and pull out all the account numbers that we care about.

	-- We'd use temporary tables for these two guys, except that they get populated via
	-- some dynamic SQL, so temporary tables would fail (being inside a separate execution
	-- scope, sadface)
	truncate table CP2_relevant_boxes
	commit --


	declare @scanning_day               date
	commit

	set @scanning_day = dateadd(day, -1, @profiling_thursday)
	commit

	insert into	CP2_relevant_boxes
	select
			account_number
		,	subscriber_id
		,	service_instance_id
	from	Capping2_00_Raw_Uncapped_Events
	where
			/*event_start_date_time_utc	>=	@scanning_day
		and	event_start_date_time_utc	<=	dateadd(day, 1, @scanning_day) -- we could replace this with the variable @profiling_thursday
		and*/ panel_id					in	(11,12)
		and	account_number				is not null
		and subscriber_id				is not null       
	group by
			account_number
		,	subscriber_id
		,	service_instance_id
	commit
	
	execute M00_2_output_to_logger  ' Days processed: ' || dateformat(@scanning_day, 'dd/mm/yyyy') || '-' || dateformat(dateadd(day, 1, @scanning_day), 'dd/mm/yyyy')
	commit


	-- We also need to populate the CP2_box_lookup table:
	execute M00_2_output_to_logger '@ M04 : Update CP2_box_lookup'
	COMMIT
	
	insert into	CP2_box_lookup	(
										subscriber_id
									,	account_number
									,	service_instance_id
								)
	select
			subscriber_id
		,	min(account_number)
		,	min(service_instance_id)
	from	CP2_relevant_boxes
	where
			subscriber_id	is not null -- dunno if there are any, but we need to check
		and	subscriber_id	<>	-1
		and	account_number	is not null
	group by	subscriber_id
	commit

	-- Maybe have some QA somewhere checking for duplication between account number / subscriber
	-- id / service instance id? the min(.) method is kind of ugly

	set @QA_catcher = -1
	commit

	select @QA_catcher = count(1)
	from CP2_box_lookup
	commit 
	
	execute M00_2_output_to_logger  ' J01: Complete! (Box lookup built) ' || coalesce(@QA_catcher, -1)
	commit 

	-------------------------------------------------------------------------------------------------
	-- J02) PRIMARY & SECONDARY BOX FLAGS
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M04 : Determine Primary and Secondary STB flags'
	COMMIT
	
	execute DROP_LOCAL_TABLE 'CP2_deduplicated_accounts'
	commit	
	
	
	-- For pulling stuff out of the customer database: we would join on service instance ID,
	-- except that it's not indexed in cust_subs_hist. So instead we pull out everything for
	-- these accounts, and then join back on service instance ID later.
	select
			account_number
		,	1	as Dummy
	into	CP2_deduplicated_accounts
	from	CP2_relevant_boxes
	group by
			account_number
		,	Dummy
	commit
	
	create unique index fake_pk on CP2_deduplicated_accounts (account_number)
	commit 

	-- OK, now we can go get get P/S flgs:
	execute DROP_LOCAL_TABLE 'all_PS_flags'
	commit	

	select
			csh.service_instance_id
		,	case	csh.subscription_sub_type
				when	'DTV Primary Viewing'		then	'P'
				when	'DTV Extra Subscription'	then	'S'
			end				as PS_flag
	into	all_PS_flags
	from
					CP2_deduplicated_accounts	as	da
		inner join	cust_subs_hist				as	csh		on	da.account_number			=	csh.account_number
															and	csh.SUBSCRIPTION_SUB_TYPE	in	(
																										'DTV Primary Viewing'
																									,	'DTV Extra Subscription'
																								)
															and	csh.status_code				in	('AC','AB','PC')
															and	csh.effective_from_dt		<=	@profiling_thursday
															and	csh.effective_to_dt			>	@profiling_thursday
	group by
			csh.service_instance_id
		,	PS_flag
	commit 

	-- ^^ This guy, on the test build (300k distinct accounts) took 8 minutes. That's managable.


	-- OK, so building P/S off what's active on the Thursday could cause issues with
	-- recent activators not having subscriptions which give them flags, but I'm okay
	-- with there being a few 'U' entries for recent joiners to Sky for the first week
	-- they're on the Vespa panel. It's not about recently joining Vespa, it's about
	-- recently joining Sky, so it shouldn't be much of an issue at all.

	-- Index *should* be unique, but might not be if there are conflicts in Olive. So,
	-- more QA, check that these are actually unique.
	create index idx1 on all_PS_flags (service_instance_id)
	commit 

	update	CP2_box_lookup
	set	CP2_box_lookup.PS_flag	=	apsf.PS_flag
	from
					CP2_box_lookup
		inner join	all_PS_flags	as	apsf	on	CP2_box_lookup.service_instance_id	=	apsf.service_instance_id
	commit
	
	---------------------------------------
	-- Clean up and finish
	---------------------------------------
	execute DROP_LOCAL_TABLE 'CP2_deduplicated_accounts'
	commit	
	
	execute DROP_LOCAL_TABLE 'all_PS_flags'
	commit	
	-- Need some QA on the these numbers, including warning about guys still flagged
	-- as 'U', but the process all seems okay.

	set @QA_catcher = -1
	commit

	select @QA_catcher = count(1)
	from CP2_box_lookup
	where PS_flag in ('P', 'S')
	commit 

	execute M00_2_output_to_logger  ' J02: Complete! (Derive P/S per box) ' || coalesce(@QA_catcher, -1)
	commit 

end;
commit;

grant execute on V306_CP2_M02_Capping_Stage1 to vespa_group_low_security;
commit;

    --------------------------------------------------------------------------------
    -- A00) SET UP OF METADATA VARIABLES.
    --------------------------------------------------------------------------------
-- metadata variables


create or replace procedure V306_CP2_M05_Build_Day_Caps
                             @target_date                    date    =     NULL     -- Date of daily table caps to cache
                     ,       @iteration_number               int     =     NULL     -- current iteration
                     ,       @exclude_pre_standby_events     bit     =     0        -- if =0 then pre_standby events will be excluded from the NTile setting process
as begin


	execute M00_2_output_to_logger '@ M05 : V306_CP2_M05_Build_Day_Caps day iteration'
	COMMIT
			
	--------------------------------------------------------------------------------
	-- A01) SET UP.
	--------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : A01) SET UP'
	COMMIT

	/*create or replace variable*/ declare @targetDateIsWeekend int
	commit
	
	set @targetDateIsWeekend=(case when datepart(weekday,@target_date) in (1,7) then 1 else 0 end)
	commit
	
	/*create or replace variable*/ declare @current_metadata_row_number int -- the id of the row that contains current data
	commit

	set	@current_metadata_row_number	=	(
												select	max(row_id)
												from	CP2_metadata_table
												where
														@target_date	between	EFFECTIVE_FROM
																		and		EFFECTIVE_TO
													and	BANK_HOLIDAY_WEEKEND	=	@targetDateIsWeekend
											)
	commit
	
	-- create and populate variables
	/*create or replace variable*/ declare @playback_days          tinyint         -- How many days back worth of playback viewing data to consider
	commit --; --^^ to be removed
	set     @playback_days = 28                     -- Want to be able to treat timeshifting of up to 28 days
	commit --; --^^ to be removed

	commit --; --^^ to be removed
	/*create or replace variable*/ declare @BARB_day_cutoff_time   time            -- because we don't know if the day starts at 2AM or 4 or 6 or 9 and this means it's easily changed
	commit --; --^^ to be removed
	set     @BARB_day_cutoff_time = '02:00:00'      -- Treating the day as 2AM to 2AM. Currently not really using this though?
	commit --; --^^ to be removed

	-- The following parameters are currently static in the metadata table
	commit --; --^^ to be removed
	/*create or replace variable*/ declare @max_cap_bound_minutes  integer         -- The largest a capping bound can possibly be. This is important as we have to get viewing records out of the previous day's table to cap ea
	commit --; --^^ to be removed
	set     @max_cap_bound_minutes = (select MAXIMUM_CUT_OFF from CP2_metadata_table where row_id=@current_metadata_row_number )--120
	commit --; --^^ to be removed

	commit --; --^^ to be removed
	/*create or replace variable*/ declare @min_cap_bound_minutes  integer         -- The smallest a capping bound can possibly be
	commit --; --^^ to be removed
	set     @min_cap_bound_minutes = (select MINIMUM_CUT_OFF from CP2_metadata_table where row_id=@current_metadata_row_number )--20
	commit --; --^^ to be removed

	commit --; --^^ to be removed
	/*create or replace variable*/ declare @min_view_duration_sec  tinyint         -- The bound below which views are ignored, in seconds
	commit --; --^^ to be removed
	set     @min_view_duration_sec  = (select SHORT_DURATION_CAP_THRESHOLD from CP2_metadata_table where row_id=@current_metadata_row_number )--6
	commit --; --^^ to be removed

	commit --; --^^ to be removed
	/*create or replace variable*/ declare @uncapped_sample_pop_max integer        -- The maximum number of uncapped events to consider per bucket for selecting new end times for each capped event
	commit --; --^^ to be removed
	set     @uncapped_sample_pop_max = (select SAMPLE_MAX_POP from CP2_metadata_table where row_id=@current_metadata_row_number )--10000        -- About 2.5 hours to process the matching loop for one day; good thing we're scheduling these overnight
	commit --; --^^ to be removed
																																									-- Update: Now pulled back to 10k given we're also batching by initial channel now too

	commit --; --^^ to be removed
	/*create or replace variable*/ declare @var_sql                varchar(15000)   -- For dynamic SQL over daily tables, though we're only capping one day so there's no looping
	commit --; --^^ to be removed
	/*create or replace variable*/ declare @QA_catcher             integer

	commit --; --^^ to be removed
	/*create or replace variable*/ declare @gmt_start                              date                     -- To capture when the clocks go back in Autumn
	commit --; --^^ to be removed
	/*create or replace variable*/ declare @bst_start                              date                     -- To capture when the clocks go forward in Spring
	commit --; --^^ to be removed

	commit --; --^^ to be removed
	set @bst_start = dateadd(dy, -(datepart(dw, datepart(year, today()) || '-03-31') -1),datepart(year, today()) || '-03-31')  -- to get last Sunday in March
	commit --; --^^ to be removed
	set @gmt_start = dateadd(dy, -(datepart(dw, datepart(year, today()) || '-10-31') -1),datepart(year, today()) || '-10-31')  -- to get last Sunday in October
	commit --; --^^ to be removed

	-- Dev purposes only:
	--set @target_date = '2012-01-30';
	-- For dev build, we're going through to the end of Feb.

	execute M00_2_output_to_logger  'A01: CP2 caching caps for ' || convert(varchar(10),@target_date,123)
	commit --;-- ^^ originally a commit


	--------------------------------------------------------------------------------
	-- Removing redundant tables which do not longer exist or has been renamed
	--------------------------------------------------------------------------------
	commit --; --^^ to be removed
	if object_id('CP2_Viewing_Records')                 is not null drop table CP2_Viewing_Records
	commit --; --^^ to be removed
	if object_id('CP2_01_Viewing_Records')              is not null drop table CP2_01_Viewing_Records
	commit --; --^^ to be removed
	if object_id('Uncleansed_Viewing_Totals')           is not null drop table Uncleansed_Viewing_Totals
	commit --; --^^ to be removed
	if object_id('CP2_4BARB_internal_viewing')          is not null drop table CP2_4BARB_internal_viewing
	commit --; --^^ to be removed
	if object_id('CP2_4BARB_view_endpoints')            is not null drop table CP2_4BARB_view_endpoints
	commit --; --^^ to be removed
	if object_id('cumulative_playback_corrections')     is not null drop table cumulative_playback_corrections
	commit --; --^^ to be removed
	if object_id('cumulative_playback_corrections_2')   is not null drop table cumulative_playback_corrections_2
	commit --; --^^ to be removed
	if object_id('CP2_viewing_control_cap_distrib')     is not null drop table CP2_viewing_control_cap_distrib
	commit --; --^^ to be removed
	if object_id('CP2_viewing_control_distribs')        is not null drop table CP2_viewing_control_distribs
	commit --; --^^ to be removed
	if object_id('CP2_viewing_control_totals')          is not null drop table CP2_viewing_control_totals
	commit --; --^^ to be removed


	-- select 'start B', now(); --^^ just for debug

	--------------------------------------------------------------------------------
	-- B) - Get The viewing Data
	--------------------------------------------------------------------------------

	--------------------------------------------------------------------------------
	-- B02 - Get the viewing data
	--------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : B02 - Get the viewing data'
	COMMIT

	if object_id('Capping2_01_Viewing_Records') is not null drop table Capping2_01_Viewing_Records
	commit --; --^^ to be removed
	create table Capping2_01_Viewing_Records (
					ID_Key                              bigint          primary key identity
					,cb_row_ID                          bigint          not null
					,Account_Number                     varchar(20)     not null
					,Subscriber_Id                      integer
					,X_Type_Of_Viewing_Event            varchar(40)     not null
					,Adjusted_Event_Start_Time          datetime
					,X_Adjusted_Event_End_Time          datetime
					,X_Viewing_Start_Time               datetime
					,X_Viewing_End_Time                 datetime
					,Tx_Start_Datetime_UTC              datetime
					,X_Event_Duration                   decimal(10,0)
					,X_Programme_Viewed_Duration        decimal(10,0)
					,Programme_Trans_Sk                 bigint          not null
					,Pre_standby_event_flag             bit             default 0       --added to track pre-standby events
                                        ,Service_Key                        bigint
					,daily_table_date                   date            not null
					,live                               bit
					,genre                              varchar(25)
					,sub_genre                          varchar(25)
					,epg_channel                        varchar(30)
					,channel_name                       varchar(30)
					,program_air_date                   date
					,program_air_datetime               datetime
					,event_start_day                    integer         default null
					,event_start_hour                   integer         default null
					,box_subscription                   varchar(1)      default 'U'
					,initial_genre                      varchar(30)     default null
					,initial_channel_name               varchar(30)     default null
					,Initial_Service_Key                bigint          default null
					,pack                               varchar(100)    default null
					,pack_grp                           varchar(30)     default null
					,bucket_id                          integer         default null
	)

	commit --; --^^ to be removed
	create hg   index idx2 on Capping2_01_Viewing_Records (Subscriber_Id)
	commit --; --^^ to be removed
	create dttm index idx3 on Capping2_01_Viewing_Records (Adjusted_Event_Start_Time)
	commit --; --^^ to be removed
	create dttm index idx4 on Capping2_01_Viewing_Records (X_Viewing_Start_Time)
	commit --; --^^ to be removed
	create hg   index idx5 on Capping2_01_Viewing_Records (bucket_id)
	commit --; --^^ to be removed
	create lf   index idx6 on Capping2_01_Viewing_Records (event_start_hour)
	commit --; --^^ to be removed
	create lf   index idx7 on Capping2_01_Viewing_Records (event_start_day)
	commit --; --^^ to be removed



	commit --; --^^ to be removed
	/*create or replace variable*/ declare @varBroadcastMinDate  int
	commit --; --^^ to be removed
	/*create or replace variable*/ declare @varEventStartHour    int
	commit --; --^^ to be removed
	/*create or replace variable*/ declare @varEventEndHour      int
	commit --; --^^ to be removed

	set @varBroadcastMinDate  = (dateformat(@target_date - @playback_days, 'yyyymmdd00'))
	commit --; --^^ to be removed
	set @varEventStartHour    = (dateformat(@target_date - 1, 'yyyymmdd23'))      -- Event to start no earlier than at 23:00 on the previous day
	commit --; --^^ to be removed
	set @varEventEndHour      = (dateformat(@target_date, 'yyyymmdd23'))          -- Event to start no later than at 23:59 on the next day
	commit --; --^^ to be removed

	insert into	Capping2_01_Viewing_Records (
													cb_row_ID
												,	Account_Number
												,	Subscriber_Id
												,	X_Type_Of_Viewing_Event
												,	Adjusted_Event_Start_Time
												,	X_Adjusted_Event_End_Time
												,	X_Viewing_Start_Time
												,	X_Viewing_End_Time
												,	Tx_Start_Datetime_UTC
												,	X_Event_Duration
												,	X_Programme_Viewed_Duration
                                                                                                ,	Programme_Trans_Sk
                                                                                                ,       Pre_standby_event_flag												
												,	Service_Key
												,	daily_table_date
												,	live
												,	genre
												,	sub_genre
												,	epg_channel
												,	channel_name
												,	program_air_date
												,	program_air_datetime
												,	event_start_day
												,	event_start_hour
											)
	select
			pk_viewing_prog_instance_fact              	as Cb_Row_Id
		,	Account_Number                             	as Account_Number
		,	Subscriber_Id                              	as Subscriber_Id
		,	Type_Of_Viewing_Event                      	as X_Type_Of_Viewing_Event
		,	EVENT_START_DATE_TIME_UTC                  	as Adjusted_Event_Start_Time
		,	EVENT_END_DATE_TIME_UTC                    	as X_Adjusted_Event_End_Time
		,	INSTANCE_START_DATE_TIME_UTC               	as X_Viewing_Start_Time
		,	INSTANCE_END_DATE_TIME_UTC                 	as X_Viewing_End_Time
		,	BROADCAST_START_DATE_TIME_UTC             	as Tx_Start_Datetime_UTC
		,	Duration									as X_Event_Duration
		,	datediff(second,INSTANCE_START_DATE_TIME_UTC, INSTANCE_END_DATE_TIME_UTC)
														as X_Programme_Viewed_Duration
		,	dk_programme_instance_dim                  	as Programme_Trans_Sk
                ,       coalesce(Pre_standby_event_flag, 0)             as Pre_Standby_Event_Flag
		,	Service_Key                                	as Service_Key
		,	@target_date                               	as Daily_Table_Date
		,	case
				when	live_recorded = 'LIVE'	then	1
				else									0
			end											as live
		,	case
				when	Genre_Description is null	then	'Unknown'
				else										Genre_Description
			end											as genre
		,	case
				when	Sub_Genre_Description is null	then	'Unknown'
				else											Sub_Genre_Description
			end											as sub_genre
		,	null                                       	as epg_channel               -- epg_channel
		,	channel_name                               	as channel_name
		,	date(BROADCAST_START_DATE_TIME_UTC)        	as program_air_date
		,	BROADCAST_START_DATE_TIME_UTC              	as program_air_datetime
		,	datepart	(		day
							,	case
									when (EVENT_START_DATE_TIME_UTC between  dateadd(hh, 1, convert(datetime, @bst_start)) and dateadd(hh, 2, convert(datetime, @gmt_start))) then dateadd(hour, 1, EVENT_START_DATE_TIME_UTC) -- cortb added (2014-04-22) to check to see we are in BST or GMT
									else EVENT_START_DATE_TIME_UTC -- we're in GMT
								end
						)                            	as event_start_day
		,	datepart	(		hour
							,	case
									when (EVENT_START_DATE_TIME_UTC between  dateadd(hh, 1, convert(datetime, @bst_start)) and dateadd(hh, 2, convert(datetime, @gmt_start))) then dateadd(hour, 1, EVENT_START_DATE_TIME_UTC) -- cortb added (2014-04-22) to check to see we are in BST or GMT
									else EVENT_START_DATE_TIME_UTC -- we're in GMT
								end
						)                            	as event_start_hour
		/*,datepart(day, -- cortb commented out (2014-04-22) as this was outdated
												case
																when (EVENT_START_DATE_TIME_UTC <  '2012-03-25 01:00:00') then EVENT_START_DATE_TIME_UTC                      -- prior Mar 12 - no change, consider UTC = local
																when (EVENT_START_DATE_TIME_UTC <  '2012-10-28 02:00:00') then dateadd(hour, 1, EVENT_START_DATE_TIME_UTC)    -- Mar 12-Oct 12 => DST, add 1 hour to UTC (http://www.timeanddate.com/worldclock/timezone.html?n=136)
																when (EVENT_START_DATE_TIME_UTC <  '2013-03-31 01:00:00') then EVENT_START_DATE_TIME_UTC                      -- Oct 12-Mar 13 => UTC = Local
																when (EVENT_START_DATE_TIME_UTC <  '2013-10-27 02:00:00') then dateadd(hour, 1, EVENT_START_DATE_TIME_UTC)    -- Mar 13-Oct 13 => DST, add 1 hour to UTC
																when (EVENT_START_DATE_TIME_UTC <  '2014-03-30 01:00:00') then EVENT_START_DATE_TIME_UTC                      -- Oct 13-Mar 14 => UTC = Local
																		else NULL                                                                                                   -- the scrippt will have to be updated past Mar 2014
														end)                            as event_start_day
		,datepart(hour,
												case
																when (EVENT_START_DATE_TIME_UTC <  '2012-03-25 01:00:00') then EVENT_START_DATE_TIME_UTC                      -- prior Mar 12 - no change, consider UTC = local
																when (EVENT_START_DATE_TIME_UTC <  '2012-10-28 02:00:00') then dateadd(hour, 1, EVENT_START_DATE_TIME_UTC)    -- Mar 12-Oct 12 => DST, add 1 hour to UTC (http://www.timeanddate.com/worldclock/timezone.html?n=136)
																when (EVENT_START_DATE_TIME_UTC <  '2013-03-31 01:00:00') then EVENT_START_DATE_TIME_UTC                      -- Oct 12-Mar 13 => UTC = Local
																when (EVENT_START_DATE_TIME_UTC <  '2013-10-27 02:00:00') then dateadd(hour, 1, EVENT_START_DATE_TIME_UTC)    -- Mar 13-Oct 13 => DST, add 1 hour to UTC
																when (EVENT_START_DATE_TIME_UTC <  '2014-03-30 01:00:00') then EVENT_START_DATE_TIME_UTC                      -- Oct 13-Mar 14 => UTC = Local
																		else NULL                                                                                                   -- the scrippt will have to be updated past Mar 2014
														end)                            as event_start_hour */
	from Capping2_00_Raw_Uncapped_Events
	where
			live_recorded in ('LIVE','RECORDED')
		and Duration > @min_view_duration_sec                             -- Maintain minimum event duration
		and INSTANCE_START_DATE_TIME_UTC < INSTANCE_END_DATE_TIME_UTC     -- Remove 0sec instances
		-- and Panel_id = 12                                              -- THIS IS SUPPOSED TO HANDLED IN THE INPUT TABLE
		-- and type_of_viewing_event <> 'Non viewing event'               -- THIS IS SUPPOSED TO HANDLED IN THE INPUT TABLE
		-- and type_of_viewing_event is not null                          -- THIS IS SUPPOSED TO HANDLED IN THE INPUT TABLE
		and DK_BROADCAST_START_DATEHOUR_DIM >= @varBroadcastMinDate
		and account_number is not null
		and DK_EVENT_START_DATEHOUR_DIM >= @varEventStartHour             -- Start with 2300 hours on the previous day to pick UTC records in DST time (DST = UTC + 1 between April & October)
		and DK_EVENT_START_DATEHOUR_DIM <= @varEventEndHour               -- End up with additional records for the next day, up to 04:00am
		and subscriber_id is not null                                     -- There shouldnt be any nulls, but there are
		and BROADCAST_START_DATE_TIME_UTC is not null
	commit --;-- ^^ originally a commit

	-- Start off the control totals:
	if object_id('Capping2_tmp_Uncleansed_Viewing_Totals') is not null drop table Capping2_tmp_Uncleansed_Viewing_Totals
	commit --; --^^ to be removed

	select
			subscriber_id
		,	round(sum(datediff(ss, X_Viewing_Start_Time, X_Viewing_End_Time)) / 60.0, 0)	as	total_box_viewing
	into	Capping2_tmp_Uncleansed_Viewing_Totals
	from	Capping2_01_Viewing_Records
	where	daily_table_date	=	@target_date
	group by	subscriber_id
	commit --;-- ^^ originally a commit

	delete from	CP2_QA_daily_average_viewing
	where	build_date	=	@target_date
	commit --;-- ^^ originally a commit

	insert into	CP2_QA_daily_average_viewing	(
														build_date
													,	subscriber_count
													,	average_uncleansed_viewing
												)
	select
			@target_date
		,	count(1)
		,	avg(total_box_viewing)
	from	Capping2_tmp_Uncleansed_Viewing_Totals
	commit --;-- ^^ originally a commit

	if object_id('Capping2_tmp_Uncleansed_Viewing_Totals') is not null drop table Capping2_tmp_Uncleansed_Viewing_Totals
	commit --;-- ^^ originally a commit


	set @QA_catcher = -1
	commit --; --^^ to be removed

	select	@QA_catcher = count(1)
	from	Capping2_01_Viewing_Records
	commit --; --^^ to be removed

	execute M00_2_output_to_logger  'B01: Extract raw viewing completed' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	--select 'start C', now(); --^^ just for debug


	-------------------------------------------------------------------------------------------------
	-- SECTION C: CREATE THE CAPS
	-------------------------------------------------------------------------------------------------

	-------------------------------------------------------------------------------------------------
	-- C01) ASSEMBLE REQUIRED DAILY VIEWING DATA
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : C01) ASSEMBLE REQUIRED DAILY VIEWING DATA'
	COMMIT

	if object_id('Capping2_tmp_View_dupe_Culling_1') is not null drop table Capping2_tmp_View_dupe_Culling_1
	commit --; --^^ to be removed

	-- First off: Kick out the duplicates out that come in from the weird day wrapping stuff
	select
			subscriber_id
		,	adjusted_event_start_time
		,	X_Viewing_Start_Time
		,	min(ID_Key)				as Min_ID_Key
	into	Capping2_tmp_View_dupe_Culling_1
	from	Capping2_01_Viewing_Records
	group by
			subscriber_id
		,	adjusted_event_start_time
		,	X_Viewing_Start_Time
	commit --;-- ^^ originally a commit

	create unique index idx1 on Capping2_tmp_View_dupe_Culling_1 (Min_ID_Key)
	commit --;-- ^^ originally a commit

			-- Delete records with non-existing ID_Key in the deduped table
	delete from	Capping2_01_Viewing_Records
	from
					Capping2_01_Viewing_Records			a
		left join	Capping2_tmp_View_dupe_Culling_1	b	on	a.ID_Key	=	b.Min_ID_Key
	where b.Min_ID_Key is null
	commit --;-- ^^ originally a commit


	-- For logging and flagging and QA and stuff:

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from Capping2_tmp_View_dupe_Culling_1
	commit --; --^^ to be removed

	execute M00_2_output_to_logger  'C01: Midway 1/3 (Deduplication)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	if object_id('Capping2_tmp_View_dupe_Culling_1') is not null drop table Capping2_tmp_View_dupe_Culling_1
	commit --;-- ^^ originally a commit

	-- Deduplication complete!
	if object_id('Capping2_tmp_Cumulative_Playback_Corrections') is not null drop table Capping2_tmp_Cumulative_Playback_Corrections
	commit --; --^^ to be removed
	
	if object_id('Capping2_tmp_Cumulative_Playback_Corrections_2') is not null drop table Capping2_tmp_Cumulative_Playback_Corrections_2
	commit --; --^^ to be removed

	-- Need to fix the broken viewing times for the playback records: Still debugging this section ########
	select
			cb_row_id
		,	subscriber_id
		,	adjusted_event_start_time
		,	x_programme_viewed_duration
		,	rank()	over	(
								partition by
										subscriber_id
									,	adjusted_event_start_time
								order by
										x_viewing_start_time
									,	tx_start_datetime_utc	desc
									,	x_viewing_end_time
									,	cb_row_id
							)	as	sequencer
		-- We've got one nasty duplicate thing in here; see cb_row_id 6463255125634728347 vs 6463255125650111897
		-- on the 11th of February: same subscriber ID and event start time, different end times, stuff like
		-- that shouldn't happen. Probably ordering by viewing end time is going to do terrible things to the
		-- viewing data (given it's for a correction in the playback sequence) but whatever, if it's a big deal
		-- the unit tests will catch it, and right now we just want something to run. If this doesn't work, we'll
		-- just clip everything from the conflicting events out of the data set, the loss won't be big at all.
	into	Capping2_tmp_Cumulative_Playback_Corrections
	from	Capping2_01_Viewing_Records
	where	live	=	0
	commit --;-- ^^ originally a commit

	-- Slight annoyance: Sybase won't let you order by anything other than numeric
	-- fields, so we still need this funny in-between table...
	create unique index sequencing_key on Capping2_tmp_Cumulative_Playback_Corrections (subscriber_id, adjusted_event_start_time, sequencer)
	commit --;-- ^^ originally a commit

	select
			cb_row_ID
		,	cast	(
						sum(x_programme_viewed_duration)	over	(
																		partition by
																				subscriber_id
																			,	adjusted_event_start_time
																		order by	sequencer
																	)	as	int
					)	as	x_cumul_programme_viewed_duration 	--Jon - this is the crazy thing where Sybase didn't like this field because it was an
																--integer-expression, so we have to convert it to an integer
	into	Capping2_tmp_Cumulative_Playback_Corrections_2
	from	Capping2_tmp_Cumulative_Playback_Corrections
	commit --;-- ^^ originally a commit

	create unique index fake_pk on Capping2_tmp_Cumulative_Playback_Corrections_2 (cb_row_ID)
	commit --; --^^ to be removed

	-- Push those back into the viewing table...
	update	Capping2_01_Viewing_Records
	set
			x_viewing_end_time	=	dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
		,	x_viewing_start_time	=	dateadd(second,-x_programme_viewed_duration,dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time))
	from
					Capping2_01_Viewing_Records
		inner join	Capping2_tmp_Cumulative_Playback_Corrections_2	as	cpc2	on	Capping2_01_Viewing_Records.cb_row_id	=	cpc2.cb_row_id
	commit --; --^^ to be removed

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from Capping2_tmp_Cumulative_Playback_Corrections_2
	commit --; --^^ to be removed


	IF object_id('Capping2_tmp_Cumulative_Playback_Corrections') is not null drop table Capping2_tmp_Cumulative_Playback_Corrections
	commit --; --^^ to be removed
	IF object_id('Capping2_tmp_Cumulative_Playback_Corrections_2') is not null drop table Capping2_tmp_Cumulative_Playback_Corrections_2
	commit --; --^^ to be removed


	execute M00_2_output_to_logger  'C01: Midway 2/3 (Patch durations)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-- Finally, eliminate these "illegitimate" viewing records:

	--remove illegitimate playback views - these views are those that start on event end time and go beyond event end time
	delete from Capping2_01_Viewing_Records
	where X_Adjusted_Event_End_Time<x_viewing_end_time
	and x_viewing_start_time>=X_Adjusted_Event_End_Time
	commit --; --^^ to be removed
	-- Small, tiny, minisclue proportion of stuff...
	--2,596,912, 30m left
	--reset x_viewing_end_times for playback views
	
	update	Capping2_01_Viewing_Records
	set	x_viewing_end_time=X_Adjusted_Event_End_Time
	where
			X_Adjusted_Event_End_Time	<	x_viewing_end_time
		and	x_viewing_start_time		<	X_Adjusted_Event_End_Time
	commit --;-- ^^ originally a commit
	-- similarly tiny proportion.

	
	-- That table "Capping2_01_Viewing_Records" is the one that we take as our ball of viewing data.
	-- Okay, so we want some basic counts of total events and things like that, maybe even a
	-- profile of event duration distribution...
	if object_id('Capping2_tmp_Uncapped_Viewing_Totals') is not null drop table Capping2_tmp_Uncapped_Viewing_Totals
	commit --; --^^ to be removed

	select
			subscriber_id
		,	round(sum(datediff(ss, X_Viewing_Start_Time, X_Viewing_End_Time)) / 60.0, 0)	as total_box_viewing
	into	Capping2_tmp_Uncapped_Viewing_Totals
	from	Capping2_01_Viewing_Records
	where	daily_table_date	=	@target_date
	group by	subscriber_id
	commit --;-- ^^ originally a commit

	select @QA_catcher = avg(total_box_viewing)
	from Capping2_tmp_Uncapped_Viewing_Totals
	commit --;-- ^^ originally a commit

	update	CP2_QA_daily_average_viewing
	set	average_uncapped_viewing	=	@QA_catcher
	where	build_date	=	@target_date
	commit --;-- ^^ originally a commit

	if object_id('Capping2_tmp_Uncapped_Viewing_Totals') is not null drop table Capping2_tmp_Uncapped_Viewing_Totals
	commit --; --^^ to be removed

	-- We've got a bunch of instances where we clear any current control totals out of the QA tables
	-- so that there's some safety against re-running the procedure on the sameday.
	delete from CP2_QA_viewing_control_totals
	where build_date = @target_date

	commit --;-- ^^ originally a commit

	insert into CP2_QA_viewing_control_totals
	select
			@target_date
		,	convert(varchar(20), '1.) Collect')
		,	program_air_date
		,	live
		,	genre
		,	count(1)	as viewing_records
		,	round(sum(coalesce(datediff(second, X_Viewing_Start_Time, X_Viewing_End_Time),0)) / 60.0 / 60 / 24.0, 2)
	from	Capping2_01_Viewing_Records
	group by
			program_air_date
		,	live
		,	genre
	commit --;-- ^^ originally a commit

	-- Distribution of event profiles will get done later...

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from Capping2_01_Viewing_Records
	commit --; --^^ to be removed

	execute M00_2_output_to_logger  'C01: Complete! (Data cleansing)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-------------------------------------------------------------------------------------------------
	-- C02) CONDENSE VIEWING DATA INTO LISTING OF EVENTS
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : C02) CONDENSE VIEWING DATA INTO LISTING OF EVENTS'
	COMMIT

	-- One Week is getting turned into CP2_event_listing...
	--IF object_id('one_week') is not null drop table one_week
	if object_id('CP2_event_listing') is not null drop table CP2_event_listing
	commit --; --^^ to be removed

	create table CP2_event_listing	(
											Subscriber_Id                      integer         not null
										,	account_number                     varchar(20)     not null
										,	fake_cb_row_id                     bigint          not null    -- we just need it to break some ties it'll still be unique
										,	X_Type_Of_Viewing_Event            varchar(40)     not null
                                                                                ,       Pre_Standby_Event_Flag             bit             default 0
										,	Adjusted_Event_Start_Time          datetime        not null
										,	X_Adjusted_Event_End_Time          datetime
										,	event_start_hour                   tinyint
										,	event_start_day                    tinyint
										,	X_Event_Duration                   decimal(10,0)
										,	event_dur_mins                     integer
										,	live                               bit
										,	initial_genre                      varchar(25)
										,	initial_sub_genre                  varchar(25)
										,	initial_channel_name               varchar(30)     -- This guy gets populated as uppercase and trimmed
										,	Initial_Service_Key                bigint
										,	program_air_date                   date
										,	program_air_datetime               datetime
										,	num_views                          int
										,	num_genre                          int
										,	num_sub_genre                      int
										,	viewed_duration                    int

										-- These guys are a channel categorisation lookup
										,	pack                               varchar(100)
										,	pack_grp                           varchar(30)
										-- We also use P/S box flags:
										,	box_subscription                   varchar(1)

										-- Columns used in applying caps:
										,	bucket_id                          integer         -- Composite lookup for: event_start_hour, event_start_day, initial_channel_name, Live
										,	max_dur_mins                       int             default null
										,	capped_event                       bit             default 0

										-- Yeah, structure is always good:
										,	primary key (Subscriber_Id, Adjusted_Event_Start_Time) -- So we... *shouldn't* have any more than one event starting at the same time per box... might have to manage some deduplication...
									)
	-- We'll also need indices on this guy...
	commit --; --^^ to be removed

	create index    for_joins           on CP2_event_listing (account_number)
	commit --; --^^ to be removed
	create index    start_time_index    on CP2_event_listing (Adjusted_Event_Start_Time)
	commit --; --^^ to be removed
	create index    init_channel_index  on CP2_event_listing (initial_channel_name)
	commit --; --^^ to be removed
	create index for_the_joining_group  on CP2_event_listing (event_start_hour, event_start_day, initial_genre, box_subscription, pack_grp, Live)
	commit --; --^^ to be removed
	create index by_bucket_index        on CP2_event_listing (bucket_id, pack_grp, box_subscription)
	commit --;-- ^^ originally a commit

	--obtain event view
	insert into CP2_event_listing	(
											Subscriber_Id
										,	account_number
										,	fake_cb_row_id
										,	Adjusted_Event_Start_Time
										,	X_Type_Of_Viewing_Event
                                                                                ,       Pre_Standby_Event_Flag
										,	X_Adjusted_Event_End_Time
										,	X_Event_Duration
										,	event_start_hour
										,	event_start_day
										,	Live
										,	num_views
										,	num_genre
										,	num_sub_genre
										,	viewed_duration
										,	event_dur_mins
										,	pack_grp
										,	box_subscription
										,	bucket_id
										,	initial_genre
										,	initial_sub_genre
										,	initial_channel_name
										,	Initial_Service_Key
									)
	select
			Subscriber_Id
		,	min(account_number)            -- should be unique given the subscriber_id
		,	min(cb_row_id)                 -- we just need something unique to break some ties
		,	Adjusted_Event_Start_Time
		,	min(X_Type_Of_Viewing_Event)   -- should also be determined give nsubscriber ID and Adjusted_Event_Start_Time
		,       min(Pre_Standby_Event_Flag)    -- shouldnt need this, but if in doubt dont treat as pre-standby
                ,	min(X_Adjusted_Event_End_Time) --
		,	min(X_Event_Duration)          --
		,	min(event_start_hour)          --
		,	min(event_start_day)           --
		,	min(Live)                      -- All these min(.) values should be determined by
		,	count(1)
		,	count(distinct genre)
		,	count(distinct sub_genre)
		,	sum(x_programme_viewed_duration)
		,	cast(min(x_event_duration) / 60 as int)
		-- Other columns we have to specifically mention because Sybase can't handle defaults going into compound indices
		,	null                           -- pack_grp needs it
		,	null                           -- same with box_subscription
		,	null                           -- and bucket_id
		-- So trying to update all the events with initial genre / channel goes badly, so
		-- we're going to hack in a guess here which will probably be wrong for every event
		-- with num_views>=2 but it does mean we don't have to update records with num_views=1
		-- and that might help us dodge the temp space errors we're getting. Maybe :-/
		,	min(genre)
		,	min(sub_genre)
		,	upper(trim(min(channel_name)))
		,	min(Service_Key)
	from	Capping2_01_Viewing_Records
	group by
			Subscriber_Id
		,	Adjusted_Event_Start_Time
	commit --;-- ^^ originally a commit

	-- OK, event listing is now build.

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_event_listing
	commit --; --^^ to be removed

	execute M00_2_output_to_logger  'C02: Complete! (Event listing)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-------------------------------------------------------------------------------------------------
	-- C03.1) ATTACH METADATA: GENRE AT EVENT START
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : C03.1) ATTACH METADATA: GENRE AT EVENT START'
	COMMIT

	IF object_id('CP2_First_Programmes_In_Event') IS NOT NULL DROP TABLE CP2_First_Programmes_In_Event
	commit --; --^^ to be removed

	create table CP2_First_Programmes_In_Event	(
														subscriber_id                       integer         not null
													,	adjusted_event_start_time          datetime        not null
													-- For the genre assignement bits:
													,	genre                              varchar(25)
													,	sub_genre                          varchar(25)
													,	channel_name                       varchar(30)
													,	Service_Key                        bigint
													-- Things needed to assign caps to end of first program viewed (sectino C02.e)
													,	X_Adjusted_Event_End_Time          datetime
													,	x_viewing_end_time                 datetime
													,	sequencer                          integer         -- only needed for deduplication
													,	primary key	(
																			subscriber_id
																		,	adjusted_event_start_time
																		,	sequencer
																	)
												)
	commit --; --^^ to be removed

	-- Build table for first viewing record in each event
	insert into CP2_First_Programmes_In_Event
	select
		-- OK, so we're clipping CP2_First_Programmes_In_Event down to things that actually get referenced:
			subscriber_id
		,	adjusted_event_start_time
		,	genre
		,	sub_genre
		,	channel_name
		,	Service_Key
		-- Things needed to assign caps to end of first program viewed (sectino C02.e)
		,	X_Adjusted_Event_End_Time
		,	x_viewing_end_time
		,	rank() over	(
							partition by
									subscriber_id
								,	adjusted_event_start_time
							order by
									x_viewing_start_time
								,	cb_row_id desc
						)
	from	Capping2_01_Viewing_Records
	commit --;-- ^^ originally a commit
	--34723382
	-- delete all records which aren't necessary due to trank
	delete from	CP2_First_Programmes_In_Event
	where	sequencer	<>	1
	commit --;-- ^^ originally a commit

	/* Kept for reference while still in dev: we changed a few of the names to more sensible things
	--add channel, genre and sub genre
	alter table one_week                            -- => "CP2_event_listing"
	add genre_at_event_start_time varchar(30),      -- => "CP2_event_listing.initial_genre"
	add sub_genre_at_event_start_time varchar(30),  -- => "CP2_event_listing.initial_sub_genre"
	add channel_at_event_start_time varchar(30),    -- => "CP2_event_listing.initial_channel_name"
	add pack varchar(100) default null,             -- => "CP2_event_listing.pack"
	add pack_grp varchar(20) default null;          -- => "CP2_event_listing.pack_grp"
	*/

	-- Okay, so doing the full update gives us errors, so we're again going to roll out the trick
	-- that we only need to worry about updating the records that aren't first in event.
	commit --; --^^ to be removed
	update	CP2_event_listing
	set
			initial_genre          = fpie.genre
		,	initial_sub_genre      = fpie.sub_genre
		,	Initial_Service_Key    = fpie.Service_Key
	from
					CP2_event_listing
		inner join	CP2_First_Programmes_In_Event	as	fpie	on	CP2_event_listing.subscriber_id				=	fpie.subscriber_id
																and	CP2_event_listing.adjusted_event_start_time	=	fpie.adjusted_event_start_time
	where	CP2_event_listing.num_views	>	1
	-- Awesome, temp space issue averted. Going to have to keep an eye on that, and generally
	-- try to amange the "first item in event" things like this...
	commit --;-- ^^ originally a commit

	-- After that, the table still gets used in a later section when we might
	-- opt to cap the event to end of first viewing record (ie end of first
	-- programme)

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_event_listing
	where Initial_Service_Key is not null
	commit --; --^^ to be removed

	execute M00_2_output_to_logger  'C03.1: Complete! (Genre at start)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-------------------------------------------------------------------------------------------------
	-- C03.2) ATTACH METADATA: CHANNEL PACKS & PACK GROUPINGS
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : C03.2) ATTACH METADATA: CHANNEL PACKS & PACK GROUPINGS'
	COMMIT

	--add pack & network [Update: Network no longer in play]
	update	CP2_event_listing base
	set	base.pack	=	trim(cl.channel_pack)
	from	Vespa_Analysts.Channel_Map_Prod_Service_Key_Attributes as cl
	where base.Initial_Service_Key	=	cl.Service_Key
	commit --;-- ^^ originally a commit
	-- Would be much better to update the channel lookup, but hey...

	--add pack groups
	update CP2_event_listing
	set pack_grp = coalesce(pack, 'Other')
	from CP2_event_listing
	commit --;-- ^^ originally a commit

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_event_listing
	where pack_grp <> 'Other'
	commit --; --^^ to be removed

	execute M00_2_output_to_logger  'C03.2: Complete! (Pack grouping)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-------------------------------------------------------------------------------------------------
	-- C03.3) ATTACH METADATA: PRIMARY / SECONDARY BOX
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : C03.3) ATTACH METADATA: PRIMARY / SECONDARY BOX'
	COMMIT

	-- Yeah, in the new build we just pull the reference in from the weekly profiling build...
	update CP2_event_listing
	set CP2_event_listing.box_subscription = bl.PS_flag
	from CP2_box_lookup as bl
	where CP2_event_listing.subscriber_id = bl.subscriber_id
	-- That's much easier than going back to the customer database for each day separately
	commit --;-- ^^ originally a commit
	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_event_listing
	where box_subscription in ('P', 'S')
	commit --; --^^ to be removed

	execute M00_2_output_to_logger  'C03.3: Complete! (Primary / secondary box)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-------------------------------------------------------------------------------------------------
	-- C03.4) ATTACH METADATA: CAPPING BUCKET ID
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : C03.4) ATTACH METADATA: CAPPING BUCKET ID'
	COMMIT

	-- This guy is a composite key that summarises event_start_hour, event_start_day, initial_genre
	-- and live into one integer that's easy to use (/index/join).
	delete from CP2_capping_buckets
	commit --;-- ^^ originally a commit

	insert into CP2_capping_buckets (
											event_start_hour
										,	event_start_day
										,	initial_genre
										,	live
									)
	select
			event_start_hour
		,	event_start_day
		,	initial_genre
		,	live
	from CP2_event_listing
	group by
			event_start_hour
		,	event_start_day
		,	initial_genre
		,	live
	commit --;-- ^^ originally a commit

	-- Push the bucket keys back onto the event listings:

	update	CP2_event_listing
	set		CP2_event_listing.bucket_id	=	cb.bucket_id
	from
					CP2_event_listing
		inner join	CP2_capping_buckets	as	cb	on  CP2_event_listing.event_start_hour = cb.event_start_hour
												and CP2_event_listing.event_start_day  = cb.event_start_day
												and CP2_event_listing.initial_genre    = cb.initial_genre
												and CP2_event_listing.live             = cb.live
	commit --;-- ^^ originally a commit

	-- We'll put bucket_id on the viewing data in section D01 after we've put the
	-- metadata there too.

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_event_listing
	where bucket_id is not null
	commit --; --^^ to be removed

	execute M00_2_output_to_logger  'C03.4: Complete! (Bucket IDs)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-------------------------------------------------------------------------------------------------
	-- C04) BUILDING N-TILES FOR CAPPING
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : C04) BUILDING N-TILES FOR CAPPING'
	COMMIT

	-- OK so from here I might change the strategy and not mess around that much with how these
	-- structures actually work (besides plugging them into the appropriately normalised structures).
	-- I'm 600 lines through with twice as much to go, need to switch up the approach if we're to
	-- have any hope of actually getting this completed. Here's a list of tables we renamed so as to
	-- offer at leaast *some* clarity as to what's going on, or at least, which tables are owned and
	-- used by the Capping 2 process:
	--      ntiles_week     -> CP2_ntiles_week
	--      nt_4_19         -> CP2_nt_4_19
	--      nt_20_3         -> CP2_nt_20_3
	--      nt_lp           -> CP2_nt_lp
	--      week_caps       -> CP2_calculated_viewing_caps
	--      h23_3           -> CP2_h23_3
	--      h4_14           -> CP2_h4_14
	--      h15_19          -> CP2_h15_19
	--      h20_22          -> CP2_h20_22
	--      lp              -> CP2_lp
	-- Other tables which are still floating around but will probably get killed when we normalise
	-- things out:
	--      CP2_QA_viewing_control_cap_distrib
	--      all_events              <- we already have CP2_event_listing, don't think we need this table. Update: it's GONE
	--      uncapped                => CP2_uncapped_events_lookup
	--      capped_events           => CP2_capped_events_lookup
	--      capped_events2          => CP2_capped_events_with_endpoints
	--      batching_lookup         -- depreciated - well, kind of replaced with CP2_capping_buckets, though buckets do different things
	--

	-- So yeah, this bit is super not-safe for accidental multiple runs. One might drop the tables
	-- the other is still using, and then it all goes way south.

	if object_id('CP2_ntiles_week') is not null drop table CP2_ntiles_week
	commit --; --^^ to be removed

	--calculate ntiles for caps
        declare @_SQL                           varchar(5000)

        set @_SQL = '
	select
			Live
		,	cast(adjusted_event_start_time as date) as event_date -- do we need this now were processing one day at a time?
		,	event_start_day
		,	event_start_hour
		,	box_subscription
		,	pack_grp
		,	initial_genre
		,	event_dur_mins
		,	ntile(200) over	(
								partition by
										Live
									,	event_start_day
								order by	x_event_duration
							)										as	ntile_lp
		,	ntile(200) over	(
								partition by
										Live
									,	event_start_day
									,	event_start_hour
									,	box_subscription
									,	pack_grp
									,	initial_genre
								order by	x_event_duration
							)										as	ntile_1	-- incl. partition by box_subscription
		,	ntile(200) over (
								partition by
										Live
									,	event_start_day
									,	event_start_hour
									,	pack_grp
									,	initial_genre 
								order by	x_event_duration
							)										as	ntile_2	-- excl. partition by box_subscription
		,	x_event_duration
		,	viewed_duration
		,	num_views
	into	CP2_ntiles_week
	from	CP2_event_listing
	where	x_event_duration	<	86400 -- 86400 seconds in a day or something
        '
        IF @exclude_pre_standby_events = 1 SET @_SQL = @_SQL + '  and   pre_standby_event_flag = 0 '

        SET @_SQL = @_SQL + '
        commit'

        execute(@_SQL)

	-- Wait... One_week doesn't get used past this? does the CP2_event_listing get used past this?
	-- Not yet, heh, this is the last use of it... okey... looks like we'll be able to either trim
	-- some stuff out or renormalise some things or generally tidy up...


	--create indexes
	create hng index idx1 on CP2_ntiles_week(event_start_day)
	commit --; --^^ to be removed
	create hng index idx2 on CP2_ntiles_week(event_start_hour)
	commit --; --^^ to be removed
	--create hng index idx3 on CP2_ntiles_week(Live); -- Not any more, not now it's a bit
	create hng index idx4 on CP2_ntiles_week(box_subscription)
	commit --; --^^ to be removed
	create hng index idx5 on CP2_ntiles_week(pack_grp)
	commit --; --^^ to be removed
	create hng index idx6 on CP2_ntiles_week(initial_genre)
	commit --;-- ^^ originally a commit
	-- Which of these do we need? which ones are actually helping?


	--select distinct event_date,event_start_day from CP2_ntiles_week

	--check data
	--select count(*),sum(num_views) from CP2_ntiles_week
	--count(*)        sum(CP2_ntiles_week.num_views)
	--25928067        34274204

	--select count(*),sum(num_views) from one_week where band_dur_days = 0
	--25928067        34274204

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_ntiles_week
	commit --; --^^ to be removed

	execute M00_2_output_to_logger  'C04: Complete! (n-Tile generation)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-------------------------------------------------------------------------------------------------
	-- C05) TABLES OF N-TILES (?)
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : C05) TABLES OF N-TILES (?)'
	COMMIT

	-- All the different caps tables for the different regimes of capping

	--create capping limits for start hours 4-19
	if object_id('CP2_nt_4_19') is not null drop table CP2_nt_4_19
	commit --; --^^ to be removed

	SELECT
			Live
		,	event_date
		,	event_start_day
		,	event_start_hour
		,	box_subscription
		,	pack_grp
		,	initial_genre
		,	ntile_1
		,	min(event_dur_mins) as min_dur_mins
		,	max(event_dur_mins) as max_dur_mins
		,	PERCENTILE_disc(0.5) WITHIN GROUP (ORDER BY event_dur_mins) as median_dur_mins
		,	count(*) as num_events
		,	sum(num_views) as tot_views
		,	sum(x_event_duration) as event_duration
		,	sum(viewed_duration) as viewed_duration
	into	CP2_nt_4_19
	FROM	CP2_ntiles_week
	where
			event_start_hour	>=	4
		and event_start_hour	<=	19
		and	Live				=	1
	group by
			Live
		,	event_date
		,	event_start_day
		,	event_start_hour
		,	box_subscription
		,	pack_grp
		,	initial_genre
		,	ntile_1
	commit --;-- ^^ originally a commit

	--create indexes
	create hng index idx1 on CP2_nt_4_19(event_start_day)
	commit --; --^^ to be removed
	create hng index idx2 on CP2_nt_4_19(event_start_hour)
	commit --; --^^ to be removed
	create hng index idx4 on CP2_nt_4_19(box_subscription)
	commit --; --^^ to be removed
	create hng index idx5 on CP2_nt_4_19(pack_grp)
	commit --; --^^ to be removed
	create hng index idx6 on CP2_nt_4_19(initial_genre)

	commit --;-- ^^ originally a commit

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_nt_4_19
	commit --; --^^ to be removed

	execute M00_2_output_to_logger  'C05: Midway 1/3 (_nt_4_19)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	--create capping limits start hours 20-3
	if object_id('CP2_nt_20_3') is not null drop table CP2_nt_20_3
	commit --; --^^ to be removed

	SELECT
			Live
		,	event_date
		,	event_start_day
		,	event_start_hour
		,	box_subscription
		,	pack_grp
		,	initial_genre
		,	ntile_2
		,	min(event_dur_mins) as min_dur_mins
		,	max(event_dur_mins) as max_dur_mins
		,	PERCENTILE_disc(0.5) WITHIN GROUP (ORDER BY event_dur_mins) as median_dur_mins
		,	count(*) as num_events
		,	sum(num_views) as tot_views
		,	sum(x_event_duration) as event_duration
		,	sum(viewed_duration) as viewed_duration
	into	CP2_nt_20_3
	FROM	CP2_ntiles_week
	where
			event_start_hour	in (20,21,22,23,0,1,2,3)
		and	Live				=	1
	group by
			Live
		,	event_date
		,	event_start_day
		,	event_start_hour
		,	box_subscription
		,	pack_grp
		,	initial_genre
		,	ntile_2
	commit --;-- ^^ originally a commit

	--create indexes
	create hng index idx1 on CP2_nt_20_3(event_start_day)
	commit --; --^^ to be removed
	create hng index idx2 on CP2_nt_20_3(event_start_hour)
	commit --; --^^ to be removed
	create hng index idx4 on CP2_nt_20_3(pack_grp)
	commit --; --^^ to be removed
	create hng index idx5 on CP2_nt_20_3(initial_genre)
	commit --;-- ^^ originally a commit

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_nt_20_3
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'C05: Midway 2/3 (_nt_20_3)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-----------------------------
	-- --create capping limits for all live events
	-- execute drop_local_table 'CP2_nt_live'
	-- commit --; --^^ to be removed
	
	-- SELECT
			-- Live
		-- ,	event_date
		-- ,	event_start_day
		-- ,	event_start_hour
		-- ,	box_subscription
		-- ,	pack_grp
		-- ,	initial_genre
		-- ,	case
				-- when	event_start_hour	between	4 and 19	then	ntile_1
				-- else													ntile_2
			-- end															as ntile_live
		-- ,	min(event_dur_mins)											as min_dur_mins
		-- ,	max(event_dur_mins)											as max_dur_mins
		-- ,	PERCENTILE_disc(0.5) WITHIN GROUP (ORDER BY event_dur_mins)	as median_dur_mins
		-- ,	count(*)													as num_events
		-- ,	sum(num_views)												as tot_views
		-- ,	sum(x_event_duration)										as event_duration
		-- ,	sum(viewed_duration)										as viewed_duration
	-- into	CP2_nt_live
	-- FROM	CP2_ntiles_week
	-- where	Live	=	1
	-- group by
			-- Live
		-- ,	event_date
		-- ,	event_start_day
		-- ,	event_start_hour
		-- ,	box_subscription
		-- ,	pack_grp
		-- ,	initial_genre
		-- ,	ntile_live
	-- commit --;-- ^^ originally a commit

	-- --create indexes
	-- create hng index idx1 on CP2_nt_live(event_start_day)
	-- commit --; --^^ to be removed
	-- create hng index idx2 on CP2_nt_live(event_start_hour)
	-- commit --; --^^ to be removed
	-- create hng index idx4 on CP2_nt_live(box_subscription)
	-- commit --; --^^ to be removed
	-- create hng index idx5 on CP2_nt_live(pack_grp)
	-- commit --; --^^ to be removed
	-- create hng index idx6 on CP2_nt_live(initial_genre)

	-- commit --;-- ^^ originally a commit
	-----------------------------



	--create capping limits for playback
	if object_id('CP2_nt_lp') is not null drop table CP2_nt_lp
	commit --; --^^ to be removed

	SELECT
			Live
		,	event_start_day
		,	ntile_lp
		,	min(event_dur_mins) as min_dur_mins
		,	max(event_dur_mins) as max_dur_mins
		,	PERCENTILE_disc(0.5) WITHIN GROUP (ORDER BY event_dur_mins) as median_dur_mins
		,	count(*) as num_events
		,	sum(num_views) as tot_views
		,	sum(x_event_duration) as event_duration
		,	sum(viewed_duration) as viewed_duration
	into	CP2_nt_lp
	FROM	CP2_ntiles_week
	where	Live	=	0
	group by
			Live
		,	event_start_day
		,	ntile_lp
	commit --;-- ^^ originally a commit

	--create indexes
	create hng index idx1 on CP2_nt_lp(event_start_day)
	commit --;-- ^^ originally a commit

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_nt_lp
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'C05: Complete! (_nt_lp)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-------------------------------------------------------------------------------------------------
	-- C06) ALL KINDS OF DIFFERENT CAPPING TABLES
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : C06) ALL KINDS OF DIFFERENT CAPPING TABLES'
	COMMIT

	/*create or replace variable*/ declare @median_dur_min int commit
	/*create or replace variable*/ declare @threshold_nontile int commit
	/*create or replace variable*/ declare @threshold_ntile int commit
	/*create or replace variable*/ declare @playback_ntile int commit
	/*create or replace variable*/ declare @startTime time commit
	/*create or replace variable*/ declare @endTime time commit
	/*create or replace variable*/ declare @startTime1 time commit -- used when there are two hour ranges
	/*create or replace variable*/ declare @endTime1 time commit
	/*create or replace variable*/ declare @startTime2 time commit -- used when there are two hour ranges
	/*create or replace variable*/ declare @endTime2 time commit

	-----------------------------
	

	--obtain max cap limits for live events

	------------------------------------------------------------------------------------------
	-- execute drop_local_table 'CP2_h_all'
	-- commit

	-- --identify ntile threshold for ALL event start hours
	-- select
			-- Live
		-- ,	event_date
		-- ,	event_start_day
		-- ,	event_start_hour
		-- ,	case
				-- when	event_start_hour	between	4 and 19	then	NT.box_subscription
				-- else													NULL
			-- end						as	box_subscription
		-- ,	pack_grp
		-- ,	initial_genre
		-- ,	min	(
					-- case
						-- when	median_dur_mins	>=	MET.BOX_SHUT_DOWN			then	ntile_live
						-- else													null
					-- end
				-- )					as	pri_ntile
		-- ,	max(ntile_live)			as	sec_ntile
		-- ,	case
				-- when	pri_ntile is null	then	sec_ntile - MET.THRESHOLD_NONTILE
				-- else								pri_ntile - MET.THRESHOLD_NTILE
			-- end						as	cap_ntile
		-- ,	cast(null as integer)	as	min_dur_mins
	-- into	CP2_h_all
	-- from
					-- CP2_nt_live		AS	NT
		-- inner join	(
                	-- select
                            -- START_TIME
                        -- ,   END_TIME
                        -- ,   BANK_HOLIDAY_WEEKEND
                        -- ,   COMMON_PARAMETER_GROUP
                        -- ,	max(BOX_SHUT_DOWN)		OVER (PARTITION BY BANK_HOLIDAY_WEEKEND, COMMON_PARAMETER_GROUP) AS	BOX_SHUT_DOWN
                        -- ,	max(THRESHOLD_NONTILE)	OVER (PARTITION BY BANK_HOLIDAY_WEEKEND, COMMON_PARAMETER_GROUP) AS	THRESHOLD_NONTILE
                    	-- ,	max(THRESHOLD_NTILE)	OVER (PARTITION BY BANK_HOLIDAY_WEEKEND, COMMON_PARAMETER_GROUP) AS	THRESHOLD_NTILE
						-- ,	EFFECTIVE_FROM
						-- ,	EFFECTIVE_TO
                	-- from	CP2_metadata_table
					-- )				AS	MET		ON		NT.EVENT_START_HOUR			BETWEEN	DATEPART(HOUR,MET.START_TIME)
																					-- AND		DATEPART(HOUR,MET.END_TIME)
												-- AND     NT.event_date		  		BETWEEN MET.EFFECTIVE_FROM
																					-- AND     MET.EFFECTIVE_TO
												-- AND		MET.BANK_HOLIDAY_WEEKEND	=		@targetDateIsWeekend
	-- where	Live	=	1
	-- group by
			-- Live
		-- ,	event_date
		-- ,	event_start_day
		-- ,	event_start_hour
		-- ,	box_subscription
		-- ,	pack_grp
		-- ,	initial_genre
		-- ,	MET.THRESHOLD_NONTILE
		-- ,	MET.THRESHOLD_NTILE
	-- commit --;-- ^^ originally a commit

	-- update	CP2_h_all t1
	-- set		min_dur_mins	=	t2.min_dur_mins
	-- from	CP2_nt_live t2
	-- where
			-- t1.Live				=	t2.Live
		-- and t1.event_start_day	=	t2.event_start_day
		-- and t1.event_start_hour	=	t2.event_start_hour
		-- and t1.box_subscription	=	t2.box_subscription
		-- and t1.pack_grp			=	t2.pack_grp
		-- and t1.initial_genre	=	t2.initial_genre
		-- and t1.cap_ntile		=	t2.ntile_live
	-- commit
	------------------------------------------------------------------------------------------
	
	if object_id('CP2_h23_3') is not null drop table CP2_h23_3
	commit --; --^^ to be removed

	--identify ntile threshold for event start hours 23-3
	select
			Live
		,	event_date
		,	event_start_day
		,	event_start_hour
		,	pack_grp
		,	initial_genre
		,	min	(
					case
						when	median_dur_mins	>=	MET.BOX_SHUT_DOWN			then	ntile_2
						else													null
					end
				)					as	pri_ntile
		,	max(ntile_2)			as	sec_ntile
		,	case
				when	pri_ntile is null	then	sec_ntile - MET.THRESHOLD_NONTILE
				else								pri_ntile - MET.THRESHOLD_NTILE
			end						as	cap_ntile
		,	cast(null as integer)	as min_dur_mins
	into	CP2_h23_3
	from
					CP2_nt_20_3			AS	NT
		inner join	CP2_metadata_table	AS	MET		ON		NT.EVENT_START_HOUR			BETWEEN	DATEPART(HOUR,MET.START_TIME)
																						AND		DATEPART(HOUR,MET.END_TIME)
    												AND     NT.event_date		  		BETWEEN MET.EFFECTIVE_FROM
    																					AND     MET.EFFECTIVE_TO
    												AND		MET.BANK_HOLIDAY_WEEKEND	=		@targetDateIsWeekend
	where
			event_start_hour	in (23,0,1,2,3)
		and	Live				=	1
	group by
			Live
		,	event_date
		,	event_start_day
		,	event_start_hour
		,	pack_grp
		,	initial_genre
		,	MET.THRESHOLD_NONTILE
		,	MET.THRESHOLD_NTILE
	commit --;-- ^^ originally a commit

	update	CP2_h23_3 t1
	set		min_dur_mins	=	t2.min_dur_mins
	from	CP2_nt_20_3 t2
	where
			t1.Live				=	t2.Live
		and t1.event_start_day	=	t2.event_start_day
		and t1.event_start_hour	=	t2.event_start_hour
		and t1.pack_grp			=	t2.pack_grp
		and t1.initial_genre	=	t2.initial_genre
		and t1.cap_ntile		=	t2.ntile_2
	commit --;-- ^^ originally a commit

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_h23_3
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'C06: Midway 1/4 (_h23_3)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit


	-----------------------------

	if object_id('CP2_h4_14') is not null drop table CP2_h4_14
	commit --; --^^ to be removed



	--identify ntile threshold for event start hours 4-14
	select
			Live
		,	event_date
		,	event_start_day
		,	event_start_hour
		,	box_subscription
		,	pack_grp
		,	initial_genre
		,	min	(
					case
						when	median_dur_mins >= MET.BOX_SHUT_DOWN	then	ntile_1
						else												null
					end
				)					as	pri_ntile
		,	max(ntile_1)			as	sec_ntile
		,	case
				when	pri_ntile is null		then	sec_ntile - MET.THRESHOLD_NONTILE
				else									pri_ntile - MET.THRESHOLD_NTILE
			end						as	cap_ntile
		,	cast(null as integer)	as	min_dur_mins
	into	CP2_h4_14
	from
					CP2_nt_4_19			AS	NT
		inner join	CP2_metadata_table	AS	MET		ON		NT.EVENT_START_HOUR			BETWEEN	DATEPART(HOUR,MET.START_TIME)
																						AND		DATEPART(HOUR,MET.END_TIME)
    												AND     NT.event_date		  		BETWEEN MET.EFFECTIVE_FROM
    																					AND     MET.EFFECTIVE_TO
    												AND		MET.BANK_HOLIDAY_WEEKEND	=		@targetDateIsWeekend
	where
			event_start_hour	in	(4,5,6,7,8,9,10,11,12,13,14)
		and	Live				=	1
	group by
			Live
		,	event_date
		,	event_start_day
		,	event_start_hour
		,	box_subscription
		,	pack_grp
		,	initial_genre
		,	MET.THRESHOLD_NONTILE
		,	MET.THRESHOLD_NTILE
	commit --;-- ^^ originally a commit

	update	CP2_h4_14 		t1
	set		min_dur_mins	=	t2.min_dur_mins
	from	CP2_nt_4_19		t2
	where
			t1.Live				=	t2.Live
		and t1.event_start_day	=	t2.event_start_day
		and t1.event_start_hour	=	t2.event_start_hour
		and t1.box_subscription	=	t2.box_subscription
		and t1.pack_grp			=	t2.pack_grp
		and t1.initial_genre	=	t2.initial_genre
		and t1.cap_ntile		=	t2.ntile_1
	commit --;-- ^^ originally a commit

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_h4_14
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'C06: Midway 2/4 (_h4_14)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-----------------------------


	if object_id('CP2_h15_19') is not null drop table CP2_h15_19
	commit --; --^^ to be removed

	--identify ntile threshold for event start hours 15-19
	select
			Live
		,	event_date
		,	event_start_day
		,	event_start_hour
		,	box_subscription
		,	pack_grp
		,	initial_genre
		,	max(ntile_1) as ntile
		,	ntile - MET.THRESHOLD_NTILE	as	cap_ntile
		,	cast(null as integer)		as	min_dur_mins
	into	CP2_h15_19
	from			CP2_nt_4_19			AS	NT
		inner join	CP2_metadata_table	AS	MET		ON		NT.EVENT_START_HOUR			BETWEEN	DATEPART(HOUR,MET.START_TIME)
																						AND		DATEPART(HOUR,MET.END_TIME)
    												AND     NT.event_date		  		BETWEEN MET.EFFECTIVE_FROM
    																					AND     MET.EFFECTIVE_TO
    												AND		MET.BANK_HOLIDAY_WEEKEND	=		@targetDateIsWeekend
	where
			event_start_hour	in	(15,16,17,18,19)
		and	Live				=	1
	group by
			Live
		,	event_date
		,	event_start_day
		,	event_start_hour
		,	box_subscription
		,	pack_grp
		,	initial_genre
		,	MET.THRESHOLD_NTILE
	commit --;-- ^^ originally a commit

	update	CP2_h15_19		t1
	set		min_dur_mins	=	t2.min_dur_mins
	from	CP2_nt_4_19		t2
	where
			t1.Live					=	t2.Live
		and t1.event_start_day		=	t2.event_start_day
		and t1.event_start_hour		=	t2.event_start_hour
		and t1.box_subscription		=	t2.box_subscription
		and t1.pack_grp				=	t2.pack_grp
		and t1.initial_genre		=	t2.initial_genre
		and t1.cap_ntile			=	t2.ntile_1
	commit --;-- ^^ originally a commit

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_h15_19
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'C06: Midway 3/4 (_h15_19)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-----------------------------


	if object_id('CP2_h20_22') is not null drop table CP2_h20_22
	commit --; --^^ to be removed

	--identify ntile threshold for event start hours 20-22
	select
			Live
		,	event_date
		,	event_start_day
		,	event_start_hour
		,	pack_grp
		,	initial_genre
		,	min	(
					case
						when	median_dur_mins	>=	((23-event_start_hour-1)*60) + MET.BOX_SHUT_DOWN	then	ntile_2
						else																					null
					end
				)					as	pri_ntile
		,	max(ntile_2)			as	sec_ntile
		,	case
				when	pri_ntile is null		then	sec_ntile - MET.THRESHOLD_NONTILE
				else									pri_ntile - MET.THRESHOLD_NTILE
			end						as	cap_ntile
		,	cast(null as integer)	as	min_dur_mins
	into	CP2_h20_22
	from
					CP2_nt_20_3			AS	NT
		inner join	CP2_metadata_table	AS	MET		ON		NT.EVENT_START_HOUR			BETWEEN	DATEPART(HOUR,MET.START_TIME)
																						AND		DATEPART(HOUR,MET.END_TIME)
    												AND     NT.event_date		  		BETWEEN MET.EFFECTIVE_FROM
    																					AND     MET.EFFECTIVE_TO
    												AND		MET.BANK_HOLIDAY_WEEKEND	=		@targetDateIsWeekend
	where
			event_start_hour	in	(20,21,22)
		and	Live				=	1
	group by
			Live
		,	event_date
		,	event_start_day
		,	event_start_hour
		,	pack_grp
		,	initial_genre
		,	MET.THRESHOLD_NONTILE
		,	MET.THRESHOLD_NTILE
	commit --;-- ^^ originally a commit

	update	CP2_h20_22		t1
	set		min_dur_mins	=	t2.min_dur_mins
	from CP2_nt_20_3		t2
	where
			t1.Live					=	t2.Live
		and t1.event_start_day		=	t2.event_start_day
		and t1.event_start_hour		=	t2.event_start_hour
		and t1.pack_grp				=	t2.pack_grp
		and t1.initial_genre		=	t2.initial_genre
		and t1.cap_ntile			=	t2.ntile_2
	commit --;-- ^^ originally a commit

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_h20_22
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'C06: Complete! (_h20_22)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-------------------------------------------------------------------------------------------------
	-- C07) BUILDING CENTRAL LISTING OF DERIVED CAPS
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : C07) BUILDING CENTRAL LISTING OF DERIVED CAPS'
	COMMIT

	delete from CP2_calculated_viewing_caps
	commit --; --^^ to be removed

	--identify caps for each variable dimension
	insert into	CP2_calculated_viewing_caps	(
													Live
												,	event_start_day
												,	event_start_hour
												,	box_subscription
												,	pack_grp
												,	initial_genre
												-- Again, managing the cannot-handle-defaults-into-multi-column-indices thing
												,	bucket_id
											)
	select
			Live
		,	event_start_day
		,	event_start_hour
		,	box_subscription
		,	pack_grp
		,	initial_genre
		,	null	as	bucket_id
	from	CP2_ntiles_week
	group by
			Live
		,	event_start_day
		,	event_start_hour
		,	box_subscription
		,	pack_grp
		,	initial_genre
		,	bucket_id
	commit --;-- ^^ originally a commit

	-- This is the last use of CP2_ntiles_week... if we reconstruct it's behaviour up to this point, we're good


	-- Throw on the bucket_id, it'll help us join a few things later:
	update	CP2_calculated_viewing_caps
	set		CP2_calculated_viewing_caps.bucket_id	=	cb.bucket_id
	from
					CP2_calculated_viewing_caps
		inner join	CP2_capping_buckets		as	cb		on  CP2_calculated_viewing_caps.event_start_day  = cb.event_start_day
														and CP2_calculated_viewing_caps.event_start_hour = cb.event_start_hour
														and CP2_calculated_viewing_caps.initial_genre    = cb.initial_genre
														and CP2_calculated_viewing_caps.Live             = cb.Live
	commit --;-- ^^ originally a commit

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_calculated_viewing_caps
	where bucket_id is not null
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'C07: Midway 1/3 (Buckets)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-- doesn't split out across the pack group and the box subscription, but it still helps.

	-- If we do want to throw on an integer key for the buckets, we need to use:
	--      event_start_day, event_start_hour, pack_grp, initial_genre, live
	-- Sometimes the box subscription isn't used, that has to stay out as a different
	-- thing. Still doing the multi-column join then, but at least the integer key
	-- will be a *bit* better. Update: we now have the cap_bucket_id field on the
	-- CP2_calculated_viewing_caps table, able we'll come up with some other workaround for the
	-- caps that are applied uniformly across box subscription type.

	-- update threshold table with cap limits
	update	CP2_calculated_viewing_caps	t1
	set		max_dur_mins	=	t2.min_dur_mins
	from	CP2_h23_3					t2
	where
			t1.Live				=	t2.Live
		and t1.event_start_day	=	t2.event_start_day
		and t1.event_start_hour	=	t2.event_start_hour
		and t1.pack_grp			=	t2.pack_grp
		and t1.initial_genre	=	t2.initial_genre
	commit --;-- ^^ originally a commit

	update	CP2_calculated_viewing_caps t1
	set		max_dur_mins	=	t2.min_dur_mins
	from	CP2_h4_14 t2
	where
			t1.Live				=	t2.Live
		and t1.event_start_day	=	t2.event_start_day
		and t1.event_start_hour	=	t2.event_start_hour
		and t1.box_subscription	=	t2.box_subscription
		and t1.pack_grp			=	t2.pack_grp
		and t1.initial_genre	=	t2.initial_genre
	commit --;-- ^^ originally a commit

	update	CP2_calculated_viewing_caps t1
	set	max_dur_mins	=	t2.min_dur_mins
	from	CP2_h15_19 t2
	where
			t1.Live				=	t2.Live
		and t1.event_start_day	=	t2.event_start_day
		and t1.event_start_hour	=	t2.event_start_hour
		and t1.box_subscription	=	t2.box_subscription
		and t1.pack_grp			=	t2.pack_grp
		and t1.initial_genre	=	t2.initial_genre
	commit --;-- ^^ originally a commit

	update	CP2_calculated_viewing_caps t1
	set	max_dur_mins	=	t2.min_dur_mins
	from	CP2_h20_22 t2
	where
			t1.Live				=	t2.Live
		and t1.event_start_day	=	t2.event_start_day
		and t1.event_start_hour	=	t2.event_start_hour
		and t1.pack_grp			=	t2.pack_grp
		and t1.initial_genre	=	t2.initial_genre
	commit --; --^^ to be removed

	----------------------------------------------------------------
	-- update	CP2_calculated_viewing_caps	t1
	-- set		max_dur_mins	=	t2.min_dur_mins
	-- from	CP2_h_all	t2
	-- where
			-- t1.Live				=	t2.Live
		-- and t1.event_start_day	=	t2.event_start_day
		-- and t1.event_start_hour	=	t2.event_start_hour
		-- and t1.box_subscription	=	t2.box_subscription
		-- and t1.pack_grp			=	t2.pack_grp
		-- and t1.initial_genre	=	t2.initial_genre
	-- commit --;-- ^^ originally a commit
	----------------------------------------------------------------

	
	commit --;-- ^^ originally a commit
	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_calculated_viewing_caps
	where max_dur_mins is not null
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'C07: Midway 2/3 (Live only)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

    set @playback_ntile=(select max(PLAYBACK_NTILE) from cp2_metadata_table where BANK_HOLIDAY_WEEKEND	=	@targetDateIsWeekend) -- hereeeeeeeeeeeeeeeeeeeeeee

	if object_id('CP2_lp') is not null drop table CP2_lp
	commit --; --^^ to be removed

	--identify ntile threshold for playback events
	select
			Live
		,	event_start_day
		,	@playback_ntile /*max(ntile_lp)*/ as ntile
		,	ntile					as	cap_ntile
		,	cast(null as integer)	as	min_dur_mins
	into	CP2_lp
	FROM	CP2_nt_lp
	where	Live	=	0
	group by
			Live
		,	event_start_day
	commit --; --^^ to be removed

	update	CP2_lp t1
	set		min_dur_mins	=	t2.min_dur_mins
	from	CP2_nt_lp t2
	where
			t1.Live				=	t2.Live
		and t1.event_start_day	=	t2.event_start_day
		and t1.cap_ntile		=	t2.ntile_lp
	commit --;-- ^^ originally a commit

	--update playback limits in caps table
	update	CP2_calculated_viewing_caps t1
	set		max_dur_mins	=	t2.min_dur_mins
	from	CP2_lp t2
	where
			t1.Live				=	t2.Live
		and t1.event_start_day	=	t2.event_start_day
		and t1.max_dur_mins		is null
	commit --;-- ^^ originally a commit

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_calculated_viewing_caps
	where max_dur_mins is not null
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'C07: Complete! (Central cap listing)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-------------------------------------------------------------------------------------------------
	-- C08) GLOBAL CAPPING BOUNDS
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : C08) GLOBAL CAPPING BOUNDS'
	COMMIT

	--reset capping limits that are less than the lower limit (see variables in the setup section)
	update	CP2_calculated_viewing_caps
	set	max_dur_mins	=	@min_cap_bound_minutes
	where
			(
					max_dur_mins	<	@min_cap_bound_minutes
				or	max_dur_mins	is null
			)
		and	Live	=	1
	commit --; --^^ to be removed

	--reset capping limits that are more than upper limit (see variables in the setup section)
	update	CP2_calculated_viewing_caps
	set		max_dur_mins = @max_cap_bound_minutes
	where max_dur_mins > @max_cap_bound_minutes
	and Live=1

	commit --;-- ^^ originally a commit
	set @QA_catcher = -1
	commit --; --^^ to be removed

	select	@QA_catcher	=	count(1)
	from	CP2_calculated_viewing_caps
	where	max_dur_mins	in	(
										@min_cap_bound_minutes
									,	@max_cap_bound_minutes
								)
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'C08: Complete! (Global cap bounds)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-------------------------------------------------------------------------------------------------
	-- C09) DISTRIBUTION OF CAPPING BOUNDS JUST FOR QA
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : C09) DISTRIBUTION OF CAPPING BOUNDS JUST FOR QA'
	COMMIT

	-- Note that this isn't a profile over the use of caps, it's just on the caps that as get built;
	-- there's no extra weight here for caps that get used more often. Oh, also, all our caps should
	-- be between 20 and 120, so that's just a hundred entries that we can just go out and graph...

	delete from CP2_QA_viewing_control_cap_distrib
	where build_date = @target_date

	commit --;-- ^^ originally a commit

	-- OK, here we're using the cumulative ranking duplication trick since we don't have any unique
	-- keys to force the rank to be unique over entries;
	insert into CP2_QA_viewing_control_cap_distrib	(
															build_date
														,	max_dur_mins
														,	cap_instances
													)
	select
			@target_date
		,	max_dur_mins
		,	count(1)
	from	CP2_calculated_viewing_caps
	group by	max_dur_mins
	commit --;-- ^^ originally a commit

	/* ##QA##EQ##: Extraction query: graph this guy in Excel I guess
	select * from CP2_QA_viewing_control_cap_distrib
	order by max_dur_mins;
	*/

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_QA_viewing_control_cap_distrib
	where build_date = @target_date
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'C09: Complete! (Cap distributions)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit


	--select 'start D', now(); --^^ just for debug

	-------------------------------------------------------------------------------------------------
	-- D) APPLYING CAPS TO VIEWING DATA
	-------------------------------------------------------------------------------------------------

	-------------------------------------------------------------------------------------------------
	-- D01) ATTACH CUSTOMER METADATA TO VIEWING
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : D01) ATTACH CUSTOMER METADATA TO VIEWING'
	COMMIT

	/* Now on core table creation
	--add primary/secondary flag to views so thresholds can be applied
	alter table Capping2_01_Viewing_Records
	--add src_system_id varchar(50),                -- This is only ever populated, never used, we don't need it
	add box_subscription varchar(1) default 'U';
	*/

	update	Capping2_01_Viewing_Records
	set	box_subscription = bl.PS_flag
	from
					Capping2_01_Viewing_Records	as	vr
		inner join	CP2_box_lookup				as	bl	on	vr.subscriber_id	=	bl.subscriber_id
	commit --;-- ^^ originally a commit

	--add genre, channel and pack to views so thresholds can be applied
	/* These guys have been added to the permanent table, and have also been given better names in some cases:
	alter table Capping2_01_Viewing_Records
	add initial_genre varchar(30),
	add channel_at_event_start_time varchar(30),            => initial_channel_name
	add pack varchar(100) default null,
	add pack_grp varchar(20) default null;
	*/

	-- A lot of this processing is just a duplication of stuff on the viewing records
	-- table rather than the events table, but, whatever.

	-- Wait, why does this happen here? I thought we were capping on the events level table?
	-- Maybe we only use that to build the caps, and then the capping happens directly on viewing
	-- data? We'l check how that works.

	-- ahahah awesome, this guy is giving us Query Temp space errors on it. Thing is,
	-- we're joining both tables by subscriber_id and adjusted_event_start_time, while
	-- we have both pairs indexed in both tables (both have other things in the index
	-- afterwards, but the multicolumn indices still work over just the initial columns)
	/* NEEDING REFACTORING:
	update Capping2_01_Viewing_Records
	set Capping2_01_Viewing_Records.initial_genre        = t2.genre
				,Capping2_01_Viewing_Records.initial_channel_name = t2.channel_name
	from Capping2_01_Viewing_Records
	inner join CP2_First_Programmes_In_Event t2
	on  Capping2_01_Viewing_Records.subscriber_id             = t2.subscriber_id
	and Capping2_01_Viewing_Records.adjusted_event_start_time = t2.adjusted_event_start_time;
	*/

	-- Okay, so we can't join in the first event stuff from the other table, Sybase is
	-- too small. But the initial genre is going to be exactly the genre for the first
	-- item in each event, which is going to permit simpler treatment for a huge portion
	-- of the data set:
	update	Capping2_01_Viewing_Records
	set
			Capping2_01_Viewing_Records.initial_genre			=	genre
		,	Capping2_01_Viewing_Records.initial_channel_name	=	channel_name
	where	adjusted_event_start_time	=	x_viewing_start_time
	commit --;-- ^^ originally a commit

	-- and now we can tack on the updates for subsequent viewing items:
	--    update Capping2_01_Viewing_Records
	--    set Capping2_01_Viewing_Records.initial_genre        = t2.genre
	--       ,Capping2_01_Viewing_Records.initial_channel_name = t2.channel_name
	--    from Capping2_01_Viewing_Records
	--    inner join CP2_First_Programmes_In_Event t2
	--    on  Capping2_01_Viewing_Records.subscriber_id             = t2.subscriber_id
	--    and Capping2_01_Viewing_Records.adjusted_event_start_time = t2.adjusted_event_start_time
	--    where Capping2_01_Viewing_Records.initial_genre is null
	--    commit

	--replaced the above code with this as I ran into the temp tablespace issue.  This code needs
	--QA'd!!
	if object_id('temp_chan_genre') is not null drop table temp_chan_genre
	commit --; --^^ to be removed

	select
			rec.cb_row_id
		,	event.genre
		,	event.channel_name
	into	temp_chan_genre
	from
			CP2_First_Programmes_In_Event event
		,	Capping2_01_Viewing_Records rec
	where
			event.subscriber_id				=	rec.subscriber_id
		and event.adjusted_event_start_time	=	rec.adjusted_event_start_time
		and rec.initial_genre				is null
	commit --;-- ^^ originally a commit

	create index tmp_idx_genre on temp_chan_genre (cb_row_id)
	commit --; --^^ to be removed


	update	Capping2_01_Viewing_Records
	set
			Capping2_01_Viewing_Records.initial_genre        = t2.genre
		,	Capping2_01_Viewing_Records.initial_channel_name = t2.channel_name
	from
					Capping2_01_Viewing_Records
		inner join	temp_chan_genre		t2		on	Capping2_01_Viewing_Records.cb_row_id	=	t2.cb_row_id
	commit --;-- ^^ originally a commit

	if object_id('temp_chan_genre') is not null drop table temp_chan_genre
	commit --; --^^ to be removed

	-- query temp space issues averted! (though, it only reduced the size of the update
	-- my a factor of three, and the panel is probably growing more than that, so it
	-- might just turn up again following the ramp up and that'd be funny too.)

	-- OK, now we have the metadata on the viewing items, we can get the bucket IDs too:
	update	Capping2_01_Viewing_Records
	set	Capping2_01_Viewing_Records.bucket_id	=	cb.bucket_id
	from
					Capping2_01_Viewing_Records
		inner join	CP2_capping_buckets			as	cb	on	Capping2_01_Viewing_Records.event_start_hour	=	cb.event_start_hour
														and Capping2_01_Viewing_Records.event_start_day  	=	cb.event_start_day
														and Capping2_01_Viewing_Records.initial_genre    	=	cb.initial_genre
														and Capping2_01_Viewing_Records.live             	=	cb.live
	commit --;-- ^^ originally a commit

	-- Wait, we already added this to the event listing, do we really need it on the
	-- viewing records too? whatever...
	--update Capping2_01_Viewing_Records base
	--   set base.pack = t2.channel_pack
	--  from Vespa_Analysts.Channel_Map_Cbi_Prod_Service_Key_Attributes as t2
	-- where base.Initial_Service_Key = t2.Service_Key
	--commit

	--add pack group so thresholds can be applied
	--update Capping2_01_Viewing_Records
	--set pack_grp = coalesce(pack, 'Other')
	--from Capping2_01_Viewing_Records
	--
	--commit

	-- Okay, so at this point, the control totals should till be identical...
	delete from	CP2_QA_viewing_control_totals
	where	(
					data_state	like	'2%'
				or	data_state	like	'3%'
				or	data_state	like	'4%'
			)
	and	build_date	=	@target_date
	commit --;-- ^^ originally a commit

	-- (clearing out all future control totals to so as to not cause any confusion)
	insert into CP2_QA_viewing_control_totals
	select
			@target_date
		,	convert(varchar(20),'2.) Pre-Cap') -- aliases are handled in table construction
		,	program_air_date
		,	live
		,	genre
		,	count(1)
		,	round	(
							sum	(
									coalesce	(
														datediff(second, X_Viewing_Start_Time, X_Viewing_End_Time)
													,	0
												)
								) / 60.0 / 60 / 24.0
						,	2
					)
	from	Capping2_01_Viewing_Records
	group by
			program_air_date
		,	live
		,	genre
	commit --;-- ^^ originally a commit

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select	@QA_catcher	= count(1)
	from	Capping2_01_Viewing_Records
	where
			pack				is not null
		and bucket_id			is not null
		and box_subscription	in ('P', 'S')
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'D01: Complete! (Metadata on viewing)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-------------------------------------------------------------------------------------------------
	-- D02) COMPARE VIEWING DATA TO CAPS: DETERMINE CAPPING APPLICATION
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : D02) COMPARE VIEWING DATA TO CAPS: DETERMINE CAPPING APPLICATION'
	COMMIT

	-- We're not ever using max_dur_mins on the viewing data table, it just gets applied to
	-- events, and even then it's only used to mark the ones that get capped so that we can
	-- assign endpoints of the uncapped guys to the capped items. So we're not even going to
	-- bother putting the duration on the viewing table (like it originally was), instead only
	-- put it on the table at the event aggregation level

	update	CP2_event_listing
	set	max_dur_mins	=	caps.max_dur_mins
	from
					CP2_event_listing			as	base
		inner join	CP2_calculated_viewing_caps	as	caps	on	base.bucket_id        =	caps.bucket_id
															and base.pack_grp         =	caps.pack_grp
															and base.box_subscription =	caps.box_subscription
	commit --;-- ^^ originally a commit
	-- (Do we actually need it while it's on the viewing data? do we only ever need it
	-- when it's on whole events? Actually, we never even use it on the events table either,
	-- because we use it to assign new event end times based on the distribution of other
	-- viewing events in the bucket; it only gets used to build the capped_event and then
	-- never gets seen again.)


	-- Follwing that, we'll make this decision about which of the events need to get capped:
	update	CP2_event_listing
	set	capped_event	=	case
								when dateadd(minute, max_dur_mins, adjusted_event_start_time) >= X_Adjusted_Event_End_Time	then	0
								else																								1
							end
	commit --; --^^ to be removed

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_event_listing
	where capped_event = 1
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'D02: Midway 1/3 (Find cappables)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-- Okay, update: trying to select the uncapped evens like this is just crazy, there are
	-- to many of them even before the ramp up. Instead, we're going to trim it down to just
	-- a sample of uncapped events, governed by the "@uncapped_sample_pop_max" at the top of the
	-- script, and use those to determine the capping bounds. This might give us a reasonable
	-- number of events all terminating at exactly the same second, and that'll only get worse
	-- when the boxes are scaled up to the Sky base, but whatever, it'll give us *some* kind
	-- of capping, as opposed to the temp space errors we're seeing at the moment. But, that
	-- was always going to be the case with this approach to capping anyway, because we're not
	-- fitting within a distribuion, we're just matching the endpoint of some other lucky
	-- viewing event.

	-- Because of how Sybase whines about how window functions work, we keep having to select
	-- these into different tables too, awesome.

	-- First stage: append a random number to do the random selection for us
	if object_id('CP2_uncapped_events_lookup_midway_1') is not null drop table CP2_uncapped_events_lookup_midway_1
	commit --; --^^ to be removed
	
	if object_id('CP2_uncapped_events_lookup_midway_2') is not null drop table CP2_uncapped_events_lookup_midway_2
	commit --; --^^ to be removed

	
	select
			bucket_id
		,	initial_channel_name
		,	X_Adjusted_Event_End_Time
		-- we're going to rank over this random variable to pick our sample:
		,	rand(number() * datepart(ms, now())) as sequencer
		-- We need these guys later to do the ordering that the endpoint selection thing uses;
		-- we can't build the event_id at this stage because the min_row & max_row trick wants
		-- the ranking to be dense,	 because a random element is selected between them.
		,	fake_cb_row_id
		,	adjusted_event_start_time
	into	CP2_uncapped_events_lookup_midway_1
	from	CP2_event_listing
	where	capped_event            =   0
          and   pre_standby_event_flag  =   0
	commit --;-- ^^ originally a commit

	-- CP2_uncapped_events_lookup also gets used to select the end times for capped events
	-- once the index lookup stuff is done, but other than that neither the uncapped nor the
	-- capped table gets used past the loop which populates that CP2_capped_events_with_endpoints table, which
	-- we may indeed keep as a seperate table and build that iteratively as we're going to
	-- need some way to drag the cb_row_id back in from the raw viewing table... maybe that
	-- happens at section D06.

	CREATE INDEX for_ranking on CP2_uncapped_events_lookup_midway_1 (bucket_id, initial_channel_name, sequencer)
	commit --;-- ^^ originally a commit

	-- Second stage: rank by this random number
	select
			* -- yes this is a horrible form, but we've already clipped the source table down to only the things we need.
		,	rank()	over	(
								partition by
										bucket_id
									,	initial_channel_name
								order by	sequencer
							)	as	cull_ranking
	into	CP2_uncapped_events_lookup_midway_2
	from	CP2_uncapped_events_lookup_midway_1
	commit --; --^^ to be removed
	-- We'd have done it in the same query, but Sybase doesn't like trying to put
	-- things which determine on number() inside a rank function, oh well.

	-- Third part: cull all the overpopulated buckets
	delete from CP2_uncapped_events_lookup_midway_2
	where	cull_ranking > @uncapped_sample_pop_max
	commit --;-- ^^ originally a commit

	create index for_ordering on CP2_uncapped_events_lookup_midway_2	(
																				bucket_id
																			,	initial_channel_name
																			,	adjusted_event_start_time
																			,	X_Adjusted_Event_End_Time
																			,	fake_cb_row_id
																		)
	commit --; --^^ to be removed


	if object_id('CP2_uncapped_events_lookup') is not null drop table CP2_uncapped_events_lookup
	commit --; --^^ to be removed

	-- Third stage: build the uncapped table lookup using just the remaiing sample of stuff
	select
			bucket_id
		,	initial_channel_name
		,	rank()	over	(
								order by
										bucket_id
									,	initial_channel_name
									,	X_Adjusted_Event_End_Time
									,	adjusted_event_start_time
									,	fake_cb_row_id
							)		as	event_id
		,	X_Adjusted_Event_End_Time
	into	CP2_uncapped_events_lookup
	from	CP2_uncapped_events_lookup_midway_2
	commit --;-- ^^ originally a commit

	-- Done! Clear out the intermediates.
	if object_id('CP2_uncapped_events_lookup_midway_1') is not null drop table CP2_uncapped_events_lookup_midway_1
	commit --; --^^ to be removed
	if object_id('CP2_uncapped_events_lookup_midway_2') is not null drop table CP2_uncapped_events_lookup_midway_2
	commit --;-- ^^ originally a commit

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_uncapped_events_lookup
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'D02: Midway 2/3 (Uncapped lookup)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-- (## QA item: check how many buckets ended up with the maximum number of uncapped
	-- events in them? bearing in mind we now have channel + bucket...)

	-- identify capped universe
	if object_id('CP2_capped_events_lookup') is not null drop table CP2_capped_events_lookup
	commit --; --^^ to be removed

	select
			subscriber_id
		,	bucket_id
		,	initial_channel_name
		,	adjusted_event_start_time
		,	X_Adjusted_Event_End_Time
		,	max_dur_mins
	into	CP2_capped_events_lookup
	from	CP2_event_listing
	where	capped_event            =   1
           OR   pre_standby_event_flag  =   1  --an OR condition is used here as we want to cap all pre-standby events, or those that are marked as requiring capping
	commit --;-- ^^ originally a commit

	-- create indexes to speed up processing
	create unique index fake_pk on CP2_uncapped_events_lookup (event_id)
	commit --; --^^ to be removed

	create        index idx1    on CP2_uncapped_events_lookup (bucket_id, initial_channel_name, X_Adjusted_Event_End_Time)
	commit --; --^^ to be removed

	create unique index fake_pk on   CP2_capped_events_lookup (bucket_id, initial_channel_name, adjusted_event_start_time, subscriber_id)
	-- bucket_id and cahnel name aren't needed there for completeness, but it really *really* helps on the query

	commit --;-- ^^ originally a commit
	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_capped_events_lookup
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'D02: Complete! (Capped lookup)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-- Okay, so using these buckets for this match will reduce the IO, simplify index use, tidy
	-- up the batching process too, generally a good move all around.

	-- And hey, turns out we can grab this number as a pretty good indicator of batch progress:
	/*create or replace variable*/ declare @number_of_capped_events float
	commit --; --^^ to be removed
	
	set @number_of_capped_events = @QA_catcher
	commit --;-- ^^ originally a commit

	-------------------------------------------------------------------------------------------------
	-- D03) INDEX THE DURATION-REPLACEMENT LOOKUP
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : D03) INDEX THE DURATION-REPLACEMENT LOOKUP'
	COMMIT

	-- So we've given this section a pretty thorough overhaul and now it actually runs on a
	-- full day of data. Main points introduced:
	--  a. We're now batching by start hour, initial genre, and live/playback
	--  b. We've got a single numerical key which defines all combinations of the above in 1 integer
	--  c. Columns that never get used are no longer included on the capped and uncapped tables
	--  d. Indices are now directly tuned to support the joins in play
	--  e. We've bounded the uncapped population to a fixed number of items per bucket
	-- That said, this action is still the bottleneck for this entire capping build; it's taking
	-- hours to process even a single day's worth of caps, and this is pre-ramp up. Past ramp up
	-- with 3x as much data returned? We might end up capping one day of data in each overnight
	-- build. Good thing we have a scheduler that's able to check if the build has gone too far
	-- into the morning, leave things for the next day, and just pick up where it left off. And
	-- depending on how we made the prioritisation work... it can still build the regular reports
	-- before continuing with the capping tasks outstanding from the previous builds. Nice that
	-- we built a scheduler as solidly as we did then, eh?

	-- The buckets are simultaneously more agressive batching (smaller batches) and a lot cleaner
	-- to work with that the batches that just took dat & start hour. We don't even need the
	-- batching lookup table any more, we can just run it off the buckets table.
	/*create or replace variable*/ declare @the_bucket     int
	commit --; --^^ to be removed
	/*create or replace variable*/ declare @max_buckets    int
	commit --; --^^ to be removed
	/*create or replace variable*/ declare @bucket_offset  int
	commit --; --^^ to be removed
	/*
	create variable @the_bucket     int;
	create variable @max_buckets    int;
	create variable @bucket_offset  int;
	*/
	-- need somewhere to put the results:
	if object_id('CP2_capped_events_with_endpoints') is not null drop table CP2_capped_events_with_endpoints
	commit --; --^^ to be removed
	
	create table CP2_capped_events_with_endpoints	( -- currently not a temp thing because we want to be able to track how data gets into that table...
															subscriber_id                  integer
														,	Adjusted_Event_Start_Time      datetime
														,	X_Adjusted_Event_End_Time      datetime    -- Uncapped event time: needed for control total purposes
														,	max_dur_mins                   integer
														,	bucket_id                      integer
														,	initial_channel_name           varchar(30)
														,	firstrow                       integer
														,	lastrow                        integer
														-- Variables that get played with later as caps are set:
														,	rand_num                       float       default null
														,	uncap_row_num                  integer     default null
														,	capped_event_end_time          datetime    default null
													)
	-- If we need the start time, initial channel etc we can just go into the bucket lookup
	-- and get that stuff afterwards.

	-- We'll add indices after all the data goes in.

	commit --;-- ^^ originally a commit

	-- Here is the start of the work loop:
	select
		-- Need this min/max thing because the buckets table has an IDENTITY key,
		-- and that doesn't get reset between builds...
			@the_bucket		=	min(bucket_id)
		,	@bucket_offset	=	min(bucket_id)
		,	@max_buckets	=	max(bucket_id)
	from	CP2_capping_buckets
	commit --;-- ^^ originally a commit

	-- Okay, now we can actually assemble the table:
	while	@the_bucket	<=	@max_buckets
	begin

		insert into	CP2_capped_events_with_endpoints	(
																subscriber_id
															,	adjusted_event_start_time
															,	X_Adjusted_Event_End_Time
															,	max_dur_mins
															,	bucket_id
															,	initial_channel_name
															,	firstrow
															,	lastrow
														)
		select
				t1.subscriber_id
			,	t1.adjusted_event_start_time
			,	min(t1.X_Adjusted_Event_End_Time)  -- they're all the same anyways
			,	min(t1.max_dur_mins)
			,	@the_bucket
			,	min(t1.initial_channel_name)       -- also determined by adjusted_event_start_time
			-- identify first and last row id in uncapped events that have same profile as capped event
			,	min(t2.event_id)
			,	max(t2.event_id)
		from
						CP2_capped_events_lookup	as	t1
			left join	CP2_uncapped_events_lookup	as	t2	on	t1.bucket_id              		=	@the_bucket
															and t2.bucket_id              		=	@the_bucket
															and t1.initial_channel_name   		=	t2.initial_channel_name
															and t2.X_Adjusted_Event_End_Time	>	dateadd(second, @min_view_duration_sec, t1.adjusted_event_start_time)     -- Capped event min length restriction (7 seconds)
															and t2.X_Adjusted_Event_End_Time	<=	dateadd(second, 180 * 60, t1.adjusted_event_start_time)                   -- Capped event max length restriction (180 minutes)
															and t2.X_Adjusted_Event_End_Time	<=	t1.X_Adjusted_Event_End_Time
		where	t1.bucket_id	=	@the_bucket
		-- and   t2.bucket_id            = @the_bucket     -- SBE: this condition is removed to retain capped event with no matching uncapped events
		group by
				t1.subscriber_id
			,	t1.adjusted_event_start_time
--        ,t1.X_Adjusted_Event_End_Time -- isn't this entirely determined by the event start time? in fact yes, it is... worse than that, it's never used later in the build...
		commit

		-- Check the control totals every now and then for progress tracking purposes:
		if mod(@the_bucket - @bucket_offset + 1, 40) = 0     -- notify every 40 buckets; the first demo build had ~470 buckets to consider
		begin
		
			set @QA_catcher = -1
			commit

			-- How many items have we resolved thus far?
			select @QA_catcher = count(1)
			from CP2_capped_events_with_endpoints
			commit

			-- Note that the counter we're tracking should ultimately head towards the number of capped
			-- items. hey, let's put that in as a progress meter...

			execute M00_2_output_to_logger 'D03: Processed bucket ' || convert(varchar(10), @the_bucket - @bucket_offset + 1) || ' out of ' || convert(varchar(10), @max_buckets - @bucket_offset + 1) || ' (Events: ' || round(100 * @QA_catcher/@number_of_capped_events,1) || '%) ' || coalesce(@QA_catcher, -1)
			commit
			-- Bear in mind that the later buckets are going to be much smaller than the earlier ones,
			-- because buckets with not very many events are less likely to be discovered earlier in
			-- the DISTINCT pass; progress will probably jump with the first few cycles of buckets,
			-- and then spend a lot of buckets just tidying up little sparsely populated edge cases.

		end

		-- Move on to the next bucket
		set @the_bucket = @the_bucket + 1
		commit
		
	end
	commit --;-- ^^ originally a commit
	-- This guy still takes ages, but it's now a managed/mitigated bottleneck rather
	-- than a showstopper.

	CREATE hg     INDEX idx1    ON CP2_capped_events_with_endpoints (uncap_row_num)
	commit --;-- ^^ originally a commit
	create unique index fake_PK on CP2_capped_events_with_endpoints (subscriber_id, adjusted_event_start_time)
	commit --;-- ^^ originally a commit

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_capped_events_with_endpoints
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'D03: Complete! (Duration replacement)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-------------------------------------------------------------------------------------------------
	-- D04) RANDOMLY CHOOSE REPLACEMENT DURATION FOR CAPPED EVENTS
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : D04) RANDOMLY CHOOSE REPLACEMENT DURATION FOR CAPPED EVENTS'
	COMMIT

	--create a pretty random multiplier
	/*create or replace variable*/ declare @multiplier bigint              --has to be a bigint if you are dealing with millions of records.
					--create variable @multiplier bigint
	commit --; --^^ to be removed
	
	SET @multiplier = DATEPART(millisecond,now())+1 -- pretty random number between 1 and 1000
	commit --; --^^ to be removed

	--generate random number for each capped event
	update	CP2_capped_events_with_endpoints
	set	rand_num	=	rand(number(*)*@multiplier)      --the number(*) function just gives a sequential number.
	commit --;-- ^^ originally a commit

	--identify row id in uncapped universe to select
	update	CP2_capped_events_with_endpoints
	set	uncap_row_num	=	case
								when	firstrow > 0	then	round(((lastrow - firstrow) * rand_num + firstrow),0)
								else							null
							end
	commit --;-- ^^ originally a commit

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select	@QA_catcher = count(1)
	from	CP2_capped_events_with_endpoints
	where	uncap_row_num is not null
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'D04: Complete! (Select replacements)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-------------------------------------------------------------------------------------------------
	-- D05) ASSIGN NEW END TIMES
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : D05) ASSIGN NEW END TIMES'
	COMMIT

	--assign new event end time to capped events
	update	CP2_capped_events_with_endpoints	t1
	set	capped_event_end_time	=	t2.X_Adjusted_Event_End_Time
	from
					CP2_capped_events_with_endpoints	as	t1
		inner join	CP2_uncapped_events_lookup			as	t2	on	t1.uncap_row_num	=	t2.event_id
	commit --;-- ^^ originally a commit

	-- And that's the last use of the "uncapped" table
	if object_id('CP2_uncapped_events_lookup') is not null drop table CP2_uncapped_events_lookup
	commit --; --^^ to be removed
	if object_id('CP2_capped_events_lookup') is not null drop table CP2_capped_events_lookup
	commit --;-- ^^ originally a commit


	--assign end time of first programme to capped events if no uncapped distribution is available
	update CP2_capped_events_with_endpoints t1
	set	capped_event_end_time	=	case	-- when capped time is still missing and first instance duration > max_dur_mins => max_dur_mins
										when
												t1.capped_event_end_time is null
											and	(datediff(second, t2.adjusted_event_start_time, t2.x_viewing_end_time) > max_dur_mins * 60)		then	dateadd(minute, max_dur_mins, t2.adjusted_event_start_time)
										when
												t1.capped_event_end_time is null																then	t2.x_viewing_end_time
										else																											t1.capped_event_end_time
									end
	from	CP2_First_Programmes_In_Event t2
	where
			t1.subscriber_id				=	t2.subscriber_id
		and t1.adjusted_event_start_time	=	t2.adjusted_event_start_time
		and t1.firstrow						is null
	commit --;-- ^^ originally a commit
	--2501
	-- Joining to CP2_First_Programmes_In_Event again? Should the broadcast end time be already in the record
	-- we're treating, ie, can do with a single table update, no join? -Yes, but what if the event
	-- ends before the first show ends? using the first program table, we dodge that issue. Also: predcting
	-- temp space errors on this guy too, oh well.

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_capped_events_with_endpoints
	where capped_event_end_time is not null
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'D05: Complete! (Assign end times)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	--select now(), 'landmark' ; --^^ just for debug


	-------------------------------------------------------------------------------------------------
	-- D06) PUSH CAPPING BACK ONTO INITIAL VIEWING TABLE
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : D06) PUSH CAPPING BACK ONTO INITIAL VIEWING TABLE'
	COMMIT

	-- Heh, no, we're going to throw all of this viewing into a new dynamically named table which
	-- matches the timestamp of the daily table. Or... well, we'll probably build the thing here
	-- and then do it as one single port, rather than have a whole bunch of dynamically named tables.
	-- But still... at this point we only need a tiny selection of what's on the viewing data tables,
	-- we're pretty close to the end of the process.

	-- Man, I hope I don't have to batch this thing as well.... nah, we seem okay.

	delete from CP2_capped_data_holding_pen
	commit --;-- ^^ originally a commit

	-- First we're only adding the capped data, have to remember to throw in
	-- the uncapped stuff too at a later stage when all the capping processing
	-- is done (but before the generic stuff like BARB minute and scaling
	-- weighting coeficients)
	insert into CP2_capped_data_holding_pen	(
													cb_row_id
												,	subscriber_id
												,	account_number
												,	programme_trans_sk
												,	adjusted_event_start_time
												,	X_Adjusted_Event_End_Time
												,	x_viewing_start_time
												,	x_viewing_end_time
												,	capped_event_end_time
												,	timeshifting
												,	capped_flag
                                                                                                ,       pre_standby_event_flag
												-- Things we need for control totals:
												,	program_air_date
												,	live
												,	genre
											)
	select
			vr.cb_row_id
		,	vr.subscriber_id
		,	vr.account_number
		,	vr.programme_trans_sk
		,	vr.adjusted_event_start_time
		,	vr.X_Adjusted_Event_End_Time
		,	vr.x_viewing_start_time
		,	vr.x_viewing_end_time
		,	cewe.capped_event_end_time
		,	case
				when	vr.live = 1		then	'LIVE'
				when	vr.live = 0		then	'TIMESHIFT'   -- Will later be replaced with 'VOSDAL' or 'PLAYBACK7' or 'PLAYBACK28'
				else							'FAIL!'                        -- ## QA Check that there aren't any of these
			end
		,	case
				when	cewe.subscriber_id is not null		then	11	-- 11 for things that need capping treatment
				else												0	-- 0 for no capping
			end
		,       vr.pre_standby_event_flag
                ,	vr.program_air_date
		,	vr.live
		,	vr.genre
	from
				Capping2_01_Viewing_Records		as	vr
		left join	CP2_capped_events_with_endpoints	as	cewe	on	cewe.subscriber_id             =	vr.subscriber_id
																	and	cewe.adjusted_event_start_time =	vr.adjusted_event_start_time
	commit --;-- ^^ originally a commit
	-- WAIT! ## we need to get TIMESHIFTING flag in here too. Thouhg we haven't
	-- checked VOSDAL / PLAYBACK7 / PLAYBACK28 yet, but we can flag LIVE stuff.


	/*
	--append fields to table to store additional metrics for capping
	alter table Capping2_01_Viewing_Records
	add (capped_event_end_time datetime
					,capped_x_viewing_start_time datetime
					,capped_x_viewing_end_time datetime
					,capped_x_programme_viewed_duration integer
					,capped_flag integer );

	--update daily view table with revised end times for capped events
	update Capping2_01_Viewing_Records t1
	set capped_event_end_time=t2.capped_event_end_time
	from CP2_capped_events_with_endpoints t2
	where t1.subscriber_id=t2.subscriber_id
	and t1.adjusted_event_start_time=t2.adjusted_event_start_time;

	commit;
	*/

	-- How are we going to get the control totals out if we don't put the capping back onto
	-- the viewing records table? hmmm....

	-- Okay, before we get rid of the holding pen, let's grab the event distributions from it:
	delete from CP2_QA_event_control_distribs
	where build_date = @target_date
	commit --;-- ^^ originally a commit

	-- okey, the way these GROUP BY statements go, this guy could be slow...


	insert into CP2_QA_event_control_distribs	(
														build_date
													,	data_state
													,	duration_interval
													,	viewing_events
												)
	select
			@target_date
		,	convert(varchar(20), '1.) Uncapped')
		,	datediff(minute, Adjusted_Event_Start_Time, X_Adjusted_Event_End_Time) as grouping_minute -- batched into 1m chunks, so 0 means viewing durations between 0s and 1 minute
		,	count(1)
	from	CP2_capped_events_with_endpoints
	group by	grouping_minute
	commit --;-- ^^ originally a commit

	insert into CP2_QA_event_control_distribs	(
														build_date
													,	data_state
													,	duration_interval
													,	viewing_events
												)
	select
			@target_date
		,	convert(varchar(20), '2.) Capped')
		,	datediff(minute, Adjusted_Event_Start_Time,	capped_event_end_time) as grouping_minute -- batched into 1m chunks,	 so 0 means viewing durations between 0s and 1 minute
		,	count(1)
	from	CP2_capped_events_with_endpoints
	group by	grouping_minute
	commit --;-- ^^ originally a commit


	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_capped_data_holding_pen
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'D06: Midway 1/3 (Populate holding pen)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-- More column renamiings: because we're not on the
	--  capped_x_viewing_start_time         => viewing_starts
	--  capped_x_viewing_end_time           => viewing_stops
	--  capped_x_programme_viewed_duration  => viewing_duration
	-- We're also discontinuing use of the Capping2_01_Viewing_Records table, because
	-- we've grabbed everything we want and are now just working in the holding
	-- pen. Actually, we could empty out the vireing records table (wait, have
	-- we imported all the uncapped data yet?)

	--update table to create revised start and end viewing times
	update	CP2_capped_data_holding_pen
	set
			viewing_starts	=	case
									-- if start of viewing_time is beyond capped end time then flag as null
									when	capped_event_end_time <= x_viewing_start_time	then	null
									-- else leave start of viewing time unchanged
									else															x_viewing_start_time
								end
		,	viewing_stops	=	case
									-- if start of viewing_time is beyond capped end time then flag as null
									when	capped_event_end_time <= x_viewing_start_time	then	null
									-- if capped event end time is beyond end time then leave end time unchanged
									when	capped_event_end_time > x_viewing_end_time		then	x_viewing_end_time
									-- if capped event end time is null then leave end time unchanged
									when	capped_event_end_time is null					then	x_viewing_end_time
									-- otherwise set end time to capped event end time
									else															capped_event_end_time
								end
	where	capped_flag	=	11  -- Only bother with the capped events...
	commit --;-- ^^ originally a commit

	-- And now the more basic case where there's no capping;
	update	CP2_capped_data_holding_pen
	set
			viewing_starts	=	x_viewing_start_time
		,	viewing_stops	=	x_viewing_end_time
	where	capped_flag	=	0
	commit --;-- ^^ originally a commit

	--calculate revised programme viewed duration
	update	CP2_capped_data_holding_pen
	set	viewing_duration = datediff(second, viewing_starts, viewing_stops)
	commit --;-- ^^ originally a commit

	--set capped_flag based on nature of capping
	--1 programme view not affected by capping
	--2 if programme view has been shortened by a long duration capping rule
	--3 if programme view has been excluded by a long duration capping rule

	--identify views which need to be capped
	update	CP2_capped_data_holding_pen
	set	capped_flag	=	case
							when	viewing_stops < x_viewing_end_time	then	2
							when	viewing_starts is null				then	3
							else												1
						end
	where	capped_flag	=	11
	commit --;-- ^^ originally a commit

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_capped_data_holding_pen
	where viewing_duration is not null
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'D06: Midway 2/3 (Calculate view bounds)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit


	-- Now the total viewing should be different... though there's no midpoint, it just *chunk* turns up all at once
	delete from	CP2_QA_viewing_control_totals
	where	(
					data_state	like	'3%'
				or	data_state	like	'4%'
			)
	and	build_date = @target_date
	commit --;-- ^^ originally a commit

	insert into CP2_QA_viewing_control_totals
	select
			@target_date
		,	convert(varchar(20), '3.) Capped') -- aliases are handled in table construction
		,	program_air_date
		,	live
		,	genre
		,	count(1)
		,	round(sum(coalesce(datediff(second, viewing_starts, viewing_stops),0)) / 60.0 / 60 / 24.0, 2)
	from	CP2_capped_data_holding_pen
	group by
			program_air_date
		,	live
		,	genre
	commit --; --^^ to be removed

	-- OK, so that's the total of what's left, but we also want the breakdown by
	-- each capping action, so we can check that they all add up:

	-- First clear out the marks in case we're rerunning this section without starting
	-- from the top of the script:
	delete from CP2_QA_viewing_control_totals
	where data_state like '4%'
	and build_date = @target_date
	commit --;-- ^^ originally a commit

	-- The total time in events that were not capped:
	insert into CP2_QA_viewing_control_totals
	select
			@target_date
		,	convert(varchar(20), '4a.) Uncapped')
		,	program_air_date
		,	live
		,	genre
		,	count(1)
		,	round(sum(coalesce(datediff(second, viewing_starts, viewing_stops),0)) / 60.0 / 60 / 24.0, 2)
	from	CP2_capped_data_holding_pen
	where	capped_flag	=	0
	group by
			program_air_date
		,	live
		,	genre
	commit --; --^^ to be removed

	-- Total time in events that were capped but the viewing for that record wasn't affected:
	insert into	CP2_QA_viewing_control_totals
	select
			@target_date
		,	convert(varchar(20), '4b.) Unaffected')
		,	program_air_date
		,	live
		,	genre
		,	count(1)
		,	round(sum(coalesce(datediff(second, viewing_starts, viewing_stops),0)) / 60.0 / 60 / 24.0, 2)
	from	CP2_capped_data_holding_pen
	where	capped_flag	=	1
	group by
			program_air_date
		,	live
		,	genre
	commit --; --^^ to be removed

	-- Total time in events that were just dropped:
	insert into	CP2_QA_viewing_control_totals
	select
			@target_date
		,	convert(varchar(20), '4c.) Excluded')
		,	program_air_date
		,	live
		,	genre
		,	count(1)
		,	round(sum(coalesce(datediff(second, x_viewing_start_time, x_viewing_end_time),0)) / 60.0 / 60 / 24.0, 2)
	from	CP2_capped_data_holding_pen
	where	capped_flag	=	3
	group by
			program_air_date
		,	live
		,	genre
	commit --;-- ^^ originally a commit

	-- The total time left in events that were capped:
	insert into	CP2_QA_viewing_control_totals
	select
			@target_date
		,	convert(varchar(20), '4d.) Truncated')
		,	program_air_date
		,	live
		,	genre
		,	count(1)
		,	round(sum(coalesce(datediff(second, viewing_starts, viewing_stops),0)) / 60.0 / 60 / 24.0, 2)
	from	CP2_capped_data_holding_pen
	where	capped_flag	=	2
	group by
			program_air_date
		,	live
		,	genre
	commit --; --^^ to be removed

	-- Total time removed from events that were capped
	insert into	CP2_QA_viewing_control_totals
	select
			@target_date
		,	convert(varchar(20), '4e.) T-Margin')
		,	program_air_date
		,	live
		,	genre
		,	count(1)
		,	round	(
							(
									sum(coalesce(datediff(second, x_viewing_start_time, x_viewing_end_time),0))
								-	sum(coalesce(datediff(second, viewing_starts, viewing_stops),0))
							) / 60.0 / 60 / 24.0
						,	2
					)
	from	CP2_capped_data_holding_pen
	where	capped_flag	=	2
	group by
			program_air_date
		,	live
		,	genre
	commit --;-- ^^ originally a commit

	set @QA_catcher = -1
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'D06: Midway 1/2 (Control totals)'
	commit --;-- ^^ originally a commit

	-- Wait, so where's the bit where we delete all the records that were excluded by capping? Maybe we
	-- just don't migrate them into the dynamic table? Update: Nope, here it is:

	delete from  CP2_capped_data_holding_pen
	where capped_flag = 3

	commit --;-- ^^ originally a commit

	-- At the same time, we're also reducing it to viewing records that are strictly contained
	-- within the viewing table we're processing.
	if object_id('capped_viewing_totals') is not null drop table capped_viewing_totals
	commit --; --^^ to be removed

	-- Oh, we should also grab the total average viewing (again)
	select
			subscriber_id
		,	round(sum(viewing_duration) / 60.0, 0)	as	total_box_viewing
	into	capped_viewing_totals
	from	CP2_capped_data_holding_pen
	group by	subscriber_id
	-- Don't need the WHERE filter, we've already removed things not on the daily table
	commit --;-- ^^ originally a commit

	select @QA_catcher = avg(total_box_viewing)
	from capped_viewing_totals
	commit --;-- ^^ originally a commit

	update CP2_QA_daily_average_viewing
	set average_capped_viewing = @QA_catcher
	where build_date = @target_date
	commit --;-- ^^ originally a commit

	if object_id('capped_viewing_totals') is not null drop table capped_viewing_totals
	commit --; --^^ to be removed

	-- Section mostly complete!

	set @QA_catcher = -1
	commit --;-- ^^ originally a commit

	select @QA_catcher = count(1)
	from CP2_capped_data_holding_pen
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'D06: Complete! (Capping on viewing table)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit

	-------------------------------------------------------------------------------------------------
	-- D07) CHECKING TOTAL VIEWING BEFORE AND AFTER CAPPING
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : D07) CHECKING TOTAL VIEWING BEFORE AND AFTER CAPPING'
	COMMIT

	/* ##QA##EQ##: Pivot the results of this extraction query in Excel I guess:
	select * from CP2_QA_viewing_control_totals
	order by data_state, program_air_date, live, genre
	*/

	/* What we expect:
					*. '1.) Collect' should match '2.) Pre-Cap'
					*. '4a.) Uncapped' + '4b.) Unafected' + '4d.) Truncated' should add up to '3.) Capped',
					*. '4a.) Uncapped' + '4b.) Unafected' + '4c.) Excluded' + '4d.) Truncated' + '4e.) T-Margin' should add up to '1.) Collect'
	They should match pretty much exactly, since we've rounded everything to 2dp in hours.
	*/

	set @QA_catcher = -1
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'D07: NYIP! (Total viewing before / after capping)'
	commit --;-- ^^ originally a commit

	-------------------------------------------------------------------------------------------------
	-- D08) LOOKING AT VIEWING DURATION PROFILE BEFORE AND AFTER CAPPING
	-------------------------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : D08) LOOKING AT VIEWING DURATION PROFILE BEFORE AND AFTER CAPPING'
	COMMIT

	-- Okay, but we're going to batch it into 10s histogram thing, because other these tables will be
	-- huge, and we should still be able to get all the detail want from this view even:

	-- Wait, except this is going on viewing records and not on event lengths... neet to build the one
	-- that actually goes on event lengths... though do we have that anywhere? Er... the only place it
	-- currently lives at event level is on the temp table - CP2_capped_events_with_endpoints - maybe
	-- we make that guy permanent so we can interrogate it later? We'll also need the event start time
	-- and end time too so we can properly get all the durations we need directly from it. Okey.

	delete from CP2_QA_viewing_control_distribs
	where build_date = @target_date
	commit --;-- ^^ originally a commit

	insert into CP2_QA_viewing_control_distribs	(
														build_date
													,	data_state
													,	duration_interval
													,	viewing_events
												)
	select
			@target_date
		,	convert(varchar(20), '1.) Uncapped')
		,	floor(x_programme_viewed_duration / 10) * 10 as grouping_guy -- batched into 10s chunks, so 0 means viewing durations between 0s and 10s
		,	count(1)
	from	Capping2_01_Viewing_Records
	where	x_programme_viewed_duration	>	0
	group by	grouping_guy
	commit --;-- ^^ originally a commit

	-- Is this the last time we need Capping2_01_Viewing_Records? From here, everything
	-- should be happening in the holding pen...


	insert into CP2_QA_viewing_control_distribs	(
														build_date
													,	data_state
													,	duration_interval
													,	viewing_events
												)
	select
			@target_date
		,	convert(varchar(20), '2.) Capped')
		,	floor(viewing_duration / 10) * 10 as grouping_guy -- again giving the alias so as to make the grouping more transparent
		,	count(1)
	from	CP2_capped_data_holding_pen
	where	viewing_duration	>	0
	group by	grouping_guy
	commit --;-- ^^ originally a commit

	/* ##QA##EQ##: Extraction query: make a graph in Excel or something
	select * from CP2_QA_viewing_control_distribs
	order by data_state, duration_interval
	*/

	set @QA_catcher = -1
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'D08: Complete! (Viewing duration profile)'
	commit --;-- ^^ originally a commit


	--select now(), 'E start'; --^^ just for debug

	/*
	--------------------------------------------------------------------------------
	-- E - Add Additional Feilds to the Viewing data
	--------------------------------------------------------------------------------

	E01 - Add Playback and Vosdal flags

	--------------------------------------------------------------------------------
	*/


	--------------------------------------------------------------------------------
	-- E01  Add Playback and Vosdal flags to the viewing data
	--------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : E01) Add Playback and Vosdal flags to the viewing data'
	COMMIT

	/*
	vosdal (viewed on same day as live) and playback = within 7 days of air

	viewing_starts - adjusted for playback and capped events, otherwise it is the original viewing time

	*/

	/* Don't need this, we're just playing with the timeshifting flag
	--Add the additional fields to the viewing table
	ALTER TABLE CP2_capped_data_holding_pen
	add (VOSDAL             as integer default 0
	,Playback           as integer default 0
	,playback_date      as date
	,playback_post7     as integer default 0 );
	*/

	-- Update the fields:
	Update	CP2_capped_data_holding_pen
	set	timeshifting	=	case
								when	live = 1																			then	'LIVE'
								when	date(viewing_starts) = program_air_date												then	'VOSDAL'
								when
										date(viewing_starts)	>	program_air_date
									and	viewing_starts			<=	dateadd(hour, 170, cast(program_air_date as datetime))	then	'PLAYBACK7'
								when	viewing_starts > dateadd(hour, 170, cast(program_air_date as datetime))				then	'PLAYBACK28'
							end
	commit --; --^^ to be removed

	/* The old build:
		set  VOSDAL        = (case when viewing_starts <= dateadd(hh, 26,cast( cast( program_air_date as date) as datetime)) and live = 0
																																																																																																																																		then 1 else 0 end)

						,Playback       =       (case when viewing_starts <= (dateadd(hour, 170, program_air_date))
																																				and  viewing_starts > (cast(dateadd(hour,26,program_air_date)  as datetime))and live = 0 then 1 else 0 end)

						,playback_post7 =       (case when viewing_starts > (dateadd(day, 170, program_air_date))and live = 0
																																																																																																																									then 1 else 0 end); -- flag these so identifying mismatchs is easy later
	*/
	--select top 10 * from CP2_capped_data_holding_pen where vosdal = 1
	--select top 10 * from CP2_capped_data_holding_pen where playback = 1
	--select top 10 * from CP2_capped_data_holding_pen where playback_post7 = 1

	-- So at this point all the capping and processing is done, and we can transfer these guys
	-- to the dynamic daily augmentation table.

	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_capped_data_holding_pen
	where timeshifting is not null
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'E01: Complete! (Calculate view bounds)' || coalesce(@QA_catcher, -1)
	commit --;-- ^^ originally a commit



	--select now(), 'G start'; --^^ just for debug


	--------------------------------------------------------------------------------
	-- PART G) - DYNAMIC DAILY CAPPED TABLE
	--------------------------------------------------------------------------------
	-- So we've done all this work, and now we need to dynamically put the results
	-- into a table named after the same day as the source daily table. Oh and we have
	-- to leave off the items that were in the previous daily table which we only
	-- included to get the right capping behaviour for early items.

	delete from	CP2_capped_data_holding_pen
	where	date(viewing_starts) <> @target_date
	commit --;-- ^^ originally a commit
	
	
	--------------------------------------------------------------------------------
	-- G01) - ADD SCALING WEIGHTS
	--------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : G01) - ADD SCALING WEIGHTS'
	COMMIT
	
	update	CP2_capped_data_holding_pen		as	a
	set		a.Scaling_Weighting	=	b.adsmart_scaling_weight
	from	CP2_accounts	as	b
	where	a.account_number	=	b.account_number
	and		b.reference_date	=	@target_date
	commit


	--------------------------------------------------------------------------------
	-- G02) - DYNAMIC DAILY AUGMENTATION TABLE: POPULATION
	--------------------------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : G02) - DYNAMIC DAILY AUGMENTATION TABLE: POPULATION'
	COMMIT
	
	-- Specifically no filters here as we're running the QA actions over CP2_capped_data_holding_pen
	-- since that makes it a lot easier to get the totals and checks etc into logger than when
	-- doing everything dynamically of the daily augmented tables.

	insert into Vespa_Daily_Augs	(
											Cb_Row_Id
										,	target_date
										,	iteration_number
										,	Account_Number
										,	Subscriber_Id
										,	Programme_Trans_Sk
										,	Timeshifting
										,	Viewing_Starts
										,	Viewing_Stops
										,	Viewing_Duration
										,	Capped_Flag
										,	Capped_Event_End_Time
										,	Scaling_Segment_Id
										,	Scaling_Weighting
										,	BARB_Minute_Start
										,	BARB_Minute_End
										,	adjusted_event_start_time
										,	X_Adjusted_Event_End_Time
										,	x_viewing_start_time
										,	x_viewing_end_time
										-- Other things we only need to maintain our control totals:
										,	program_air_date
										,	live
										,	genre
									)
	select
			Cb_Row_Id
		,	@target_date
		,	@iteration_number
		,	Account_Number
		,	Subscriber_Id
		,	Programme_Trans_Sk
		,	Timeshifting
		,	Viewing_Starts
		,	Viewing_Stops
		,	Viewing_Duration
		,	Capped_Flag
		,	Capped_Event_End_Time
		,	Scaling_Segment_Id
		,	Scaling_Weighting
		,	BARB_Minute_Start
		,	BARB_Minute_End
		,	adjusted_event_start_time
		,	X_Adjusted_Event_End_Time
		,	x_viewing_start_time
		,	x_viewing_end_time
		-- Other things we only need to maintain our control totals:
		,	program_air_date
		,	live
		,	genre
	from	CP2_capped_data_holding_pen
	commit --;-- ^^ originally a commit


	set @QA_catcher = -1
	commit --; --^^ to be removed

	select @QA_catcher = count(1)
	from CP2_capped_data_holding_pen
	where date(viewing_starts) = @target_date
	commit --; --^^ to be removed

	execute M00_2_output_to_logger 'G02: Aug table completed' || coalesce(@QA_catcher, -1)
	commit --; --^^ to be removed


end; -- procedure CP2_build_days_caps_CUSTOM
commit;

--select '5 end', now()
grant execute on V306_CP2_M05_Build_Day_Caps to vespa_group_low_security;
commit;







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




**Module:                               V306_CP2_M05_2_Time_Tables

Creation of the time base used by BARB and VESPA modules.

*/

create or replace procedure V306_CP2_M05_2_Time_Tables
											@capping_date	date = NULL
as begin

	execute M00_2_output_to_logger '@ M05 : V306_CP2_M05_2_Time_Tables'
	COMMIT



	----------------------------------------------------------------
	-- Create minute-by-minute vector for a single day
	----------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : Create minute-by-minute vector for a single day'
	COMMIT

	SELECT	ROW_NUM
	INTO	#MINUTES_VECTOR
	FROM	SA_ROWGENERATOR(0,1439)
	commit

	CREATE UNIQUE LF INDEX U_LF_IDX_1 ON #MINUTES_VECTOR(ROW_NUM)
	commit



	----------------------------------------------------------------
	-- Create minute-by-minute time base for date of interest
	----------------------------------------------------------------

	execute M00_2_output_to_logger '@ M05 : Create minute-by-minute time base for date of interest'
	COMMIT

	execute DROP_LOCAL_TABLE 'UTC'
	commit

	SELECT
			CAL.UTC_DAY_DATE
		,   DATEADD(MINUTE,MINS.ROW_NUM,CAST(CAL.UTC_DAY_DATE AS TIMESTAMP))   AS  UTC_DATEHOURMIN
		,   DATEADD	(
							HOUR
						,   DATEPART(HOUR,UTC_DATEHOURMIN)
						,   CAST(DATE(UTC_DATEHOURMIN) AS TIMESTAMP)
					)                                                       AS  UTC_DATEHOUR
	INTO    UTC
	FROM
					sk_prod.VIQ_DATE	AS  CAL
		CROSS JOIN	#MINUTES_VECTOR		AS  MINS
		INNER JOIN	V306_CAPPING_DATES	AS	DAT		ON	CAL.UTC_DAY_DATE	=	DAT.capping_date
	GROUP BY
			UTC_DAY_DATE
		,   UTC_DATEHOURMIN
		,   UTC_DATEHOURMIN
	ORDER BY
			UTC_DAY_DATE
		,   UTC_DATEHOURMIN
		,   UTC_DATEHOURMIN
	COMMIT

	CREATE DATE INDEX DATE_IDX_1 ON UTC(UTC_DAY_DATE)	COMMIT
	CREATE DTTM INDEX DTTM_IDX_1 ON UTC(UTC_DATEHOURMIN)	COMMIT
	CREATE DTTM INDEX DTTM_IDX_2 ON UTC(UTC_DATEHOUR)	COMMIT

end;
commit;

grant execute on V306_CP2_M05_2_Time_Tables to vespa_group_low_security;
commit;
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
					AND UTC_TV_Event_Start_Date_Time    BETWEEN (SELECT MIN(UTC_DAY_DATE) FROM UTC)		-- apply time limits appropriate to our analysis window
														AND     (SELECT MAX(UTC_DAY_DATE) + 1 FROM UTC)
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
			,	CASE
					WHEN    VESPA.live=1		THEN	'LIVE'
					WHEN    VESPA.live=0		THEN	'PLAYBACK'
					ELSE						NULL -- never happens, left for legacy
				END																		AS	STREAM_TYPE
			,   /*SUM(VESPA.weighted_total_people_viewing)*/ cast(null as int)			AS	TOTAL_INDIVIDUAL_WEIGHTED_MINS
			,   SUM(VESPA.scaling_weighting)											AS	TOTAL_HOUSEHOLD_WEIGHED_MINS
			,   SUM(VESPA.unit_weight)													AS	TOTAL_HOUSEHOLD_UNWEIGHED_MINS
			,	VESPA.scaled_account_flag			
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
								end		as	event_end_time
							,	live
							,	1		as	unit_weight
							,	cast	(
											case
												when	ACC.account_number	is null	then	0
												else										1
											end
											as	bit
										)	as	scaled_account_flag
						from
									Vespa_Daily_Augs	VDA
						LEFT JOIN	CP2_accounts		ACC		ON 	VDA.account_number	=	ACC.account_number
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
							,	scaled_account_flag
					)       AS      VESPA           ON  UTC.UTC_DATEHOURMIN     BETWEEN VESPA.adjusted_event_start_time -- Local_BARB_Instance_Start_Date_Time
																				AND     VESPA.event_end_time --Local_BARB_Instance_End_Date_Time
        GROUP BY
				UTC.UTC_DATEHOURMIN
			,	UTC.UTC_DATEHOUR
			,	STREAM_TYPE
			,	VESPA.scaled_account_flag
        ORDER BY
				UTC.UTC_DATEHOURMIN
			,	UTC.UTC_DATEHOUR
			,	STREAM_TYPE
			,	VESPA.scaled_account_flag
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
			,	VESPA.X_Adjusted_Event_End_Time -- Local_TV_Event_End_Date_Time
			,	STREAM_TYPE
			,   SUM(TOTAL_WEIGHTED_MINS)			AS  TOTAL_WEIGHTED_MINS
			,	scaled_account_flag
			,   SUM(TOTAL_UNWEIGHTED_MINS)			AS  TOTAL_UNWEIGHTED_MINS
        INTO    #VESPA_EVENT_WEIGHTED_MINUTES
        FROM	(   -- Calculate the weighted viewing minutes per VESPA viewing instance, BUT then aggregating them by their common EVENT start times per household
					SELECT
							VDA.account_number -- Household_number
						,	adjusted_event_start_time --Local_TV_Event_Start_Date_Time
						,	X_Adjusted_Event_End_Time -- Local_TV_Event_End_Date_Time
						,	CASE
								WHEN    live=1		THEN    'LIVE'
								WHEN    live=0		THEN    'PLAYBACK'
								ELSE        		NULL -- never happens, left for legacy
							END AS STREAM_TYPE
						,   DATEDIFF    (
											MINUTE
											,   viewing_starts -- Local_BARB_Instance_Start_Date_Time
											,   viewing_stops -- Local_BARB_Instance_End_Date_Time
										)						AS	DT
						,   SUM(scaling_weighting   *   DT)		AS	TOTAL_WEIGHTED_MINS
						,	cast	(
										case
											when	ACC.account_number	is null	then	0
											else										1
										end
										as	bit
									)	as	scaled_account_flag
						,   SUM(DT)								AS	TOTAL_UNWEIGHTED_MINS
					FROM
								Vespa_Daily_Augs	VDA
					LEFT JOIN	CP2_accounts		ACC		ON 	VDA.account_number	=	ACC.account_number		-- limit analysis to scaled accounts only
															AND	VDA.target_date		=	ACC.reference_date
					GROUP BY
							VDA.account_number -- Household_number
						,	adjusted_event_start_time --Local_TV_Event_Start_Date_Time
						,	X_Adjusted_Event_End_Time --Local_TV_Event_End_Date_Time
						,	STREAM_TYPE
						,	DT
						,	scaled_account_flag
				)   AS  VESPA
		GROUP BY
				VESPA.account_number -- Household_number
			,   VESPA.adjusted_event_start_time --Local_TV_Event_Start_Date_Time
			,	VESPA.X_Adjusted_Event_End_Time --Local_TV_Event_End_Date_Time
			,	STREAM_TYPE
			,	scaled_account_flag
        ORDER BY
				VESPA.account_number -- Household_number
			,   VESPA.adjusted_event_start_time --Local_TV_Event_Start_Date_Time
			,	VESPA.X_Adjusted_Event_End_Time --Local_TV_Event_End_Date_Time
			,	STREAM_TYPE
			,	scaled_account_flag
        COMMIT

        CREATE HG INDEX VS_HG_IDX_1 ON #VESPA_EVENT_WEIGHTED_MINUTES(account_number/*Household_number*/)
		COMMIT

        CREATE DTTM INDEX VS_DTTM_IDX_1 ON #VESPA_EVENT_WEIGHTED_MINUTES(adjusted_event_start_time/*Local_TV_Event_Start_Date_Time*/)
        COMMIT


        -- Now, join onto the previously generated time base. First aggregate by start minute.

        SELECT
				UTC.UTC_DATEHOURMIN
			,	UTC.UTC_DATEHOUR
			,	VESPA.STREAM_TYPE
			,	scaled_account_flag
			,	SUM(VESPA.TOTAL_WEIGHTED_MINS)		AS	TOTAL_HOUSEHOLD_WEIGHED_MINS_BY_STARTMIN
			,	SUM(VESPA.TOTAL_UNWEIGHTED_MINS)	AS	TOTAL_HOUSEHOLD_UNWEIGHED_MINS_BY_STARTMIN
        INTO	#VESPA_WEIGHTED_MINS_BY_EVENT_STARTMIN
        FROM
						UTC								AS  UTC -- Minute-by-minute time base
			-- Join time base onto VESPA viewing data
			LEFT JOIN   #VESPA_EVENT_WEIGHTED_MINUTES	AS  VESPA       ON      UTC.UTC_DATEHOURMIN =   VESPA.adjusted_event_start_time --Local_TV_Event_Start_Date_Time
        GROUP BY
				UTC.UTC_DATEHOURMIN
			,	UTC.UTC_DATEHOUR
			,	VESPA.STREAM_TYPE
			,	scaled_account_flag
        ORDER BY
				UTC.UTC_DATEHOURMIN
			,	UTC.UTC_DATEHOUR
			,	VESPA.STREAM_TYPE
			,	scaled_account_flag
        COMMIT

        CREATE DTTM INDEX VS_DTTM_IDX_1 ON #VESPA_WEIGHTED_MINS_BY_EVENT_STARTMIN(UTC_DATEHOURMIN)
		COMMIT
		
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
				DATEADD(HOUR,-1,UTC.UTC_DATEHOURMIN)			AS	UTC_DATEHOURMIN
			,	DATEADD(HOUR,-1,UTC.UTC_DATEHOUR)				AS	UTC_DATEHOUR
			,	S.STREAM_TYPE
			,	A.TOTAL_HOUSEHOLD_WEIGHED_MINS					AS	VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME
			,	A.TOTAL_HOUSEHOLD_UNWEIGHED_MINS				AS	VESPA_UNWEIGHTED_MINUTES_BY_VIEWING_TIME
			,	B.TOTAL_HOUSEHOLD_WEIGHED_MINS_BY_STARTMIN		AS	VESPA_WEIGHTED_MINUTES_BY_EVENT_START_TIME
			,	B.TOTAL_HOUSEHOLD_UNWEIGHED_MINS_BY_STARTMIN	AS	VESPA_UNWEIGHTED_MINUTES_BY_EVENT_START_TIME
			,	SCA.scaled_account_flag
        INTO	VESPA_MINUTE_BY_MINUTE_WEIGHTED_VIEWING
        FROM
                UTC													AS  UTC
                CROSS JOIN  (
								SELECT  CAST('LIVE' AS VARCHAR)			AS	STREAM_TYPE
								UNION ALL
								SELECT  CAST('PLAYBACK' AS VARCHAR)		AS	STREAM_TYPE
							)	AS  S
                CROSS JOIN  (
								SELECT  row_num			AS	scaled_account_flag
								FROM	sa_rowgenerator(0,1)
							)	AS  SCA
                LEFT JOIN   #VESPA_WEIGHTED_MINS					AS  A	ON	UTC.UTC_DATEHOURMIN	=	A.UTC_DATEHOURMIN
																			AND UTC.UTC_DATEHOUR	=	A.UTC_DATEHOUR
																			AND	S.STREAM_TYPE		=	A.STREAM_TYPE
																			AND	SCA.scaled_account_flag		=	A.scaled_account_flag
                LEFT JOIN   #VESPA_WEIGHTED_MINS_BY_EVENT_STARTMIN	AS	B	ON	UTC.UTC_DATEHOURMIN	=	B.UTC_DATEHOURMIN
																			AND UTC.UTC_DATEHOUR	=	B.UTC_DATEHOUR
																			AND S.STREAM_TYPE		=	B.STREAM_TYPE
																			AND	SCA.scaled_account_flag		=	B.scaled_account_flag
        ORDER BY
				UTC.UTC_DATEHOURMIN
			,   UTC.UTC_DATEHOUR
			,   S.STREAM_TYPE
			,	SCA.scaled_account_flag
        COMMIT

                
        execute M00_2_output_to_logger '@ M07: V306_CP2_M07_VESPA_Minutes end'
        commit
        

end;
commit;

grant execute on V306_CP2_M07_VESPA_Minutes to vespa_group_low_security;
commit;





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




**Module:                               M07_1_BARB_vs_VESPA
This module quantifies the difference between daily viewing in BARB and VESPA

**      Part A:       Populating 
**              A00 - Prepare metrics table with info on minute-by-minute BARB and VESPA viewing
**              A01 - Fill metrics table with minute-by-minute information categorized by stream
**              A02 - Fill metrics table with minute-by-minute information independent of stream
**              A03 - Fill metrics table with hour-by-hour information categorized by stream
**              A04 - Fill metrics table with hour-by-hour information independent of stream
**              A05 - Fill metrics table with day-by-day information categorized by stream
**              A06 - Fill metrics table with day-by-day information independent of stream
**      Part B:       Save metrics results in historic table 
**              B00 - Save metrics results in historic table
*/
/*
The date of interest is the date for which we are processing data, it starts from day_of_interest-1 at 23:00 and ends on day_of_interest at 23:59
*/


create or replace procedure V306_CP2_M07_1_BARB_vs_VESPA
									@target_date		date	=	NULL     -- Date of daily table caps to cache
								,	@sample_size		tinyint	=	100  -- 0-100, indicates whether we are considering the full sample of accounts or just a share of it, for VESPA, default is 100, which means the whole lot of accounts
								,	@iteration_number	int		=	NULL
as begin

	declare @dummy bigint /* variable to store some row counts */

	execute M00_2_output_to_logger '@ M07_1 : BARB_vs_VESPA start...'
	commit

	/****************** A00 - Prepare metrics table with info on minute-by-minute BARB and VESPA viewing ******************/
	execute M00_2_output_to_logger '@ M07_1 : A00 - Prepare metrics table with info on minute-by-minute BARB and VESPA viewing'
	commit
	-- the table is filled with information on minute-by-minute BARB and VESPA viewing
	insert into	VESPAvsBARB_metrics_table	(
													iteration_number
												,	UTC_DATEHOURMIN
												,	UTC_DATEHOUR
												,	UTC_DAY_OF_INTEREST
												,	stream_type
												,	scaled_account_flag
												,	BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME
												,	BARB_WEIGHTED_MINUTES_BY_EVENT_START_TIME
												,	VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME
												,	VESPA_WEIGHTED_MINUTES_BY_EVENT_START_TIME
												,	VESPA_UNWEIGHTED_MINUTES_BY_VIEWING_TIME
												,	VESPA_UNWEIGHTED_MINUTES_BY_EVENT_START_TIME
												,	percentageDiff_by_minute_stream
												,	variance_by_minute_stream
											)
	select
			@iteration_number
		,	ves.UTC_DATEHOURMIN
		,	ves.UTC_DATEHOUR
		,	date(dateadd(hour,1,ves.UTC_DATEHOUR))	--	shift by an hour to account for 11pm-11pm window. (was @target_date variable)
		,	ves.stream_type
		,	ves.scaled_account_flag
		,	coalesce(BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME,	0)															as	BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME
		,	coalesce(BARB_WEIGHTED_MINUTES_BY_EVENT_START_TIME,	0)														as	BARB_WEIGHTED_MINUTES_BY_EVENT_START_TIME
		,	coalesce(VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME,0)*(100.0/cast(coalesce(@sample_size,100) as double))		as	VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME		-- rescale back to up 100% sample
		,	coalesce(VESPA_WEIGHTED_MINUTES_BY_EVENT_START_TIME,0)*(100.0/cast(coalesce(@sample_size,100) as double))	as	VESPA_WEIGHTED_MINUTES_BY_EVENT_START_TIME		-- rescale back to up 100% sample
		,	coalesce(VESPA_UNWEIGHTED_MINUTES_BY_VIEWING_TIME,0)*(100.0/cast(coalesce(@sample_size,100) as double))		as	VESPA_UNWEIGHTED_MINUTES_BY_VIEWING_TIME
		,	coalesce(VESPA_UNWEIGHTED_MINUTES_BY_EVENT_START_TIME,0)*(100.0/cast(coalesce(@sample_size,100) as double))	as	VESPA_UNWEIGHTED_MINUTES_BY_EVENT_START_TIME
		,	case
				when	(
								BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME	is not null
							and	BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME	!=	0
						)																then	((VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME-BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME)/BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME)*100.0
				else																			0
			end																											as 	percentageDiff_by_minute_stream
		,		(VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME-BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME)
			*	(VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME-BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME)							as	variance_by_minute_stream
		from
					VESPA_MINUTE_BY_MINUTE_WEIGHTED_VIEWING	ves
		left join	BARB_MINUTE_BY_MINUTE_WEIGHTED_VIEWING 	bar	on	ves.UTC_DATEHOURMIN			=	bar.UTC_DATEHOURMIN
																and	ves.stream_type				=	bar.stream_type
																and	ves.scaled_account_flag		=	1
	commit -- ;-- ^^ originally a commit

	-- /****************** A01 - Fill metrics table with minute-by-minute information categorized by stream ******************/
	-- execute M00_2_output_to_logger '@ M07_1 : A01 - Fill metrics table with minute-by-minute information categorized by stream'
	-- commit

	-- select
			-- ves.UTC_DATEHOURMIN
		-- ,	UTC_DAY_OF_INTEREST
		-- ,	ves.stream_type
		-- ,	case
				-- when	(
								-- BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME	is not null
							-- and	BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME	!=	0
						-- )																then	((VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME-BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME)/BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME)*100.0
				-- else																			0
			-- end																																									as 	percentageDiff
		-- ,	(VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME-BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME)*(VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME-BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME)		as	varianceDiff
	-- into	#minute_stream_grouping
	-- from	VESPAvsBARB_metrics_table ves
	-- where	UTC_DAY_OF_INTEREST	=	@target_date
	-- commit -- ;-- ^^ originally a commit


	-- -- update the metrics table with minute-by-minute info independent of stream
	-- update  VESPAvsBARB_metrics_table met
	-- set
			-- percentageDiff_by_minute_stream	=	str.percentageDiff
		-- ,	variance_by_minute_stream		=	str.varianceDiff
	-- from	#minute_stream_grouping str
	-- where
			-- met.UTC_DAY_OF_INTEREST	=	str.UTC_DAY_OF_INTEREST
		-- and	met.UTC_DATEHOURMIN		=	str.UTC_DATEHOURMIN
		-- and	met.stream_type			=	str.stream_type
	-- commit -- ;-- ^^ originally a commit


	/****************** A02 - Fill metrics table with minute-by-minute information independent of stream ******************/
	execute M00_2_output_to_logger '@ M07_1 : A02 - Fill metrics table with minute-by-minute information independent of stream'
	commit

	-- prepare minute-by-minute figures independent of stream type
	select
		ves.UTC_DATEHOURMIN
	,	UTC_DAY_OF_INTEREST
	,	scaled_account_flag
	,	sum(VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME)	as	tot_VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME
	,	sum(BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME)	as	tot_BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME
	,	case
			when	(
							tot_BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME is not null
						and	tot_BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME != 0
					)																then	((tot_VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME-tot_BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME)/tot_BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME)*100.0
			else																			0
		end											as	percentageDiff
	,	(tot_BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME-tot_VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME)*(tot_BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME-tot_VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME) as varianceDiff
	into	#minute_grouping
	from	VESPAvsBARB_metrics_table ves
	where	scaled_account_flag	=	1
	group by
			UTC_DAY_OF_INTEREST
		,	ves.UTC_DATEHOURMIN
		,	scaled_account_flag
	commit -- ;-- ^^ originally a commit


	-- update the metrics table with minute-by-minute info independent of stream
	update  VESPAvsBARB_metrics_table met
	set
			percentageDiff_by_minute	=	str.percentageDiff
		,	variance_by_minute			=	str.varianceDiff
	from	#minute_grouping str
	where
			met.UTC_DAY_OF_INTEREST	=	str.UTC_DAY_OF_INTEREST
		and	met.UTC_DATEHOURMIN		=	str.UTC_DATEHOURMIN
		and	met.scaled_account_flag	=	str.scaled_account_flag
	commit -- ;-- ^^ originally a commit
	
	-- Clean up
	drop table #minute_grouping
	commit
	

	/****************** A03 - Fill metrics table with hour-by-hour information categorized by stream ******************/
	execute M00_2_output_to_logger '@ M07_1 : A03 - Fill metrics table with hour-by-hour information categorized by stream'
	commit

	-- prepare hour-by-hour stream-dependent figures
	select
			ves.UTC_DATEHOUR
		,	UTC_DAY_OF_INTEREST
		,	ves.stream_type
		,	scaled_account_flag
		--,count() as nr_of_contributions
		,	sum(VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME)	as	tot_VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME
		,	sum(BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME)	as	tot_BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME
		,	case
				when	(
								tot_BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME is not null
							and	tot_BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME != 0
						)				then	((tot_VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME-tot_BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME)/tot_BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME)*100.0
				else							0
			end											as	percentageDiff
		,		(tot_BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME-tot_VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME)
			*	(tot_BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME-tot_VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME)	as	varianceDiff
	into	#hour_stream_grouping
	from	VESPAvsBARB_metrics_table ves
	where	scaled_account_flag	=	1
	group by
			UTC_DAY_OF_INTEREST
		,	ves.UTC_DATEHOUR
		,	ves.stream_type
		,	scaled_account_flag
	commit -- ;-- ^^ originally a commit


	-- update the metrics table with hour-by-hour stream-dependent info
	update  VESPAvsBARB_metrics_table met
	set
			percentageDiff_by_hour_stream	=	str.percentageDiff
		,	variance_by_hour_stream			=	str.varianceDiff
	from	#hour_stream_grouping str
	where
			met.UTC_DAY_OF_INTEREST	=	str.UTC_DAY_OF_INTEREST
		and	met.UTC_DATEHOUR		=	str.UTC_DATEHOUR
		and	met.stream_type			=	str.stream_type
		and	met.scaled_account_flag	=	str.scaled_account_flag
	commit -- ;-- ^^ originally a commit
	
	-- Clean up
	drop table #hour_stream_grouping
	commit
	

	/****************** A04 - Fill metrics table with hour-by-hour information independent of stream ******************/
	execute M00_2_output_to_logger '@ M07_1 : A04 - Fill metrics table with hour-by-hour information independent of stream'
	commit


	-- prepare hour-by-hour stream-INdependent figures
	select
			ves.UTC_DATEHOUR
		,	UTC_DAY_OF_INTEREST
		,	scaled_account_flag
	--,ves.stream_type
	--,count() as nr_of_contributions
		,	sum(VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME) as tot_VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME
		,	sum(BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME) as tot_BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME
		,	case when (tot_BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME is not null) and (tot_BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME != 0) then ((tot_VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME-tot_BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME)/tot_BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME)*100.0 else 0 end as percentageDiff
		,	(tot_BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME-tot_VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME)*(tot_BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME-tot_VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME) as varianceDiff
	into	#hour_grouping
	from	VESPAvsBARB_metrics_table ves
	where	scaled_account_flag	=	1
	group by
			UTC_DAY_OF_INTEREST
		,	ves.UTC_DATEHOUR
		,	scaled_account_flag
	commit -- ;-- ^^ originally a commit


	-- update the metrics table with hour-by-hour stream-INdependent info
	update  VESPAvsBARB_metrics_table met
	set
			percentageDiff_by_hour	=	str.percentageDiff
		,	variance_by_hour		=	str.varianceDiff
	from	#hour_grouping str
	where
			met.UTC_DAY_OF_INTEREST	=	str.UTC_DAY_OF_INTEREST
		and	met.UTC_DATEHOUR		=	str.UTC_DATEHOUR
		and	met.scaled_account_flag	=	str.scaled_account_flag
	commit -- ;-- ^^ originally a commit
	
	
	-- Clean up
	drop table #hour_grouping
	commit
	

	/****************** A05 - Fill metrics table with day-by-day information categorized by stream ******************/
	execute M00_2_output_to_logger '@ M07_1 : A05 - Fill metrics table with day-by-day information categorized by stream'
	commit

	-- prepare day-by-day stream-dependent figures
	select
		--ves.UTC_DATEHOUR
			UTC_DAY_OF_INTEREST
		,	ves.stream_type
		,	scaled_account_flag
		--,count() as nr_of_contributions
		,	sum(VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME) as tot_VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME
		,	sum(BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME) as tot_BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME
		,	case
				when	(
								tot_BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME is not null
							and	tot_BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME != 0
						)		then ((tot_VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME-tot_BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME)/tot_BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME)*100.0 else 0 end as percentageDiff
		,	(tot_BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME-tot_VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME)*(tot_BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME-tot_VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME) as varianceDiff
		into	#day_stream_grouping
		from	VESPAvsBARB_metrics_table ves
		where	scaled_account_flag	=	1
		group by
				UTC_DAY_OF_INTEREST
			,	ves.stream_type
		,	scaled_account_flag
	commit -- ;-- ^^ originally a commit
	
	
	-- update the metrics table with day-by-day stream-dependent info
	update	VESPAvsBARB_metrics_table met
	set
			percentageDiff_by_day_stream	=	str.percentageDiff
		,	variance_by_day_stream			=	str.varianceDiff
	from	#day_stream_grouping str
	where
			met.UTC_DAY_OF_INTEREST	=	str.UTC_DAY_OF_INTEREST
		and	met.stream_type			=	str.stream_type
		and	met.scaled_account_flag	=	str.scaled_account_flag
	commit -- ;-- ^^ originally a commit


	-- Clean up
	drop table #day_stream_grouping
	commit

	
	
	/****************** A06 - Fill metrics table with day-by-day information independent of stream ******************/
	execute M00_2_output_to_logger '@ M07_1 : A06 - Fill metrics table with day-by-day information independent of stream'
	commit

	-- prepare day-by-day stream-INdependent figures
	select
		--ves.UTC_DATEHOUR
			UTC_DAY_OF_INTEREST
		,	scaled_account_flag
		--,ves.stream_type
		--,count() as nr_of_contributions
		,	sum(VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME) as tot_VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME
		,	sum(BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME) as tot_BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME
		,	case
				when	(
								tot_BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME is not null
							and	tot_BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME != 0
						)		then ((tot_VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME-tot_BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME)/tot_BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME)*100.0 else 0 end as percentageDiff
		,		(tot_BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME-tot_VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME)
			*	(tot_BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME-tot_VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME) as varianceDiff
	into	#day_grouping
	from	VESPAvsBARB_metrics_table ves
	where	scaled_account_flag	=	1
	group by
			UTC_DAY_OF_INTEREST
		,	scaled_account_flag
	commit -- ;-- ^^ originally a commit


	-- update the metrics table with day-by-day stream-INdependent info
	update	VESPAvsBARB_metrics_table met
	set
			percentageDiff_by_day	=	str.percentageDiff
		,	variance_by_day			=	str.varianceDiff
	from	#day_grouping str
	where
			met.UTC_DAY_OF_INTEREST	=	str.UTC_DAY_OF_INTEREST
		and	met.scaled_account_flag	=	str.scaled_account_flag
	commit -- ;-- ^^ originally a commit
	
	
	-- Clean up
	drop table #day_grouping
	commit


	/****************** B00 - Save metrics results in historic table ******************/
	execute M00_2_output_to_logger '@ M07_1 : B00 - Save metrics results in historic table begin...'
	commit

	insert into	VESPAvsBARB_metrics_historic_table	(
															iteration_number
														,	UTC_DATEHOURMIN
														,	UTC_DATEHOUR
														,	UTC_DAY_OF_INTEREST
														,	stream_type
														,	scaled_account_flag
														,	BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME
														,	BARB_WEIGHTED_MINUTES_BY_EVENT_START_TIME
														,	VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME
														,	VESPA_WEIGHTED_MINUTES_BY_EVENT_START_TIME
														,	VESPA_UNWEIGHTED_MINUTES_BY_VIEWING_TIME
														,	VESPA_UNWEIGHTED_MINUTES_BY_EVENT_START_TIME
														,	percentageDiff_by_minute_stream
														,	variance_by_minute_stream
														,	percentageDiff_by_minute
														,	variance_by_minute
														,	percentageDiff_by_hour_stream
														,	variance_by_hour_stream
														,	percentageDiff_by_hour
														,	variance_by_hour
														,	percentageDiff_by_day_stream
														,	variance_by_day_stream
														,	percentageDiff_by_day
														,	variance_by_day
													)
	select
			iteration_number
		,	UTC_DATEHOURMIN
		,	UTC_DATEHOUR
		,	UTC_DAY_OF_INTEREST
		,	stream_type
		,	scaled_account_flag
		,	BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME
		,	BARB_WEIGHTED_MINUTES_BY_EVENT_START_TIME
		,	VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME
		,	VESPA_WEIGHTED_MINUTES_BY_EVENT_START_TIME
		,	VESPA_UNWEIGHTED_MINUTES_BY_VIEWING_TIME
		,	VESPA_UNWEIGHTED_MINUTES_BY_EVENT_START_TIME
		,	percentageDiff_by_minute_stream
		,	variance_by_minute_stream
		,	percentageDiff_by_minute
		,	variance_by_minute
		,	percentageDiff_by_hour_stream
		,	variance_by_hour_stream
		,	percentageDiff_by_hour
		,	variance_by_hour
		,	percentageDiff_by_day_stream
		,	variance_by_day_stream
		,	percentageDiff_by_day
		,	variance_by_day
	from	VESPAvsBARB_metrics_table
	where	/*UTC_DAY_OF_INTEREST	=	@target_date
	and*/	  iteration_number	=	@iteration_number
	commit
			
	set @dummy = @@rowcount
	commit
	execute M00_2_output_to_logger '@ M07_1 : B00 - Save metrics results in historic table end: ' || @dummy || ' records saved'
	commit

	execute M00_2_output_to_logger '@ M07_1 : ...BARB_vs_VESPA end'
	commit


end;
commit;


grant execute on V306_CP2_M07_1_BARB_vs_VESPA to vespa_group_low_security;
commit;/*


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



** Module:                               M07_5_Save_metadata_params

**      Part A:      Save metadata parameters in historic table
**              A00 - Save metadata parameters in historic table
*/

/*
The date of interest is the date for which we are processing data, it starts from day_of_interest-1 at 23:00 and ends on day_of_interest at 23:59
*/


create or replace procedure V306_CP2_M07_5_Save_metadata_params
        @iteration_number int = NULL
        as begin

        execute M00_2_output_to_logger '@ M07_5 : Save_metadata_params start...'
        commit


        /****************** A00 - Save metadata parameters in historic table ******************/

        insert into     CP2_metadata_historic_table (
                        iteration_number
                ,       row_id
                ,       CAPPING_METADATA_KEY
                ,       START_TIME
                ,       END_TIME
                ,       DAY_PART_DESCRIPTION
                ,       THRESHOLD_NTILE
                ,       THRESHOLD_NONTILE
                ,       PLAYBACK_NTILE
                ,       BANK_HOLIDAY_WEEKEND
                ,       BOX_SHUT_DOWN
                ,       HOUR_IN_MINUTES
                ,       HOUR_24_CLOCK_LAST_HOUR
                ,       MINIMUM_CUT_OFF
                ,       MAXIMUM_CUT_OFF
                --,     MAXIMUM_ITERATIONS int -- used for scaling
                --,     MINIMUM_HOUSEHOLD_FOR_SCALING int -- used for scaling
                ,       SAMPLE_MAX_POP
                ,       SHORT_DURATION_CAP_THRESHOLD
                ,       MINIMUM_HOUSEHOLD_FOR_CAPPING
                ,       CURRENT_FLAG
                ,       EFFECTIVE_FROM
                ,       EFFECTIVE_TO
				,		COMMON_PARAMETER_GROUP
        )
        select
						@iteration_number
                ,       row_id
                ,       CAPPING_METADATA_KEY
                ,       START_TIME
                ,       END_TIME
                ,       DAY_PART_DESCRIPTION
                ,       THRESHOLD_NTILE
                ,       THRESHOLD_NONTILE
                ,       PLAYBACK_NTILE
                ,       BANK_HOLIDAY_WEEKEND
                ,       BOX_SHUT_DOWN
                ,       HOUR_IN_MINUTES
                ,       HOUR_24_CLOCK_LAST_HOUR
                ,       MINIMUM_CUT_OFF
                ,       MAXIMUM_CUT_OFF
                --,     MAXIMUM_ITERATIONS int -- used for scaling
                --,     MINIMUM_HOUSEHOLD_FOR_SCALING int -- used for scaling
                ,       SAMPLE_MAX_POP
                ,       SHORT_DURATION_CAP_THRESHOLD
                ,       MINIMUM_HOUSEHOLD_FOR_CAPPING
                ,       CURRENT_FLAG
                ,       EFFECTIVE_FROM
                ,       EFFECTIVE_TO
				,		COMMON_PARAMETER_GROUP
        from CP2_metadata_table

        execute M00_2_output_to_logger '@ M07_5 : ...Save_metadata_params end'
        commit
        
        
end;


grant execute on V306_CP2_M07_5_Save_metadata_params to vespa_group_low_security;
commit;

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



** Module:                               V306_CP2_M07_6_calculate_diff_by_time_range

**      Part A:      Calculate differences between 2 consecutive iterations in each time range
**              A00 - Save metadata parameters in historic table
*/

/*
The date of interest is the date for which we are processing data, it starts from day_of_interest-1 at 23:00 and ends on day_of_interest at 23:59
*/


create or replace procedure V306_CP2_M07_6_calculate_diff_by_time_range_LIVE
									@target_date		date	=	NULL     -- Date of daily table caps to cache                                                                         
								,	@analysis_window	tinyint	=	NULL    -- Number of days to calibrate Capping over
								,	@iteration_number	int		=	NULL
								,	@processBankHoliday	bit		=	NULL -- indicates whether this is going to process bank holidays data (1) or normal weekday data (0)
as begin

	execute M00_2_output_to_logger '@ M07_6 : V306_CP2_M07_6_calculate_diff_by_time_range_LIVE start...'
	commit

	if @processBankHoliday = 0
			execute M00_2_output_to_logger ' executing for normal workdays LIVE viewing'
	else
			execute M00_2_output_to_logger ' executing for weekends LIVE viewing'

	commit


	/****************** A00 - Save metadata parameters in historic table ******************/

	declare @startTime time commit
	declare @endTime time commit

	declare @first_interval_day date commit
	declare @last_interval_day date commit
	declare @group_cnt int commit
	declare @max_group_cnt int commit
	declare @sum_VESPA double commit
	declare @sum_BARB double commit
	declare @tmp_VESPA double commit -- variable to store a temporary value of the sum of viewing values
	declare @tmp_BARB double commit -- variable to store a temporary value of the sum of viewing values
	declare @row_within_metadata_group int commit
	declare @row_max_within_metadata_group int commit
	declare @variance_diff double commit
	declare @percentage_diff double commit

	set @last_interval_day=dateadd(dd,-1,@target_date)
	commit
	set @first_interval_day=dateadd(dd,(-1) *@analysis_window,@target_date)
	commit

	select
			DAY_PART_DESCRIPTION as dp
		,	row_number() over (order by bank_holiday_weekend,DAY_PART_DESCRIPTION) as grouping_key
	into	#CP2_metadata_table_groupkey_num
	from	CP2_metadata_table
	group by
			bank_holiday_weekend
		,	DAY_PART_DESCRIPTION
	commit

	update	#CP2_metadata_table_groupkey_num groupnum
	set		grouping_key=rntoset
	from	(
				select	grouping_key	as	rntoset
				from	#CP2_metadata_table_groupkey_num
				where	dp	=	'Early Morning Weekday'
			)	tb2
	where	groupnum.dp	in	(
									'Midday Viewing Weekday'
								,	'Peak Morning Weekday'
								,	'Early Morning Weekday'
							)
	commit

	update	#CP2_metadata_table_groupkey_num groupnum
	set	grouping_key=rntoset
	from	(
				select	grouping_key	as	rntoset
				from	#CP2_metadata_table_groupkey_num
				where	dp	=	'Early Morning Weekend'
			)	tb2
	where	groupnum.dp	in	(
									'Midday Viewing Weekend'
								,	'Peak Morning Weekend'
								,	'Early Morning Weekend'
							)
	commit

	select
			row_number() over (partition by grouping_key order by row_id) as row_nr
		,	*
	into	#CP2_metadata_table_rownum
	from
					CP2_metadata_table metatab
		inner join	#CP2_metadata_table_groupkey_num	grouptab		on	metatab.DAY_PART_DESCRIPTION	=	grouptab.dp
	commit                          

	set	@group_cnt	=	(
							select	min(grouping_key)
							from	#CP2_metadata_table_rownum
							where	bank_holiday_weekend	=	@processBankHoliday
						)
	commit

	set	@max_group_cnt	=	(
								select	max(grouping_key)
								from	#CP2_metadata_table_rownum
								where	bank_holiday_weekend	=	@processBankHoliday
							)
	commit
					
	execute M00_2_output_to_logger '@ M07_6 : min group cnt is: ' || @group_cnt || ', max group cnt: ' || @max_group_cnt
	commit

	create table #CP2_metadata_iterations_diff_local (
				   iteration_number                                                int
			,       grouping_key	                                                tinyint
			,       BANK_HOLIDAY_WEEKEND                                    		int
			,       LIVE_PLAYBACK		                                    		varchar(15)
			,       THRESHOLD_NTILE                                                 int
			,       THRESHOLD_NONTILE                                               int
			,       PLAYBACK_NTILE                                                  int
			,       short_duration_cap_threshold                                                   int
			,       SUM_BARB                                                        bigint
			,       SUM_VESPA                                                       bigint
			,       VARIANCE_DIFF                                                   DOUBLE
			,       PERCENTAGE_DIFF                                                 DOUBLE
	)
	commit

	while	@group_cnt	<=	@max_group_cnt
		begin
			/* if the grouping_key we are processing does not exist */
			execute M00_2_output_to_logger '@ M07_6 : now processing group: ' || @group_cnt
			commit
			if not exists (select top 1 * from #CP2_metadata_table_rownum where grouping_key=@group_cnt)
			begin
				set @group_cnt = @group_cnt + 1 /* we need to update the counter */
				commit
				continue
			end
			
			execute M00_2_output_to_logger '@ M07_6 : check for group existance OK!'
			commit
			set @sum_VESPA=0 commit         
			set @sum_BARB=0 commit          

			/* we now use a temp table just with rows that fall within our grouping key */
			select *
			into #CP2_metadata_table_rownum_subgroup
			from #CP2_metadata_table_rownum
			where grouping_key=@group_cnt

			execute M00_2_output_to_logger '@ M07_6 : creating table #CP2_metadata_table_rownum_subgroup, nr of rows: ' || @@rowcount
			commit
			
			set @row_max_within_metadata_group=(select max(row_nr) from #CP2_metadata_table_rownum_subgroup)
			commit

			set @row_within_metadata_group=(select min(row_nr) from #CP2_metadata_table_rownum_subgroup) -- by construction this should always be 1, but I use the min function just to foresee the unforeseeable
			commit

			execute M00_2_output_to_logger '@ M07_6 : within group ' || @group_cnt || ' min row :' || @row_within_metadata_group || ', max row :' || @row_max_within_metadata_group
			commit

			while @row_within_metadata_group <= @row_max_within_metadata_group
			begin
			
				set	@startTime	=	(
										select	start_time
										from	#CP2_metadata_table_rownum_subgroup
										where	row_nr	=	@row_within_metadata_group
									)
				commit
				
				set	@endTime	=	(
										select	end_time
										from	#CP2_metadata_table_rownum_subgroup
										where	row_nr	=	@row_within_metadata_group
									)
				commit

				execute M00_2_output_to_logger '@ M07_6 : current row :' || @row_within_metadata_group || ', start time :' || @startTime || ', end time :' || @endTime
				commit
				
				if @processBankHoliday = 0
					begin
						set	@tmp_VESPA	=	(
												select	sum(VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME)
												from	VESPAvsBARB_metrics_table
												where
														stream_type								=		'LIVE'
													and	iteration_number						=		@iteration_number
													and	datepart(weekday,UTC_DAY_OF_INTEREST)	not in	(1,7)
													and	UTC_DAY_OF_INTEREST						between	@first_interval_day
																								and		@last_interval_day
													and	cast(UTC_DATEHOURMIN as time)			between	@startTime
																								and		@endTime
											)
						commit

						set	@tmp_BARB	=	(
												select	sum(BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME)
												from	VESPAvsBARB_metrics_table
												where
														stream_type								=		'LIVE'
													and	iteration_number						=		@iteration_number
													and	datepart(weekday,UTC_DAY_OF_INTEREST)	not in	(1,7)
													and UTC_DAY_OF_INTEREST						between	@first_interval_day
																								and		@last_interval_day
													and	cast(UTC_DATEHOURMIN as time)			between	@startTime
																								and		@endTime
											)
						commit
					end
				else
					begin
						set	@tmp_VESPA	=	(
												select	sum(VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME)
												from	VESPAvsBARB_metrics_table
												where
														stream_type								=	'LIVE'
													and	iteration_number						=	@iteration_number
													and datepart(weekday,UTC_DAY_OF_INTEREST)	in	(1,7)
													and	UTC_DAY_OF_INTEREST						between	@first_interval_day
																								and		@last_interval_day
													and cast(UTC_DATEHOURMIN as time)			between	@startTime
																								and		@endTime
											)
						commit
						
						set	@tmp_BARB	=	(	
												select	sum(BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME)
												from	VESPAvsBARB_metrics_table
												where
														stream_type								=	'LIVE'
													and	iteration_number						=	@iteration_number
													and datepart(weekday,UTC_DAY_OF_INTEREST) 	in	(1,7)
													and	UTC_DAY_OF_INTEREST						between	@first_interval_day
																								and		@last_interval_day
													and	cast(UTC_DATEHOURMIN as time)			between	@startTime
																								and		@endTime
											)
						commit
					end

				execute M00_2_output_to_logger '@ M07_6 : sum for the row VESPA :' || @tmp_VESPA || ', sum for the row BARB :' || @tmp_BARB
				commit

				set	@sum_VESPA	=	@sum_VESPA	+	@tmp_VESPA
				commit
				
				set	@sum_BARB	=	@sum_BARB	+	@tmp_BARB
				commit
				
				execute M00_2_output_to_logger '@ M07_6 : current aggregate sum VESPA :' || @sum_VESPA || ', current aggregate sum BARB :' || @sum_BARB
				commit
				
				set	@row_within_metadata_group	=	@row_within_metadata_group	+	1
				commit
				
			end -- @row_within_metadata_group=<@row_max_within_metadata_group
			
			commit

			if ((@sum_BARB != 0) and (@sum_VESPA != 0))
			begin
				set @percentage_diff = ((@sum_VESPA-@sum_BARB)/@sum_BARB)*100.0
				commit
				set @variance_diff = (@sum_VESPA-@sum_BARB)*(@sum_VESPA-@sum_BARB)
				commit
			end
			else
			begin
				set @percentage_diff = NULL
				commit
				set @variance_diff = NULL
				commit
			end

			commit

			insert into #CP2_metadata_iterations_diff_local	(
																	iteration_number
																,	grouping_key
																,	BANK_HOLIDAY_WEEKEND
																,	LIVE_PLAYBACK
																,	THRESHOLD_NTILE
																,	THRESHOLD_NONTILE
																,	PLAYBACK_NTILE
																,	short_duration_cap_threshold
																,	SUM_BARB
																,	SUM_VESPA
																,	VARIANCE_DIFF
																,	PERCENTAGE_DIFF
															)
			select
				@iteration_number
			,	@group_cnt
			,	@processBankHoliday
			,	'LIVE'
			,	max(THRESHOLD_NTILE)
			,	max(THRESHOLD_NONTILE)
			,	max(PLAYBACK_NTILE)
			,	max(short_duration_cap_threshold)
			,	@sum_BARB
			,	@sum_VESPA
			,	@variance_diff
			,	@percentage_diff
			from	#CP2_metadata_table_rownum_subgroup
			commit
			
			execute M00_2_output_to_logger '@ M07_6 : Inserting data in buffer table, nr of rows: ' || @@rowcount
			commit
			
			drop table #CP2_metadata_table_rownum_subgroup
			commit
			
			set @group_cnt = @group_cnt + 1
			commit

		end -- while @group_cnt<5

	/* currently start and end times are hardcoded: it works until the following is used in the table construction: (order by bank_holiday_weekend,DAY_PART_DESCRIPTION) as grouping_key */

	/* Update both current and historic diff tables */
	insert into	CP2_metadata_iterations_diff(
													iteration_number
												,	grouping_key
												,	grouping_key_start_time
												,	grouping_key_end_time
												,	BANK_HOLIDAY_WEEKEND
												,	LIVE_PLAYBACK
												,	THRESHOLD_NTILE
												,	THRESHOLD_NONTILE
												,	PLAYBACK_NTILE
												,	short_duration_cap_threshold
												,	SUM_BARB
												,	SUM_VESPA
												,	VARIANCE_DIFF
												,	PERCENTAGE_DIFF
											)
	select
			iteration_number
		,	grouping_key
		,	case
				when grouping_key  in (1,7)		then	'04:00:00' 
				when grouping_key  in (2,8)		then	'15:00:00' 
				when grouping_key  in (3,9)		then	'23:00:00'
				when grouping_key  in (6,12)	then	'20:00:00'
			end
		,	case
				when grouping_key in (1,7)		then	'14:59:59' 
				when grouping_key in (2,8)		then	'19:59:59' 
				when grouping_key in (3,9)		then	'03:59:59' 
				when grouping_key in (6,12)		then	'22:59:59'
			end
		,	BANK_HOLIDAY_WEEKEND
		,	LIVE_PLAYBACK
		,	THRESHOLD_NTILE
		,	THRESHOLD_NONTILE
		,	PLAYBACK_NTILE
		,	short_duration_cap_threshold
		,	SUM_BARB
		,	SUM_VESPA
		,	VARIANCE_DIFF
		,	PERCENTAGE_DIFF
	from #CP2_metadata_iterations_diff_local
	commit
	
	execute M00_2_output_to_logger '@ M07_6 : saving data in CP2_metadata_iterations_diff, nr of rows: ' || @@rowcount
	commit

	insert into	CP2_metadata_iterations_diff_historic_table	(
																	target_date
																,	analysis_window
																,	iteration_number
																,	grouping_key
																,	grouping_key_start_time
																,	grouping_key_end_time
																,	BANK_HOLIDAY_WEEKEND
																,	LIVE_PLAYBACK
																,	THRESHOLD_NTILE
																,	THRESHOLD_NONTILE
																,	PLAYBACK_NTILE
																,	short_duration_cap_threshold
																,	SUM_BARB
																,	SUM_VESPA
																,	VARIANCE_DIFF
																,	PERCENTAGE_DIFF
															)
	select
			@target_date
		,	@analysis_window
		,	iteration_number
		,	grouping_key
		,	case
				when grouping_key  in (1,7)  then '04:00:00' 
				when grouping_key  in (2,8)  then '15:00:00' 
				when grouping_key  in (3,9) then '23:00:00'
				when grouping_key  in (6,12)  then '20:00:00'
			end
		,	case
				when grouping_key in (1,7) then '14:59:59' 
				when grouping_key in (2,8) then '19:59:59' 
				when grouping_key in (3,9) then '03:59:59' 
				when grouping_key in (6,12) then '22:59:59'
			end
		,	BANK_HOLIDAY_WEEKEND
		,	LIVE_PLAYBACK
		,	THRESHOLD_NTILE
		,	THRESHOLD_NONTILE
		,	PLAYBACK_NTILE
		,	short_duration_cap_threshold
		,	SUM_BARB
		,	SUM_VESPA
		,	VARIANCE_DIFF
		,	PERCENTAGE_DIFF
	from	#CP2_metadata_iterations_diff_local
	commit

	execute M00_2_output_to_logger '@ M07_6 : saving data in CP2_metadata_iterations_diff_historic_table, nr of rows: ' || @@rowcount
	commit

	drop table #CP2_metadata_iterations_diff_local
	commit
	execute M00_2_output_to_logger '@ M07_6 : ...V306_CP2_M07_6_calculate_diff_by_time_range_LIVE end'
	commit
end;
commit;


grant execute on V306_CP2_M07_6_calculate_diff_by_time_range_LIVE to vespa_group_low_security;
commit;/*


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



** Module:                               V306_CP2_M07_6_calculate_diff_by_time_range

**      Part A:      Calculate differences between 2 consecutive iterations in each time range
**              A00 - Save metadata parameters in historic table
*/

/*
The date of interest is the date for which we are processing data, it starts from day_of_interest-1 at 23:00 and ends on day_of_interest at 23:59
*/


create or replace procedure V306_CP2_M07_6_calculate_diff_by_time_range_PLAYBACK
        @target_date       date = NULL     -- Date of daily table caps to cache                                                                         
		,@analysis_window       tinyint = NULL    -- Number of days to calibrate Capping over
        ,@iteration_number int = NULL
        as begin

        execute M00_2_output_to_logger '@ M07_6 : V306_CP2_M07_6_calculate_diff_by_time_range_PLAYBACK start...'
        commit


        /****************** A00 - Save metadata parameters in historic table ******************/

		declare @startTime time commit
		declare @endTime time commit

		declare @first_interval_day date commit
		declare @last_interval_day date commit
		declare @playback_ntile int commit
		declare @sum_VESPA double commit
		declare @sum_BARB double commit
		declare @variance_diff double commit
		declare @percentage_diff double commit
		
		set @last_interval_day=dateadd(dd,-1,@target_date)
		commit
		set @first_interval_day=dateadd(dd,(-1) *@analysis_window,@target_date)               
		commit
		
		set @playback_ntile=(select max(PLAYBACK_NTILE) from CP2_metadata_table) -- PLAYBACK_NTILE is constant in the table
		commit
		
		set @sum_VESPA=(select sum(VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME) from VESPAvsBARB_metrics_table where (stream_type = 'PLAYBACK') and (iteration_number=@iteration_number) and ( (UTC_DAY_OF_INTEREST>=@first_interval_day) and (UTC_DAY_OF_INTEREST<=@last_interval_day) ) )
		commit
		set @sum_BARB=(select sum(BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME) from VESPAvsBARB_metrics_table where (stream_type = 'PLAYBACK') and (iteration_number=@iteration_number) and ( (UTC_DAY_OF_INTEREST>=@first_interval_day) and (UTC_DAY_OF_INTEREST<=@last_interval_day) ) )
		commit

		if ((@sum_BARB != 0) and (@sum_VESPA != 0))
		begin
				set @percentage_diff = ((@sum_VESPA-@sum_BARB)/@sum_BARB)*100.0
				commit
				set @variance_diff = (@sum_VESPA-@sum_BARB)*(@sum_VESPA-@sum_BARB)
				commit
		end
		else
		begin
				set @percentage_diff = NULL
				commit
				set @variance_diff = NULL
				commit
		end
				
		insert into CP2_metadata_iterations_diff (
		iteration_number
				,       LIVE_PLAYBACK
				,       PLAYBACK_NTILE
				,       SUM_BARB
				,       SUM_VESPA
				,       VARIANCE_DIFF
				,       PERCENTAGE_DIFF
		)
		select 
								@iteration_number
				,               'PLAYBACK'
				,       		@playback_ntile
				,               @sum_BARB
				,               @sum_VESPA
				,               @variance_diff
				,               @percentage_diff
		commit
		
		insert into
		CP2_metadata_iterations_diff_historic_table(
				target_date
		,		analysis_window
		,       iteration_number
		,       LIVE_PLAYBACK
		,       PLAYBACK_NTILE
		,       SUM_BARB
		,       SUM_VESPA
		,       VARIANCE_DIFF
		,       PERCENTAGE_DIFF
		)
		select
				@target_date
		,		@analysis_window
		,       @iteration_number
		,       'PLAYBACK'
		,       @playback_ntile
		,       @sum_BARB
		,       @sum_VESPA
		,       @variance_diff
		,       @percentage_diff
		commit
				
		execute M00_2_output_to_logger '@ M07_6 : ...V306_CP2_M07_6_calculate_diff_by_time_range_PLAYBACK end'
        commit
end;


grant execute on V306_CP2_M07_6_calculate_diff_by_time_range_PLAYBACK to vespa_group_low_security;
commit;

----------------------------------------------------------------------------------
--	Generic SQL stored procedure for "smart" cleaning-up of local tables 
----------------------------------------------------------------------------------


CREATE OR REPLACE PROCEDURE	DROP_LOCAL_TABLE
									@TARGET_TABLE_NAME	VARCHAR(255)	=	NULL
AS	BEGIN

	IF EXISTS	(
					SELECT	1
					FROM	SYSOBJECTS
					WHERE
							[NAME]			=	@TARGET_TABLE_NAME
						AND UID				=	USER_ID()
						AND	UPPER([TYPE])	=	'U'
				)
       	BEGIN
 
			DECLARE	@SQL_ VARCHAR(255) = NULL
            COMMIT

    		SET @SQL_	=	'DROP TABLE ' || USER_NAME() || '.' || @TARGET_TABLE_NAME
            COMMIT

            EXECUTE(@SQL_)
			COMMIT
			
			MESSAGE CAST(NOW() AS TIMESTAMP) || ' | DROPPED LOCAL TABLE : ' || @TARGET_TABLE_NAME TO CLIENT
		END

END;	-- PROCEDURE
COMMIT;

GRANT EXECUTE ON DROP_LOCAL_TABLE TO VESPA_GROUP_LOW_SECURITY;
COMMIT;