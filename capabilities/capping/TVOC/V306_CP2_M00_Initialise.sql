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
                                                @hard_initialise        =       1
                                        and     exists                  (
                                                                                        select  1
                                                                                        from    sysobjects
                                                                                        where
                                                                                                                        [name]                  =       'CP2_metadata_table'
                                                                                                        and uid                                 =       user_id()
                                                                                                        and     upper([type])   =       'U'
                                                                                )
                )
                        drop table CP2_metadata_table
        commit
        
        
        -- Create and populate CP2_metadata_table
        if not  exists  (
                                                        select  1
                                                                                from    sysobjects
                                                                                where
                                                                                                                [name]                  =       'CP2_metadata_table'
                                                                                                and uid                                 =       user_id()
                                                                                                and     upper([type])   =       'U'
                                                )
        begin

                        execute M00_2_output_to_logger '@ M00 : Creating table CP2_metadata_table...'
                        commit

                        -- table does not exist, let's create it!!!
                        create table CP2_metadata_table (
                                                        row_id                                                                  int                     primary key identity -- primary key
                                        ,       CAPPING_METADATA_KEY                                    int --
                                        ,       START_TIME                                                              time-- Start time that the day part, used when joining to events data to determine which metadata record to use
                                        ,       END_TIME                                                                time-- End time of the day part, used when joining to events data to determine which metadata record to use
                                        ,       DAY_PART_DESCRIPTION                                    varchar(25)
                                        ,       THRESHOLD_NTILE                                                 int
                                        ,       THRESHOLD_NONTILE                                               int
                                        ,       PLAYBACK_NTILE                                                  int                             default 198
                                        --4new fields added to deal with playback thresholds 2016-04-26
                                        ,       RECORDED_NTILE                                                  int                             default 198
                                        ,       VOSDAL_1H_NTILE                                                 int                             default 190
                                        ,       VOSDAL_1H_24H_NTILE                                             int                             default 194
                                        ,       PUSHVOD_NTILE                                                   int                             default 199

                                        ,       BANK_HOLIDAY_WEEKEND                                            int
                                        ,       BOX_SHUT_DOWN                                                   int
                                        ,       HOUR_IN_MINUTES                                                 int
                                        ,       HOUR_24_CLOCK_LAST_HOUR                                         int
                                        ,       MINIMUM_CUT_OFF                                                 int                             default 20
                                        ,       MAXIMUM_CUT_OFF                                                 int                             default 120
                                        --,     MAXIMUM_ITERATIONS int -- used for scaling
                                        --,     MINIMUM_HOUSEHOLD_FOR_SCALING int -- used for scaling
                                        ,       SAMPLE_MAX_POP                                                  int                             default 30000
                                        ,       SHORT_DURATION_CAP_THRESHOLD                                    int                             default 6
                                        ,       MINIMUM_HOUSEHOLD_FOR_CAPPING                                   int                             default null
                                        ,       CURRENT_FLAG                                                    int
                                        ,       EFFECTIVE_FROM                                                  date
                                        ,       EFFECTIVE_TO                                                    date
                                        ,       COMMON_PARAMETER_GROUP                                                                                  varchar(255)                                    default null
                        )
                        commit
                        
                        create lf index lf1 on CP2_metadata_table(COMMON_PARAMETER_GROUP)
                        commit


                        -- Populate with some workable default values
                        insert into CP2_metadata_table  (
                                                                                                        CAPPING_METADATA_KEY
                                                                                                ,       START_TIME
                                                                                                ,       END_TIME
                                                                                                ,       DAY_PART_DESCRIPTION
                                                                                                ,       THRESHOLD_NTILE
                                                                                                ,       THRESHOLD_NONTILE
                                                                                                ,       PLAYBACK_NTILE
                                                                                                --4new fields added to deal with playback thresholds 2016-04-26
                                                                                                ,       RECORDED_NTILE
                                                                                                ,       VOSDAL_1H_NTILE
                                                                                                ,       VOSDAL_1H_24H_NTILE
                                                                                                ,       PUSHVOD_NTILE
                                                                                                ,       BANK_HOLIDAY_WEEKEND
                                                                                                ,       BOX_SHUT_DOWN
                                                                                                ,       HOUR_IN_MINUTES
                                                                                                ,       HOUR_24_CLOCK_LAST_HOUR
                                                                                                ,       MINIMUM_CUT_OFF
                                                                                                ,       MAXIMUM_CUT_OFF
                                                                                                --,             MAXIMUM_ITERATIONS int -- used for scaling
                                                                                                --,             MINIMUM_HOUSEHOLD_FOR_SCALING int -- used for scaling
                                                                                                ,       SAMPLE_MAX_POP
                                                                                                ,       SHORT_DURATION_CAP_THRESHOLD
                                                                                                ,       MINIMUM_HOUSEHOLD_FOR_CAPPING
                                                                                                ,       CURRENT_FLAG
                                                                                                ,       EFFECTIVE_FROM
                                                                                                ,       EFFECTIVE_TO
                                                                                                ,       COMMON_PARAMETER_GROUP
                                                                                        )
                                        -- values       (       12,     '00:00:00',     '03:59:59',     'Late Evening Weekday',         0,      20,             196,    0,      122,    NULL,   NULL,   20,     120,    10000,  6,      NULL,   1,      '2012-12-25',   '9999-09-09',   'Late Evening'                  )
                                -- ,    values  (       14,     '00:00:00',     '03:59:59',     'Late Evening Weekend',         0,      20,             196,    1,      122,    NULL,   NULL,   20,     120,    10000,  6,      NULL,   1,      '2012-12-25',   '9999-09-09',   'Late Evening'                  )
                                -- ,    values  (       1,      '04:00:00',     '05:59:59',     'Early Morning Weekday',        2,      25,             196,    0,      243,    NULL,   NULL,   20,     120,    10000,  6,      NULL,   1,      '2012-12-25',   '9999-09-09',   'Morning to Afternoon'  )
                                -- ,    values  (       2,      '04:00:00',     '05:59:59',     'Early Morning Weekend',        2,      25,             196,    1,      243,    NULL,   NULL,   20,     120,    10000,  6,      NULL,   1,      '2012-12-25',   '9999-09-09',   'Morning to Afternoon'  )
                                -- ,    values  (       3,      '06:00:00',     '09:59:59',     'Peak Morning Weekday',         2,      25,             196,    0,      243,    NULL,   NULL,   20,     120,    10000,  6,      NULL,   1,      '2012-12-25',   '9999-09-09',   'Morning to Afternoon'  )
                                -- ,    values  (       4,      '06:00:00',     '09:59:59',     'Peak Morning Weekend',         2,      25,             196,    1,      243,    NULL,   NULL,   20,     120,    10000,  6,      NULL,   1,      '2012-12-25',   '9999-09-09',   'Morning to Afternoon'  )
                                -- ,    values  (       5,      '10:00:00',     '14:59:59',     'Midday Viewing Weekday',       2,      25,             196,    0,      243,    NULL,   NULL,   20,     120,    10000,  6,      NULL,   1,      '2012-12-25',   '9999-09-09',   'Morning to Afternoon'  )
                                -- ,    values  (       6,      '10:00:00',     '14:59:59',     'Midday Viewing Weekend',       2,      25,             196,    1,      243,    NULL,   NULL,   20,     120,    10000,  6,      NULL,   1,      '2012-12-25',   '9999-09-09',   'Morning to Afternoon'  )
                                -- -- , values  (       7,      '15:00:00',     '19:59:59',     'Late Afternoon Weekday',       2,      1,              196,    0,      NULL,   NULL,   NULL,   20,     120,    10000,  6,      NULL,   1,      '2012-12-25',   '9999-09-09',   'Late Afternoon'                )
                                -- -- , values  (       8,      '15:00:00',     '19:59:59',     'Late Afternoon Weekend',       2,      1,              196,    1,      NULL,   NULL,   NULL,   20,     120,    10000,  6,      NULL,   1,      '2012-12-25',   '9999-09-09',   'Late Afternoon'                )
                                -- ,    values  (       7,      '15:00:00',     '19:59:59',     'Late Afternoon Weekday',       2,      NULL,           196,    0,      NULL,   NULL,   NULL,   20,     120,    10000,  6,      NULL,   1,      '2012-12-25',   '9999-09-09',   'Late Afternoon'                )
                                -- ,    values  (       8,      '15:00:00',     '19:59:59',     'Late Afternoon Weekend',       2,      NULL,           196,    1,      NULL,   NULL,   NULL,   20,     120,    10000,  6,      NULL,   1,      '2012-12-25',   '9999-09-09',   'Late Afternoon'                )
                                -- ,    values  (       9,      '20:00:00',     '20:59:59',     'Prime Time Weekday',           0,      20,             196,    0,      122,    60,             23,             20,     120,    10000,  6,      NULL,   1,      '2012-12-25',   '9999-09-09',   'Prime Time'                    )
                                -- ,    values  (       10,     '20:00:00',     '20:59:59',     'Prime Time Weekend',           0,      20,             196,    1,      122,    60,             23,             20,     120,    10000,  6,      NULL,   1,      '2012-12-25',   '9999-09-09',   'Prime Time'                    )
                                -- ,    values  (       15,     '21:00:00',     '21:59:59',     'Prime Time Weekday',           0,      20,             196,    0,      122,    60,             23,             20,     120,    10000,  6,      NULL,   1,      '2012-12-25',   '9999-09-09',   'Prime Time'                    )
                                -- ,    values  (       17,     '21:00:00',     '21:59:59',     'Prime Time Weekend',           0,      20,             196,    1,      122,    60,             23,             20,     120,    10000,  6,      NULL,   1,      '2012-12-25',   '9999-09-09',   'Prime Time'                    )
                                -- ,    values  (       16,     '22:00:00',     '22:59:59',     'Prime Time Weekday',           0,      20,             196,    0,      122,    60,             23,             20,     120,    10000,  6,      NULL,   1,      '2012-12-25',   '9999-09-09',   'Prime Time'                    )
                                -- ,    values  (       18,     '22:00:00',     '22:59:59',     'Prime Time Weekend',           0,      20,             196,    1,      122,    60,             23,             20,     120,    10000,  6,      NULL,   1,      '2012-12-25',   '9999-09-09',   'Prime Time'                    )
                                -- ,    values  (       11,     '23:00:00',     '23:59:59',     'Late Evening Weekday',         0,      20,             196,    0,      122,    NULL,   NULL,   20,     120,    10000,  6,      NULL,   1,      '2012-12-25',   '9999-09-09',   'Late Evening'                  )
                                -- ,    values  (       13,     '23:00:00',     '23:59:59',     'Late Evening Weekend',         0,      20,             196,    1,      122,    NULL,   NULL,   20,     120,    10000,  6,      NULL,   1,      '2012-12-25',   '9999-09-09',   'Late Evening'                  )


--                                                                                                                                           Playback_ntile, RECORDED_NTILE, VOSDAL_1H_NTILE, VOSDAL_1H_24H_NTILE, PUSHVOD_NTILE, Bank_holiday, Box_shutdown                                                                           
                                        values  (       1,      '00:00:00',     '00:59:59',     'Late Evening Weekday',         0,      20,             196,            198,             190,                 194,           196,            0,      243,    NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Late Evening'                  )
                                ,       values  (       2,      '01:00:00',     '01:59:59',     'Late Evening Weekday',         0,      20,             196,            198,             190,                 194,           196,            0,      243,    NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Late Evening'                  )
                                ,       values  (       3,      '02:00:00',     '02:59:59',     'Late Evening Weekday',         0,      20,             196,            198,             190,                 194,           196,            0,      243,    NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Late Evening'                  )
                                ,       values  (       4,      '03:00:00',     '03:59:59',     'Late Evening Weekday',         0,      20,             196,            198,             190,                 194,           196,            0,      243,    NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Late Evening'                  )
                                ,       values  (       5,      '00:00:00',     '00:59:59',     'Late Evening Weekend',         0,      20,             196,            198,             190,                 194,           196,            1,      243,    NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Late Evening'                  )
                                ,       values  (       6,      '01:00:00',     '01:59:59',     'Late Evening Weekend',         0,      20,             196,            198,             190,                 194,           196,            1,      243,    NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Late Evening'                  )
                                ,       values  (       7,      '02:00:00',     '02:59:59',     'Late Evening Weekend',         0,      20,             196,            198,             190,                 194,           196,            1,      243,    NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Late Evening'                  )
                                ,       values  (       8,      '03:00:00',     '03:59:59',     'Late Evening Weekend',         0,      20,             196,            198,             190,                 194,           196,            1,      243,    NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Late Evening'                  )
                                ,       values  (       9,      '04:00:00',     '04:59:59',     'Early Morning Weekday',        8,      25,             196,            198,             190,                 194,           196,            0,      243,    NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Morning to Afternoon'  )
                                ,       values  (       10,     '05:00:00',     '05:59:59',     'Early Morning Weekday',        8,      25,             196,            198,             190,                 194,           196,            0,      243,    NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Morning to Afternoon'  )
                                ,       values  (       11,     '04:00:00',     '04:59:59',     'Early Morning Weekend',        8,      25,             196,            198,             190,                 194,           196,            1,      243,    NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Morning to Afternoon'  )
                                ,       values  (       12,     '05:00:00',     '05:59:59',     'Early Morning Weekend',        8,      25,             196,            198,             190,                 194,           196,            1,      243,    NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Morning to Afternoon'  )
                                ,       values  (       13,     '06:00:00',     '06:59:59',     'Peak Morning Weekday',         0,      25,             196,            198,             190,                 194,           196,            0,      243,    NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Morning to Afternoon'  )
                                ,       values  (       14,     '07:00:00',     '07:59:59',     'Peak Morning Weekday',         0,      25,             196,            198,             190,                 194,           196,            0,      243,    NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Morning to Afternoon'  )
                                ,       values  (       15,     '08:00:00',     '08:59:59',     'Peak Morning Weekday',         0,      25,             196,            198,             190,                 194,           196,            0,      243,    NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Morning to Afternoon'  )
                                ,       values  (       16,     '09:00:00',     '09:59:59',     'Peak Morning Weekday',         0,      25,             196,            198,             190,                 194,           196,            0,      243,    NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Morning to Afternoon'  )
                                ,       values  (       17,     '06:00:00',     '06:59:59',     'Peak Morning Weekend',         0,      25,             196,            198,             190,                 194,           196,            1,      243,    NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Morning to Afternoon'  )
                                ,       values  (       18,     '07:00:00',     '07:59:59',     'Peak Morning Weekend',         0,      25,             196,            198,             190,                 194,           196,            1,      243,    NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Morning to Afternoon'  )
                                ,       values  (       19,     '08:00:00',     '08:59:59',     'Peak Morning Weekend',         0,      25,             196,            198,             190,                 194,           196,            1,      243,    NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Morning to Afternoon'  )
                                ,       values  (       20,     '09:00:00',     '09:59:59',     'Peak Morning Weekend',         0,      25,             196,            198,             190,                 194,           196,            1,      243,    NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Morning to Afternoon'  )
                                ,       values  (       21,     '10:00:00',     '10:59:59',     'Midday Viewing Weekday',       0,      25,             196,            198,             190,                 194,           196,            0,      243,    NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Morning to Afternoon'  )
                                ,       values  (       22,     '11:00:00',     '11:59:59',     'Midday Viewing Weekday',       0,      25,             196,            198,             190,                 194,           196,            0,      243,    NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Morning to Afternoon'  )
                                ,       values  (       23,     '12:00:00',     '12:59:59',     'Midday Viewing Weekday',       0,      25,             196,            198,             190,                 194,           196,            0,      243,    NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Morning to Afternoon'  )
                                ,       values  (       24,     '13:00:00',     '13:59:59',     'Midday Viewing Weekday',       0,      25,             196,            198,             190,                 194,           196,            0,      243,    NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Morning to Afternoon'  )
                                ,       values  (       25,     '14:00:00',     '14:59:59',     'Midday Viewing Weekday',       0,      25,             196,            198,             190,                 194,           196,            0,      243,    NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Morning to Afternoon'  )
                                ,       values  (       26,     '10:00:00',     '10:59:59',     'Midday Viewing Weekend',       0,      25,             196,            198,             190,                 194,           196,            1,      243,    NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Morning to Afternoon'  )
                                ,       values  (       27,     '11:00:00',     '11:59:59',     'Midday Viewing Weekend',       0,      25,             196,            198,             190,                 194,           196,            1,      243,    NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Morning to Afternoon'  )
                                ,       values  (       28,     '12:00:00',     '12:59:59',     'Midday Viewing Weekend',       0,      25,             196,            198,             190,                 194,           196,            1,      243,    NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Morning to Afternoon'  )
                                ,       values  (       29,     '13:00:00',     '13:59:59',     'Midday Viewing Weekend',       0,      25,             196,            198,             190,                 194,           196,            1,      243,    NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Morning to Afternoon'  )
                                ,       values  (       30,     '14:00:00',     '14:59:59',     'Midday Viewing Weekend',       0,      25,             196,            198,             190,                 194,           196,            1,      243,    NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Morning to Afternoon'  )
                                ,       values  (       31,     '15:00:00',     '15:59:59',     'Late Afternoon Weekday',       7,      NULL,           196,            198,             190,                 194,           196,            0,      NULL,   NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Late Afternoon'        )
                                ,       values  (       32,     '16:00:00',     '16:59:59',     'Late Afternoon Weekday',       7,      NULL,           196,            198,             190,                 194,           196,            0,      NULL,   NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Late Afternoon'        )
                                ,       values  (       33,     '17:00:00',     '17:59:59',     'Late Afternoon Weekday',       7,      NULL,           196,            198,             190,                 194,           196,            0,      NULL,   NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Late Afternoon'        )
                                ,       values  (       34,     '18:00:00',     '18:59:59',     'Late Afternoon Weekday',       7,      NULL,           196,            198,             190,                 194,           196,            0,      NULL,   NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Late Afternoon'        )
                                ,       values  (       35,     '19:00:00',     '19:59:59',     'Late Afternoon Weekday',       7,      NULL,           196,            198,             190,                 194,           196,            0,      NULL,   NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Late Afternoon'        )
                                ,       values  (       36,     '15:00:00',     '15:59:59',     'Late Afternoon Weekend',       7,      NULL,           196,            198,             190,                 194,           196,            1,      NULL,   NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Late Afternoon'        )
                                ,       values  (       37,     '16:00:00',     '16:59:59',     'Late Afternoon Weekend',       7,      NULL,           196,            198,             190,                 194,           196,            1,      NULL,   NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Late Afternoon'        )
                                ,       values  (       38,     '17:00:00',     '17:59:59',     'Late Afternoon Weekend',       7,      NULL,           196,            198,             190,                 194,           196,            1,      NULL,   NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Late Afternoon'        )
                                ,       values  (       39,     '18:00:00',     '18:59:59',     'Late Afternoon Weekend',       7,      NULL,           196,            198,             190,                 194,           196,            1,      NULL,   NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Late Afternoon'        )
                                ,       values  (       40,     '19:00:00',     '19:59:59',     'Late Afternoon Weekend',       7,      NULL,           196,            198,             190,                 194,           196,            1,      NULL,   NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Late Afternoon'        )
                                ,       values  (       41,     '20:00:00',     '20:59:59',     'Prime Time Weekday',           0,      10,             196,            198,             190,                 194,           196,            0,       122,     60,     23,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Prime Time'            )
                                ,       values  (       42,     '20:00:00',     '20:59:59',     'Prime Time Weekend',           0,      10,             196,            198,             190,                 194,           196,            1,       122,     60,     23,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Prime Time'            )
                                ,       values  (       43,     '21:00:00',     '21:59:59',     'Prime Time Weekday',           0,      6,              196,            198,             190,                 194,           196,            0,       122,     60,     23,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Prime Time'            )
                                ,       values  (       44,     '21:00:00',     '21:59:59',     'Prime Time Weekend',           0,      6,              196,            198,             190,                 194,           196,            1,       122,     60,     23,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Prime Time'            )
                                ,       values  (       45,     '22:00:00',     '22:59:59',     'Prime Time Weekday',           0,      2,              196,            198,             190,                 194,           196,            0,       122,     60,     23,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Prime Time'            )
                                ,       values  (       46,     '22:00:00',     '22:59:59',     'Prime Time Weekend',           0,      2,              196,            198,             190,                 194,           196,            1,       122,     60,     23,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Prime Time'            )
                                ,       values  (       47,     '23:00:00',     '23:59:59',     'Late Evening Weekday',         0,      1,              196,            198,             190,                 194,           196,            0,       243,   NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Late Evening'          )
                                ,       values  (       48,     '23:00:00',     '23:59:59',     'Late Evening Weekend',         0,      1,              196,            198,             190,                 194,           196,            1,       243,   NULL,   NULL,   20,     120,    30000,  7,      20000,  1,      '2012-12-25',   '9999-09-09',   'Late Evening'          )
                        commit
                        

--                      -- FOR TEST/DEV PURPOSES
--                      insert into CP2_metadata_table  (
--                                                                                                      CAPPING_METADATA_KEY
--                                                                                              ,       START_TIME
--                                                                                              ,       END_TIME
--                                                                                              ,       DAY_PART_DESCRIPTION
--                                                                                              ,       THRESHOLD_NTILE
--                                                                                              ,       THRESHOLD_NONTILE
--                                                                                              ,       PLAYBACK_NTILE
--                                                                                              ,       BANK_HOLIDAY_WEEKEND
--                                                                                              ,       BOX_SHUT_DOWN
--                                                                                              ,       HOUR_IN_MINUTES
--                                                                                              ,       HOUR_24_CLOCK_LAST_HOUR
--                                                                                              ,       MINIMUM_CUT_OFF
--                                                                                              ,       MAXIMUM_CUT_OFF
--                                                                                              --,             MAXIMUM_ITERATIONS int -- used for scaling
--                                                                                              --,             MINIMUM_HOUSEHOLD_FOR_SCALING int -- used for scaling
--                                                                                              ,       SAMPLE_MAX_POP
--                                                                                              ,       SHORT_DURATION_CAP_THRESHOLD
--                                                                                              ,       MINIMUM_HOUSEHOLD_FOR_CAPPING
--                                                                                              ,       CURRENT_FLAG
--                                                                                              ,       EFFECTIVE_FROM
--                                                                                              ,       EFFECTIVE_TO
--                                                                                              ,       COMMON_PARAMETER_GROUP
--                                                                                      )
--                      select
--                                              CAPPING_METADATA_KEY
--                                      ,       START_TIME
--                                      ,       END_TIME
--                                      ,       DAY_PART_DESCRIPTION
--                                      ,       THRESHOLD_NTILE
--                                      ,       THRESHOLD_NONTILE
--                                      ,       PLAYBACK_NTILE
--                                      ,       BANK_HOLIDAY_WEEKEND
--                                      ,       BOX_SHUT_DOWN
--                                      ,       HOUR_IN_MINUTES
--                                      ,       HOUR_24_CLOCK_LAST_HOUR
--                                      ,       MINIMUM_CUT_OFF
--                                      ,       MAXIMUM_CUT_OFF
--                                      ,       SAMPLE_MAX_POP
--                                      ,       SHORT_DURATION_CAP_THRESHOLD
--                                      ,       MINIMUM_HOUSEHOLD_FOR_CAPPING
--                                      ,       CURRENT_FLAG
--                                      ,       EFFECTIVE_FROM
--                                      ,       EFFECTIVE_TO
--                                      ,       COMMON_PARAMETER_GROUP
--                      from    tanghoi.CP2_metadata_table_referenceCopy
--                      commit

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
                                        ,view_type                          varchar(25) default null    
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
        create hng index idx0 on CP2_calculated_viewing_caps(view_type)         commit
        create hng index idx1 on CP2_calculated_viewing_caps(event_start_day)   commit
        create hng index idx2 on CP2_calculated_viewing_caps(event_start_hour)  commit
        create hng index idx4 on CP2_calculated_viewing_caps(box_subscription)  commit
        create hng index idx5 on CP2_calculated_viewing_caps(pack_grp)          commit
        create hng index idx6 on CP2_calculated_viewing_caps(initial_genre)     commit

        -- This one, however, supports the application of caps to viewing data:
        create unique index forcing_uniqueness on CP2_calculated_viewing_caps
                                        (event_start_hour, event_start_day, initial_genre, box_subscription, pack_grp, Live, view_type)
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
                                        bucket_id                           integer             identity primary key
                                        ,event_start_hour                   tinyint             not null
                                        ,event_start_day                    tinyint             not null
                                        ,initial_genre                      varchar(30)         not null
                                        ,view_type                          varchar(25)         default null
        )
        commit

        -- So this table still isn't as wildely used as it could be in the build, it's
        -- implemented in a few places to facilitate a few things, but the big messy
        -- middle bit of the code which makes the caps according to the various rules
        -- doesn't really use it. But stuff there is split up enough to not really
        -- need it. Maybe pushing it back onto the viewing data will need it, but we
        -- are okay so far.

        create unique index for_uniqueness on CP2_capping_buckets
                                        (event_start_hour, event_start_day, initial_genre, view_type)
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
                                        ,timeshifting               varchar(25)     -- aka "view_type"  'LIVE' or 'VOSDAL' (same day as live) or 'PLAYBACK7' (playback within 7 days) or 'PLAYBACK28' (otherwise)
                                        ,capped_flag                tinyint                             -- 0-3 depending on capping treatment, or 11 if there are lingering events that are not yet treated
                                      
										,pre_standby_event_flag		tinyint
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
                                        ,view_type                  varchar(25)         default null  -- also known as timeshifting in some tables
										,lag_event_flag				smallint			default 0
                                        ,genre                      varchar(25)
                                        ,viewing_records            int
                                        ,total_viewing_in_days      decimal(8,2)        not null
                                        ,primary key (build_date, data_state, program_air_date, view_type, genre,lag_event_flag)
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
										,view_type					varchar(25)			default null
                                        ,duration_interval          int                 not null -- batched into 10s chunks, so 0 means viewing durations between 0s and 10s
                                        ,viewing_events             int                          -- Er... but these are not events, but viewing bits... oh well
                                        ,primary key (build_date, data_state, view_type, duration_interval)
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
										,view_type					varchar(25)			default null
                                        ,duration_interval          int                 not null -- batched into 1m chunks, so 0 means viewing durations between 0s and 1 minute
                                        ,viewing_events             int
                                        ,primary key (build_date, data_state, view_type, duration_interval)
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
										,view_type					varchar(25)			default null
                                        ,max_dur_mins               int                 not null
                                        ,cap_instances              int                 not null
                                        ,primary key (build_date, view_type,max_dur_mins)
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
                                ,target_date                             date
                                ,iteration_number                        int
                ,Account_Number              varchar(20)         not null
                ,Subscriber_Id               bigint              not null
                ,Programme_Trans_Sk          bigint                             -- to help out with the minute-by-minute stuff
                ,Timeshifting                varchar(25)
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


        create hg   index idx1 on Vespa_Daily_Augs (Subscriber_Id)      commit
        create hg   index idx2 on Vespa_Daily_Augs (Account_Number)     commit
        create hg   index idx3 on Vespa_Daily_Augs (Programme_Trans_Sk) commit
        create dttm index idx4 on Vespa_Daily_Augs (Viewing_Starts)     commit
        create dttm index idx5 on Vespa_Daily_Augs (Viewing_Stops)      commit

        execute M00_2_output_to_logger '@ M00 : Creating table Vespa_Daily_Augs...DONE'
        COMMIT

        execute DROP_LOCAL_TABLE 'VESPAvsBARB_metrics_table'
                COMMIT

        execute M00_2_output_to_logger '@ M00 : Creating table VESPAvsBARB_metrics_table...'
        COMMIT

                create table    VESPAvsBARB_metrics_table   (
                                                                                                                        iteration_number                                                int
                                                                                                                ,       UTC_DATEHOURMIN                                                 timestamp
                                                                                                                ,       UTC_DATEHOUR                                                    timestamp
                                                                                                                ,       UTC_DAY_OF_INTEREST                                             date
                                                                                                                ,       stream_type                                                             varchar(8)
	                                                                                                            ,       viewing_type                                                             varchar(20)																											
                                                                                                                ,       scaled_account_flag                                             bit		default 0
                                                                                                                ,       BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME   double
                                                                                                                ,       BARB_WEIGHTED_MINUTES_BY_EVENT_START_TIME       double
                                                                                                                ,       VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME  double
                                                                                                                ,       VESPA_WEIGHTED_MINUTES_BY_EVENT_START_TIME      double
                                                                                                                ,       VESPA_UNWEIGHTED_MINUTES_BY_VIEWING_TIME        double
                                                                                                                ,       VESPA_UNWEIGHTED_MINUTES_BY_EVENT_START_TIME    double
                                                                                                                ,       percentageDiff_by_minute_stream                 float
                                                                                                                ,       variance_by_minute_stream                               double
                                                                                                                ,       percentageDiff_by_minute                                float
                                                                                                                ,       variance_by_minute                                              double
                                                                                                                ,       percentageDiff_by_hour_stream                   float
                                                                                                                ,       variance_by_hour_stream                                 double
                                                                                                                ,       percentageDiff_by_hour                                  float
                                                                                                                ,       variance_by_hour                                                double
                                                                                                                ,       percentageDiff_by_day_stream                    float
                                                                                                                ,       variance_by_day_stream                                  double
                                                                                                                ,       percentageDiff_by_day                                   float
                                                                                                                ,       variance_by_day                                                 double
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
-- should probably add the different playback methods to this                                        
        create table CP2_metadata_iterations_diff (
                        primary_row_id                                                  int                     primary key identity -- primary key
                ,       iteration_number                                                int
                ,       grouping_key                                                    tinyint
                                ,       grouping_key_start_time                                                                                 time
                                ,       grouping_key_end_time                                                                                   time
                ,       BANK_HOLIDAY_WEEKEND                                                    int
                ,       LIVE_PLAYBACK                                                           varchar(15)
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
        create table VESPAvsBARB_metrics_historic_table (
                                                                                                                                iteration_number                                                        int
                                                                                                                        ,       UTC_DATEHOURMIN                                                         timestamp
                                                                                                                        ,       UTC_DATEHOUR                                                            timestamp
                                                                                                                        ,       UTC_DAY_OF_INTEREST                                                     date
                                                                                                                        ,       stream_type                                                                     varchar(8)
	                                                                                                                    ,       viewing_type                                                                     varchar(20)
                                                                                                                        ,       scaled_account_flag                                                     bit
                                                                                                                        ,       BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME           double
                                                                                                                        ,       BARB_WEIGHTED_MINUTES_BY_EVENT_START_TIME       double
                                                                                                                        ,       VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME          double
                                                                                                                        ,       VESPA_WEIGHTED_MINUTES_BY_EVENT_START_TIME      double
                                                                                                                        ,       VESPA_UNWEIGHTED_MINUTES_BY_VIEWING_TIME                double
                                                                                                                        ,       VESPA_UNWEIGHTED_MINUTES_BY_EVENT_START_TIME    double
                                                                                                                        ,       percentageDiff_by_minute_stream                         float
                                                                                                                        ,       variance_by_minute_stream                                       double
                                                                                                                        ,       percentageDiff_by_minute                                        float
                                                                                                                        ,       variance_by_minute                                                      double
                                                                                                                        ,       percentageDiff_by_hour_stream                           float
                                                                                                                        ,       variance_by_hour_stream                                         double
                                                                                                                        ,       percentageDiff_by_hour                                          float
                                                                                                                        ,       variance_by_hour                                                        double
                                                                                                                        ,       percentageDiff_by_day_stream                            float
                                                                                                                        ,       variance_by_day_stream                                          double
                                                                                                                        ,       percentageDiff_by_day                                           float
                                                                                                                        ,       variance_by_day                                                         double
                                                                                                                )
        commit
        
        create hg   index idx1 on VESPAvsBARB_metrics_historic_table (iteration_number) commit
        create dttm index idx3 on VESPAvsBARB_metrics_historic_table (UTC_DATEHOUR)  commit
        create dttm index idx4 on VESPAvsBARB_metrics_historic_table (UTC_DATEHOURMIN)  commit
        create date index idx2 on VESPAvsBARB_metrics_historic_table (UTC_DAY_OF_INTEREST)     commit

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
        create table CP2_metadata_historic_table        (
                                                                                                                        primary_row_id                                  int                             primary key identity -- primary key
                                                                                                                ,       iteration_number                                int
                                                                                                                ,   row_id                                                      int                     -- this refers to the row_id in the current table from which we save the parameters in this historic table
                                                                                                                ,   CAPPING_METADATA_KEY                        int --
                                                                                                                ,   START_TIME                                          time-- Start time that the day part, used when joining to events data to determine which metadata record to use
                                                                                                                ,   END_TIME                                            time-- End time of the day part, used when joining to events data to determine which metadata record to use
                                                                                                                ,   DAY_PART_DESCRIPTION                        varchar(25)
                                                                                                                ,   THRESHOLD_NTILE                                     int
                                                                                                                ,   THRESHOLD_NONTILE                           int
                                                                                                                ,   PLAYBACK_NTILE                                      int
                                                                                                                ,   recorded_NTILE                                      int                                                            
                                                                                                                ,   vosdal_1h_NTILE                                      int
                                                                                                                ,   vosdal_1h_24h_NTILE                                      int
                                                                                                                ,   pushvod_NTILE                                      int
																												,   BANK_HOLIDAY_WEEKEND                        int
                                                                                                                ,   BOX_SHUT_DOWN                                       int
                                                                                                                ,   HOUR_IN_MINUTES                                     int
                                                                                                                ,   HOUR_24_CLOCK_LAST_HOUR                     int
                                                                                                                ,   MINIMUM_CUT_OFF                                     int
                                                                                                                ,   MAXIMUM_CUT_OFF                                     int
                                                                                                                --  MAXIMUM_ITERATIONS int -- used for scaling
                                                                                                                --  MINIMUM_HOUSEHOLD_FOR_SCALING int -- used for scaling
                                                                                                                ,   SAMPLE_MAX_POP                                      int
                                                                                                                ,   SHORT_DURATION_CAP_THRESHOLD        int
                                                                                                                ,   MINIMUM_HOUSEHOLD_FOR_CAPPING       int                     default null
                                                                                                                ,   CURRENT_FLAG                                        int
                                                                                                                ,   EFFECTIVE_FROM                                      date
                                                                                                                ,   EFFECTIVE_TO                                        date
        /*                                                                                                      ,       RECORDED_NTILE                                  int                             default 198
                                                                                                                ,       VOSDAL_1H_NTILE                                 int                             default 190
                                                                                                                ,       VOSDAL_1H_24H_NTILE                             int                             default 194
                                                                                                                ,       PUSHVOD_NTILE                                   int                             default 199
        */
                                                                                                                ,       COMMON_PARAMETER_GROUP                  varchar(255)    default null
                                                                                                        )
        commit
        create hg   index idx1 on CP2_metadata_historic_table (iteration_number)        commit
                
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
                ,       target_date                                                                                                     date
                ,       analysis_window                                                                                     tinyint
                ,       iteration_number                                                int
                ,       grouping_key                                                    tinyint
                                ,       grouping_key_start_time                                                                                 time
                                ,       grouping_key_end_time                                                                                   time
                ,       BANK_HOLIDAY_WEEKEND                                                    int
                ,       LIVE_PLAYBACK                                                           varchar(15)
                ,       THRESHOLD_NTILE                                                 int
                ,       THRESHOLD_NONTILE                                               int
                ,       playback_NTILE                                               int
                ,       recorded_NTILE                                               int
                ,       vosdal_1h_NTILE                                               int
                ,       vosdal_1h_24h_NTILE                                               int
                ,       pushvod_NTILE                                                  int
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
                create hg index hgran ON CP2_accounts(rand_num) commit
				
				
        -- Reset CP2_accounts_lag table
                if exists  (
                                        select  1
                                        from    sysobjects
                                        where
                                                        [name]                  =       'CP2_accounts_lag'
                                                and uid                         =       user_id()
                                                and     upper([type])   =       'U'
                                )
        begin
                
                        execute DROP_LOCAL_TABLE 'CP2_accounts_lag'
                        commit
                end
                
                create table CP2_accounts_lag (
                                                account_number    varchar(20)
                                ,       adsmart_scaling_weight   numeric(13,6)
                                ,       rand_num      float
                                ,       reference_date  date
                                )
                commit

                create hg   index idx1_CP2_accounts on CP2_accounts_lag (account_number)        commit
                create hg index hgran ON CP2_accounts_lag (rand_num) commit

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
        
        create table CP2_metadata_parameter_space       (
                                                                                                                parameter_name  varchar(255)
                                                                                                        ,       min_value               int
                                                                                                        ,       max_value               int
                                                                                                        ,       step_size               int             default 1
                                                                                                )
        commit
        
        insert into CP2_metadata_parameter_space
                        values  ('THRESHOLD_NTILE',             0,      30,             1)
                ,       values  ('THRESHOLD_NONTILE',   0,      30,             1)
                ,       values  ('PLAYBACK_NTILE',              1,      200,    1)
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
        
        create table CP2_metadata_parameter_scores      (
                                                                                                                row_id                                  int                             primary key             identity
                                                                                                        ,       parameter_name                  varchar(255)
                                                                                                        ,       parameter_value                 int
                                                                                                        ,       COMMON_PARAMETER_GROUP  varchar(255)    default null
                                                                                                        ,       bank_holiday_weekend    bit
                                                                                                        ,       score                                   double                  default null
                                                                                                )
        commit
        

        insert into     CP2_metadata_parameter_scores   (
                                                                                                                parameter_name
                                                                                                        ,       parameter_value
                                                                                                        ,       COMMON_PARAMETER_GROUP
                                                                                                        ,       bank_holiday_weekend
                                                                                                )
        select
                        MET.parameter_name
                ,   t0.row_num      as  parameter_value
                ,       c.COMMON_PARAMETER_GROUP
                ,       d.bank_holiday_weekend
        from
                                        CP2_metadata_parameter_space    MET
                cross join      (
                                                select  row_num
                                                from
                                                                                (
                                                                                        select
                                                                                                        min(min_value)          as      min_v
                                                                                                ,       max(max_value)          as      max_v
                                                                                                ,       min(step_size)          as      stepstep
                                                                                        from    CP2_metadata_parameter_space
                                                                                )                                                                                                               a
                                                        cross join  sa_rowgenerator(min_v,max_v,stepstep)                               b
                                        )                                                               t0
                cross join      (
                                                select  distinct        COMMON_PARAMETER_GROUP
                                                from    CP2_metadata_table
                                        )                                                               c
                cross join      (
                                                select  cast(row_num as bit)    as      bank_holiday_weekend
                                                from    sa_rowgenerator(0,1,1)
                                        )                                                               d
        where   parameter_value between MET.min_value
                                                        and             MET.max_value
        commit
        
        
        
        update  CP2_metadata_parameter_scores
        set             score   =       rand(row_id*datepart(us,now()))
        commit



end; -- procedure
commit;

grant execute on V306_CP2_M00_Initialise to vespa_group_low_security;
commit;
