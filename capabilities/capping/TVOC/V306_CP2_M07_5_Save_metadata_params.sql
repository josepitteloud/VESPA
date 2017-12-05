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
				,		RECORDED_NTILE
				,		VOSDAL_1H_NTILE
				,		VOSDAL_1H_24H_NTILE
				,		PUSHVOD_NTILE
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
				,		RECORDED_NTILE
				,		VOSDAL_1H_NTILE
				,		VOSDAL_1H_24H_NTILE
				,		PUSHVOD_NTILE
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

