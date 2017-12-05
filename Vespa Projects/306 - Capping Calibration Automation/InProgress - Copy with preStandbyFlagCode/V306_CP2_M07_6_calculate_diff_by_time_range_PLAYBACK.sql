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

