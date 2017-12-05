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


create or replace procedure V306_CP2_M07_7_tune_iteration_params
        @processBankHoliday bit = 0 -- indicates whether this is going to process bank holidays data (1) or normal weekday data (0)
		,@ntile_threshold_step int = 2
        as begin

        execute M00_2_output_to_logger '@ M07_7: V306_CP2_M07_7_tune_iteration_params start...'
        commit

                if @processBankHoliday = 0
                                execute M00_2_output_to_logger ' executing for normal workdays LIVE viewing'
                else
                                execute M00_2_output_to_logger ' executing for weekends LIVE viewing'
        commit


        /****************** A00 - Cycle through parameters ******************/

                declare @iteration_nr int commit
				declare @max_group_cnt int commit
				declare @min_group_cnt int commit
				declare @group_cnt int commit
				declare @start_time time commit
				declare @end_time time commit
				declare @row_count int commit
				declare @apply_algorithm bit commit				

				declare @current_threshold_ntile_val int commit -- VALUE FROM CURRENT ITERATION
				declare @next_iteration_threshold_ntile_val int commit -- VALUE to be set in next ITERATION
				declare @previous_iteration_threshold_ntile_val int commit -- VALUE FROM PREVIOUS ITERATION (the one before current)
				
				declare @percentage_diff double commit
				declare @percentage_diff_previous_iteration double commit
                                
                set @iteration_nr=(select max(iteration_number) from CP2_metadata_iterations_diff where (BANK_HOLIDAY_WEEKEND=@processBankHoliday) and (LIVE_PLAYBACK='LIVE'))
                commit

				execute M00_2_output_to_logger '@ M07_7: current iteration: ' || @iteration_nr
				commit
				/* better to have a small table with the subset of data */
				select *
				into #CP2_metadata_iterations_diff_current_iteration
				from
				CP2_metadata_iterations_diff
				where 
				(BANK_HOLIDAY_WEEKEND=@processBankHoliday) and (LIVE_PLAYBACK='LIVE') and (iteration_number=@iteration_nr)
				commit
				
				set @min_group_cnt=(select min(grouping_key) from #CP2_metadata_iterations_diff_current_iteration)
                commit

				set @max_group_cnt=(select max(grouping_key) from #CP2_metadata_iterations_diff_current_iteration)
                commit
                
				set @group_cnt=@min_group_cnt
				commit                          
                
                while @group_cnt<=@max_group_cnt
                begin
                        /* if the grouping_key we are processing does not exist */
						set @percentage_diff=(select PERCENTAGE_DIFF from #CP2_metadata_iterations_diff_current_iteration where (grouping_key=@group_cnt))
						commit
						
						execute M00_2_output_to_logger '@ M07_7: now processing group ' || @group_cnt || ', percentage difference in this iteration is: ' || @percentage_diff
						commit

						if (@iteration_nr > 1)
						begin
							set @percentage_diff_previous_iteration=(select PERCENTAGE_DIFF from CP2_metadata_iterations_diff where (iteration_number=(@iteration_nr-1)) and (grouping_key=@group_cnt))
							execute M00_2_output_to_logger '@ M07_7: percentage difference previous iteration was: ' || @percentage_diff_previous_iteration
							commit
						end
						else
						begin
							set @percentage_diff_previous_iteration=sign(@percentage_diff)*(abs(@percentage_diff)+10) /* no previous iteration as this is the first, so initialize to a value that is higher that the one we are considering (to prevent it to be considered as better than the current) */
						end


						/* detect start and end time pertaining to this grouping key */
						set @start_time=(select grouping_key_start_time from #CP2_metadata_iterations_diff_current_iteration where (grouping_key=@group_cnt))
						commit
						
						set @end_time=(select grouping_key_end_time from #CP2_metadata_iterations_diff_current_iteration where (grouping_key=@group_cnt))
						commit

						set @current_threshold_ntile_val=(select threshold_ntile from #CP2_metadata_iterations_diff_current_iteration  where (grouping_key=@group_cnt))
						-- set @current_threshold_ntile_val=(select threshold_ntile from #CP2_metadata_iterations_diff_current_iteration where (start_time=@start_time) /*and (end_time=@end_time)*/ and (bank_holiday_weekend=@processBankHoliday))
						commit
						set @next_iteration_threshold_ntile_val=@current_threshold_ntile_val /* by default we put the new = to old, we will change this if needed */
						commit

						execute M00_2_output_to_logger '@ M07_7: current threshold ntile: ' || @current_threshold_ntile_val
						commit

						if (@percentage_diff is null) or (@percentage_diff = 0) or (abs(@percentage_diff_previous_iteration)<=abs(@percentage_diff)) -- or (@current_threshold_ntile_val=0) !!!!!!!!!!!!!!!1
						begin
							execute M00_2_output_to_logger '@ M07_7: we will NOT apply the algorithm'
							commit
							set @apply_algorithm=0
							commit
							if (abs(@percentage_diff_previous_iteration) < abs(@percentage_diff)) and (@iteration_nr > 1) /* Leo: the (@iteration_nr > 1) condition should be redundant */
							begin
								-- next iteration val will be the value from previous iteration
								set @previous_iteration_threshold_ntile_val=(select threshold_ntile from CP2_metadata_iterations_diff where (iteration_number=(@iteration_nr-1)) and (grouping_key=@group_cnt))
								commit
								set @next_iteration_threshold_ntile_val=@previous_iteration_threshold_ntile_val
								commit
								execute M00_2_output_to_logger '@ M07_7: previous iteration had better difference so we stop the algorithm and take previous ntile value: ' || @next_iteration_threshold_ntile_val
								commit
							end
						end
						else
						begin
							set @apply_algorithm=1
							commit
							execute M00_2_output_to_logger '@ M07_7: we will apply the algorithm'
							commit
						end
						
                        if (@apply_algorithm != 0)
						begin
										
										execute M00_2_output_to_logger '@ M07_7: start time: ' || @start_time || ', end time: ' || @end_time
										commit
										if (@percentage_diff > 0)
										begin
												-- more aggressive capping, we are above BARB
												set @next_iteration_threshold_ntile_val=@current_threshold_ntile_val+@ntile_threshold_step
												commit
												execute M00_2_output_to_logger '@ M07_7: percentage diff > 0'
												commit
										end
										else if (@percentage_diff < 0)
												begin
														-- less aggressive capping, we are below BARB
														
														execute M00_2_output_to_logger '@ M07_7: percentage diff < 0'
														commit
														if (@current_threshold_ntile_val >= @ntile_threshold_step)
														begin
																set @next_iteration_threshold_ntile_val=@current_threshold_ntile_val-@ntile_threshold_step
																commit
														end
														else
														begin
																set @next_iteration_threshold_ntile_val=0
																commit
														end
														
												end
										
										
						end -- if percentage_diff not null

						if (@next_iteration_threshold_ntile_val != @current_threshold_ntile_val)
						begin
								update CP2_metadata_table
								set THRESHOLD_NTILE=@next_iteration_threshold_ntile_val
								where (start_time=@start_time) /*and (end_time=@end_time)*/ and (bank_holiday_weekend=@processBankHoliday)
								commit

								set @row_count=@@rowcount
								commit

								execute M00_2_output_to_logger '@ M07_7: updating threshold ntile, from current iteration val ' || @current_threshold_ntile_val || ' to next iteration val ' || @next_iteration_threshold_ntile_val
								commit
								execute M00_2_output_to_logger '@ M07_7: affected rows: ' || @row_count
								commit
						end
                                                
						set @group_cnt = @group_cnt + 1
                        commit

                end -- while @group_cnt<5


                execute M00_2_output_to_logger '@ M07_7: V306_CP2_M07_7_tune_iteration_params end'
        commit
end;


grant execute on V306_CP2_M07_7_tune_iteration_params to vespa_group_low_security;
commit;

