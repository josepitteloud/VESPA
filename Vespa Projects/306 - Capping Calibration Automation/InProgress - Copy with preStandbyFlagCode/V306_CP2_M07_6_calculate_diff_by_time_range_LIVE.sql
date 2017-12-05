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
commit;