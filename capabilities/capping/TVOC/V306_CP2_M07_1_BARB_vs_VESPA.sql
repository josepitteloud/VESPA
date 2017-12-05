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
												,	viewing_type
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
		,	ves.viewing_type
		,	ves.scaled_account_flag
		,	coalesce(BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME,	0)															as	BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME
		,	coalesce(BARB_WEIGHTED_MINUTES_BY_EVENT_START_TIME,	0)														as	BARB_WEIGHTED_MINUTES_BY_EVENT_START_TIME
		,	coalesce(VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME,0)*(100.0/cast(coalesce(@sample_size,100) as double))		as	VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME		-- rescale back to up 100% sample
		,	coalesce(VESPA_WEIGHTED_MINUTES_BY_EVENT_START_TIME,0)*(100.0/cast(coalesce(@sample_size,100) as double))	as	VESPA_WEIGHTED_MINUTES_BY_EVENT_START_TIME		-- rescale back to up 100% sample
		,	coalesce(VESPA_UNWEIGHTED_MINUTES_BY_VIEWING_TIME,0)														as	VESPA_UNWEIGHTED_MINUTES_BY_VIEWING_TIME
		,	coalesce(VESPA_UNWEIGHTED_MINUTES_BY_EVENT_START_TIME,0)													as	VESPA_UNWEIGHTED_MINUTES_BY_EVENT_START_TIME
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
		,	ves.viewing_type
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
		,	viewing_type
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
		and met.viewing_type		=	str.viewing_type
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
		,	ves.viewing_type
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
			,	ves.viewing_type
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
		and met.viewing_type		= 	str.viewing_type
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
														,	viewing_type
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
		,	viewing_type
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
commit;