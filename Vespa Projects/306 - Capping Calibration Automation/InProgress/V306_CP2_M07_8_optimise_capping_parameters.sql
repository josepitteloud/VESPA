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


create or replace procedure V306_CP2_M07_8_optimise_capping_parameters
								@iteration_number	int	=	NULL	-- POSSIBLY NOT NEEDED
as begin

	execute M00_2_output_to_logger '@ M07_8: V306_CP2_M07_8_optimise_capping_parameters start...'
	commit
	
	
	----------------------------------------------------------------------------------------------------------
	-- Define parameters for the optimisation engine
	----------------------------------------------------------------------------------------------------------
	
	-- Define score decay rate (percentage drop per iteration)
	declare @score_decay_rate double =	0.90
	commit



	----------------------------------------------------------------------------------------------------------
	-- Update scores to simulate iteration-wise decay of selection likelihood
	----------------------------------------------------------------------------------------------------------
	
	update	CP2_metadata_parameter_scores
	set		score	=	score * @score_decay_rate
	commit




	
	----------------------------------------------------------------------------------------------------------
	-- Calculate scores for capping parameters for the current iteration
	----------------------------------------------------------------------------------------------------------
	
	execute M00_2_output_to_logger '@ M07_8: Update scores...'
	commit

	-- Global variance	-- MOVE TO INITIALISATION MODULE
	execute DROP_LOCAL_TABLE	'CP2_global_scores'
	commit
	
	create table CP2_global_scores	(
											iteration_number		int
										,	utc_day_of_interest		date
										,	stream_type				varchar(255)
										,	bank_holiday_weekend	bit
										,	rms_variance			double
										,	percentage_difference	double
									)
	commit
	
	
	insert into	CP2_global_scores
	select
			@iteration_number	as	iteration_number
    	,	utc_day_of_interest
		,	stream_type
		,	case
        		when	datepart(caldayofweek,utc_day_of_interest)	in	(1,7)	then	1
                else																	0
            end													as	bank_holiday_weekend
		,	sqrt(sum(variance_by_minute_stream) / count(1))		as	rms_variance
        ,		100.0 * (sum(VESPA_WEIGHTED_MINUTES_BY_VIEWING_TIME) - sum(BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME)) 
			/	sum(BARB_WEIGHTED_MINUTES_BY_VIEWING_TIME)		as	percentage_difference
	from	VESPAvsBARB_metrics_historic_table
	group by
			iteration_number
    	,	utc_day_of_interest
		,	stream_type
		,	bank_holiday_weekend
	commit





	----------------------------------------------------------------------------------------------------------
	-- Randomly select parameter values (according to their respective scores) for the next iteration and update the metadata table
	----------------------------------------------------------------------------------------------------------


	-- Randomly select from available parameter values
	select
			parameter_name
		,   common_parameter_group
		,   bank_holiday_weekend
		,   max(parameter_value)	as	chosen_parameter_value
	into	#CP2_new_parameter_values
	from
		(
			select
					*
				,   sum(score)  over    (
											partition by
													parameter_name
												,   common_parameter_group
												,   bank_holiday_weekend
											order by    parameter_value
											rows between unbounded preceding and current row
										)
					/
					sum(score)  over    (
											partition by
													parameter_name
												,   common_parameter_group
												,   bank_holiday_weekend
										)   as  cumsum_likelihood
				,   rand(row_id*datepart(us,now()))		as  rnd
			from    CP2_metadata_parameter_scores
		)   t0
	where   cumsum_likelihood	<	rnd
	group by
			parameter_name
		,   common_parameter_group
		,   bank_holiday_weekend
	commit
	
	create lf index lf1 on #CP2_new_parameter_values(common_parameter_group)
	commit
	
	
	
	-- Iterate over each parameter name and update with new values for the next capping run
	select
			*
		,   row_number()    over    (order by parameter_name)	as	rnum
	into	#tmp_param_space
	from	CP2_metadata_parameter_space
	commit
	
	declare	@i int	=	0
	commit
	
	declare	@param_name	varchar(255)
	commit
	
	declare	@sql_ varchar(2048)
	commit

	while	@i	<	(
						select	max(rnum)
						from	#tmp_param_space
					)
	begin
	
		set	@i	=	@i	+	1
		commit

		set	@param_name	=	(
								select	parameter_name
								from	#tmp_param_space
								where	rnum	=	@i
							)
		commit

		set	@sql_	=	'
			update	CP2_metadata_table
			set		' ||	@param_name	|| '	=	b.chosen_parameter_value
			from
							CP2_metadata_table			a
				inner join	#CP2_new_parameter_values	b	on	a.common_parameter_group	=	b.common_parameter_group
															and	a.bank_holiday_weekend		=	b.bank_holiday_weekend
			where	b.parameter_name	=	''' ||	@param_name	||	'''
			'
		commit
			
		execute(@sql_)
		-- select	@sql_
		commit
		
	end
	
	
	-- Clean up
	drop table #tmp_param_space	commit
	drop table #CP2_new_parameter_values	commit


	
	
	
	

	


	
end;
commit;

grant execute on V306_CP2_M07_8_optimise_capping_parameters to vespa_group_low_security;
commit;