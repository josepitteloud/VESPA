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
										@processing_date	date		=	today()
										
										-- Number of days to calibrate Capping over
									,	@analysis_window	tinyint		=	14
									
										-- Upper limit in iterations over Capping parameters
									,	@max_calibration_iterations	tinyint		=	20
									
										-- Sample size selection
									,	@sample_size		tinyint		=	100
									
										-- Hard initialise
									,	@hard_initialise	bit			=	0
									
										-- days lag for midnight boundary event collection
									,	@lag_days		smallint			=	-7
as begin
	
	-- Display execution parameters to console
	execute	M00_2_output_to_logger 
			'@ M01 : V306_CP2_M01_Process_Manager, params: @processing_date = '	||	@processing_date
		||	', @analysis_window = '	||	@analysis_window
		||	', @max_calibration_iterations = '	||	@max_calibration_iterations
		||	', @sample_size = '	||	@sample_size
		||	'%,	@hard_initialise = '	|| @hard_initialise
		||	',	@lag_days = '	|| @lag_days
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
	declare @VESPA_table_name_lag varchar(150)
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
--	execute	V306_CP2_M06_BARB_Minutes
--	commit





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
		
 	
		
		-- Create sample selections based on scaled acccounts for each capping date (selections will remain static per date)
		execute V306_CP2_M03_Capping_Stage2_phase1
								@capping_date
							,	@sample_size
							,	@VESPA_table_name	-- not actually used here now that we've split out Stage2 into 2 phases
							,	@lag_days
		commit
				
	end	-- while	@iter_days	<	@analysis_window




	
	while
				@acceptable			=	0
		and		@iter_calibration	<	@max_calibration_iterations
	begin



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
								,	@lag_days	
								,	@VESPA_table_name output
								,	@VESPA_table_name_lag output
			commit
			
			

			-- Create view to apply sample selection on accounts and remove duplicated instances
			execute V306_CP2_M03_Capping_Stage2_phase2
									@capping_date
								,	@sample_size
								,	@VESPA_table_name
								,	@VESPA_table_name_lag
								,	@lag_days
			commit
			
			
			-- STB profiling (add primary/secondary flags)
			execute V306_CP2_M04_Profiling
									@capping_date -- dateformat((@capping_date - datepart(weekday,@capping_date))-2, 'YYYY-MM-DD')	-- gets the previous Thursday
			commit
			
			
			-- Apply core Capping algorithm
                        -- Apply core Capping algorithm
			execute V306_CP2_M05_Prepare_Day_Caps         	@capping_date
                                                            , @iter_calibration 
															, @lag_days
			commit						

			-- Apply core Capping algorithm
			execute V306_CP2_M05_Build_Day_Caps
									@capping_date
									, @iter_calibration
			commit

						execute V306_CP2_M05_Apply_Day_Caps
									@capping_date
									, @iter_calibration
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




	end	-- while @acceptable = 0 and @iter_calibration < @max_calibration_iterations

	
	-------------------------------------------------------------
	-- Finish and output
	-------------------------------------------------------------

end; -- procedure V306a_CP2_M01_Process_Manager
commit;

grant execute on V306_CP2_M01_Process_Manager to vespa_group_low_security;
commit;


