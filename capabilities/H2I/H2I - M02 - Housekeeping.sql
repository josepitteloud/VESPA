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

--------------------------------------------------------------------------------------------------------------
**Project Name:							Skyview H2I
**Analysts:                             Angel Donnarumma	(angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jason Thompson		(Jason.Thompson@skyiq.co.uk)
										,Hoi Yu Tang		(HoiYu.Tang@skyiq.co.uk)
										,Jose Pitteloud		(jose.pitteloud@skyiq.co.uk)
**Stakeholder:                          SkyIQ
										,Jose Loureda		(Jose.Loureda@skyiq.co.uk)
**Due Date:                             11/07/2014
**Project Code (Insight Collation):     v289
**Sharepoint Folder:    

	http://sp-department.bskyb.com/sites/SIGEvolved/Shared%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FSIGEvolved%2FShared%20Documents%2F01%20Analysis%20Requests%2FV289%20-%20Skyview%20Futures%2F01%20Plans%20Briefs%20and%20Project%20Admin                                                        
	                                                          
**Business Brief:

	This Module goal is to assign a session size to all the events using Monte Carlo simulation process. 

**Module:
	
	M02: Housekeeping
			M02.0 - Initialising Environment
			M02.1 - Checking for Fresh Start flag 
			M02.2 - Maintaining Base tables
			M02.3 - Initialising the logger
			M02.4 - Returning Results
	
--------------------------------------------------------------------------------------------------------------
*/


-----------------------------------
-- M02.0 - Initialising Environment
-----------------------------------
create or replace procedure v289_m02_housekeeping
	@fresh_start	bit	= 0
	,@log_id		bigint 	output
as begin

	MESSAGE cast(now() as timestamp)||' | Begining  M02.0 - Initialising Environment' TO CLIENT
	
	-- variables
	declare	@tasks_done		smallint
	declare @total_tasks	smallint
	declare @logbatch_id	varchar(20)
	declare @logrefres_id	varchar(40)
	
	MESSAGE cast(now() as timestamp)||' | @ M02.0: Initialising Environment DONE' TO CLIENT

----------------------------------------
-- M02.1 - Checking for Fresh Start flag
----------------------------------------

	MESSAGE cast(now() as timestamp)||' | Begining  M02.1 - Checking for Fresh Start flag' TO CLIENT
	
	if @fresh_start = 1
	begin
	
		MESSAGE cast(now() as timestamp)||' | @ M02.1: Fresh Start requested: Resting process table' TO CLIENT
		
		update	v289_m01_t_process_manager
		set		status = 0
		
		commit
		
		MESSAGE cast(now() as timestamp)||' | @ M02.1: Checking for Fresh Start flag DONE' TO CLIENT
		
	end
	
----------------------------------
-- M02.2 - Maintaining Base tables
----------------------------------
	
	else
	begin
	
		/*
			checking if all tasks have been executed so meaning we need to restart
			their status else they will never get executed
			
			status = 1 means DONE
			status = 0 means PENDING
		*/
	
		MESSAGE cast(now() as timestamp)||' | @ M02.1: No Fresh Start requested' TO CLIENT
		MESSAGE cast(now() as timestamp)||' | Begining  M02.2 - Maintaining Base tables' TO CLIENT
		
		select	@tasks_done = count(1) from v289_m01_t_process_manager where status > 0
		select	@total_tasks = count(1) from v289_m01_t_process_manager
		
		if @tasks_done = @total_tasks
		begin
			
			MESSAGE cast(now() as timestamp)||' | @ M02.2: Reseting Status in Process Table' TO CLIENT
			
			update 	v289_m01_t_process_manager
			set		status = 0
			
			commit
			
			MESSAGE cast(now() as timestamp)||' | @ M02.2: Reseting Status in Process Table DONE' TO CLIENT
			
		end
		
	end
	
	if	( @fresh_start = 1 or @tasks_done = @total_tasks)
	begin
		
		MESSAGE cast(now() as timestamp)||' | @ M02.2: Cleaning Base tables' TO CLIENT
		
		/*
		if object_id('PI_BARB_import') is not null drop table PI_BARB_import	
		if object_id('BARB_Individual_Panel_Member_Details') is not null drop table BARB_Individual_Panel_Member_Details	
		if object_id('BARB_Panel_Member_Responses_Weights_and_Viewing_Categories') is not null drop table BARB_Panel_Member_Responses_Weights_and_Viewing_Categories	
		if object_id('BARB_PVF_Viewing_Record_Panel_Members') is not null drop table BARB_PVF_Viewing_Record_Panel_Members	
		if object_id('BARB_PVF06_Viewing_Record_Panel_Members') is not null drop table BARB_PVF06_Viewing_Record_Panel_Members	
		if object_id('BARB_Panel_Demographic_Data_TV_Sets_Characteristics') is not null drop table BARB_Panel_Demographic_Data_TV_Sets_Characteristics	
		if object_id('BARB_PVF04_Individual_Member_Details') is not null drop table BARB_PVF04_Individual_Member_Details
		if object_id('BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories') is not null	truncate table BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories		
		*/
		--if object_id('V289_PIV_Grouped_Segments_desc') is not null			drop table V289_PIV_Grouped_Segments_desc	 			[STATIC]
		--if object_id('V289_M08_SKY_HH_composition') is not null 			truncate table V289_M08_SKY_HH_composition					[SEMI-STATIC]
		--if object_id('V289_M08_SKY_HH_view') is not null 					truncate table V289_M08_SKY_HH_view							[SEMI-STATIC]
		if object_id('V289_M12_Skyview_weighted_duration') is not null		truncate table V289_M12_Skyview_weighted_duration	
		if object_id('v289_M06_dp_raw_data') is not null 					truncate table v289_M06_dp_raw_data	
		if object_id('V289_M07_dp_data') is not null 						truncate table V289_M07_dp_data	
		--if object_id('SC3I_Variables_lookup_v1_1') is not null 				truncate table SC3I_Variables_lookup_v1_1				[STATIC]
		--if object_id('SC3I_Segments_lookup_v1_1') is not null 				truncate table SC3I_Segments_lookup_v1_1	    		[I THINK IS STATIC]
		--if object_id('SC3I_Sky_base_segment_snapshots') is not null 		truncate table SC3I_Sky_base_segment_snapshots				[STATIC]
		if object_id('SC3I_Todays_panel_members') is not null 				truncate table SC3I_Todays_panel_members
		if object_id('SC3I_weighting_working_table') is not null 			truncate table SC3I_weighting_working_table
		if object_id('SC3I_category_working_table') is not null 			truncate table SC3I_category_working_table
		--if object_id('SC3I_category_subtotals') is not null 				truncate table SC3I_category_subtotals						[STATIC]
		--if object_id('SC3I_metrics') is not null 							truncate table SC3I_metrics									[STATIC]
		--if object_id('SC3I_non_convergences') is not null 					truncate table SC3I_non_convergences					[STATIC]
		--if object_id('SC3I_Weightings') is not null 						truncate table SC3I_Weightings								[STATIC]
		--if object_id('SC3I_Intervals') is not null 							truncate table SC3I_Intervals							[STATIC]
		--if object_id('V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING') is not null 	truncate table V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING	[STATIC]
		if object_id('V289_M11_04_Barb_weighted_population') is not null 	truncate table V289_M11_04_Barb_weighted_population
		if object_id('SC3_Weightings') is not null 							truncate table SC3_Weightings
		if object_id('SC3_Intervals') is not null 							truncate table SC3_Intervals
		if object_id('VESPA_HOUSEHOLD_WEIGHTING') is not null 				truncate table VESPA_HOUSEHOLD_WEIGHTING
		if object_id('SC3_Sky_base_segment_snapshots') is not null 			truncate table SC3_Sky_base_segment_snapshots
		if object_id('SC3_Todays_panel_members') is not null 				truncate table SC3_Todays_panel_members
		if object_id('SC3_Todays_segment_weights') is not null 				truncate table SC3_Todays_segment_weights
		if object_id('SC3_scaling_weekly_sample') is not null 				truncate table SC3_scaling_weekly_sample
		if object_id('SC3_weighting_working_table') is not null 			truncate table SC3_weighting_working_table
		if object_id('SC3_category_working_table') is not null 				truncate table SC3_category_working_table
		if object_id('SC3_category_subtotals') is not null 					truncate table SC3_category_subtotals
		--if object_id('SC3_metrics') is not null 							truncate table SC3_metrics
		if object_id('SC3_non_convergences') is not null 					truncate table SC3_non_convergences
	
		commit
		MESSAGE cast(now() as timestamp)||' | @ M02.2: Cleaning Base tables DONE' TO CLIENT
	
	end
	
	MESSAGE cast(now() as timestamp)||' | @ M02.2: Maintaining Base tables DONE' TO CLIENT
	
----------------------------------
-- M02.3 - Initialising the logger
----------------------------------	
	
	MESSAGE cast(now() as timestamp)||' | Begining  M02.3 - Initialising the logger' TO CLIENT
	
	-- Now automatically detecting if it's a test build and logging appropriately...
	
	if lower(user) = 'vespa_analysts'
		set @logbatch_id = 'H2I'
	else
		set @logbatch_id = 'H2I test ' || upper(right(user,1)) || upper(left(user,2))

	set @logrefres_id = convert(varchar(10),today(),123) || ' H2I refresh'
	
	execute citeam.logger_create_run @logbatch_id, @logrefres_id, @log_ID output

	--execute citeam.logger_add_event @log_ID, 3, 'M02: Log initialised'
	
	MESSAGE cast(now() as timestamp)||' | @ M02.3: Initialising the logger DONE' TO CLIENT
	
----------------------------
-- M02.4 - Returning Results
----------------------------

	MESSAGE cast(now() as timestamp)||' | Begining  M02.4 - Returning Results' TO CLIENT
	MESSAGE cast(now() as timestamp)||' | @ M02.4: Returning Results DONE' TO CLIENT
	
	MESSAGE cast(now() as timestamp)||' | M02 Finished' TO CLIENT	

end;

commit;
grant execute on v289_m02_housekeeping to vespa_group_low_security;
commit;