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
		IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'PI_BARB_IMPORT') 											drop table PI_BARB_import	
		IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'BARB_INDIVIDUAL_PANEL_MEMBER_DETAILS') 						drop table BARB_Individual_Panel_Member_Details	
		IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'BARB_PANEL_MEMBER_RESPONSES_WEIGHTS_AND_VIEWING_CATEGORIES') drop table BARB_Panel_Member_Responses_Weights_and_Viewing_Categories	
		IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'BARB_PVF_VIEWING_RECORD_PANEL_MEMBERS') 						drop table BARB_PVF_Viewing_Record_Panel_Members	
		IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'BARB_PVF06_VIEWING_RECORD_PANEL_MEMBERS') 					drop table BARB_PVF06_Viewing_Record_Panel_Members	
		IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'BARB_PANEL_DEMOGRAPHIC_DATA_TV_SETS_CHARACTERISTICS') 		drop table BARB_Panel_Demographic_Data_TV_Sets_Characteristics	
		IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'BARB_PVF04_INDIVIDUAL_MEMBER_DETAILS') 						drop table BARB_PVF04_Individual_Member_Details
		IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'BARB_PVF05_PANEL_MEMBER_RESPONSES_WEIGHTS_AND_VIEWING_CATEGORIES') truncate table BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories		
		*/
		--IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'V289_PIV_GROUPED_SEGMENTS_DESC') 							drop table V289_PIV_Grouped_Segments_desc	 			[STATIC]
		--IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'V289_M08_SKY_HH_COMPOSITION') 								truncate table V289_M08_SKY_HH_composition					[SEMI-STATIC]
		--IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'V289_M08_SKY_HH_VIEW') 									truncate table V289_M08_SKY_HH_view							[SEMI-STATIC]
		--IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3I_VARIABLES_LOOKUP_V1_1') 								truncate table SC3I_Variables_lookup_v1_1				[STATIC]
		--IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3I_SEGMENTS_LOOKUP_V1_1') 								truncate table SC3I_Segments_lookup_v1_1	    		[I THINK IS STATIC]
		--IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3I_SKY_BASE_SEGMENT_SNAPSHOTS') 		truncate table SC3I_Sky_base_segment_snapshots				[STATIC]
		--IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3I_CATEGORY_SUBTOTALS') 				truncate table SC3I_category_subtotals						[STATIC]
		--IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3I_METRICS') 						truncate table SC3I_metrics									[STATIC]
		--IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3I_NON_CONVERGENCES') 				truncate table SC3I_non_convergences					[STATIC]
		--IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3I_WEIGHTINGS') 						truncate table SC3I_Weightings								[STATIC]
		--IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3I_INTERVALS') 						truncate table SC3I_Intervals							[STATIC]
		--IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING') 	truncate table V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING	[STATIC]
		--IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3_METRICS') 							truncate table SC3_metrics
		
		IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'V289_M12_SKYVIEW_WEIGHTED_DURATION') 	truncate table V289_M12_Skyview_weighted_duration	
		IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'V289_M06_DP_RAW_DATA') 					truncate table v289_M06_dp_raw_data	
		IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'V289_M07_DP_DATA') 						truncate table V289_M07_dp_data	
		IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3I_TODAYS_PANEL_MEMBERS') 				truncate table SC3I_Todays_panel_members
		IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3I_WEIGHTING_WORKING_TABLE') 			truncate table SC3I_weighting_working_table
		IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3I_CATEGORY_WORKING_TABLE') 			truncate table SC3I_category_working_table
		IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'V289_M11_04_BARB_WEIGHTED_POPULATION') 	truncate table V289_M11_04_Barb_weighted_population
		IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3_WEIGHTINGS') 						truncate table SC3_Weightings
		IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3_INTERVALS') 							truncate table SC3_Intervals
		IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'VESPA_HOUSEHOLD_WEIGHTING') 				truncate table VESPA_HOUSEHOLD_WEIGHTING
		IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3_SKY_BASE_SEGMENT_SNAPSHOTS') 		truncate table SC3_Sky_base_segment_snapshots
		IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3_TODAYS_PANEL_MEMBERS') 				truncate table SC3_Todays_panel_members
		IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3_TODAYS_SEGMENT_WEIGHTS') 			truncate table SC3_Todays_segment_weights
		IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3_SCALING_WEEKLY_SAMPLE') 				truncate table SC3_scaling_weekly_sample
		IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3_WEIGHTING_WORKING_TABLE') 			truncate table SC3_weighting_working_table
		IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3_CATEGORY_WORKING_TABLE') 			truncate table SC3_category_working_table
		IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3_CATEGORY_SUBTOTALS') 				truncate table SC3_category_subtotals
		IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3_NON_CONVERGENCES') 					truncate table SC3_non_convergences
		IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'V289_M13_INDIVIDUAL_VIEWING') 			truncate table V289_M13_individual_viewing
		IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'V289_M13_INDIVIDUAL_DETAILS') 			truncate table V289_M13_individual_details
		IF EXISTS	(
						SELECT	tname 
						FROM 	syscatalog 
						WHERE 	creator = user_name() 
						and 	tabletype = 'TABLE' 
						and 	lower(tname) = 'v289_m16_dq_mct_checks'
					)
		begin
		
			update	v289_m16_dq_mct_checks
			set		processing_date = null
					,actual_value 	= 0
					,test_result	= 'Pending'
					
			commit
			
		end
		
		IF EXISTS	(
						SELECT	tname 
						FROM 	syscatalog 
						WHERE 	creator = user_name() 
						and 	tabletype = 'TABLE' 
						and 	lower(tname) = 'v289_m16_dq_fact_checks'
					)
		begin
		
			truncate table v289_m16_dq_fact_checks
			commit
			
		end
		
		
		
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