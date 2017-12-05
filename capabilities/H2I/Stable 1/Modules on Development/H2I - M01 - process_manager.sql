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
**Project Name:                                                 Skyview H2I
**Analysts:                             Angel Donnarumma        (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jason Thompson          (Jason.Thompson@skyiq.co.uk)
                                                                                ,Hoi Yu Tang            (HoiYu.Tang@skyiq.co.uk)
                                                                                ,Jose Pitteloud         (jose.pitteloud@skyiq.co.uk)
**Stakeholder:                          SkyIQ
                                                                                ,Jose Loureda           (Jose.Loureda@skyiq.co.uk)
**Due Date:                             11/07/2014
**Project Code (Insight Collation):     v289
**Sharepoint Folder:

        http://sp-department.bskyb.com/sites/SIGEvolved/Shared%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FSIGEvolved%2FShared%20Documents%2F01%20Analysis%20Requests%2FV289%20-%20Skyview%20Futures%2F01%20Plans%20Briefs%20and%20Project%20Admin

**Business Brief:

        This module is in charge of managing the execution of all other modules in the right sequence to
        automate the production of the H2I view, and to act as a centralized point of execution (rather
        than executing all modules manually one after another)...

**Module:

        M01: Process Manager
                        M01.0 - Initialising Environment
                        M01.1 - Identifying Pending TasksHousekeeping
                        M01.2 - Tasks Execution
                        M01.3 - Returning results

--------------------------------------------------------------------------------------------------------------
*/


-----------------------------------
-- M01.0 - Initialising Environment
-----------------------------------

create or replace procedure v289_m01_process_manager
    @fresh_start    bit         = 0
    ,@proc_date     date        = null
    ,@sample_prop   smallint    = 100
as begin

        MESSAGE cast(now() as timestamp)||' | Begining  M01.0 - Initialising Environment' TO CLIENT

        -- Variables
        declare @thetask    varchar(100)
        declare @sql_       varchar(2000)
        declare @exe_status     integer
        declare @log_id         bigint
        declare @gtg_flag       bit
        declare @Module_id      varchar(3)
        declare @thursday       date
		declare	@good_to_go		bit

        -- we currently need this Thursday for when processing scaling...
        select  @thursday = dateformat((@proc_date - datepart(weekday,@proc_date))-2, 'YYYY-MM-DD')

        set @Module_id = 'M01'
        set @exe_status = -1

        if @fresh_start = 1
        begin

                MESSAGE cast(now() as timestamp)||' | @ M01.0: Fresh Start, Dropping tables' TO CLIENT

				IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'V289_M12_SKYVIEW_WEIGHTED_DURATION')		drop table V289_M12_Skyview_weighted_duration
                IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'V289_M06_DP_RAW_DATA')                   drop table v289_M06_dp_raw_data
                IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'V289_M07_DP_DATA') 						drop table V289_M07_dp_data
                IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'V289_M17_VOD_RAW_DATA') 					drop table V289_M17_vod_raw_data
                IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3I_TODAYS_PANEL_MEMBERS') 				drop table SC3I_Todays_panel_members
                IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3I_WEIGHTING_WORKING_TABLE') 			DROP table SC3I_weighting_working_table
                IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3I_CATEGORY_WORKING_TABLE')            drop table SC3I_category_working_table
                IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'V289_M11_04_BARB_WEIGHTED_POPULATION') 	drop table V289_M11_04_Barb_weighted_population
                IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3_WEIGHTINGS')                         drop table SC3_Weightings
                IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3_INTERVALS') 							drop table SC3_Intervals
                IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'VESPA_HOUSEHOLD_WEIGHTING') 				drop table VESPA_HOUSEHOLD_WEIGHTING
                IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3_SKY_BASE_SEGMENT_SNAPSHOTS') 		drop table SC3_Sky_base_segment_snapshots
                IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3_TODAYS_PANEL_MEMBERS') 				drop table SC3_Todays_panel_members
                IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3_TODAYS_SEGMENT_WEIGHTS') 			drop table SC3_Todays_segment_weights
                IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3_SCALING_WEEKLY_SAMPLE') 				drop table SC3_scaling_weekly_sample
                IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3_WEIGHTING_WORKING_TABLE') 			drop table SC3_weighting_working_table
                IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3_CATEGORY_WORKING_TABLE') 			drop table SC3_category_working_table
                IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3_CATEGORY_SUBTOTALS') 				drop table SC3_category_subtotals
                IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3_METRICS') 							drop table SC3_metrics
                IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3_NON_CONVERGENCES') 					drop table SC3_non_convergences
                IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'V289_PIV_GROUPED_SEGMENTS_DESC') 		drop table V289_PIV_Grouped_Segments_desc
                IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'SC3I_VARIABLES_LOOKUP_V1_1') 			drop table SC3I_Variables_lookup_v1_1
                commit

				
                MESSAGE cast(now() as timestamp)||' | @ M01.0: Fresh Start, Dropping tables DONE' TO CLIENT

        end

        execute @exe_status = v289_m00_initialisation @proc_date

        if      @exe_status = 0
        begin

			-- EXECUTE v289_m16_data_quality_checks @proc_date, @good_to_go output
			set @good_to_go = 1 commit
			
			if @good_to_go = 1
			begin
                set @exe_status = -1

                execute @exe_status = v289_m02_housekeeping @fresh_start, @log_id output

                --execute citeam.logger_add_event @log_ID, 1, @Module_id || ' : EXE M02', @exe_status

                if      @exe_status = 0
                begin

                        MESSAGE cast(now() as timestamp)||' | @ M01.0: Initialising Environment DONE' TO CLIENT

                ------------------------------------------------
                -- M01.1 - Identifying Pending TasksHousekeeping
                ------------------------------------------------

                        MESSAGE cast(now() as timestamp)||' | Begining  M01.1 - Identifying Pending TasksHousekeeping' TO CLIENT

                        while exists    (
                                                                select  first status
                                                                from    v289_m01_t_process_manager
                                                                where   status = 0                      --> Any tasks Pending?...
                                                        )
                        begin

                                MESSAGE cast(now() as timestamp)||' | @ M01.1: Pending Tasks Found' TO CLIENT

                                -- What task to execute?...
                                select  @thetask = task
                                from    v289_m01_t_process_manager
                                where   sequencer = (
                                                                                select  min(sequencer)
                                                                                from    v289_m01_t_process_manager
                                                                                where   status = 0
                                                                        )

                                MESSAGE cast(now() as timestamp)||' | @ M01.1: Task '||@thetask||' Pending' TO CLIENT


                                MESSAGE cast(now() as timestamp)||' | @ M01.1: Identifying Pending TasksHousekeeping DONE' TO CLIENT

                --------------------------
                -- M01.2 - Tasks Execution
                --------------------------

                                MESSAGE cast(now() as timestamp)||' | Begining  M01.2 - Tasks Execution' TO CLIENT

                                MESSAGE cast(now() as timestamp)||' | @ M01.2: Executing ->'||@thetask TO CLIENT

                                set @exe_status = -1

                                set @sql_ = 'execute @exe_status = '||  case    when @thetask = 'v289_m04_barb_data_preparation'				then @thetask||' '''||@proc_date||''''
																				when @thetask = 'v289_m06_DP_data_extraction'                   then @thetask||' '''||@proc_date||''','||@sample_prop
																				when @thetask = 'V289_M11_01_SC3_v1_1__do_weekly_segmentation'  then @thetask||' '''||@thursday||''','||@log_ID||','''||today()||''''
																				when @thetask = 'V289_M11_02_SC3_v1_1__prepare_panel_members'   then @thetask||' '''||@thursday||''','''||@proc_date||''','''||today()||''','||@log_ID
																				when @thetask = 'V289_M11_03_SC3I_v1_1__add_individual_data'    then @thetask||' '''||@thursday||''','''||today()||''','||@log_ID
																				when @thetask = 'V289_M11_04_SC3I_v1_1__make_weights_BARB'      then @thetask||' '''||@thursday||''','''||@proc_date||''','''||today()||''','||@log_ID
																				when @thetask = 'v289_m18_panel_matching'		 				then @thetask||' '''||@proc_date||''' '
																				when @thetask = 'v289_m15_non_viewers_assignment' 				then @thetask||' '''||@proc_date||''' '
																				WHEN @thetask = 'v289_m12_validation'      						then @thetask||' '''||@proc_date||''' '
																				when @thetask = 'v289_m16_data_quality_checks_post'				then @thetask||' '''||@proc_date||''','||@sample_prop
                                                                                when @thetask = 'v289_m17_PullVOD_data_extraction'				then @thetask||' '''||@proc_date||''' '
																				else @thetask
                                                                        end
                                MESSAGE cast(now() as timestamp)||' | @ M01.2 - SQL :'||@sql_ TO CLIENT

                                execute (@sql_)

                                if @exe_status = 0
                                begin
                                        update  v289_m01_t_process_manager
                                        set             status          = 1
                                                        ,audit_date     = today()
                                        where   task = @thetask
                                        and             status = 0

                                        MESSAGE cast(now() as timestamp)||' | @ M01.2: '||@thetask||' DONE' TO CLIENT
                                        --execute citeam.logger_add_event @log_ID, 1, @Module_id || ' : EXE '||@thetask||' DONE', @exe_status

                                        commit
                                end
                                else
                                begin
                                        MESSAGE cast(now() as timestamp)||' | @ M01.2: '||@thetask||' FAILED('||@exe_status||')' TO CLIENT
                                        --execute citeam.logger_add_event @log_ID, 1, @Module_id || ' : EXE '||@thetask||' FAILED', @exe_status
                                        break
                                end

                                MESSAGE cast(now() as timestamp)||' | @ M01.2: Tasks Execution DONE' TO CLIENT

                        end
                end
                else
                begin

				MESSAGE cast(now() as timestamp)||' | @ M01.3: Initialisation (M00) failure!!!' TO CLIENT

                end
			end
			else
			begin
				MESSAGE cast(now() as timestamp)||' | @ M01.3: Data Quality Checks FAILURES (M16) failure!!!' TO CLIENT
			end

                
        end
        else
        begin

                MESSAGE cast(now() as timestamp)||' | @ M01.3: Initialisation (M00) failure!!!' TO CLIENT

        end
----------------------------
-- M01.3 - Returning results
----------------------------

        MESSAGE cast(now() as timestamp)||' | Begining  M01.3 - Returning results' TO CLIENT
        MESSAGE cast(now() as timestamp)||' | @ M01.3: Returning results DONE' TO CLIENT

        MESSAGE cast(now() as timestamp)||' | M01 Finished' TO CLIENT
        commit

end;

commit;
grant execute on v289_m01_process_manager to vespa_group_low_security;
commit;
