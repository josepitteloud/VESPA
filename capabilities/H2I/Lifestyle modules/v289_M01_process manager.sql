CREATE PROCEDURE pitteloudj.v289_m01_process_manager_sv (
	@fresh_start BIT = 0
	,@proc_date DATE = NULL
	,@sample_prop SMALLINT = 100
	)
AS
BEGIN
	message convert(TIMESTAMP, now()) || ' | Begining  M01.0 - Initialising Environment' TO client

	DECLARE @thetask VARCHAR(100)
	DECLARE @sql_ VARCHAR(2000)
	DECLARE @exe_status INTEGER
	DECLARE @log_id BIGINT
	DECLARE @gtg_flag BIT
	DECLARE @Module_id VARCHAR(3)
	DECLARE @thursday DATE
	DECLARE @good_to_go BIT

	SELECT @thursday = DATEFORMAT (
			(@proc_date - datepart(weekday, @proc_date)) - 2
			,'YYYY-MM-DD'
			)

	SET @Module_id = 'M01'
	SET @exe_status = - 1

	IF @fresh_start = 1
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M01.0: Fresh Start, Dropping tables' TO client

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = user_name()
					AND tabletype = 'TABLE'
					AND upper(tname) = 'V289_M12_Skyview_weighted_duration_sv'
				)
			DROP TABLE V289_M12_Skyview_weighted_duration_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = user_name()
					AND tabletype = 'TABLE'
					AND upper(tname) = 'v289_M06_dp_raw_data_sv'
				)
			DROP TABLE v289_M06_dp_raw_data_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = user_name()
					AND tabletype = 'TABLE'
					AND upper(tname) = 'V289_M07_dp_data_sv'
				)
			DROP TABLE V289_M07_dp_data_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = user_name()
					AND tabletype = 'TABLE'
					AND upper(tname) = 'v289_M17_vod_raw_data_sv'
				)
			DROP TABLE v289_M17_vod_raw_data_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = user_name()
					AND tabletype = 'TABLE'
					AND upper(tname) = 'SC3I_Todays_panel_members_sv'
				)
			DROP TABLE SC3I_Todays_panel_members_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = user_name()
					AND tabletype = 'TABLE'
					AND upper(tname) = 'SC3I_weighting_working_table_sv'
				)
			DROP TABLE SC3I_weighting_working_table_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = user_name()
					AND tabletype = 'TABLE'
					AND upper(tname) = 'SC3I_category_working_table_sv'
				)
			DROP TABLE SC3I_category_working_table_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = user_name()
					AND tabletype = 'TABLE'
					AND upper(tname) = 'V289_M11_04_Barb_weighted_population_sv'
				)
			DROP TABLE V289_M11_04_Barb_weighted_population_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = user_name()
					AND tabletype = 'TABLE'
					AND upper(tname) = 'SC3_Weightings_sv'
				)
			DROP TABLE SC3_Weightings_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = user_name()
					AND tabletype = 'TABLE'
					AND upper(tname) = 'SC3_Intervals_sv'
				)
			DROP TABLE SC3_Intervals_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = user_name()
					AND tabletype = 'TABLE'
					AND upper(tname) = 'VESPA_HOUSEHOLD_WEIGHTING_sv'
				)
			DROP TABLE VESPA_HOUSEHOLD_WEIGHTING_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = user_name()
					AND tabletype = 'TABLE'
					AND upper(tname) = 'SC3_Sky_base_segment_snapshots_sv'
				)
			DROP TABLE SC3_Sky_base_segment_snapshots_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = user_name()
					AND tabletype = 'TABLE'
					AND upper(tname) = 'SC3_Todays_panel_members_sv'
				)
			DROP TABLE SC3_Todays_panel_members_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = user_name()
					AND tabletype = 'TABLE'
					AND upper(tname) = 'SC3_Todays_segment_weights_sv'
				)
			DROP TABLE SC3_Todays_segment_weights_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = user_name()
					AND tabletype = 'TABLE'
					AND upper(tname) = 'SC3_scaling_weekly_sample_sv'
				)
			DROP TABLE SC3_scaling_weekly_sample_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = user_name()
					AND tabletype = 'TABLE'
					AND upper(tname) = 'SC3_weighting_working_table_sv'
				)
			DROP TABLE SC3_weighting_working_table_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = user_name()
					AND tabletype = 'TABLE'
					AND upper(tname) = 'SC3_category_working_table_sv'
				)
			DROP TABLE SC3_category_working_table_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = user_name()
					AND tabletype = 'TABLE'
					AND upper(tname) = 'SC3_category_subtotals_sv'
				)
			DROP TABLE SC3_category_subtotals_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = user_name()
					AND tabletype = 'TABLE'
					AND upper(tname) = 'SC3_metrics_sv'
				)
			DROP TABLE SC3_metrics_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = user_name()
					AND tabletype = 'TABLE'
					AND upper(tname) = 'SC3_non_convergences_sv'
				)
			DROP TABLE SC3_non_convergences_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = user_name()
					AND tabletype = 'TABLE'
					AND upper(tname) = 'V289_PIV_Grouped_Segments_desc_sv'
				)
			DROP TABLE V289_PIV_Grouped_Segments_desc_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = user_name()
					AND tabletype = 'TABLE'
					AND upper(tname) = 'SC3I_Variables_lookup_v1_1_sv'
				)
			DROP TABLE SC3I_Variables_lookup_v1_1_sv

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M01.0: Fresh Start, Dropping tables DONE' TO client
	END

	EXECUTE @exe_status = v289_m00_initialisation_sv @proc_date

	IF @exe_status = 0
	BEGIN
		SET @good_to_go = 1

		COMMIT WORK

		IF @good_to_go = 1
		BEGIN
			SET @exe_status = - 1

			EXECUTE @exe_status = v289_m02_housekeeping_sv @fresh_start
				,@log_id

			IF @exe_status = 0
			BEGIN
				message convert(TIMESTAMP, now()) || ' | @ M01.0: Initialising Environment DONE' TO client message convert(TIMESTAMP, now()) || ' | Begining  M01.1 - Identifying Pending TasksHousekeeping' TO client

				WHILE EXISTS (
						SELECT first STATUS
						FROM v289_m01_t_process_manager_sv
						WHERE STATUS = 0
						)
				BEGIN
					message convert(TIMESTAMP, now()) || ' | @ M01.1: Pending Tasks Found' TO client

					SELECT @thetask = task
					FROM v289_m01_t_process_manager_sv
					WHERE sequencer = (
							SELECT min(sequencer)
							FROM v289_m01_t_process_manager_sv
							WHERE STATUS = 0
							) message convert(TIMESTAMP, now()) || ' | @ M01.1: Task ' || @thetask || ' Pending' TO client message convert(TIMESTAMP, now()) || ' | @ M01.1: Identifying Pending TasksHousekeeping DONE' TO client message convert(TIMESTAMP, now()) || ' | Begining  M01.2 - Tasks Execution' TO client message convert(TIMESTAMP, now()) || ' | @ M01.2: Executing ->' || @thetask TO client

					SET @exe_status = - 1
					SET @sql_ = 'execute @exe_status = ' || CASE 
							WHEN @thetask = 'v289_m04_barb_data_preparation_sv'
								THEN @thetask || ' ''' || @proc_date || ''''
							WHEN @thetask = 'v289_m06_DP_data_extraction_sv'
								THEN @thetask || ' ''' || @proc_date || ''',' || @sample_prop
							WHEN @thetask = 'v289_M19_Non_Viewing_Households'
								THEN @thetask || ' ''' || @proc_date || ''''
							WHEN @thetask = 'V289_M11_01_SC3_v1_1__do_weekly_segmentation_sv'
								THEN @thetask || ' ''' || @thursday || ''',' || @log_ID || ',''' || today() || ''''
							WHEN @thetask = 'V289_M11_02_SC3_v1_1__prepare_panel_members_sv'
								THEN @thetask || ' ''' || @thursday || ''',''' || @proc_date || ''',''' || today() || ''',' || @log_ID
							WHEN @thetask = 'V289_M11_03_SC3I_v1_1__add_individual_data_sv'
								THEN @thetask || ' ''' || @thursday || ''',''' || today() || ''',' || @log_ID
							WHEN @thetask = 'V289_M11_04_SC3I_v1_1__make_weights_sv_BARB_sv'
								THEN @thetask || ' ''' || @thursday || ''',''' || @proc_date || ''',''' || today() || ''',' || @log_ID
							WHEN @thetask = 'v289_m15_non_viewers_assignment'
								THEN @thetask || ' ''' || @proc_date || ''' '
							WHEN @thetask = 'v289_m12_validation_sv'
								THEN @thetask || ' ''' || @proc_date || ''' '
							WHEN @thetask = 'v289_m16_data_quality_checks_post'
								THEN @thetask || ' ''' || @proc_date || ''',' || @sample_prop
							WHEN @thetask = 'v289_m17_PullVOD_data_extraction_sv'
								THEN @thetask || ' ''' || @proc_date || ''' '
							WHEN @thetask = 'v289_M13_Create_Final_TechEdge_Output_Tables_sv'
								THEN @thetask || ' ''' || @proc_date || ''' '
							ELSE @thetask
							END message convert(TIMESTAMP, now()) || ' | @ M01.2 - SQL :' || @sql_ TO client

					EXECUTE (@sql_)

					IF @exe_status = 0
					BEGIN
						UPDATE v289_m01_t_process_manager_sv
						SET STATUS = 1
							,audit_date = today()
						WHERE task = @thetask
							AND STATUS = 0 message convert(TIMESTAMP, now()) || ' | @ M01.2: ' || @thetask || ' DONE' TO client

						COMMIT WORK
					END
					ELSE
					BEGIN
						message convert(TIMESTAMP, now()) || ' | @ M01.2: ' || @thetask || ' FAILED(' || @exe_status || ')' TO client

						BREAK
					END message convert(TIMESTAMP, now()) || ' | @ M01.2: Tasks Execution DONE' TO client
				END
			END
			ELSE
			BEGIN
				message convert(TIMESTAMP, now()) || ' | @ M01.3: Initialisation (M00) failure!!!' TO client
			END
		END
		ELSE
		BEGIN
			message convert(TIMESTAMP, now()) || ' | @ M01.3: Data Quality Checks FAILURES (M16) failure!!!' TO client
		END
	END
	ELSE
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M01.3: Initialisation (M00) failure!!!' TO client
	END message convert(TIMESTAMP, now()) || ' | Begining  M01.3 - Returning results' TO client message convert(TIMESTAMP, now()) || ' | @ M01.3: Returning results DONE' TO client message convert(TIMESTAMP, now()) || ' | M01 Finished' TO client

	COMMIT WORK
END
