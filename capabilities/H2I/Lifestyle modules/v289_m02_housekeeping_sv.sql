create or replace procedure pitteloudj.v289_m02_housekeeping_sv (
	@fresh_start BIT = 0
	,@log_id BIGINT OUTPUT
	)
AS
BEGIN
	message convert(TIMESTAMP, now()) || ' | Begining  M02.0 - Initialising Environment' TO client

	DECLARE @tasks_done SMALLINT
	DECLARE @total_tasks SMALLINT
	DECLARE @logbatch_id VARCHAR(20)
	DECLARE @logrefres_id VARCHAR(40) message convert (
		TIMESTAMP
		,now()
		) || ' | @ M02.0: Initialising Environment DONE' TO client message convert (
		TIMESTAMP
		,now()
		) || ' | Begining  M02.1 - Checking for Fresh Start flag' TO client

	IF @fresh_start = 1
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M02.1: Fresh Start requested: Resting process table' TO client

		UPDATE v289_m01_t_process_manager_sv
		SET STATUS = 0

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M02.1: Checking for Fresh Start flag DONE' TO client
	END
	ELSE
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M02.1: No Fresh Start requested' TO client message convert(TIMESTAMP, now()) || ' | Begining  M02.2 - Maintaining Base tables' TO client

		SELECT @tasks_done = count(1)
		FROM v289_m01_t_process_manager_sv
		WHERE STATUS > 0

		SELECT @total_tasks = count(1)
		FROM v289_m01_t_process_manager_sv

		IF @tasks_done = @total_tasks
		BEGIN
			message convert(TIMESTAMP, now()) || ' | @ M02.2: Reseting Status in Process Table' TO client

			UPDATE v289_m01_t_process_manager_sv
			SET STATUS = 0

			COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M02.2: Reseting Status in Process Table DONE' TO client
		END
	END

	IF (
			@fresh_start = 1
			OR @tasks_done = @total_tasks
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M02.2: Cleaning Base tables' TO client

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = 'pitteloudj'
					AND tabletype = 'TABLE'
					AND upper(tname) = 'V289_M12_Skyview_weighted_duration_sv'
				)
			TRUNCATE TABLE V289_M12_Skyview_weighted_duration_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = 'pitteloudj'
					AND tabletype = 'TABLE'
					AND upper(tname) = 'v289_M06_dp_raw_data_sv'
				)
			TRUNCATE TABLE v289_M06_dp_raw_data_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = 'pitteloudj'
					AND tabletype = 'TABLE'
					AND upper(tname) = 'V289_M07_dp_data_sv'
				)
			TRUNCATE TABLE V289_M07_dp_data_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = 'pitteloudj'
					AND tabletype = 'TABLE'
					AND upper(tname) = 'SC3I_Todays_panel_members_sv'
				)
			TRUNCATE TABLE SC3I_Todays_panel_members_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = 'pitteloudj'
					AND tabletype = 'TABLE'
					AND upper(tname) = 'SC3I_weighting_working_table_sv'
				)
			TRUNCATE TABLE SC3I_weighting_working_table_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = 'pitteloudj'
					AND tabletype = 'TABLE'
					AND upper(tname) = 'SC3I_category_working_table_sv'
				)
			TRUNCATE TABLE SC3I_category_working_table_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = 'pitteloudj'
					AND tabletype = 'TABLE'
					AND upper(tname) = 'V289_M11_04_Barb_weighted_population_sv'
				)
			TRUNCATE TABLE V289_M11_04_Barb_weighted_population_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = 'pitteloudj'
					AND tabletype = 'TABLE'
					AND upper(tname) = 'SC3_Weightings_sv'
				)
			TRUNCATE TABLE SC3_Weightings_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = 'pitteloudj'
					AND tabletype = 'TABLE'
					AND upper(tname) = 'SC3_Intervals_sv'
				)
			TRUNCATE TABLE SC3_Intervals_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = 'pitteloudj'
					AND tabletype = 'TABLE'
					AND upper(tname) = 'VESPA_HOUSEHOLD_WEIGHTING_sv'
				)
			TRUNCATE TABLE VESPA_HOUSEHOLD_WEIGHTING_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = 'pitteloudj'
					AND tabletype = 'TABLE'
					AND upper(tname) = 'SC3_Sky_base_segment_snapshots_sv'
				)
			TRUNCATE TABLE SC3_Sky_base_segment_snapshots_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = 'pitteloudj'
					AND tabletype = 'TABLE'
					AND upper(tname) = 'SC3_Todays_panel_members_sv'
				)
			TRUNCATE TABLE SC3_Todays_panel_members_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = 'pitteloudj'
					AND tabletype = 'TABLE'
					AND upper(tname) = 'SC3_Todays_segment_weights_sv'
				)
			TRUNCATE TABLE SC3_Todays_segment_weights_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = 'pitteloudj'
					AND tabletype = 'TABLE'
					AND upper(tname) = 'SC3_scaling_weekly_sample_sv'
				)
			TRUNCATE TABLE SC3_scaling_weekly_sample_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = 'pitteloudj'
					AND tabletype = 'TABLE'
					AND upper(tname) = 'SC3_weighting_working_table_sv'
				)
			TRUNCATE TABLE SC3_weighting_working_table_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = 'pitteloudj'
					AND tabletype = 'TABLE'
					AND upper(tname) = 'SC3_category_working_table_sv'
				)
			TRUNCATE TABLE SC3_category_working_table_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = 'pitteloudj'
					AND tabletype = 'TABLE'
					AND upper(tname) = 'SC3_category_subtotals_sv'
				)
			TRUNCATE TABLE SC3_category_subtotals_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = 'pitteloudj'
					AND tabletype = 'TABLE'
					AND upper(tname) = 'SC3_non_convergences_sv'
				)
			TRUNCATE TABLE SC3_non_convergences_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = 'pitteloudj'
					AND tabletype = 'TABLE'
					AND upper(tname) = 'V289_M13_individual_viewing_sv'
				)
			TRUNCATE TABLE V289_M13_individual_viewing_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = 'pitteloudj'
					AND tabletype = 'TABLE'
					AND upper(tname) = 'V289_M13_individual_details_sv'
				)
			TRUNCATE TABLE V289_M13_individual_details_sv

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = 'pitteloudj'
					AND tabletype = 'TABLE'
					AND lower(tname) = 'v289_m16_dq_mct_checks'
				)
		BEGIN
			UPDATE v289_m16_dq_mct_checks
			SET processing_date = NULL
				,actual_value = 0
				,test_result = 'Pending'

			COMMIT WORK
		END

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = 'pitteloudj'
					AND tabletype = 'TABLE'
					AND lower(tname) = 'v289_m16_dq_fact_checks'
				)
		BEGIN
			TRUNCATE TABLE pitteloudjv289_m16_dq_fact_checks

			COMMIT WORK
		END

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M02.2: Cleaning Base tables DONE' TO client
	END message convert(TIMESTAMP, now()) || ' | @ M02.2: Maintaining Base tables DONE' TO client message convert(TIMESTAMP, now()) || ' | Begining  M02.3 - Initialising the logger' TO client

	IF lower(CURRENT user) = 'vespa_analysts'
		SET @logbatch_id = 'H2I'
	ELSE
		SET @logbatch_id = 'H2I test ' || upper(right(CURRENT user, 1)) || upper(left(CURRENT user, 2))

	SET @logrefres_id = convert(VARCHAR(10), today(), 123) || ' H2I refresh'

	EXECUTE citeam.logger_create_run @logbatch_id
		,@logrefres_id
		,@log_ID message convert(TIMESTAMP, now()) || ' | @ M02.3: Initialising the logger DONE' TO client message convert(TIMESTAMP, now()) || ' | Begining  M02.4 - Returning Results' TO client message convert(TIMESTAMP, now()) || ' | @ M02.4: Returning Results DONE' TO client message convert(TIMESTAMP, now()) || ' | M02 Finished' TO client
END;
GO 
commit;
