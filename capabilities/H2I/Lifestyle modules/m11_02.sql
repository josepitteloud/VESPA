CREATE OR REPLACE PROCEDURE V289_M11_02_SC3_v1_1__prepare_panel_members_sv (
	@profiling_date DATE
	,@scaling_day DATE
	,@batch_date DATETIME = now()
	,@Scale_refresh_logging_ID BIGINT = NULL
	)
AS
BEGIN
	TRUNCATE TABLE jsk01.SC3_Todays_panel_members_sv

	COMMIT WORK

	DECLARE @QA_catcher INTEGER

	COMMIT WORK

	DECLARE @dp_tname VARCHAR(50)
	DECLARE @query VARCHAR(5000)
	DECLARE @from_dt INTEGER
	DECLARE @to_dt INTEGER

	COMMIT WORK

	SET @dp_tname = 'SK_PROD.VESPA_DP_PROG_VIEWED_'

	COMMIT WORK

	SELECT @from_dt = convert(INTEGER, (
				DATEFORMAT (
					dateadd(hour, 6, @scaling_day)
					,'YYYYMMDD'
					) || '00'
				))

	COMMIT WORK

	SELECT @to_dt = convert(INTEGER, (
				DATEFORMAT (
					dateadd(hour, 30, @scaling_day)
					,'YYYYMMDD'
					) || '23'
				))

	COMMIT WORK

	SET @dp_tname = @dp_tname || datepart(year, @scaling_day) || right(('00' || convert(VARCHAR(2), datepart(month, @scaling_day))), 2) message convert(TIMESTAMP, now()) || ' | M11.2 - dp_tname:' || @dp_tname TO client message convert(TIMESTAMP, now()) || ' | M11.2 - to_dt:' || @to_dt TO client message convert(TIMESTAMP, now()) || ' | M11.2 - from_dt:' || @from_dt TO client

	CREATE TABLE #raw_logs_dump_temp (
		account_number VARCHAR(20) NOT NULL
		,service_instance_id VARCHAR(30) NOT NULL
		,
		)

	COMMIT WORK

	SET @query = 'insert into #raw_logs_dump_temp ' || 'SELECT         distinct ' || 'account_number ' || ',service_instance_id ' || 'FROM ' || @dp_tname || ' where dk_event_start_datehour_dim between ' || @from_dt || ' and ' || @to_dt || ' and           (panel_id = 12 or panel_id = 11) ' || 'and    account_number is not null ' || 'and    service_instance_id is not null '

	COMMIT WORK

	EXECUTE (@query) message convert(TIMESTAMP, now()) || ' | M11.2 - raw_logs_dump_temp:' || @@rowcount TO client

	COMMIT WORK

	CREATE hg INDEX idx1 ON #raw_logs_dump_temp (account_number)

	COMMIT WORK

	CREATE hg INDEX idx2 ON #raw_logs_dump_temp (service_instance_id)

	COMMIT WORK

	CREATE TABLE #raw_logs_dump (
		account_number VARCHAR(20) NOT NULL
		,service_instance_id VARCHAR(30) NOT NULL
		,
		)

	COMMIT WORK

	INSERT INTO #raw_logs_dump
	SELECT DISTINCT account_number
		,service_instance_id
	FROM #raw_logs_dump_temp

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M11.2 - raw_logs_dump:' || @@rowcount TO client

	CREATE INDEX some_key ON #raw_logs_dump (account_number)

	COMMIT WORK

	SELECT account_number
		,'box_count' = count(DISTINCT service_instance_id)
		,'expected_boxes' = convert(TINYINT, NULL)
		,'scaling_segment_id' = convert(INTEGER, NULL)
	INTO #panel_options
	FROM #raw_logs_dump
	GROUP BY account_number

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M11.2 - panel_options:' || @@rowcount TO client

	CREATE UNIQUE INDEX fake_pk ON #panel_options (account_number)

	COMMIT WORK

	DROP TABLE #raw_logs_dump

	COMMIT WORK

	UPDATE #panel_options
	SET expected_boxes = sbss.expected_boxes
		,scaling_segment_id = sbss.vespa_scaling_segment_id
	FROM #panel_options
	JOIN SC3_Sky_base_segment_snapshots_sv AS sbss ON #panel_options.account_number = sbss.account_number
	WHERE sbss.profiling_date = @profiling_date

	COMMIT WORK

	TRUNCATE TABLE jsk01.SC3_Todays_panel_members_sv

	COMMIT WORK

	INSERT INTO SC3_Todays_panel_members_sv (
		account_number
		,scaling_segment_id
		)
	SELECT account_number
		,scaling_segment_id
	FROM #panel_options
	WHERE expected_boxes >= box_count
		AND scaling_segment_id IS NOT NULL 
		message convert(TIMESTAMP, now()) || ' | M11.2 - SC3_Todays_panel_members_sv:' || @@rowcount TO client

	COMMIT WORK

	DROP TABLE #panel_options

	COMMIT WORK
END
