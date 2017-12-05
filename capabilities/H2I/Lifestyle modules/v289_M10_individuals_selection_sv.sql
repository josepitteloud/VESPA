create or replace procedure ${SQLFILE_ARG001}.v289_M10_individuals_selection_sv
AS
BEGIN
	message convert(TIMESTAMP, now()) || ' | M10 - Individuals assignment module start' TO client

	COMMIT WORK

	IF EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND upper(tname) = upper('V289_M10_log_sv')
				AND tabletype = 'TABLE'
			)
		DROP TABLE V289_M10_log_sv

	COMMIT WORK

	CREATE TABLE ${SQLFILE_ARG001}.V289_M10_log_sv (
		section_id VARCHAR(255) NULL DEFAULT NULL
		,dt_completed DATETIME NULL DEFAULT NULL
		,completed BIT NOT NULL DEFAULT 0
		,
		)

	COMMIT WORK

	GRANT SELECT
		ON V289_M10_log_sv
		TO vespa_group_low_security

	COMMIT WORK

	INSERT INTO V289_M10_log_sv (section_id)
	SELECT section_id
	FROM (
		SELECT 'section_id' = 'JOB START'
			,'num' = - 1
		
		UNION
		
		SELECT 'section_id' = 'S0 - INITIALISE TABLES'
			,'num' = 0
		
		UNION
		
		SELECT 'section_id' = 'S1 - INITIALISE VARIABLES'
			,'num' = 1
		
		UNION
		
		SELECT 'section_id' = 'S2 - Produce date-agnostic PIV and re-normalise'
			,'num' = 2
		
		UNION
		
		SELECT 'section_id' = 'S3 - PREPARE VIEWING DATA'
			,'num' = 3
		
		UNION
		
		SELECT 'section_id' = 'S4 - Assign audience for single-occupancy households and whole-household audiences'
			,'num' = 4
		
		UNION
		
		SELECT 'section_id' = 'S5 - Assign audience for non-overlapping events'
			,'num' = 5
		
		UNION
		
		SELECT 'section_id' = 'S6 - Assign audience for overlapping events from the same account'
			,'num' = 6
		
		UNION
		
		SELECT 'section_id' = 'SXX - FINISH'
			,'num' = 9999
		) AS t
	ORDER BY num ASC

	COMMIT WORK

	UPDATE V289_M10_log_sv
	SET dt_completed = now()
		,completed = 1
	WHERE section_id = 'JOB START'

	COMMIT WORK

	IF (
			SELECT sum(CASE 
						WHEN n > 0
							THEN 1
						ELSE 0
						END)
			FROM (
				SELECT 'n' = count()
				FROM V289_M07_dp_data_sv
				
				UNION
				
				SELECT 'n' = count()
				FROM V289_M08_SKY_HH_composition_sv
				
				UNION
				
				SELECT 'n' = count()
				FROM V289_PIV_Grouped_Segments_desc_sv
				
				UNION
				
				SELECT 'n' = count()
				FROM v289_genderage_matrix_sv
				) AS t
			) < 4
	BEGIN
		INSERT INTO V289_M10_log_sv (
			section_id
			,dt_completed
			)
		SELECT 'At least one input table is empty! Please check data.'
			,now()

		COMMIT WORK
	END

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S0.0 - Initialise tables' TO client

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S0.2 - Initialise transient tables' TO client

	COMMIT WORK

	IF EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND upper(tname) = upper('V289_M10_combined_event_data_sv')
				AND tabletype = 'TABLE'
			)
		DROP TABLE V289_M10_combined_event_data_sv

	COMMIT WORK

	CREATE TABLE ${SQLFILE_ARG001}.V289_M10_combined_event_data_sv (
		account_number VARCHAR(20) NOT NULL
		,hh_person_number TINYINT NOT NULL
		,subscriber_id DECIMAL(10) NOT NULL
		,event_id BIGINT NOT NULL
		,event_start_utc DATETIME NOT NULL
		,chunk_start DATETIME NULL DEFAULT NULL
		,overlap_batch INTEGER NULL DEFAULT NULL
		,programme_genre VARCHAR(20) NULL DEFAULT NULL
		,session_daypart VARCHAR(11) NULL DEFAULT NULL
		,channel_pack VARCHAR(40) NULL DEFAULT NULL
		,segment_id INTEGER NULL DEFAULT NULL
		,numrow INTEGER NOT NULL
		,session_size TINYINT NULL DEFAULT NULL
		,person_gender VARCHAR(1) NULL DEFAULT NULL
		,person_ageband VARCHAR(10) NULL DEFAULT NULL
		,household_size TINYINT NULL DEFAULT NULL
		,viewer_hhsize TINYINT NULL DEFAULT NULL
		,assigned BIT NOT NULL DEFAULT 0
		,dt_assigned DATETIME NULL DEFAULT NULL
		,PIV DOUBLE NULL DEFAULT NULL
		,individuals_assigned INTEGER NOT NULL DEFAULT 0
		,
		)

	COMMIT WORK

	CREATE hg INDEX V289_M10_combined_event_data_sv_hg_idx_1 ON V289_M10_combined_event_data_sv (account_number)

	COMMIT WORK

	CREATE hg INDEX V289_M10_combined_event_data_sv_hg_idx_2 ON V289_M10_combined_event_data_sv (event_id)

	COMMIT WORK

	CREATE hg INDEX V289_M10_combined_event_data_sv_hg_idx_3 ON V289_M10_combined_event_data_sv (numrow)

	COMMIT WORK

	CREATE lf INDEX V289_M10_combined_event_data_sv_lf_idx_4 ON V289_M10_combined_event_data_sv (session_size)

	COMMIT WORK

	CREATE lf INDEX V289_M10_combined_event_data_sv_lf_idx_5 ON V289_M10_combined_event_data_sv (person_gender)

	COMMIT WORK

	CREATE lf INDEX V289_M10_combined_event_data_sv_lf_idx_6 ON V289_M10_combined_event_data_sv (person_ageband)

	COMMIT WORK

	CREATE lf INDEX V289_M10_combined_event_data_sv_lf_idx_7 ON V289_M10_combined_event_data_sv (household_size)

	COMMIT WORK

	CREATE lf INDEX V289_M10_combined_event_data_sv_lf_idx_8 ON V289_M10_combined_event_data_sv (viewer_hhsize)

	COMMIT WORK

	GRANT SELECT
		ON V289_M10_combined_event_data_sv
		TO vespa_group_low_security

	COMMIT WORK

	IF EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND upper(tname) = upper('V289_M10_PIV_default_sv')
				AND tabletype = 'TABLE'
			)
		DROP TABLE V289_M10_PIV_default_sv

	COMMIT WORK

	CREATE TABLE ${SQLFILE_ARG001}.V289_M10_PIV_default_sv (
		hhsize INTEGER NULL DEFAULT NULL
		,segment_id INTEGER NULL DEFAULT NULL
		,sex VARCHAR(10) NULL DEFAULT NULL
		,ageband VARCHAR(5) NULL DEFAULT NULL
		,sum_hours_watched INTEGER NULL DEFAULT NULL
		,sum_hours_over_all_demog INTEGER NULL DEFAULT NULL
		,PIV_default DOUBLE NULL DEFAULT NULL
		,
		)

	COMMIT WORK

	IF EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND upper(tname) = upper('V289_M10_PIV_by_date_sv')
				AND tabletype = 'TABLE'
			)
		DROP TABLE V289_M10_PIV_by_date_sv

	COMMIT WORK

	CREATE TABLE ${SQLFILE_ARG001}.V289_M10_PIV_by_date_sv (
		thedate DATE NULL DEFAULT NULL
		,hhsize INTEGER NULL DEFAULT NULL
		,segment_id INTEGER NULL DEFAULT NULL
		,sex VARCHAR(10) NULL DEFAULT NULL
		,ageband VARCHAR(5) NULL DEFAULT NULL
		,sum_hours_watched INTEGER NULL DEFAULT NULL
		,sum_hours_over_all_demog INTEGER NULL DEFAULT NULL
		,PIV_by_date DOUBLE NULL DEFAULT NULL
		,
		)

	COMMIT WORK

	CREATE TABLE #working_PIV (
		account_number VARCHAR(20) NOT NULL
		,subscriber_id DECIMAL(10) NULL DEFAULT NULL
		,event_id BIGINT NULL DEFAULT NULL
		,overlap_batch INTEGER NULL DEFAULT NULL
		,hh_person_number TINYINT NOT NULL
		,cumsum_PIV DOUBLE NULL DEFAULT NULL
		,norm_total DOUBLE NULL DEFAULT NULL
		,PIV_range DOUBLE NULL DEFAULT NULL
		,rnd DOUBLE NULL DEFAULT NULL
		,
		)

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S0.3 - Initialise output tables' TO client

	COMMIT WORK

	IF EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND upper(tname) = upper('V289_M10_session_individuals_sv')
				AND tabletype = 'TABLE'
			)
		DROP TABLE V289_M10_session_individuals_sv

	COMMIT WORK

	CREATE TABLE ${SQLFILE_ARG001}.V289_M10_session_individuals_sv (
		event_date DATE NULL DEFAULT NULL
		,event_id BIGINT NULL DEFAULT NULL
		,account_number VARCHAR(20) NULL DEFAULT NULL
		,overlap_batch INTEGER NULL DEFAULT NULL
		,chunk_start DATETIME NULL DEFAULT NULL
		,person_ageband VARCHAR(5) NULL DEFAULT NULL
		,person_gender VARCHAR(10) NULL DEFAULT NULL
		,hh_person_number TINYINT NULL DEFAULT NULL
		,last_modified_dt DATETIME NULL DEFAULT NULL
		,provider_id VARCHAR(20) NULL
		,provider_id_number INTEGER NULL
		,viewing_type_flag TINYINT NULL
		,
		)

	COMMIT WORK

	CREATE hg INDEX V289_M10_session_individuals_sv_hg_idx_1 ON V289_M10_session_individuals_sv (event_id)

	COMMIT WORK

	CREATE hg INDEX V289_M10_session_individuals_sv_hg_idx_2 ON V289_M10_session_individuals_sv (account_number)

	COMMIT WORK

	CREATE lf INDEX V289_M10_session_individuals_sv_lf_idx_3 ON V289_M10_session_individuals_sv (overlap_batch)

	COMMIT WORK

	CREATE dttm INDEX V289_M10_session_individuals_sv_dttm_idx_4 ON V289_M10_session_individuals_sv (chunk_start)

	COMMIT WORK

	CREATE DATE INDEX V289_M10_session_individuals_sv_dttm_idx_5 ON V289_M10_session_individuals_sv (event_date)

	COMMIT WORK

	CREATE dttm INDEX V289_M10_session_individuals_sv_dttm_idx_6 ON V289_M10_session_individuals_sv (last_modified_dt)

	COMMIT WORK

	GRANT SELECT
		ON V289_M10_session_individuals_sv
		TO vespa_group_low_security

	COMMIT WORK

	UPDATE V289_M10_log_sv
	SET dt_completed = now()
		,completed = 1
	WHERE section_id = 'S0 - INITIALISE TABLES'

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S1.0 - Initialise variables' TO client

	COMMIT WORK

	DECLARE @total_number_of_events INTEGER
	DECLARE @i INTEGER
	DECLARE @j TINYINT
	DECLARE @event_id BIGINT
	DECLARE @account_number VARCHAR(20)
	DECLARE @segment_id INTEGER
	DECLARE @session_size INTEGER
	DECLARE @household_size TINYINT
	DECLARE @overlap_batch TINYINT
	DECLARE @j_person_gender VARCHAR(6)
	DECLARE @j_person_ageband VARCHAR(5)
	DECLARE @j_hh_person_number TINYINT
	DECLARE @max_household_size TINYINT

	SET @max_household_size = 15

	DECLARE @max_chunk_session_size TINYINT

	UPDATE V289_M10_log_sv
	SET dt_completed = now()
		,completed = 1
	WHERE section_id = 'S1 - INITIALISE VARIABLES'

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S2.1 - Calculate default PIV' TO client

	COMMIT WORK

	SELECT 'hhsize' = row_num
	INTO #t15
	FROM sa_rowgenerator(1, 15)

	COMMIT WORK

	SELECT DISTINCT segment_id
	INTO #tseg
	FROM V289_PIV_Grouped_Segments_desc_sv

	COMMIT WORK

	SELECT DISTINCT sex
		,ageband
	INTO #tsex
	FROM v289_genderage_matrix_sv

	COMMIT WORK

	INSERT INTO V289_M10_PIV_default_sv (
		hhsize
		,segment_id
		,sex
		,ageband
		,sum_hours_watched
		,sum_hours_over_all_demog
		,PIV_default
		)
	SELECT hhsize
		,segment_id
		,sex
		,ageband
		,'sum_hours_watched' = 0
		,'sum_hours_over_all_demog' = 0
		,'PIV_default' = .001
	FROM #tseg AS b
	CROSS JOIN #tsex AS c
	CROSS JOIN #t15 AS d

	COMMIT WORK

	DELETE
	FROM V289_M10_PIV_default_sv
	WHERE sex NOT LIKE '%Undef%'
		AND ageband LIKE '0-19%'

	DELETE
	FROM V289_M10_PIV_default_sv
	WHERE sex LIKE 'Female%'
		AND ageband IN (
			'0-11'
			,'12-19'
			,'0-19'
			)

	COMMIT WORK

	UPDATE V289_M10_PIV_default_sv
	SET sex = 'Undefined'
	WHERE ageband IN (
			'0-11'
			,'12-19'
			,'0-19'
			)

	SELECT hhsize
		,segment_id
		,sex
		,ageband
		,sum_hours_watched
		,sum_hours_over_all_demog
		,PIV_default
	INTO #PIV
	FROM (
		SELECT hhsize
			,segment_id
			,'sex' = convert(VARCHAR(10), CASE 
					WHEN ageband IN (
							'0-19'
							,'12-19'
							,'0-11'
							)
						THEN 'Undefined'
					ELSE sex
					END)
			,ageband
			,uk_hhwatched
			,'uk_hhwatched_nonzero' = CASE 
				WHEN (
						uk_hhwatched = 0
						OR uk_hhwatched IS NULL
						)
					THEN .001
				ELSE uk_hhwatched
				END
			,'sum_hours_watched' = sum(uk_hhwatched_nonzero) OVER (
				PARTITION BY segment_id
				,hhsize
				,sex
				,ageband
				)
			,'sum_hours_over_all_demog' = sum(uk_hhwatched_nonzero) OVER (
				PARTITION BY segment_id
				,hhsize
				)
			,'PIV_default' = 1.0 * sum_hours_watched / sum_hours_over_all_demog
		FROM v289_genderage_matrix_sv
		WHERE ageband <> 'Undefined'
			AND full_session_flag = 0
		) AS t
	GROUP BY hhsize
		,segment_id
		,sex
		,ageband
		,sum_hours_watched
		,sum_hours_over_all_demog
		,PIV_default

	COMMIT WORK message convert(TIMESTAMP, now()) || ' #PIV generated: ' || @@rowcount TO client

	COMMIT WORK

	UPDATE V289_M10_PIV_default_sv AS a
	SET a.sum_hours_watched = j.sum_hours_watched
		,a.sum_hours_over_all_demog = j.sum_hours_over_all_demog
		,a.PIV_default = j.PIV_default
	FROM V289_M10_PIV_default_sv AS a
	JOIN #PIV AS j ON a.hhsize = j.hhsize
		AND a.segment_id = j.segment_id
		AND LEFT(a.sex, 1) = LEFT(j.sex, 1)
		AND LEFT(a.ageband, 2) = LEFT(j.ageband, 2)

	COMMIT WORK message convert(TIMESTAMP, now()) || ' V289_M10_PIV_default_sv Table Updated: ' || @@rowcount TO client

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S2.2 - Calculate date-wise PIV' TO client

	COMMIT WORK

	INSERT INTO V289_M10_PIV_by_date_sv
	SELECT thedate
		,hhsize
		,segment_id
		,sex
		,ageband
		,sum_hours_watched
		,sum_hours_over_all_demog
		,PIV_by_date
	FROM (
		SELECT thedate
			,hhsize
			,segment_id
			,'sex' = CASE 
				WHEN ageband IN (
						'0-19'
						,'12-19'
						,'0-11'
						)
					THEN 'Undefined'
				ELSE sex
				END
			,ageband
			,uk_hhwatched
			,'uk_hhwatched_nonzero' = CASE 
				WHEN (
						uk_hhwatched = 0
						OR uk_hhwatched IS NULL
						)
					THEN .001
				ELSE uk_hhwatched
				END
			,'sum_hours_watched' = sum(uk_hhwatched_nonzero) OVER (
				PARTITION BY thedate
				,hhsize
				,segment_id
				,sex
				,ageband
				)
			,'sum_hours_over_all_demog' = sum(uk_hhwatched_nonzero) OVER (
				PARTITION BY thedate
				,hhsize
				,segment_id
				)
			,'PIV_by_date' = 1.0 * sum_hours_watched / sum_hours_over_all_demog
		FROM v289_genderage_matrix_sv
		WHERE ageband <> 'Undefined'
			AND full_session_flag = 0
		) AS t
	GROUP BY thedate
		,hhsize
		,segment_id
		,sex
		,ageband
		,sum_hours_watched
		,sum_hours_over_all_demog
		,PIV_by_date

	COMMIT WORK

	UPDATE V289_M10_log_sv
	SET dt_completed = now()
		,completed = 1
	WHERE section_id = 'S2 - Produce date-agnostic PIV and re-normalise'

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S3.1 - Join all possible individuals to viewing data' TO client

	COMMIT WORK

	INSERT INTO V289_M10_combined_event_data_sv (
		account_number
		,hh_person_number
		,subscriber_id
		,event_id
		,event_start_utc
		,chunk_start
		,overlap_batch
		,programme_genre
		,session_daypart
		,channel_pack
		,segment_id
		,numrow
		,session_size
		,person_gender
		,person_ageband
		,household_size
		,viewer_hhsize
		)
	SELECT a.account_number
		,b.hh_person_number
		,a.subscriber_id
		,a.event_id
		,a.event_start_utc
		,a.chunk_start
		,a.overlap_batch
		,a.programme_genre
		,a.session_daypart
		,a.channel_pack
		,a.segment_id
		,a.numrow
		,a.session_size
		,b.person_gender
		,b.person_ageband
		,a.hhsize
		,a.viewer_hhsize
	FROM (
		SELECT account_number
			,subscriber_id
			,event_id
			,event_start_utc
			,chunk_start
			,overlap_batch
			,programme_genre
			,session_daypart
			,channel_pack
			,segment_id
			,session_size
			,hhsize
			,viewer_hhsize
			,'numrow' = row_number() OVER (
				ORDER BY account_number ASC
					,subscriber_id ASC
					,event_id ASC
					,overlap_batch ASC
				)
		FROM V289_M07_dp_data_sv
		WHERE session_size > 0
			AND segment_id IS NOT NULL
		) AS a
	JOIN (
		SELECT account_number
			,hh_person_number
			,person_gender
			,person_ageband
			,'valid_viewers' = count() OVER (PARTITION BY account_number)
		FROM V289_M08_SKY_HH_composition_sv
		WHERE person_ageband IS NOT NULL
			AND hh_person_number IS NOT NULL
			AND non_viewer = 0
			AND PANEL_FLAG = 1
		) AS b ON a.account_number = b.account_number
		AND a.viewer_hhsize = b.valid_viewers
	WHERE session_size <= a.viewer_hhsize

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S3.1 - V289_M10_combined_event_data_sv Table populated: ' || @@rowcount TO client

	COMMIT WORK

	IF EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND upper(tname) = upper('V289_M10_UNIQUE_VIEWERS_EVENT_ASSIGNMENT_sv')
				AND tabletype = 'TABLE'
			)
		DROP TABLE V289_M10_UNIQUE_VIEWERS_EVENT_ASSIGNMENT_sv

	COMMIT WORK

	SELECT ACCOUNT_NUMBER
		,HH_PERSON_NUMBER
		,'PERSON_ID' = ACCOUNT_NUMBER || '-' || HH_PERSON_NUMBER
		,'ASSIGNED' = 0
	INTO V289_M10_UNIQUE_VIEWERS_EVENT_ASSIGNMENT_sv
	FROM V289_M10_combined_event_data_sv
	GROUP BY ACCOUNT_NUMBER
		,HH_PERSON_NUMBER
		,PERSON_ID
		,ASSIGNED
	ORDER BY ACCOUNT_NUMBER ASC
		,HH_PERSON_NUMBER ASC
		,PERSON_ID ASC
		,ASSIGNED ASC

	COMMIT WORK

	CREATE UNIQUE hg INDEX UHG_IDX_1 ON V289_M10_UNIQUE_VIEWERS_EVENT_ASSIGNMENT_sv (PERSON_ID)

	COMMIT WORK

	CREATE hg INDEX HG_IDX_1 ON V289_M10_UNIQUE_VIEWERS_EVENT_ASSIGNMENT_sv (ACCOUNT_NUMBER)

	COMMIT WORK

	CREATE hg INDEX HG_IDX_2 ON V289_M10_UNIQUE_VIEWERS_EVENT_ASSIGNMENT_sv (HH_PERSON_NUMBER)

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S3.3 - Filter out overlapping events with more overlaps than available STBs' TO client

	COMMIT WORK

	SELECT DISTINCT b.event_id
		,a.overlap_batch
		,a.account_number
		,a.subscriber_id
		,b.numrow
		,'rankk' = dense_rank() OVER (
			PARTITION BY a.account_number
			,a.overlap_batch
			,a.subscriber_id ORDER BY b.event_id DESC
			)
	INTO #temp_del
	FROM (
		SELECT account_number
			,overlap_batch
			,subscriber_id
			,'hits' = COUNT(DISTINCT event_id)
		FROM V289_M10_combined_event_data_sv
		WHERE overlap_batch IS NOT NULL
		GROUP BY account_number
			,overlap_batch
			,subscriber_id
		HAVING hits > 1
		) AS a
	JOIN V289_M10_combined_event_data_sv AS b ON a.overlap_batch = b.overlap_batch
		AND a.account_number = b.account_number

	COMMIT WORK

	DELETE
	FROM V289_M10_combined_event_data_sv AS a
	FROM V289_M10_combined_event_data_sv AS a
	JOIN #temp_del AS b ON a.overlap_batch = b.overlap_batch
		AND a.account_number = b.account_number
	WHERE rankk > 1

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S3.3 - #V289_M10_batch_overcount overcounts removed: ' || @@rowcount TO client

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S3.4 - Append PIVs to individuals' TO client

	COMMIT WORK

	UPDATE V289_M10_combined_event_data_sv AS a
	SET PIV = c.PIV_by_date
	FROM V289_M10_combined_event_data_sv AS a
	JOIN V289_M10_PIV_by_date_sv AS c ON DATE (a.event_start_utc) = c.thedate
		AND a.segment_id = c.segment_id
		AND a.household_size = c.hhsize
		AND a.person_gender = left(c.sex, 1)
		AND LEFT(a.person_ageband, 2) = LEFT(c.ageband, 2)

	COMMIT WORK

	SELECT DISTINCT event_id
	INTO #tev1
	FROM V289_M10_combined_event_data_sv
	WHERE PIV IS NULL

	COMMIT WORK

	CREATE hg INDEX evi ON #tev1 (event_id)

	COMMIT WORK

	UPDATE V289_M10_combined_event_data_sv AS a
	SET PIV = b.PIV_default
	FROM V289_M10_combined_event_data_sv AS a
	JOIN #tev1 AS z ON a.event_id = z.event_id
	JOIN V289_M10_PIV_default_sv AS b ON a.segment_id = b.segment_id
		AND a.household_size = b.hhsize
		AND a.person_gender = left(b.sex, 1)
		AND LEFT(a.person_ageband, 2) = LEFT(b.ageband, 2)
	WHERE a.PIV IS NULL

	COMMIT WORK

	DROP TABLE #tev1

	COMMIT WORK

	DELETE
	FROM V289_M10_combined_event_data_sv
	WHERE PIV IS NULL

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S3.4 - Deleted from V289_M10_combined_event_data_sv due to null PIV: ' || @@rowcount TO client

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S3.5 - Filter out accounts with fewer individuals than the expected household size' TO client

	COMMIT WORK

	SELECT event_id
	INTO #tmp
	FROM (
		SELECT *
			,'individuals_with_PIV' = count() OVER (PARTITION BY event_id)
		FROM V289_M10_combined_event_data_sv
		) AS t
	WHERE individuals_with_PIV < viewer_hhsize

	COMMIT WORK

	DELETE
	FROM V289_M10_combined_event_data_sv AS a
	FROM V289_M10_combined_event_data_sv AS a
	JOIN #tmp AS b ON a.event_id = b.event_id

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S3.5 - Deleted from V289_M10_combined_event_data_sv due to less expected individuals than the household size: ' || @@rowcount TO client

	COMMIT WORK

	DROP TABLE #tmp

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S3.6 - Delete existing results from current date of data' TO client

	COMMIT WORK

	DELETE
	FROM V289_M10_session_individuals_sv
	WHERE event_date = ANY (
			SELECT 'event_date' = DATE (event_start_utc)
			FROM V289_M10_combined_event_data_sv
			GROUP BY event_date
			)

	COMMIT WORK

	UPDATE V289_M10_log_sv
	SET dt_completed = now()
		,completed = 1
	WHERE section_id = 'S3 - PREPARE VIEWING DATA'

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S4.0 - Assign audience for single-occupancy households and whole-household audiences' TO client

	COMMIT WORK

	INSERT INTO V289_M10_session_individuals_sv (
		event_date
		,event_id
		,account_number
		,person_ageband
		,person_gender
		,hh_person_number
		,last_modified_dt
		)
	SELECT 'event_date' = DATE (event_start_utc)
		,event_id
		,account_number
		,person_ageband
		,person_gender
		,hh_person_number
		,'last_modified_dt' = now()
	FROM V289_M10_combined_event_data_sv
	WHERE viewer_hhsize = session_size
	GROUP BY event_date
		,event_id
		,account_number
		,person_ageband
		,person_gender
		,hh_person_number
		,last_modified_dt

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S4.0 - Individual assigned due to single-occupancy households and whole-household audiences' || @@rowcount TO client

	COMMIT WORK

	UPDATE V289_M10_combined_event_data_sv
	SET assigned = 1
		,dt_assigned = now()
		,individuals_assigned = session_size
	WHERE viewer_hhsize = session_size

	COMMIT WORK

	UPDATE V289_M10_log_sv
	SET dt_completed = now()
		,completed = 1
	WHERE section_id = 'S4 - Assign audience for single-occupancy households and whole-household audiences'

	COMMIT WORK

	UPDATE V289_M10_UNIQUE_VIEWERS_EVENT_ASSIGNMENT_sv AS BAS
	SET ASSIGNED = 1
	FROM (
		SELECT 'PERSON_ID' = ACCOUNT_NUMBER || '-' || HH_PERSON_NUMBER
		FROM V289_M10_session_individuals_sv
		GROUP BY PERSON_ID
		) AS A
	WHERE BAS.PERSON_ID = A.PERSON_ID

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S4.0 - Unique individuals assigned : ' || @@rowcount TO client

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S5.0 - Assign audience for non-overlapping events' TO client

	COMMIT WORK

	SELECT segment_id
		,household_size
		,person_gender
		,person_ageband
		,'seg_piv' = min(piv)
	INTO #age_gender_pivs
	FROM V289_M10_combined_event_data_sv
	WHERE overlap_batch IS NULL
		AND individuals_assigned < session_size
		AND assigned = 0
	GROUP BY segment_id
		,household_size
		,person_gender
		,person_ageband

	COMMIT WORK

	SELECT segment_id
		,household_size
		,'seg_tot_piv' = sum(seg_piv)
	INTO #age_gender_tot_piv
	FROM #age_gender_pivs
	GROUP BY segment_id
		,household_size

	COMMIT WORK

	SELECT a.segment_id
		,a.household_size
		,a.person_gender
		,a.person_ageband
		,'normalised_piv' = convert(DOUBLE, seg_piv) / convert(DOUBLE, seg_tot_piv)
	INTO #age_gender_normalise_piv
	FROM #age_gender_pivs AS a
	JOIN #age_gender_tot_piv AS b ON a.segment_id = b.segment_id
		AND a.household_size = b.household_size

	COMMIT WORK

	SELECT segment_id
		,household_size
		,'segment_target' = sum(event_session)
	INTO #session_targets
	FROM (
		SELECT segment_id
			,household_size
			,event_id
			,'event_session' = min(session_size)
		FROM V289_M10_combined_event_data_sv
		WHERE overlap_batch IS NULL
			AND individuals_assigned < session_size
			AND assigned = 0
		GROUP BY segment_id
			,household_size
			,event_id
		) AS a
	GROUP BY segment_id
		,household_size

	COMMIT WORK

	SELECT a.segment_id
		,a.household_size
		,a.person_gender
		,a.person_ageband
		,'age_gender_target' = normalised_piv * segment_target
	INTO #age_gender_targets
	FROM #age_gender_normalise_piv AS a
	JOIN #session_targets AS b ON a.segment_id = b.segment_id
		AND a.household_size = b.household_size

	COMMIT WORK

	SELECT segment_id
		,household_size
		,person_gender
		,person_ageband
		,'possible_event_count' = count(1)
	INTO #age_gender_event_count
	FROM V289_M10_combined_event_data_sv
	WHERE overlap_batch IS NULL
		AND individuals_assigned < session_size
		AND assigned = 0
	GROUP BY segment_id
		,household_size
		,person_gender
		,person_ageband

	COMMIT WORK

	SELECT a.segment_id
		,a.household_size
		,a.person_gender
		,a.person_ageband
		,'new_piv' = convert(DOUBLE, a.age_gender_target) / convert(DOUBLE, b.possible_event_count)
	INTO #new_pivs
	FROM #age_gender_targets AS a
	JOIN #age_gender_event_count AS b ON a.segment_id = b.segment_id
		AND a.household_size = b.household_size
		AND a.person_gender = b.person_gender
		AND a.person_ageband = b.person_ageband

	COMMIT WORK

	UPDATE V289_M10_combined_event_data_sv AS a
	SET piv = new_piv
	FROM #new_pivs AS b
	WHERE a.segment_id = b.segment_id
		AND a.household_size = b.household_size
		AND a.person_gender = b.person_gender
		AND a.person_ageband = b.person_ageband
		AND a.assigned = 0
		AND a.overlap_batch IS NULL
		AND a.individuals_assigned < a.session_size

	COMMIT WORK

	SET @i = 0

	COMMIT WORK

	WHILE @i < @max_household_size
	BEGIN
		SET @i = @i + 1

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S5.0 - Assign audience for non-overlapping events. Iteration : ' || convert(INTEGER, @i) TO client

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S5.0 - Assign audience for non-overlapping events. Iteration : ' || convert(INTEGER, @i) || '. Checkpoint A' TO client

		COMMIT WORK

		SELECT bas.account_number
			,bas.event_id
			,bas.hh_person_number
			,'cumsum_PIV' = sum(bas.PIV) OVER (
				PARTITION BY bas.event_id rows BETWEEN unbounded preceding
					AND CURRENT row
				)
			,'norm_total' = sum(bas.PIV) OVER (PARTITION BY bas.event_id)
			,'PIV_range' = cumsum_PIV / norm_total
			,'rnd' = rand(bas.numrow + datepart(us, now()))
		INTO #t1
		FROM V289_M10_combined_event_data_sv AS bas
		WHERE bas.overlap_batch IS NULL
			AND bas.individuals_assigned < bas.session_size
			AND bas.assigned = 0

		COMMIT WORK

		CREATE hg INDEX #t1_hg_idx_1 ON #t1 (account_number)

		COMMIT WORK

		CREATE hg INDEX #t1_hg_idx_2 ON #t1 (event_id)

		COMMIT WORK

		SELECT *
			,'rnk' = row_number() OVER (
				PARTITION BY event_id ORDER BY PIV_range ASC
				)
		INTO #t2
		FROM #t1
		WHERE rnd < PIV_range

		CREATE hg INDEX #t2_hg_idx_1 ON #t2 (account_number)

		COMMIT WORK

		CREATE hg INDEX #t2_hg_idx_2 ON #t2 (event_id)

		COMMIT WORK

		DROP TABLE #t1

		COMMIT WORK

		DELETE
		FROM #t2
		WHERE rnk <> 1

		COMMIT WORK

		INSERT INTO #working_PIV (
			account_number
			,event_id
			,hh_person_number
			,cumsum_PIV
			,norm_total
			,PIV_range
			,rnd
			)
		SELECT account_number
			,event_id
			,hh_person_number
			,cumsum_PIV
			,norm_total
			,PIV_range
			,rnd
		FROM #t2

		COMMIT WORK

		DROP TABLE #t2

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S5.0 - Assign audience for non-overlapping events. Iteration : ' || convert(INTEGER, @i) || '. Checkpoint B' TO client

		UPDATE V289_M10_combined_event_data_sv AS a
		SET assigned = 1
			,dt_assigned = now()
		FROM V289_M10_combined_event_data_sv AS a
		JOIN #working_PIV AS b ON a.event_id = b.event_id
			AND a.account_number = b.account_number
			AND a.hh_person_number = b.hh_person_number

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S5.0 - Assign audience for non-overlapping events. Iteration : ' || convert(INTEGER, @i) || '. Checkpoint C' TO client

		COMMIT WORK

		UPDATE V289_M10_UNIQUE_VIEWERS_EVENT_ASSIGNMENT_sv AS BAS
		SET BAS.ASSIGNED = 1
		FROM (
			SELECT 'PERSON_ID' = ACCOUNT_NUMBER || '-' || HH_PERSON_NUMBER
			FROM V289_M10_combined_event_data_sv
			WHERE ASSIGNED = 1
			GROUP BY PERSON_ID
			) AS A
		WHERE BAS.PERSON_ID = A.PERSON_ID

		COMMIT WORK

		TRUNCATE TABLE #working_PIV

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S5.0 - Assign audience for non-overlapping events. Iteration : ' || convert(INTEGER, @i) || '. Checkpoint D' TO client

		COMMIT WORK

		UPDATE V289_M10_combined_event_data_sv AS a
		SET individuals_assigned = b.total_assigned
		FROM V289_M10_combined_event_data_sv AS a
		JOIN (
			SELECT event_id
				,'total_assigned' = sum(convert(INTEGER, assigned))
			FROM V289_M10_combined_event_data_sv
			WHERE overlap_batch IS NULL
			GROUP BY event_id
			) AS b ON a.event_id = b.event_id

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S5.0 - Assign audience for non-overlapping events. Iteration : ' || convert(INTEGER, @i) || '. Checkpoint E' TO client

		COMMIT WORK

		IF NOT EXISTS (
				SELECT 1
				FROM V289_M10_combined_event_data_sv
				WHERE overlap_batch IS NULL
					AND individuals_assigned < session_size
				)
			BREAK
	END

	COMMIT WORK

	DELETE
	FROM V289_M10_combined_event_data_sv
	WHERE overlap_batch IS NULL
		AND individuals_assigned = session_size
		AND assigned = 0

	COMMIT WORK

	INSERT INTO V289_M10_session_individuals_sv (
		event_date
		,event_id
		,account_number
		,person_ageband
		,person_gender
		,hh_person_number
		,last_modified_dt
		)
	SELECT 'event_date' = DATE (event_start_utc)
		,event_id
		,account_number
		,person_ageband
		,person_gender
		,hh_person_number
		,'last_modified_dt' = now()
	FROM V289_M10_combined_event_data_sv
	WHERE overlap_batch IS NULL
		AND assigned = 1
		AND NOT (viewer_hhsize = session_size)
	GROUP BY event_date
		,event_id
		,account_number
		,person_ageband
		,person_gender
		,hh_person_number
		,last_modified_dt

	COMMIT WORK

	UPDATE V289_M10_UNIQUE_VIEWERS_EVENT_ASSIGNMENT_sv AS BAS
	SET ASSIGNED = 1
	FROM (
		SELECT 'PERSON_ID' = ACCOUNT_NUMBER || '-' || HH_PERSON_NUMBER
		FROM V289_M10_session_individuals_sv
		GROUP BY PERSON_ID
		) AS A
	WHERE BAS.PERSON_ID = A.PERSON_ID

	COMMIT WORK

	UPDATE V289_M10_log_sv
	SET dt_completed = now()
		,completed = 1
	WHERE section_id = 'S5 - Assign audience for non-overlapping events'

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S6.0 - Assign audience for overlapping events' TO client

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S6.0 - Assign audience for overlapping events - Calculate iteration limit.' TO client

	COMMIT WORK

	SELECT @max_chunk_session_size = max(chunk_session_size)
	FROM (
		SELECT account_number
			,overlap_batch
			,'chunk_session_size' = sum(session_size)
		INTO #chunk_sessions
		FROM V289_M07_dp_data_sv
		WHERE overlap_batch IS NOT NULL
		GROUP BY account_number
			,overlap_batch
		) AS t

	COMMIT WORK

	SELECT segment_id
		,household_size
		,person_gender
		,person_ageband
		,'seg_piv' = min(piv)
	INTO #age_gender_pivs_ov
	FROM V289_M10_combined_event_data_sv
	WHERE overlap_batch IS NOT NULL
		AND individuals_assigned < session_size
		AND assigned = 0
	GROUP BY segment_id
		,household_size
		,person_gender
		,person_ageband

	COMMIT WORK

	SELECT segment_id
		,household_size
		,'seg_tot_piv' = sum(seg_piv)
	INTO #age_gender_tot_piv_ov
	FROM #age_gender_pivs_ov
	GROUP BY segment_id
		,household_size

	COMMIT WORK

	SELECT a.segment_id
		,a.household_size
		,a.person_gender
		,a.person_ageband
		,'normalised_piv' = convert(DOUBLE, seg_piv) / convert(DOUBLE, seg_tot_piv)
	INTO #age_gender_normalise_piv_ov
	FROM #age_gender_pivs_ov AS a
	JOIN #age_gender_tot_piv_ov AS b ON a.segment_id = b.segment_id
		AND a.household_size = b.household_size

	COMMIT WORK

	SELECT segment_id
		,household_size
		,'segment_target' = sum(event_session)
	INTO #session_targets_ov
	FROM (
		SELECT segment_id
			,household_size
			,event_id
			,'event_session' = min(session_size)
		FROM V289_M10_combined_event_data_sv
		WHERE overlap_batch IS NOT NULL
			AND individuals_assigned < session_size
			AND assigned = 0
		GROUP BY segment_id
			,household_size
			,event_id
		) AS a
	GROUP BY segment_id
		,household_size

	COMMIT WORK

	SELECT a.segment_id
		,a.household_size
		,a.person_gender
		,a.person_ageband
		,'age_gender_target' = normalised_piv * segment_target
	INTO #age_gender_targets_ov
	FROM #age_gender_normalise_piv_ov AS a
	JOIN #session_targets_ov AS b ON a.segment_id = b.segment_id
		AND a.household_size = b.household_size

	COMMIT WORK

	SELECT segment_id
		,household_size
		,person_gender
		,person_ageband
		,'possible_event_count' = count(1)
	INTO #age_gender_event_count_ov
	FROM V289_M10_combined_event_data_sv
	WHERE overlap_batch IS NOT NULL
		AND individuals_assigned < session_size
		AND assigned = 0
	GROUP BY segment_id
		,household_size
		,person_gender
		,person_ageband

	COMMIT WORK

	SELECT a.segment_id
		,a.household_size
		,a.person_gender
		,a.person_ageband
		,'new_piv' = convert(DOUBLE, a.age_gender_target) / convert(DOUBLE, b.possible_event_count)
	INTO #new_pivs_ov
	FROM #age_gender_targets_ov AS a
	JOIN #age_gender_event_count_ov AS b ON a.segment_id = b.segment_id
		AND a.household_size = b.household_size
		AND a.person_gender = b.person_gender
		AND a.person_ageband = b.person_ageband

	COMMIT WORK

	UPDATE V289_M10_combined_event_data_sv AS a
	SET piv = new_piv
	FROM #new_pivs_ov AS b
	WHERE a.segment_id = b.segment_id
		AND a.household_size = b.household_size
		AND a.person_gender = b.person_gender
		AND a.person_ageband = b.person_ageband
		AND a.assigned = 0
		AND a.overlap_batch IS NOT NULL
		AND a.individuals_assigned < a.session_size

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S6.2 - Iterate over individuals per chunk of overlapping events.' TO client

	COMMIT WORK

	SET @i = 0

	COMMIT WORK

	WHILE @i < @max_chunk_session_size
	BEGIN
		SET @i = @i + 1

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S6.2 - Assign audience for overlapping events. Iteration : ' || convert(INTEGER, @i) TO client

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S6.2 - Assign audience for overlapping events. Iteration : ' || convert(INTEGER, @i) || '. Checkpoint A' TO client

		COMMIT WORK

		SELECT bas.account_number
			,bas.subscriber_id
			,bas.overlap_batch
			,bas.hh_person_number
			,'cumsum_PIV' = sum(bas.PIV) OVER (
				PARTITION BY bas.account_number
				,bas.overlap_batch rows BETWEEN unbounded preceding
					AND CURRENT row
				)
			,'norm_total' = sum(bas.PIV) OVER (
				PARTITION BY account_number
				,overlap_batch
				)
			,'PIV_range' = cumsum_PIV / norm_total
			,'rnd' = rand(bas.numrow + bas.hh_person_number + datepart(us, now()))
		INTO #t1
		FROM V289_M10_combined_event_data_sv AS bas
		WHERE bas.overlap_batch IS NOT NULL
			AND bas.individuals_assigned < bas.session_size
			AND bas.assigned = 0

		COMMIT WORK

		CREATE hg INDEX #t1_hg_idx_1 ON #t1 (account_number)

		COMMIT WORK

		SELECT *
			,'rnk' = row_number() OVER (
				PARTITION BY account_number
				,overlap_batch ORDER BY PIV_range ASC
				)
		INTO #t2
		FROM #t1
		WHERE rnd < PIV_range

		COMMIT WORK

		DROP TABLE #t1

		COMMIT WORK

		DELETE
		FROM #t2
		WHERE rnk <> 1

		COMMIT WORK

		INSERT INTO #working_PIV (
			account_number
			,subscriber_id
			,overlap_batch
			,hh_person_number
			,cumsum_PIV
			,norm_total
			,PIV_range
			,rnd
			)
		SELECT account_number
			,subscriber_id
			,overlap_batch
			,hh_person_number
			,cumsum_PIV
			,norm_total
			,PIV_range
			,rnd
		FROM #t2

		COMMIT WORK

		DROP TABLE #t2

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S6.2 - Assign audience for overlapping events. Iteration : ' || convert(INTEGER, @i) || '. Checkpoint B' TO client

		COMMIT WORK

		UPDATE V289_M10_combined_event_data_sv AS a
		SET assigned = 1
			,dt_assigned = now()
		FROM V289_M10_combined_event_data_sv AS a
		JOIN #working_PIV AS c ON a.account_number = c.account_number
			AND a.subscriber_id = c.subscriber_id
			AND a.hh_person_number = c.hh_person_number
			AND a.overlap_batch = c.overlap_batch

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S6.2 - Assign audience for overlapping events. Iteration : ' || convert(INTEGER, @i) || '. Checkpoint C' TO client

		COMMIT WORK

		UPDATE V289_M10_UNIQUE_VIEWERS_EVENT_ASSIGNMENT_sv AS BAS
		SET BAS.ASSIGNED = 1
		FROM (
			SELECT 'PERSON_ID' = ACCOUNT_NUMBER || '-' || HH_PERSON_NUMBER
			FROM V289_M10_combined_event_data_sv
			WHERE ASSIGNED = 1
			GROUP BY PERSON_ID
			) AS A
		WHERE BAS.PERSON_ID = A.PERSON_ID

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S6.2 - Assign audience for overlapping events. Iteration : ' || convert(INTEGER, @i) || '. Checkpoint D' TO client

		COMMIT WORK

		DELETE
		FROM V289_M10_combined_event_data_sv AS a
		FROM V289_M10_combined_event_data_sv AS a
		JOIN #working_PIV AS c ON a.account_number = c.account_number
			AND a.subscriber_id <> c.subscriber_id
			AND a.hh_person_number = c.hh_person_number
			AND a.overlap_batch = c.overlap_batch
		WHERE a.assigned = 0

		COMMIT WORK

		TRUNCATE TABLE #working_PIV

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S6.2 - Assign audience for overlapping events. Iteration : ' || convert(INTEGER, @i) || '. Checkpoint E' TO client

		COMMIT WORK

		UPDATE V289_M10_combined_event_data_sv AS a
		SET individuals_assigned = b.total_assigned
		FROM V289_M10_combined_event_data_sv AS a
		JOIN (
			SELECT account_number
				,subscriber_id
				,overlap_batch
				,'total_assigned' = sum(convert(INTEGER, assigned))
			FROM V289_M10_combined_event_data_sv
			WHERE overlap_batch IS NOT NULL
			GROUP BY account_number
				,subscriber_id
				,overlap_batch
			) AS b ON a.account_number = b.account_number
			AND a.subscriber_id = b.subscriber_id
			AND a.overlap_batch = b.overlap_batch

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S6.2 - Assign audience for overlapping events. Iteration : ' || convert(INTEGER, @i) || '. Checkpoint F' TO client

		COMMIT WORK

		IF NOT EXISTS (
				SELECT 1
				FROM V289_M10_combined_event_data_sv
				WHERE overlap_batch IS NOT NULL
					AND individuals_assigned < session_size
				)
			BREAK
	END

	COMMIT WORK

	INSERT INTO V289_M10_session_individuals_sv (
		event_date
		,event_id
		,account_number
		,overlap_batch
		,person_ageband
		,person_gender
		,hh_person_number
		,last_modified_dt
		)
	SELECT 'event_date' = DATE (event_start_utc)
		,event_id
		,account_number
		,overlap_batch
		,person_ageband
		,person_gender
		,hh_person_number
		,'last_modified_dt' = now()
	FROM V289_M10_combined_event_data_sv
	WHERE overlap_batch IS NOT NULL
		AND assigned = 1
	GROUP BY event_date
		,event_id
		,account_number
		,overlap_batch
		,person_ageband
		,person_gender
		,hh_person_number
		,last_modified_dt

	COMMIT WORK

	UPDATE V289_M10_log_sv
	SET dt_completed = now()
		,completed = 1
	WHERE section_id = 'S6 - Assign audience for overlapping events from the same account'

	COMMIT WORK

	UPDATE V289_M10_UNIQUE_VIEWERS_EVENT_ASSIGNMENT_sv AS BAS
	SET ASSIGNED = 1
	FROM (
		SELECT 'PERSON_ID' = ACCOUNT_NUMBER || '-' || HH_PERSON_NUMBER
		FROM V289_M10_session_individuals_sv
		GROUP BY PERSON_ID
		) AS A
	WHERE BAS.PERSON_ID = A.PERSON_ID

	COMMIT WORK

	IF EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND upper(tname) = upper('V289_M10_combined_event_data_sv_adj')
				AND tabletype = 'TABLE'
			)
		DROP TABLE V289_M10_combined_event_data_sv_adj

	COMMIT WORK

	CREATE TABLE ${SQLFILE_ARG001}.V289_M10_combined_event_data_sv_adj (
		account_number VARCHAR(20) NOT NULL
		,hh_person_number TINYINT NOT NULL
		,subscriber_id DECIMAL(10) NOT NULL
		,event_id BIGINT NOT NULL
		,event_start_utc DATETIME NOT NULL
		,chunk_start DATETIME NULL DEFAULT NULL
		,overlap_batch INTEGER NULL DEFAULT NULL
		,programme_genre VARCHAR(20) NULL DEFAULT NULL
		,session_daypart VARCHAR(11) NULL DEFAULT NULL
		,channel_pack VARCHAR(40) NULL DEFAULT NULL
		,segment_id INTEGER NULL DEFAULT NULL
		,numrow INTEGER NOT NULL
		,session_size TINYINT NULL DEFAULT NULL
		,person_gender VARCHAR(1) NULL DEFAULT NULL
		,person_ageband VARCHAR(10) NULL DEFAULT NULL
		,household_size TINYINT NULL DEFAULT NULL
		,viewer_hhsize TINYINT NULL DEFAULT NULL
		,assigned BIT NOT NULL DEFAULT 0
		,dt_assigned DATETIME NULL DEFAULT NULL
		,PIV DOUBLE NULL DEFAULT NULL
		,individuals_assigned INTEGER NOT NULL DEFAULT 0
		,
		)

	COMMIT WORK

	CREATE hg INDEX V289_M10_combined_event_data_sv_adj_hg_idx_1 ON V289_M10_combined_event_data_sv_adj (account_number)

	COMMIT WORK

	CREATE hg INDEX V289_M10_combined_event_data_sv_adj_hg_idx_2 ON V289_M10_combined_event_data_sv_adj (event_id)

	COMMIT WORK

	CREATE hg INDEX V289_M10_combined_event_data_sv_adj_hg_idx_3 ON V289_M10_combined_event_data_sv_adj (numrow)

	COMMIT WORK

	CREATE lf INDEX V289_M10_combined_event_data_sv_adj_lf_idx_4 ON V289_M10_combined_event_data_sv_adj (session_size)

	COMMIT WORK

	CREATE lf INDEX V289_M10_combined_event_data_sv_adj_lf_idx_5 ON V289_M10_combined_event_data_sv_adj (person_gender)

	COMMIT WORK

	CREATE lf INDEX V289_M10_combined_event_data_sv_adj_lf_idx_6 ON V289_M10_combined_event_data_sv_adj (person_ageband)

	COMMIT WORK

	CREATE lf INDEX V289_M10_combined_event_data_sv_adj_lf_idx_7 ON V289_M10_combined_event_data_sv_adj (household_size)

	COMMIT WORK

	CREATE lf INDEX V289_M10_combined_event_data_sv_adj_lf_idx_8 ON V289_M10_combined_event_data_sv_adj (viewer_hhsize)

	COMMIT WORK

	GRANT SELECT
		ON V289_M10_combined_event_data_sv_adj
		TO vespa_group_low_security

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S7.1 - Join all possible individuals to viewing data' TO client

	COMMIT WORK

	INSERT INTO V289_M10_combined_event_data_sv_adj (
		account_number
		,hh_person_number
		,subscriber_id
		,event_id
		,event_start_utc
		,chunk_start
		,overlap_batch
		,programme_genre
		,session_daypart
		,channel_pack
		,segment_id
		,numrow
		,session_size
		,person_gender
		,person_ageband
		,household_size
		,viewer_hhsize
		)
	SELECT a.account_number
		,b.hh_person_number
		,a.subscriber_id
		,a.event_id
		,a.event_start_utc
		,a.chunk_start
		,a.overlap_batch
		,a.programme_genre
		,a.session_daypart
		,a.channel_pack
		,a.segment_id
		,a.numrow
		,a.session_size
		,b.person_gender
		,b.person_ageband
		,a.hhsize
		,a.viewer_hhsize
	FROM (
		SELECT account_number
			,subscriber_id
			,event_id
			,event_start_utc
			,chunk_start
			,overlap_batch
			,programme_genre
			,session_daypart
			,channel_pack
			,segment_id
			,session_size
			,hhsize
			,viewer_hhsize
			,'numrow' = row_number() OVER (
				ORDER BY account_number ASC
					,subscriber_id ASC
					,event_id ASC
					,overlap_batch ASC
				)
		FROM V289_M07_dp_data_sv
		WHERE session_size > 0
			AND segment_id IS NOT NULL
		) AS a
	JOIN (
		SELECT account_number
			,hh_person_number
			,person_gender
			,person_ageband
			,'valid_viewers' = count() OVER (PARTITION BY account_number)
		FROM V289_M08_SKY_HH_composition_sv
		WHERE person_ageband IS NOT NULL
			AND hh_person_number IS NOT NULL
			AND non_viewer = 0
			AND PANEL_FLAG = 1
		) AS b ON a.account_number = b.account_number
		AND a.viewer_hhsize = b.valid_viewers
	JOIN V289_M10_UNIQUE_VIEWERS_EVENT_ASSIGNMENT_sv AS c ON c.assigned = 0
		AND a.account_number = c.account_number
		AND b.hh_person_number = c.hh_person_number
	WHERE session_size <= a.viewer_hhsize

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S7.1 - V289_M10_combined_event_data_sv_adj Table populated: ' || @@rowcount TO client

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S7.4 - Append PIVs to individuals' TO client

	COMMIT WORK

	UPDATE V289_M10_combined_event_data_sv_adj AS a
	SET PIV = c.PIV_by_date
	FROM V289_M10_combined_event_data_sv_adj AS a
	JOIN V289_M10_PIV_by_date_sv AS c ON DATE (a.event_start_utc) = c.thedate
		AND a.segment_id = c.segment_id
		AND a.household_size = c.hhsize
		AND a.person_gender = left(c.sex, 1)
		AND LEFT(a.person_ageband, 2) = LEFT(c.ageband, 2)

	COMMIT WORK

	SELECT DISTINCT event_id
	INTO #tev1
	FROM V289_M10_combined_event_data_sv_adj
	WHERE PIV IS NULL

	COMMIT WORK

	CREATE hg INDEX evi ON #tev1 (event_id)

	COMMIT WORK

	UPDATE V289_M10_combined_event_data_sv_adj AS a
	SET PIV = b.PIV_default
	FROM V289_M10_combined_event_data_sv_adj AS a
	JOIN #tev1 AS z ON a.event_id = z.event_id
	JOIN V289_M10_PIV_default_sv AS b ON a.segment_id = b.segment_id
		AND a.household_size = b.hhsize
		AND a.person_gender = left(b.sex, 1)
		AND LEFT(a.person_ageband, 2) = LEFT(b.ageband, 2)

	COMMIT WORK

	DROP TABLE #tev1

	COMMIT WORK

	DELETE
	FROM V289_M10_combined_event_data_sv_adj
	WHERE PIV IS NULL

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S7.4 - Deleted from V289_M10_combined_event_data_sv_adj due to null PIV: ' || @@rowcount TO client

	COMMIT WORK

	SELECT *
	INTO #tmp_combined_event_data_adj
	FROM (
		SELECT account_number
			,hh_person_number
			,subscriber_id
			,event_id
			,PIV
			,person_ageband
			,person_gender
			,event_start_utc
			,overlap_batch
			,'PIV_rank' = row_number() OVER (
				PARTITION BY account_number
				,hh_person_number ORDER BY PIV DESC
				)
		FROM V289_M10_combined_event_data_sv_adj
		) AS a
	WHERE a.PIV_rank = 1

	COMMIT WORK

	CREATE hg INDEX tmp_combined_event_data_adj_hg_idx_1 ON #tmp_combined_event_data_adj (account_number)

	COMMIT WORK

	CREATE hg INDEX tmp_combined_event_data_adj_hg_idx_2 ON #tmp_combined_event_data_adj (event_id)

	COMMIT WORK

	CREATE hg INDEX tmp_combined_event_data_adj_hg_idx_3 ON #tmp_combined_event_data_adj (PIV_rank)

	COMMIT WORK

	CREATE lf INDEX tmp_combined_event_data_adj_lf_idx_5 ON #tmp_combined_event_data_adj (person_gender)

	COMMIT WORK

	CREATE lf INDEX tmp_combined_event_data_adj_lf_idx_6 ON #tmp_combined_event_data_adj (person_ageband)

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S7.0 - Assign audience for single-occupancy households and whole-household audiences' TO client

	COMMIT WORK

	INSERT INTO V289_M10_session_individuals_sv (
		event_date
		,event_id
		,account_number
		,overlap_batch
		,person_ageband
		,person_gender
		,hh_person_number
		,last_modified_dt
		)
	SELECT 'event_date' = DATE (event_start_utc)
		,event_id
		,account_number
		,overlap_batch
		,person_ageband
		,person_gender
		,hh_person_number
		,'last_modified_dt' = now()
	FROM #tmp_combined_event_data_adj

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S7.0 - Individuals assigned due to single-occupancy households and whole-household audiences : ' || @@rowcount TO client

	COMMIT WORK

	UPDATE V289_M10_UNIQUE_VIEWERS_EVENT_ASSIGNMENT_sv AS BAS
	SET ASSIGNED = 1
	FROM (
		SELECT 'PERSON_ID' = ACCOUNT_NUMBER || '-' || HH_PERSON_NUMBER
		FROM V289_M10_session_individuals_sv
		GROUP BY PERSON_ID
		) AS A
	WHERE BAS.PERSON_ID = A.PERSON_ID

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S7.0 - Unique individuals assigned : ' || @@rowcount TO client

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S7.0 - Update the revised session size value in V289_M07_dp_data_sv' TO client

	COMMIT WORK

	UPDATE V289_M07_dp_data_sv AS bas
	SET bas.session_size = bas.session_size + a.session_size_actual
	FROM (
		SELECT event_id
			,overlap_batch
			,'rows_check' = count()
			,'session_size_actual' = count(DISTINCT hh_person_number)
		FROM #tmp_combined_event_data_adj
		GROUP BY event_id
			,overlap_batch
		) AS a
	WHERE bas.event_id = a.event_id
		AND bas.overlap_batch = a.overlap_batch

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 S7.0 - Update the revised session size value in V289_M07_dp_data_sv... DONE. Rows affected: ' || @@rowcount TO client

	UPDATE V289_M10_session_individuals_sv AS si
	SET provider_id = dpd.provider_id
		,provider_id_number = dpd.provider_id_number
		,viewing_type_flag = dpd.viewing_type_flag
	FROM V289_M10_session_individuals_sv AS si
	JOIN V289_M07_dp_data_sv AS dpd ON dpd.event_id = si.event_id

	COMMIT WORK

	UPDATE V289_M10_log_sv
	SET dt_completed = now()
		,completed = 1
	WHERE section_id = 'SXX - FINISH'

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M10 - Individuals assignment complete!' TO client

	COMMIT WORK
END;
GO 
commit;
