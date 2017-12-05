CREATE OR REPLACE PROCEDURE ${SQLFILE_ARG001}.v289_m05_barb_Matrices_generation_sv
AS
BEGIN
	message convert(TIMESTAMP, now()) || ' | Begining M05.0 - Initialising Environment' TO client message convert(TIMESTAMP, now()) || ' | @ M05.0: Initialising Environment DONE' TO client

	DECLARE @min DECIMAL(8, 7) message convert (
		TIMESTAMP
		,now()
		) || ' | Begining M05.1 - Aggregating transient tables' TO client

	SELECT 'thedate' = convert(DATE, start_time_of_session)
		,household_number
		,session_id
		,'session_size' = count(DISTINCT person_number)
	INTO #z
	FROM skybarb_sv_fullview_sv
	GROUP BY thedate
		,household_number
		,session_id

	COMMIT WORK

	CREATE lf INDEX in1 ON #z (thedate)

	CREATE hg INDEX in2 ON #z (household_number)

	CREATE hg INDEX in3 ON #z (session_id)

	COMMIT WORK

	SELECT 'thedate' = convert(DATE, start_time_of_session)
		,household_number
		,'v_size' = count(DISTINCT person_number)
	INTO #x
	FROM skybarb_sv_fullview_sv
	GROUP BY thedate
		,household_number

	COMMIT WORK

	CREATE lf INDEX in1 ON #x (thedate)

	CREATE hg INDEX in2 ON #x (household_number)

	COMMIT WORK

	SELECT z.thedate
		,lookup.segment_id
		,skybarb_sv.hhsize
		,'full_session_flag' = CASE 
			WHEN z.session_size = x.v_size
				THEN 1
			ELSE 0
			END
		,skybarb_sv.sex
		,'ageband' = coalesce(skybarb_sv.ageband, 'Undefined')
		,'uk_hhwatched' = sum(skybarb_sv.progscaled_duration) / 60.0
	INTO #base
	FROM skybarb_sv_fullview_sv AS skybarb_sv
	JOIN V289_PIV_Grouped_Segments_desc_sv AS lookup ON skybarb_sv.session_daypart = lookup.daypart
		AND skybarb_sv.channel_pack = lookup.channel_pack
		AND skybarb_sv.programme_genre = lookup.genre
	JOIN #z AS z ON convert(DATE, start_time_of_session) = z.thedate
		AND skybarb_sv.household_number = z.household_number
		AND skybarb_sv.session_id = z.session_id
	JOIN #x AS x ON z.thedate = x.thedate
		AND z.household_number = x.household_number
	GROUP BY z.thedate
		,lookup.segment_id
		,skybarb_sv.hhsize
		,full_session_flag
		,skybarb_sv.sex
		,skybarb_sv.ageband

	COMMIT WORK

	CREATE lf INDEX lf1 ON #base (segment_id)

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M05.1: Base Table Generation DONE' TO client

	SELECT thedate
		,segment_id
		,hhsize
		,viewing_size
		,session_size
		,'uk_hhwatched' = sum(tot_mins_watch_scaled_per_hhsession) / 60.0
	INTO #base2
	FROM (
		SELECT 'thedate' = convert(DATE, skybarb_sv.start_time_of_session)
			,skybarb_sv.household_number
			,lookup.segment_id
			,skybarb_sv.hhsize
			,x.viewing_size
			,skybarb_sv.session_id
			,'session_size' = count(DISTINCT skybarb_sv.person_number)
			,'tot_mins_watch_scaled_per_hhsession' = max(duration_of_session * hh_weight)
		FROM skybarb_sv_fullview_sv AS skybarb_sv
		JOIN (
			SELECT 'viewing_size' = count(DISTINCT person_number)
				,household_number
				,'thedatex' = convert(DATE, start_time_of_session)
			FROM skybarb_sv_fullview_sv
			GROUP BY household_number
				,thedatex
			) AS x ON x.household_number = skybarb_sv.household_number
			AND x.thedatex = thedate
		JOIN V289_PIV_Grouped_Segments_desc_sv AS lookup ON skybarb_sv.session_daypart = lookup.daypart
			AND skybarb_sv.channel_pack = lookup.channel_pack
			AND skybarb_sv.programme_genre = lookup.genre
		GROUP BY thedate
			,skybarb_sv.household_number
			,lookup.segment_id
			,skybarb_sv.hhsize
			,skybarb_sv.session_id
			,x.viewing_size
		) AS base
	GROUP BY thedate
		,segment_id
		,hhsize
		,viewing_size
		,session_size

	COMMIT WORK

	CREATE lf INDEX lf1 ON #base2 (segment_id)

	CREATE lf INDEX lf2 ON #base2 (hhsize)

	CREATE lf INDEX lf3 ON #base2 (viewing_size)

	COMMIT WORK

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M05.1: Base2 Table Generation DONE' TO client

	SELECT z.thedate
		,lookup.segment_id
		,skybarb_sv.hhsize
		,'full_session_flag' = CASE 
			WHEN z.session_size = x.v_size
				THEN 1
			ELSE 0
			END
		,skybarb_sv.sex
		,'ageband' = coalesce(skybarb_sv.ageband, 'Undefined')
		,skybarb_sv.household_number
		,skybarb_sv.person_number
		,'ind_weight' = min(skybarb_sv.processing_weight)
	INTO #ind_weights
	FROM skybarb_sv_fullview_sv AS skybarb_sv
	JOIN V289_PIV_Grouped_Segments_desc_sv AS lookup ON skybarb_sv.session_daypart = lookup.daypart
		AND skybarb_sv.channel_pack = lookup.channel_pack
		AND skybarb_sv.programme_genre = lookup.genre
	JOIN #z AS z ON convert(DATE, start_time_of_session) = z.thedate
		AND skybarb_sv.household_number = z.household_number
		AND skybarb_sv.session_id = z.session_id
	JOIN #x AS x ON z.thedate = x.thedate
		AND z.household_number = x.household_number
	GROUP BY z.thedate
		,lookup.segment_id
		,skybarb_sv.hhsize
		,full_session_flag
		,skybarb_sv.sex
		,skybarb_sv.ageband
		,skybarb_sv.household_number
		,skybarb_sv.person_number

	COMMIT WORK

	SELECT thedate
		,segment_id
		,hhsize
		,full_session_flag
		,sex
		,ageband
		,'segment_weight' = sum(ind_weight)
	INTO #viewer_weights
	FROM #ind_weights
	GROUP BY thedate
		,segment_id
		,hhsize
		,full_session_flag
		,sex
		,ageband

	COMMIT WORK

	CREATE lf INDEX lf1 ON #viewer_weights (segment_id)

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M05.1: Aggregating transient tables DONE' TO client

	IF EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = user_name()
				AND tabletype = 'TABLE'
				AND upper(tname) = 'PROP_TABLE'
			)
		DROP TABLE prop_table

	IF EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = user_name()
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('v289_vsizealloc_matrix_small_sv')
			)
		DROP TABLE v289_vsizealloc_matrix_small_sv

	IF EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = user_name()
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('v289_vsizealloc_matrix_big_sv')
			)
		DROP TABLE v289_vsizealloc_matrix_big_sv

	SELECT house_id
		,'hh_processing_weight' = MAX(processing_weight)
	INTO #hh_W
	FROM skybarb_sv AS a
	JOIN barb_weights_sv AS b ON b.household_number = a.house_id
	WHERE head = 1
	GROUP BY house_id

	COMMIT WORK

	CREATE hg INDEX id1 ON #hh_W (house_id)

	COMMIT WORK

	SELECT DISTINCT a.*
		,hh_processing_weight
		,'viewer_flag' = CASE 
			WHEN c.person_number IS NOT NULL
				THEN 1.0
			ELSE 0.0
			END
		,'nv_flag' = CASE 
			WHEN c.person_number IS NULL
				THEN 1.0
			ELSE 0.0
			END
		,'viewer_weight' = CASE 
			WHEN c.person_number IS NOT NULL
				THEN hh_processing_weight
			ELSE 0
			END
		,'nv_weight' = CASE 
			WHEN c.person_number IS NULL
				THEN hh_processing_weight
			ELSE 0
			END
		,kid
		,twenties
		,'hhsize' = count(a.household_number) OVER (PARTITION BY a.household_number)
	INTO #tt1
	FROM barb_weights_sv AS a
	JOIN (
		SELECT house_id
			,'kid' = MAX(CASE 
					WHEN age BETWEEN 0
							AND 19
						THEN 1
					ELSE 0
					END)
			,'twenties' = MAX(CASE 
					WHEN age BETWEEN 20
							AND 24
						THEN 1
					ELSE 0
					END)
		FROM skybarb_sv
		GROUP BY house_id
		) AS b ON a.household_number = b.house_id
	LEFT OUTER JOIN (
		SELECT DISTINCT household_number
			,person_number
			,'date_of_activity_db1' = DATE (start_time_of_session)
		FROM skybarb_sv_fullview_sv
		) AS c ON a.household_number = c.household_number
		AND a.person_number = c.person_number
		AND a.date_of_activity_db1 = c.date_of_activity_db1
	JOIN #hh_W AS w ON w.house_id = a.household_number

	SELECT *
		,'nvsize' = sum(nv_flag) OVER (PARTITION BY household_number)
	INTO #tt2
	FROM #tt1

	SELECT hhsize
		,'viewer_size' = hhsize - nvsize
		,kid
		,twenties
		,'weight_in_hh' = sum(hh_processing_weight)
		,date_of_activity_db1
	INTO #tt32
	FROM #tt2
	WHERE hhsize <> nvsize
		AND hhsize = 2
	GROUP BY hhsize
		,viewer_size
		,date_of_activity_db1
		,twenties
		,kid

	SELECT hhsize
		,twenties
		,kid
		,'hh_size_weight' = sum(hh_processing_weight)
		,date_of_activity_db1
	INTO #tt42
	FROM #tt2
	WHERE hhsize <> nvsize
	GROUP BY hhsize
		,date_of_activity_db1
		,twenties
		,kid

	SELECT hhsize
		,'viewer_size' = hhsize - nvsize
		,'weight_in_hh' = sum(hh_processing_weight)
		,date_of_activity_db1
	INTO #tt3
	FROM #tt2
	WHERE hhsize <> nvsize
		AND hhsize <> 2
	GROUP BY hhsize
		,viewer_size
		,date_of_activity_db1

	SELECT hhsize
		,'hh_size_weight' = sum(hh_processing_weight)
		,date_of_activity_db1
	INTO #tt4
	FROM #tt2
	WHERE hhsize <> nvsize
	GROUP BY hhsize
		,date_of_activity_db1

	SELECT 'hh_size' = #tt32.hhsize
		,viewer_size
		,#tt32.date_of_activity_db1
		,#tt32.kid
		,#tt32.twenties
		,'proportion' = convert(DECIMAL(15, 6), weight_in_hh) / convert(DECIMAL(15, 6), hh_size_weight)
		,'norm' = SUM(proportion) OVER (
			PARTITION BY #tt32.hhsize
			,#tt32.date_of_activity_db1
			,#tt32.kid
			,#tt32.twenties
			)
		,'Lower_limit' = COALESCE((
				SUM(proportion) OVER (
					PARTITION BY #tt32.hhsize
					,#tt32.date_of_activity_db1
					,#tt32.kid
					,#tt32.twenties ORDER BY viewer_size ASC rows BETWEEN unbounded preceding
							AND 1 preceding
					)
				), 0) / norm
		,'Upper_limit' = SUM(proportion) OVER (
			PARTITION BY #tt32.hhsize
			,#tt32.date_of_activity_db1
			,#tt32.kid
			,#tt32.twenties ORDER BY viewer_size ASC rows BETWEEN unbounded preceding
					AND CURRENT row
			) / norm
	INTO v289_vsizealloc_matrix_small_sv
	FROM #tt32
	JOIN #tt42 ON #tt32.hhsize = #tt42.hhsize
		AND #tt32.date_of_activity_db1 = #tt42.date_of_activity_db1
		AND #tt32.kid = #tt42.kid
		AND #tt32.twenties = #tt42.twenties

	SELECT 'hh_size' = #tt3.hhsize
		,viewer_size
		,#tt3.date_of_activity_db1
		,'proportion' = convert(DECIMAL(15, 6), weight_in_hh) / convert(DECIMAL(15, 6), hh_size_weight)
		,'norm' = SUM(proportion) OVER (
			PARTITION BY #tt3.hhsize
			,#tt3.date_of_activity_db1
			)
		,'Lower_limit' = COALESCE((
				SUM(proportion) OVER (
					PARTITION BY #tt3.hhsize
					,#tt3.date_of_activity_db1 ORDER BY viewer_size ASC rows BETWEEN unbounded preceding
							AND 1 preceding
					)
				), 0) / norm
		,'Upper_limit' = SUM(proportion) OVER (
			PARTITION BY #tt3.hhsize
			,#tt3.date_of_activity_db1 ORDER BY viewer_size ASC rows BETWEEN unbounded preceding
					AND CURRENT row
			) / norm
	INTO v289_vsizealloc_matrix_big_sv
	FROM #tt3
	JOIN #tt4 ON #tt3.hhsize = #tt4.hhsize
		AND #tt3.date_of_activity_db1 = #tt4.date_of_activity_db1 message convert(TIMESTAMP, now()) || ' | Begining M05.1 - Generating Matrices' TO client

	IF EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = user_name()
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('v289_nonviewers_matrix_sv')
			)
		DROP TABLE v289_nonviewers_matrix_sv

	COMMIT WORK

	SELECT thedate
		,gender
		,ageband
		,hhsize
		,'PIV' = 1.0 - convert(DECIMAL(15, 4), viewers_count) / convert(DECIMAL(15, 4), base_person_count)
	INTO v289_nonviewers_matrix_sv
	FROM (
		SELECT base.thedate
			,'gender' = CASE 
				WHEN age <= 19
					THEN 'U'
				WHEN sex = 'Male'
					THEN 'M'
				ELSE 'F'
				END
			,'ageband' = CASE 
				WHEN age <= 19
					THEN '0-19'
				WHEN age BETWEEN 20
						AND 44
					THEN '20-44'
				ELSE '45+'
				END
			,hhsize
			,'viewers_count' = sum(CASE 
					WHEN viewers.person_number IS NULL
						THEN 0
					ELSE processing_weight
					END)
			,'base_person_count' = sum(processing_weight)
		FROM (
			SELECT hhd_views.thedate
				,a.house_id
				,a.person
				,a.age
				,a.sex
			FROM skybarb_sv AS a
			JOIN (
				SELECT 'thedate' = convert(DATE, start_time_of_session)
					,household_number
				FROM skybarb_sv_fullview_sv
				GROUP BY thedate
					,household_number
				) AS hhd_views ON a.house_id = hhd_views.household_number
			) AS base
		JOIN (
			SELECT 'house_id' = household_number
				,'hhsize' = count(1)
			FROM barb_weights_sv
			GROUP BY house_id
			) AS s ON base.house_id = s.house_id
		JOIN barb_weights_sv AS w ON base.house_id = w.household_number
			AND base.person = w.person_number
		LEFT OUTER JOIN (
			SELECT 'thedate' = convert(DATE, start_time_of_session)
				,household_number
				,person_number
			FROM skybarb_sv_fullview_sv
			GROUP BY thedate
				,household_number
				,person_number
			) AS viewers ON base.thedate = viewers.thedate
			AND base.house_id = viewers.household_number
			AND base.person = viewers.person_number
		GROUP BY base.thedate
			,gender
			,ageband
			,hhsize
		) AS summary

	IF EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = user_name()
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('v289_genderage_matrix_sv')
			)
		DROP TABLE v289_genderage_matrix_sv

	COMMIT WORK

	SELECT base.*
		,'PIV' = convert(DECIMAL(15, 4), base.uk_hhwatched) / convert(DECIMAL(15, 4), totals.tot_uk_hhwatched)
		,segment_weight
	INTO v289_genderage_matrix_sv
	FROM #base AS base
	JOIN #viewer_weights AS v ON base.thedate = v.thedate
		AND base.segment_id = v.segment_id
		AND base.hhsize = v.hhsize
		AND base.full_session_flag = v.full_session_flag
		AND base.sex = v.sex
		AND base.ageband = v.ageband
	JOIN (
		SELECT thedate
			,segment_id
			,'tot_uk_hhwatched' = sum(uk_hhwatched)
		FROM #base
		GROUP BY thedate
			,segment_id
		) AS totals ON base.thedate = totals.thedate
		AND base.segment_id = totals.segment_id
	WHERE totals.tot_uk_hhwatched > 0

	COMMIT WORK

	CREATE lf INDEX lf1 ON v289_genderage_matrix_sv (segment_id)

	COMMIT WORK

	GRANT SELECT
		ON v289_genderage_matrix_sv
		TO vespa_group_low_security

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M05.1: Sex/Age Matrix Generation DONE (v289_genderage_matrix_sv)' TO client

	IF EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = user_name()
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('v289_sessionsize_matrix_sv_sv')
			)
		DROP TABLE v289_sessionsize_matrix_sv_sv

	COMMIT WORK

	SELECT base.thedate
		,base.segment_id
		,base.viewing_size
		,base.session_size
		,'uk_hours_watched' = SUM(uk_hhwatched)
		,tot_uk_hhwatched
		,'proportion' = convert(DECIMAL(15, 4), uk_hours_watched) / convert(DECIMAL(15, 4), totals.tot_uk_hhwatched)
		,'Lower_limit' = coalesce((
				SUM(proportion) OVER (
					PARTITION BY base.thedate
					,base.segment_ID
					,base.viewing_size ORDER BY base.session_size ASC rows BETWEEN unbounded preceding
							AND 1 preceding
					)
				), 0)
		,'Upper_limit' = SUM(proportion) OVER (
			PARTITION BY base.thedate
			,base.segment_ID
			,base.viewing_size ORDER BY base.session_size ASC rows BETWEEN unbounded preceding
					AND CURRENT row
			)
	INTO v289_sessionsize_matrix_sv_sv
	FROM #base2 AS base
	JOIN (
		SELECT thedate
			,segment_id
			,viewing_size
			,'tot_uk_hhwatched' = sum(uk_hhwatched)
		FROM #base2
		GROUP BY thedate
			,segment_id
			,viewing_size
		) AS totals ON base.thedate = totals.thedate
		AND base.segment_id = totals.segment_id
		AND base.viewing_size = totals.viewing_size
	WHERE totals.tot_uk_hhwatched > 0
	GROUP BY base.thedate
		,base.segment_id
		,base.viewing_size
		,base.session_size
		,tot_uk_hhwatched

	COMMIT WORK

	CREATE lf INDEX lf1 ON v289_sessionsize_matrix_sv_sv (segment_id)

	CREATE lf INDEX lf2 ON v289_sessionsize_matrix_sv_sv (viewing_size)

	COMMIT WORK

	GRANT SELECT ON v289_sessionsize_matrix_sv_sv TO vespa_group_low_security

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M05.1: Session size Matrix Generation DONE (v289_sessionsize_matrix_sv_sv)' TO client

	IF EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = user_name()
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('v289_sessionsize_matrix_sv_sv_ID')
			)
		DROP TABLE v289_sessionsize_matrix_sv_sv_ID

	COMMIT WORK

	SELECT segment_id
		,session_size
		,viewing_size
		,'uk_hours_watched' = SUM(v289_sessionsize_matrix_sv_sv.uk_hours_watched)
		,'total_watched' = SUM(uk_hours_watched) OVER (
			PARTITION BY segment_id
			,viewing_size
			)
		,'proportion' = convert(DECIMAL(15, 4), uk_hours_watched) / convert(DECIMAL(15, 4), total_watched)
	INTO v289_sessionsize_matrix_sv_sv_ID
	FROM v289_sessionsize_matrix_sv_sv
	GROUP BY segment_id
		,session_size
		,viewing_size

	CREATE hg INDEX hg1 ON v289_sessionsize_matrix_sv_sv_ID (session_size)

	CREATE hg INDEX hg2 ON v289_sessionsize_matrix_sv_sv_ID (viewing_size)

	COMMIT WORK

	SET @min = (
			SELECT min(proportion) / 2
			FROM v289_sessionsize_matrix_sv_sv_ID
			WHERE proportion > 0
			)

	IF EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = user_name()
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('v289_sessionsize_matrix_sv_sv_DEFAULT')
			)
		DROP TABLE v289_sessionsize_matrix_sv_sv_default

	SELECT 'id' = row_num
	INTO #tnum
	FROM sa_rowgenerator(1, 8)

	COMMIT WORK

	SELECT a.segment_ID
		,b.viewing_size
		,sx.session_size
		,'proportion' = COALESCE(c.proportion, @min)
		,'norm' = SUM(proportion) OVER (
			PARTITION BY a.segment_ID
			,b.viewing_size
			)
		,'Lower_limit' = coalesce((
				SUM(proportion) OVER (
					PARTITION BY a.segment_ID
					,b.viewing_size ORDER BY sx.session_size ASC rows BETWEEN unbounded preceding
							AND 1 preceding
					) / norm
				), 0)
		,'Upper_limit' = coalesce((
				SUM(proportion) OVER (
					PARTITION BY a.segment_ID
					,b.viewing_size ORDER BY sx.session_size ASC rows BETWEEN unbounded preceding
							AND CURRENT row
					) / norm
				), 0)
	INTO v289_sessionsize_matrix_sv_sv_default
	FROM (
		SELECT DISTINCT segment_ID
		FROM V289_PIV_Grouped_Segments_desc_sv
		) AS a
	CROSS JOIN (
		SELECT 'viewing_size' = id
		FROM #tnum
		) AS b
	CROSS JOIN (
		SELECT 'session_size' = id
		FROM #tnum
		) AS sx
	LEFT OUTER JOIN v289_sessionsize_matrix_sv_sv_ID AS c ON a.segment_id = c.segment_id
		AND b.viewing_size = c.viewing_size
		AND c.session_size = sx.session_size
		AND c.proportion > 0
	WHERE b.viewing_size >= sx.session_size

	DELETE
	FROM v289_sessionsize_matrix_sv_sv_default
	WHERE session_size > viewing_size

	COMMIT WORK

	CREATE lf INDEX UW ON v289_sessionsize_matrix_sv_sv_default (segment_ID)

	CREATE lf INDEX UQ ON v289_sessionsize_matrix_sv_sv_default (viewing_size)

	COMMIT WORK

	DROP TABLE v289_sessionsize_matrix_sv_sv_ID

	GRANT ALL PRIVILEGES
		ON v289_sessionsize_matrix_sv_sv_default
		TO vespa_group_low_security

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M05.1: DEFAULT Session size Matrix Generation DONE (v289_sessionsize_matrix_sv_sv_default)' TO client message convert(TIMESTAMP, now()) || ' | @ M05.1: Generating Matrices DONE' TO client message convert(TIMESTAMP, now()) || ' | M05 Finished' TO client

END
GO
