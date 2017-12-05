create or replace procedure ${SQLFILE_ARG001}.v289_m15_non_viewers_assignment (@event_date DATE = NULL)
AS
BEGIN
	DECLARE @age VARCHAR(10)
	DECLARE @sex VARCHAR(10)
	DECLARE @iteration SMALLINT
	DECLARE @succ_alloc_total SMALLINT
	DECLARE @scaling_count SMALLINT
	DECLARE @total_alloc SMALLINT
	DECLARE @max_i SMALLINT
	DECLARE @hhsize SMALLINT
	DECLARE @nv_default_prop REAL message convert (
		TIMESTAMP
		,now()
		) || ' | Begining M09.0 - Initialising Environment' TO client

	IF EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND upper(tname) = upper('temp_house')
				AND tabletype = 'TABLE'
			)
		DROP TABLE ${SQLFILE_ARG001}.temp_house

	SELECT 'row_id' = MIN(dt.row_id)
		,dt.account_number
		,'kid' = MAX(CASE 
				WHEN person_ageband IN (
						'0-11'
						,'12-19'
						)
					THEN 1
				ELSE 0
				END)
		,'twenties' = MAX(CASE 
				WHEN person_ageband = '20-24'
					THEN 1
				ELSE 0
				END)
		,'household_size' = CASE 
			WHEN dt.household_size > 8
				THEN 8
			ELSE dt.household_size
			END
		,'random1' = convert(REAL, NULL)
		,'viewing_size' = convert(TINYINT, NULL)
		,'dif_viewer' = convert(TINYINT, NULL)
	INTO temp_house
	FROM V289_M08_SKY_HH_composition AS dt
	JOIN (
		SELECT DISTINCT account_number
		FROM V289_M07_dp_data
		) AS b ON b.account_number = dt.account_number
	WHERE panel_flag = 1
	GROUP BY dt.account_number
		,dt.household_size message convert(TIMESTAMP, now()) || ' | @ M15.1: temp_house Table created: ' || @@rowcount TO client

	COMMIT WORK

	UPDATE temp_house
	SET random1 = RAND(row_id + DATEPART(us, GETDATE()))

	CREATE hg INDEX cide1 ON ${SQLFILE_ARG001}.temp_house (account_number)

	CREATE lf INDEX icde2 ON ${SQLFILE_ARG001}.temp_house (household_size)

	CREATE hg INDEX icde3 ON ${SQLFILE_ARG001}.temp_house (random1)

	CREATE lf INDEX icde4 ON ${SQLFILE_ARG001}.temp_house (dif_viewer)

	CREATE lf INDEX icde5 ON ${SQLFILE_ARG001}.temp_house (kid)

	CREATE lf INDEX icde6 ON ${SQLFILE_ARG001}.temp_house (twenties)

	COMMIT WORK

	IF EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND upper(tname) = upper('temp_inds')
				AND tabletype = 'TABLE'
			)
		DROP TABLE ${SQLFILE_ARG001}.temp_inds

	SELECT dt.account_number
		,dt.hh_person_number
		,dt.household_size
		,'nonviewer_size' = convert(SMALLINT, 0)
		,'age' = person_ageband
		,'sex' = person_gender
		,'random1' = RAND(dt.row_id + DATEPART(us, GETDATE()))
		,'running_agesex_hhcount' = convert(SMALLINT, 0)
		,'allocatable' = convert(SMALLINT, 0)
		,'running_allocs' = convert(INTEGER, 0)
		,'non_viewer' = 0
		,'piv' = convert(REAL, 0)
	INTO temp_inds
	FROM V289_M08_SKY_HH_composition AS dt
	JOIN temp_house AS b ON b.account_number = dt.account_number
	WHERE dt.account_number IS NOT NULL
		AND panel_flag = 1 message convert(TIMESTAMP, now()) || ' | @ M15.1: temp_inds Table created: ' || @@rowcount TO client

	COMMIT WORK

	CREATE hg INDEX ide1 ON ${SQLFILE_ARG001}.temp_inds (account_number)

	CREATE lf INDEX ide2 ON ${SQLFILE_ARG001}.temp_inds (hh_person_number)

	CREATE lf INDEX ide3 ON ${SQLFILE_ARG001}.temp_inds (sex)

	CREATE lf INDEX ide4 ON ${SQLFILE_ARG001}.temp_inds (age)

	COMMIT WORK

	UPDATE temp_house AS a
	SET viewing_size = viewer_size
		,dif_viewer = household_size - viewer_size
	FROM temp_house AS a
	JOIN v289_vsizealloc_matrix_small AS b ON a.household_size = b.hh_size
		AND a.random1 > b.lower_limit
		AND a.random1 <= b.upper_limit
		AND b.date_of_activity_db1 = @event_date
		AND a.kid = b.kid
		AND a.twenties = b.twenties
	WHERE household_size = 2

	COMMIT WORK

	UPDATE temp_house AS a
	SET viewing_size = viewer_size
		,dif_viewer = household_size - viewer_size
	FROM temp_house AS a
	JOIN v289_vsizealloc_matrix_big AS b ON a.household_size = b.hh_size
		AND a.random1 > b.lower_limit
		AND a.random1 <= b.upper_limit
		AND b.date_of_activity_db1 = @event_date
	WHERE household_size > 2

	COMMIT WORK

	SET @nv_default_prop = .2

	COMMIT WORK

	UPDATE temp_inds AS i
	SET nonviewer_size = coalesce(th.dif_viewer, ceiling(th.household_size * @nv_default_prop))
	FROM temp_inds AS i
	JOIN temp_house AS th ON th.account_number = i.account_number

	COMMIT WORK

	UPDATE temp_house
	SET viewing_size = 1
		,dif_viewer = 0
	WHERE household_size = 1 message convert(TIMESTAMP, now()) || ' | @ M15.1: temp_house Table updated: ' || @@rowcount TO client message convert(TIMESTAMP, now()) || ' | @ M15.2: New viewer_size assigned : ' || @@rowcount TO client

	UPDATE temp_inds AS i
	SET piv = vm.piv
	FROM temp_inds AS i
	JOIN v289_nonviewers_matrix AS vm ON vm.gender = i.sex
		AND vm.ageband = i.age
		AND vm.hhsize = i.household_size
		AND i.household_size <= 8
		AND thedate = @event_date

	COMMIT WORK

	UPDATE temp_inds AS i
	SET piv = vm.piv
	FROM temp_inds AS i
	JOIN v289_nonviewers_matrix AS vm ON vm.gender = i.sex
		AND vm.ageband = i.age
		AND vm.hhsize = 8
		AND i.household_size > 8
		AND thedate = @event_date

	COMMIT WORK

	SELECT account_number
		,hh_person_number
		,'running_agesex_hhcount' = dense_rank() OVER (
			PARTITION BY account_number
			,sex
			,age
			,household_size ORDER BY random1 ASC
			)
	INTO #agesex_count
	FROM temp_inds

	COMMIT WORK

	UPDATE temp_inds AS i
	SET running_agesex_hhcount = as_c.running_agesex_hhcount
	FROM temp_inds AS i
	JOIN #agesex_count AS as_c ON as_c.account_number = i.account_number
		AND as_c.hh_person_number = i.hh_person_number

	COMMIT WORK

	DROP TABLE #agesex_count

	COMMIT WORK

	SELECT age
		,sex
		,household_size
		,'person_count' = count()
		,'piv' = avg(piv)
	INTO #counts
	FROM temp_inds
	GROUP BY age
		,sex
		,household_size message '5' TO client

	COMMIT WORK

	IF EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND upper(tname) = upper('age_sex_allocs')
				AND tabletype = 'TABLE'
			)
		DROP TABLE ${SQLFILE_ARG001}.age_sex_allocs

	SELECT 'sex' = vm.gender
		,'age' = vm.ageband
		,household_size
		,'total_indivs' = coalesce(c1.person_count, 0)
		,'alloc_reqd' = ceil(c1.piv * coalesce(c1.person_count, 0))
		,'id' = row_number() OVER (
			ORDER BY vm.piv DESC
			)
		,'nv_piv' = c1.piv
	INTO age_sex_allocs
	FROM v289_nonviewers_matrix AS vm
	JOIN #counts AS c1 ON c1.sex = vm.gender
		AND c1.age = vm.ageband
		AND c1.household_size = vm.hhsize
		AND vm.thedate = @event_date
		AND vm.hhsize > 1

	SELECT household_size
		,'hh_nv_piv' = sum(alloc_reqd) / sum(total_indivs)
	INTO #nv_perc_sexage
	FROM age_sex_allocs
	GROUP BY household_size

	SELECT household_size
		,'hh_nv_reqd' = sum(convert(REAL, dif_viewer)) / sum(convert(REAL, household_size))
	INTO #a1
	FROM temp_house
	WHERE household_size > 1
		AND household_size < 8
	GROUP BY household_size

	SELECT 'household_size' = 8
		,'hh_nv_reqd' = sum(convert(REAL, dif_viewer)) / sum(convert(REAL, household_size))
	INTO #a2
	FROM temp_house
	WHERE household_size >= 8
	GROUP BY household_size

	SELECT household_size
		,hh_nv_reqd
	INTO #nv_perc_sexage_reqd
	FROM (
		SELECT *
		FROM #a1
		
		UNION ALL
		
		SELECT *
		FROM #a2
		) AS b

	DROP TABLE #a1

	DROP TABLE #a2

	IF EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND upper(tname) = upper('age_sex_allocs2')
				AND tabletype = 'TABLE'
			)
		DROP TABLE ${SQLFILE_ARG001}.age_sex_allocs2

	SELECT 'sex' = vm.gender
		,'age' = vm.ageband
		,c1.household_size
		,'total_indivs' = coalesce(c1.person_count, 0)
		,'alloc_reqd' = ceil(vm.piv * coalesce(c1.person_count, 0) * coalesce(npsr.hh_nv_reqd, 0) / nullif(nps.hh_nv_piv, 0))
		,'id' = row_number() OVER (
			ORDER BY vm.piv DESC
			)
		,'nv_piv' = vm.piv
	INTO age_sex_allocs2
	FROM v289_nonviewers_matrix AS vm
	JOIN #counts AS c1 ON c1.sex = vm.gender
		AND c1.age = vm.ageband
		AND c1.household_size = vm.hhsize
	JOIN #nv_perc_sexage AS nps ON c1.household_size = nps.household_size
	JOIN #nv_perc_sexage_reqd AS npsr ON c1.household_size = npsr.household_size
		AND vm.thedate = @event_date
		AND vm.hhsize > 1

	COMMIT WORK

	DROP TABLE #counts

	DROP TABLE #nv_perc_sexage

	DROP TABLE #nv_perc_sexage_reqd message '11aaaaa' TO client message convert(TIMESTAMP, now()) || ' | @ M15.1: Starting Allocation Loop: ' TO console

	SET @max_i = (
			SELECT count(id)
			FROM age_sex_allocs2
			)
	SET @iteration = 1

	WHILE @iteration <= @max_i
	BEGIN
		SET @age = (
				SELECT age
				FROM age_sex_allocs2
				WHERE id = @iteration
				)
		SET @sex = (
				SELECT sex
				FROM age_sex_allocs2
				WHERE id = @iteration
				)
		SET @hhsize = (
				SELECT household_size
				FROM age_sex_allocs2
				WHERE id = @iteration
				)
		SET @total_alloc = (
				SELECT alloc_reqd
				FROM age_sex_allocs2
				WHERE id = @iteration
				) message convert(TIMESTAMP, now()) || ' | @ M15.1: ' || @iteration || ' - Age ' || @age || ' - Sex:' || @sex || ' -Hhsize: ' || @hhsize TO client

		SELECT account_number
			,'sum_nv' = sum(non_viewer) OVER (PARTITION BY account_number)
		INTO #nv_sum
		FROM temp_inds

		UPDATE temp_inds AS i
		SET nonviewer_size = coalesce((th.dif_viewer - nvs.sum_nv), @nv_default_prop)
		FROM temp_inds AS i
		JOIN temp_house AS th ON th.account_number = i.account_number
		JOIN #nv_sum AS nvs ON nvs.account_number = i.account_number

		COMMIT WORK

		UPDATE temp_inds AS i
		SET allocatable = CASE 
				WHEN running_agesex_hhcount <= nonviewer_size
					AND i.household_size > 1
					AND non_viewer = 0
					THEN 1
				ELSE 0
				END
		FROM temp_inds AS i
		WHERE age = @age
			AND household_size = @hhsize
			AND sex = @sex

		COMMIT WORK

		SELECT account_number
			,hh_person_number
			,'running_allocs' = sum(allocatable) OVER (
				PARTITION BY sex
				,age
				,household_size ORDER BY random1 ASC
				)
		INTO #allocs
		FROM temp_inds
		WHERE age = @age
			AND household_size = @hhsize
			AND sex = @sex

		COMMIT WORK

		UPDATE temp_inds AS i
		SET running_allocs = al.running_allocs
		FROM temp_inds AS i
		JOIN #allocs AS al ON al.account_number = i.account_number
			AND al.hh_person_number = i.hh_person_number

		COMMIT WORK

		UPDATE temp_inds AS i
		SET non_viewer = 1
		WHERE age = @age
			AND sex = @sex
			AND household_size = @hhsize
			AND running_allocs <= @total_alloc
			AND allocatable = 1
			AND non_viewer = 0

		COMMIT WORK

		SET @succ_alloc_total = (
				SELECT count()
				FROM temp_inds
				WHERE age = @age
					AND sex = @sex
					AND household_size = @hhsize
					AND non_viewer = 1
				) message convert(TIMESTAMP, now()) || ' | @ M15.1: allocated ' || @succ_alloc_total TO client message convert(TIMESTAMP, now()) || ' | @ M15.1: out of ' || @total_alloc TO client

		DROP TABLE #allocs

		DROP TABLE #nv_sum

		SET @iteration = @iteration + 1
	END

	SELECT account_number
		,'sum_nv' = sum(non_viewer) OVER (PARTITION BY account_number)
	INTO #nv_sum
	FROM temp_inds

	UPDATE temp_inds AS i
	SET nonviewer_size = coalesce((th.dif_viewer - nvs.sum_nv), @nv_default_prop)
	FROM temp_inds AS i
	JOIN temp_house AS th ON th.account_number = i.account_number
	JOIN #nv_sum AS nvs ON nvs.account_number = i.account_number

	COMMIT WORK

	SELECT account_number
		,hh_person_number
		,'running_agesex_hhcount' = dense_rank() OVER (
			PARTITION BY account_number
			,non_viewer ORDER BY random1 ASC
			)
	INTO #account_count
	FROM temp_inds

	COMMIT WORK

	UPDATE temp_inds AS i
	SET running_agesex_hhcount = as_c.running_agesex_hhcount
	FROM temp_inds AS i
	JOIN #account_count AS as_c ON as_c.account_number = i.account_number
		AND as_c.hh_person_number = i.hh_person_number

	COMMIT WORK

	UPDATE temp_inds AS i
	SET non_viewer = 1
	FROM temp_inds AS i
	WHERE running_agesex_hhcount <= nonviewer_size
		AND i.household_size > 1
		AND non_viewer = 0

	DROP TABLE #account_count

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M15.2: Non Viewer Update: ' || @@rowcount TO client

	COMMIT WORK

	UPDATE V289_M08_SKY_HH_composition AS m08
	SET non_viewer = 0

	COMMIT WORK

	UPDATE V289_M08_SKY_HH_composition AS m08
	SET non_viewer = i.non_viewer
	FROM temp_inds AS i
	WHERE m08.account_number = i.account_number
		AND m08.hh_person_number = i.hh_person_number
		AND i.non_viewer = 1

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M15.3: Non Viewer Assigned: ' || @@rowcount TO client

	UPDATE V289_M08_SKY_HH_composition AS m08
	SET viewer_hhsize = household_size

	COMMIT WORK

	UPDATE V289_M08_SKY_HH_composition AS m08
	SET viewer_hhsize = viewer_hhsize - a.adj_hhsize
	FROM (
		SELECT account_number
			,'adj_hhsize' = count(1)
		FROM V289_M08_SKY_HH_composition
		WHERE non_viewer = 1
		GROUP BY account_number
		) AS a
	WHERE m08.account_number = a.account_number

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M15.4: Non-viewers adjusted hhsize: ' || @@rowcount TO client

	COMMIT WORK

	SELECT account_number
		,'viewer_hhsize' = max(viewer_hhsize)
	INTO #viewer_size
	FROM V289_M08_SKY_HH_composition
	WHERE panel_flag = 1
	GROUP BY account_number

	COMMIT WORK

	CREATE hg INDEX hg1 ON #viewer_size (account_number)

	COMMIT WORK

	UPDATE V289_M07_dp_data AS m07
	SET viewer_hhsize = h.viewer_hhsize
	FROM #viewer_size AS h
	WHERE m07.account_number = h.account_number

	COMMIT WORK

	COMMIT WORK

	DROP TABLE #viewer_size

	DROP TABLE #nv_sum

	COMMIT WORK
END;
GO 
commit;
