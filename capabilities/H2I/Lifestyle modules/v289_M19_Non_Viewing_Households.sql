create or replace procedure ${SQLFILE_ARG001}.v289_M19_Non_Viewing_Households (@processing_day DATE)
AS
BEGIN
	message convert(TIMESTAMP, now()) || ' | Begining M19.0 - Initialising Environment' TO client message convert(TIMESTAMP, now()) || ' | @ M19.0: Initialising Environment DONE' TO client message convert(TIMESTAMP, now()) || ' | Begining M19.1 - NV viewing hhd profile from Barb' TO client

	SELECT DISTINCT household_number
	INTO #viewing_hhds
	FROM skybarb_fullview
	WHERE DATE (start_time_of_session) = @processing_day

	COMMIT WORK

	CREATE hg INDEX ind1 ON #viewing_hhds (household_number)

	COMMIT WORK

	SELECT 'household_number' = h.house_id
		,'person_number' = h.person
		,h.age
		,'gender' = CASE WHEN age <= 19
				THEN 'U' 
			WHEN h.sex = 'Male' THEN 'M'
			WHEN h.sex = 'Female' THEN 'F'
			END
		,'ageband' = CASE  WHEN age <= 11 THEN '0-11'
			WHEN age BETWEEN 12 AND 19 THEN '12-19'
			WHEN age BETWEEN 20 AND 24 THEN '20-24'
			WHEN age BETWEEN 25 AND 34 THEN '25-34'
			WHEN age BETWEEN 35 AND 44 THEN '35-44'
			WHEN age BETWEEN 45 AND 64 THEN '45-64'
			WHEN age >= 65 THEN '65+'
			END
		,'hhsize' = count(1) OVER (PARTITION BY h.house_id)
		,'processing_weight' = w.processing_weight
	INTO #barb_inds_with_sky
	FROM skybarb AS h
	JOIN barb_weights AS w ON h.house_id = w.household_number
		AND h.person = w.person_number

	COMMIT WORK

	CREATE hg INDEX ind1 ON #barb_inds_with_sky (household_number)

	COMMIT WORK

	SELECT DISTINCT a.hhsize
		,'nv_hhd_status' = CASE 
			WHEN b.household_number IS NOT NULL
				THEN 'Viewing_HHD'
			ELSE 'NonViewing_HHD'
			END
		,'wieght' = sum(processing_weight) OVER (
			PARTITION BY a.hhsize
			,nv_hhd_status
			)
		,'hh_size_wieght' = sum(processing_weight) OVER (PARTITION BY a.hhsize)
		,'piv' = wieght / convert(REAL, hh_size_wieght)
	INTO #nv_hhd_piv
	FROM #barb_inds_with_sky AS a
	LEFT OUTER JOIN #viewing_hhds AS b ON a.household_number = b.household_number

	COMMIT WORK

	CREATE hg INDEX ind1 ON #nv_hhd_piv (hhsize)

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | Begining M19.1 - NV viewing hhd profile from Barb - DONE' TO client message convert(TIMESTAMP, now()) || ' | Begining M19.2 - Assign NV HHDs' TO client

	UPDATE V289_M08_SKY_HH_COMPOSITION
	SET nonviewer_household = 0

	COMMIT WORK

	UPDATE V289_M08_SKY_HH_COMPOSITION
	SET randd = RAND(exp_cb_key_db_person + DATEPART(us, GETDATE()))

	COMMIT WORK

	SELECT household_size
		,'reqd_nvs' = ceil(count(account_number) / (1 - avg(nv.piv))) - count(account_number)
	INTO #hhd_panel_counts
	FROM V289_M08_SKY_HH_COMPOSITION AS m08
	JOIN #nv_hhd_piv AS nv ON m08.household_size = nv.hhsize
	WHERE panel_flag = 1
		AND m08.person_head = '1'
		AND nv_hhd_status = 'NonViewing_HHD'
	GROUP BY household_size

	SELECT hhc.account_number
		,household_size
		,'ranknum' = rank() OVER (
			PARTITION BY household_size ORDER BY randd ASC
			)
	INTO #aclist
	FROM V289_M08_SKY_HH_composition AS hhc
	WHERE panel_flag = 0
		AND hhc.person_head = '1'

	COMMIT WORK

	SELECT ac.account_number
	INTO #nv_hhds
	FROM #hhd_panel_counts AS hpc
	JOIN #aclist AS ac ON hpc.household_size = ac.household_size
	WHERE ac.ranknum <= hpc.reqd_nvs

	COMMIT WORK

	CREATE hg INDEX ind1 ON #nv_hhds (account_number)

	COMMIT WORK

	UPDATE V289_M08_SKY_HH_COMPOSITION AS m08
	SET nonviewer_household = 1
	FROM #nv_hhds AS nv
	WHERE m08.account_number = nv.account_number

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | Begining M19.2 - Assign NV HHDs - DONE' TO client
END;
GO 
commit;
