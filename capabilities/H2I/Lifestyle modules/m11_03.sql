CREATE OR REPLACE  PROCEDURE V289_M11_03_SC3I_v1_1__add_individual_data_sv (
	@profiling_thursday DATE
	,@batch_date DATETIME = now()
	,@Scale_refresh_logging_ID BIGINT = NULL
	)
AS
BEGIN
	DECLARE @QA_catcher INTEGER

	COMMIT WORK

	DELETE
	FROM SC3I_Sky_base_segment_snapshots_sv
	WHERE profiling_date = @profiling_thursday

	COMMIT WORK

	INSERT INTO SC3I_Sky_base_segment_snapshots_sv (
		account_number
		,profiling_date
		,HH_person_number
		,population_scaling_segment_id
		,vespa_scaling_segment_id
		,expected_boxes
		)
	SELECT DISTINCT
		b.account_number
		,b.profiling_date
		,d.HH_person_number
		,l_sc3i.scaling_segment_id
		,l_sc3i.scaling_segment_id
		,b.expected_boxes
	FROM SC3_Sky_base_segment_snapshots_sv AS b
	JOIN V289_M08_SKY_HH_composition_sv AS d ON b.account_number = d.account_number
	JOIN vespa_analysts.SC3_Segments_lookup_v1_1 AS l_sc3 ON b.population_scaling_segment_id = l_sc3.scaling_segment_id
	JOIN jsk01.SC3I_Segments_Lookup_vkuba AS l_sc3i ON l_sc3.isba_tv_region = l_sc3i.isba_tv_region
		AND l_sc3.package = l_sc3i.package
		AND d.person_head = l_sc3i.head_of_hhd
		AND d.person_gender || ' ' || d.person_ageband = l_sc3i.age_band
		AND CASE 
			WHEN d.household_size > 4
				THEN '5+'
			ELSE convert(VARCHAR(2), d.household_size)
			END = l_sc3i.hh_size
	WHERE b.profiling_date = @profiling_thursday

	COMMIT WORK

	SELECT account_number
	INTO #vespa_viewer_accounts
	FROM V289_M10_session_individuals_sv
	GROUP BY account_number

	COMMIT WORK

	CREATE hg INDEX hg1 ON #vespa_viewer_accounts (account_number)

	COMMIT WORK

	SELECT account_number
		,hh_person_number
	INTO #vespa_viewer_individuals
	FROM V289_M10_session_individuals_sv
	GROUP BY account_number
		,hh_person_number

	COMMIT WORK
	CREATE hg INDEX hg1 ON #vespa_viewer_individuals (account_number)
	CREATE hg INDEX hg2 ON #vespa_viewer_individuals (hh_person_number)
	COMMIT WORK

	SELECT m08.account_number
		,m08.hh_person_number
		,'viewed_tv' = CASE 
			WHEN ind.hh_person_number IS NOT NULL
				THEN 'Yes'
			ELSE 'NV - Viewing HHD'
			END
		,m08.person_head
		,'age_band' = m08.person_gender || ' ' || m08.person_ageband
		,'hhsize_capped' = CASE 
			WHEN m08.household_size > 4
				THEN '5+'
			ELSE convert(VARCHAR(2), m08.household_size)
			END
	INTO #t3
	FROM V289_M08_SKY_HH_composition_sv AS m08
	JOIN #vespa_viewer_accounts AS acc ON m08.account_number = acc.account_number
	LEFT OUTER JOIN #vespa_viewer_individuals AS ind ON acc.account_number = ind.account_number
		AND m08.account_number = ind.account_number
		AND m08.hh_person_number = ind.hh_person_number
	WHERE m08.panel_flag = 1
		AND m08.nonviewer_household = 0

	INSERT INTO #t3
	SELECT m08.account_number
		,m08.hh_person_number
		,'viewed_tv' = 'NV - NonView HHD'
		,m08.person_head
		,'age_band' = m08.person_gender || ' ' || m08.person_ageband
		,'hhsize_capped' = CASE 
			WHEN m08.household_size > 4
				THEN '5+'
			ELSE convert(VARCHAR(2), m08.household_size)
			END
	FROM V289_M08_SKY_HH_composition_sv AS m08
	WHERE m08.panel_flag = 0
		AND m08.nonviewer_household = 1

	COMMIT WORK
	CREATE hg INDEX ind1 ON #t3 (account_number)
	CREATE lf INDEX ind2 ON #t3 (hh_person_number)
	
	TRUNCATE TABLE jsk01.SC3I_Todays_panel_members_sv

	COMMIT WORK

	INSERT INTO SC3I_Todays_panel_members_sv
	SELECT t.account_number
		,t.HH_person_number
		,l_sc3i.scaling_segment_id
	FROM #t3 AS t
	JOIN SC3_Todays_panel_members_sv AS p ON t.account_number = p.account_number
	JOIN vespa_analysts.SC3_Segments_lookup_v1_1 AS l_sc3 ON p.scaling_segment_id = l_sc3.scaling_segment_id
	JOIN jsk01.SC3I_Segments_Lookup_vkuba AS l_sc3i ON l_sc3.isba_tv_region = l_sc3i.isba_tv_region
		AND l_sc3.package = l_sc3i.package
		AND t.person_head = l_sc3i.head_of_hhd
		AND t.age_band = l_sc3i.age_band
		AND t.hhsize_capped = l_sc3i.hh_size
		AND t.viewed_tv = l_sc3i.viewed_tv
	WHERE t.viewed_tv IN (
			'Yes'
			,'NV - Viewing HHD'
			)

	COMMIT WORK

	INSERT INTO SC3I_Todays_panel_members_sv
	SELECT t.account_number
		,t.HH_person_number
		,'scaling_segment_id' = p.population_scaling_segment_id
	FROM #t3 AS t
	JOIN SC3I_Sky_base_segment_snapshots_sv AS p ON t.account_number = p.account_number
		AND t.hh_person_number = p.hh_person_number
	JOIN jsk01.SC3I_Segments_Lookup_vkuba AS l ON p.population_scaling_segment_id = l.scaling_segment_id
	WHERE t.viewed_tv = 'NV - NonView HHD'
		AND l.viewed_tv = 'NV - NonViewing HHD'
		AND p.profiling_date = @profiling_thursday

	COMMIT WORK
END
