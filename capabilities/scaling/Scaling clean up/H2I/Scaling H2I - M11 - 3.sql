--------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------
--- Adds individual level data in some of the scaling tables for Skyview before the Rim Weighting is applied
CREATE
	OR replace PROCEDURE V289_M11_03_SC3I_v1_1__add_individual_data_clean
			@profiling_thursday DATE -- Day on which to do sky base profiling
			,@batch_date DATETIME = now () -- Day on which build was kicked off
AS
BEGIN

  MESSAGE cast(now() as timestamp)||' | M11.3 Start' TO CLIENT
	-- -- Test input arguments
	-- create or replace variable @profiling_thursday   date    =       '2015-02-05';
	-- create or replace variable @batch_date   datetime        =       now();
	-- create or replace variable @Scale_refresh_logging_ID  bigint = 3874;
	-- declare  @profiling_thursday     date    =       '2015-02-05'
	-- declare  @batch_date     datetime        =       now()
	-- declare  @Scale_refresh_logging_ID  bigint = 3874
	-- commit

	-- Clean up data from the current target profiling Thursday
	TRUNCATE TABLE SC3I_Sky_base_segment_snapshots
	COMMIT -- (^_^)

	--- Skybase segments
	-- We can convert the segments from Scaling 3.0 into Skyview scaling segments
	INSERT INTO SC3I_Sky_base_segment_snapshots (
		account_number
		,profiling_date
		,HH_person_number
		,population_scaling_segment_id
		,vespa_scaling_segment_id
		,expected_boxes
		)
	SELECT b.account_number
		,b.profiling_date
		,d.HH_person_number
		,l_sc3i.scaling_segment_id
		,l_sc3i.scaling_segment_id
		,b.expected_boxes
	FROM SC3_Sky_base_segment_snapshots AS b
	INNER JOIN V289_M08_SKY_HH_composition 				AS d 		ON b.account_number = d.account_number
	INNER JOIN vespa_analysts.SC3_Segments_lookup_v1_1 	AS l_sc3 	ON b.population_scaling_segment_id = l_sc3.scaling_segment_id
	INNER JOIN vespa_analysts.SC3I_Segments_lookup_v1_1 AS l_sc3i 	ON l_sc3.isba_tv_region = l_sc3i.isba_tv_region
		AND l_sc3.package = l_sc3i.package
		AND d.person_head = l_sc3i.head_of_hhd
		AND d.person_gender || ' ' || d.person_ageband = l_sc3i.age_band -- combine age and gender into a single attribute
		AND CASE WHEN d.household_size > 7 THEN '8+'
				ELSE cast(d.household_size AS VARCHAR(2)) END = l_sc3i.hh_size
	WHERE b.profiling_date = @profiling_thursday
	
	MESSAGE cast(now() as timestamp)||' | M11.1 SC3I_Sky_base_segment_snapshots Insert. Rows:'||@@rowcount TO CLIENT
	COMMIT -- (^_^)

	-- Ensure only accounts and individuals on Vespa extract is used
	SELECT account_number
	INTO #vespa_viewer_accounts
	FROM V289_M10_session_individuals -- changed from V289_M07_dp_data so only include accounts that make it through the whole H2I process
	GROUP BY account_number

	MESSAGE cast(now() as timestamp)||' | M11.1 #vespa_viewer_accounts Creation. Rows:'||@@rowcount TO CLIENT
	COMMIT -- (^_^)
	CREATE hg INDEX hg1 ON #vespa_viewer_accounts (account_number)
	COMMIT -- (^_^)

	-- Now get the individuals that've been assigned viewing
	SELECT account_number
		,hh_person_number
	INTO #vespa_viewer_individuals
	FROM V289_M10_session_individuals -- changed from V289_M07_dp_data so only include accounts that make it through the whole H2I process
	GROUP BY account_number
		,hh_person_number

	MESSAGE cast(now() as timestamp)||' | M11.1 #vespa_viewer_individuals Creation. Rows:'||@@rowcount TO CLIENT
	
	COMMIT -- (^_^)
	CREATE hg INDEX hg1 ON #vespa_viewer_individuals (account_number)
	CREATE hg INDEX hg2 ON #vespa_viewer_individuals (hh_person_number)
	COMMIT -- (^_^)

	-- Pick up all individuals in an account on the panel
	SELECT m08.account_number
		,m08.hh_person_number
		,CASE 
			WHEN ind.hh_person_number IS NOT NULL
				THEN 'Yes'
			ELSE 'NV - Viewing HHD'
			END AS viewed_tv
		,m08.person_head
		,m08.person_gender || ' ' || m08.person_ageband AS age_band
		,CASE 
			WHEN m08.household_size > 7
				THEN '8+'
			ELSE cast(m08.household_size AS VARCHAR(2))
			END AS hhsize_capped
	INTO #t3
	FROM V289_M08_SKY_HH_composition AS m08
	INNER JOIN #vespa_viewer_accounts AS acc ON m08.account_number = acc.account_number
	LEFT JOIN #vespa_viewer_individuals AS ind ON acc.account_number = ind.account_number -- left join back onto post-individual assignment results so that we can identify viewers/non-viewers
		AND m08.account_number = ind.account_number
		AND m08.hh_person_number = ind.hh_person_number
	WHERE m08.panel_flag = 1
		AND m08.nonviewer_household = 0

	MESSAGE cast(now() as timestamp)||' | M11.1 #t3 Creation. Rows:'||@@rowcount TO CLIENT
	
	-- Now pick up all individuals in NonViewer HHD accounts (not on the panel)
	INSERT INTO #t3
	SELECT m08.account_number
		,m08.hh_person_number
		,'NV - NonView HHD' AS viewed_tv
		,m08.person_head
		,m08.person_gender || ' ' || m08.person_ageband AS age_band
		,CASE 
			WHEN m08.household_size > 7
				THEN '8+'
			ELSE cast(m08.household_size AS VARCHAR(2))
			END AS hhsize_capped
	FROM V289_M08_SKY_HH_composition AS m08
	WHERE m08.panel_flag = 0
		AND m08.nonviewer_household = 1

	MESSAGE cast(now() as timestamp)||' | M11.1 #t3 insert. Rows:'||@@rowcount TO CLIENT
	
	COMMIT -- (^_^)
	CREATE hg INDEX ind1 ON #t3 (account_number)
	CREATE lf INDEX ind2 ON #t3 (hh_person_number)
	COMMIT -- (^_^)

	-- Get Vespa Viewers (and non-viewers) and attach their new SC3I scaling segments
	TRUNCATE TABLE SC3I_Todays_panel_members

	COMMIT -- (^_^)

	-- First deal with real panel members
	INSERT INTO SC3I_Todays_panel_members
	SELECT t.account_number
		,t.HH_person_number
		,l_sc3i.scaling_segment_id
	FROM #t3 AS t
	INNER JOIN SC3_Todays_panel_members AS p ON t.account_number = p.account_number -- we need this join to bring in the old scaling segment ID
	INNER JOIN vespa_analysts.SC3_Segments_lookup_v1_1 AS l_sc3 ON p.scaling_segment_id = l_sc3.scaling_segment_id -- this join to bring in the old scaling segments
	INNER JOIN vespa_analysts.SC3I_Segments_lookup_v1_1 AS l_sc3i ON l_sc3.isba_tv_region = l_sc3i.isba_tv_region -- finally, match the attributes to give us the new SC3I segment IDs
		AND l_sc3.package = l_sc3i.package
		AND t.person_head = l_sc3i.head_of_hhd
		AND t.age_band = l_sc3i.age_band
		AND t.hhsize_capped = l_sc3i.hh_size
		AND t.viewed_tv = l_sc3i.viewed_tv
	WHERE t.viewed_tv IN (
			'Yes'
			,'NV - Viewing HHD'
			)

	MESSAGE cast(now() as timestamp)||' | M11.1 SC3I_Todays_panel_members insert. Rows:'||@@rowcount TO CLIENT
	
	COMMIT -- (^_^)

	-- Now add those selected to be NV HHDS
	INSERT INTO SC3I_Todays_panel_members
	SELECT t.account_number
		,t.HH_person_number
		,p.population_scaling_segment_id AS scaling_segment_id
	FROM #t3 AS t
	INNER JOIN SC3I_Sky_base_segment_snapshots AS p ON t.account_number = p.account_number -- we need this join to bring in the old scaling segment ID
		AND t.hh_person_number = p.hh_person_number
	INNER JOIN vespa_analysts.SC3I_Segments_lookup_v1_1 l ON p.population_scaling_segment_id = l.scaling_segment_id
	WHERE t.viewed_tv = 'NV - NonView HHD'
		AND l.viewed_tv = 'NV - NonViewing HHD'
		AND p.profiling_date = @profiling_thursday
		
	MESSAGE cast(now() as timestamp)||' | M11.1 SC3I_Todays_panel_members 2nd insert. Rows:'||@@rowcount TO CLIENT

	COMMIT -- (^_^)
END;-- of procedure "V289_M11_03_SC3I_v1_1__add_individual_data"

COMMIT;
