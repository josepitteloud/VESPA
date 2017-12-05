----------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------
--------------------------------------------------
-------------------------------------------------- Scaling for H2I
--------------------------------------------------
----------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------
-------------------------------------------------- Running clause example
----------------------------------------------------------------------------------------------------
--exec V289_M11_01_SC3_v1_1__do_weekly_segmentation '2015-02-05', '2014-07-29' -- thurs, batch date
/***********************************************************************************************************************************************
************************************************************************************************************************************************
******* M11: SKYVIEW INDIVIDUAL AND HOUSEOLD LEVEL SCALING SCRIPT                                                                             *******
************************************************************************************************************************************************
***********************************************************************************************************************************************/
--- Skyview scaling uses 2 of the Scaling 3.0 procedures. See the repository for more details
-- \Git_repository\Vespa\ad_hoc\V154 - Scaling 3.0\Vespa Analysts - SC3\SC3 - 3 - refresh procedures [v1.1].sql
-- These procs prepare the Skybase accounts (to be done once a week for a Thursday) and valid Vespa accounts (to be run each day)
--        SC3_v1_1__do_weekly_segmentation  SKYVIEW VERSION: V289_M11_01_SC3_v1_1__do_weekly_segmentation
--        SC3_v1_1__prepare_panel_members   SKYVIEW VERSION: V289_M11_02_SC3_v1_1__prepare_panel_members
--- A new procedure has been written to add individual level data to the scaling tables
--     V289_M11_03_SC3I_v1_1__add_individual_data
--- An existing Scaling 3.0 proc has been ammended to work for SkyView
-- This proc calculates the weights using a RIM Weighting process
--         SC3_v1_1__make_weights           SKYVIEW VERSION: V289_M11_04_SC3I_v1_1__make_weights
/**************** PART L: WEEKLY SEGMENTATION BUILD ****************/

CREATE OR replace PROCEDURE V289_M11_SC3I_v1_2_weekly_segmentation
	 @profiling_thursday DATE = NULL -- Day on which to do sky base profiling
	,@batch_date DATETIME = now () -- Day on which build was kicked off
	AS
BEGIN
	------- Base Tables preparation
	MESSAGE cast(now() as timestamp)||' | M11.1 Start ' TO CLIENT
	
	DELETE FROM SC3_scaling_weekly_sample
	TRUNCATE TABLE SC3_Sky_base_segment_snapshots
	COMMIT -- (^_^)

	-- Decide when we're doing the profiling, if it's not passed in as a parameter
	IF @profiling_thursday IS NULL
	SELECT @profiling_thursday = DATEFORMAT ((now() - datepart(weekday, now())) - 2,'YYYY-MM-DD')
	
	COMMIT -- (^_^)
	MESSAGE cast(now() as timestamp)||' | M11.1 Preparation done' TO CLIENT
	
	/**************** L01: ESTABLISH POPULATION ****************/
	-- We need the segmentation over the whole Sky base so we can scale up
	-- Captures all active accounts in cust_subs_hist
	
	SELECT account_number
		,cb_key_household
		,cb_key_individual
		,current_short_description
		,RANK() OVER (PARTITION BY account_number ORDER BY effective_from_dt DESC ,cb_row_id ) AS RANK
		,CONVERT(BIT, 0) AS uk_standard_account
		,CONVERT(VARCHAR(30), NULL) AS isba_tv_region
	INTO #weekly_sample
	FROM /*sk_prod.*/ cust_subs_hist
	WHERE subscription_sub_type IN ('DTV Primary Viewing')
		AND status_code IN ('AC','AB','PC')
		AND effective_from_dt <= @profiling_thursday
		AND effective_to_dt > @profiling_thursday
		AND effective_from_dt <> effective_to_dt
		AND EFFECTIVE_FROM_DT IS NOT NULL
		AND cb_key_household > 0
		AND cb_key_household IS NOT NULL
		AND cb_key_individual IS NOT NULL
		AND account_number IS NOT NULL
		AND service_instance_id IS NOT NULL

    MESSAGE cast(now() as timestamp)||' | M11.1 #weekly_sample creation. Rows:'||@@rowcount TO CLIENT
        
	-- De-dupes accounts
	COMMIT -- (^_^)
	DELETE FROM #weekly_sample 
	WHERE RANK > 1
	COMMIT -- (^_^)

	-- Create indices
	CREATE UNIQUE INDEX fake_pk ON #weekly_sample (account_number)
	CREATE INDEX for_package_join ON #weekly_sample (current_short_description)
	COMMIT -- (^_^)

	-- Take out ROIs (Republic of Ireland) and non-standard accounts as these are not currently in the scope of Vespa
	UPDATE #weekly_sample
	SET uk_standard_account = CASE WHEN b.acct_type = 'Standard' AND b.account_number <> '?' AND b.pty_country_code = 'GBR' THEN 1
									ELSE 0 END
		,isba_tv_region = CASE 
				WHEN b.isba_tv_region = 'Border' THEN 'NI, Scotland & Border'
				WHEN b.isba_tv_region = 'Central Scotland'	THEN 'NI, Scotland & Border' 	
				WHEN b.isba_tv_region = 'East Of England'	THEN 'Wales & Midlands' 		
				WHEN b.isba_tv_region = 'HTV Wales'		THEN 'Wales & Midlands' 		
				WHEN b.isba_tv_region = 'HTV West'		THEN 'South England' 			
				WHEN b.isba_tv_region = 'London'		THEN 'London' 					
				WHEN b.isba_tv_region = 'Meridian (exc. Channel Islands)'	THEN 'South England' 			
				WHEN b.isba_tv_region = 'Midlands'		THEN 'Wales & Midlands' 		
				WHEN b.isba_tv_region = 'North East'	THEN 'North England' 			
				WHEN b.isba_tv_region = 'North Scotland'	THEN 'NI, Scotland & Border' 	
				WHEN b.isba_tv_region = 'North West'	THEN 'North England'			
				WHEN b.isba_tv_region = 'Not Defined'	THEN 'Not Defined'				
				WHEN b.isba_tv_region = 'South West'	THEN 'South England'			
				WHEN b.isba_tv_region = 'Ulster'		THEN 'NI, Scotland & Border'	
				WHEN b.isba_tv_region = 'Yorkshire'		THEN 'North England'			
				ELSE 'Not Defined'	END
		,cb_key_individual = b.cb_key_individual
	FROM #weekly_sample AS a
	INNER JOIN /*sk_prod.*/ cust_single_account_view AS b ON a.account_number = b.account_number

	MESSAGE cast(now() as timestamp)||' | M11.1 #weekly_sample update' TO CLIENT
	
	COMMIT -- (^_^)
	DELETE FROM #weekly_sample
	WHERE uk_standard_account = 0 
		OR isba_tv_region = 'Not Defined'
	COMMIT -- (^_^)

	/**************** L02: ASSIGN VARIABLES ****************/
	
	-- Populate Package & ISBA TV Region
	INSERT INTO SC3_scaling_weekly_sample (
		account_number
		,cb_key_household
		,cb_key_individual
		,isba_tv_region
		,num_mix
		,mix_pack
		,package
		)
	SELECT fbp.account_number
		,fbp.cb_key_household
		,fbp.cb_key_individual
		,fbp.isba_tv_region -- isba_tv_region
		,cel.Variety + cel.Knowledge + cel.Kids + cel.Style_Culture + cel.Music + cel.News_Events AS num_mix
		,CASE 	WHEN Num_Mix IS NULL OR Num_Mix = 0 	THEN 'Entertainment Pack'
				WHEN (cel.variety = 1 OR cel.style_culture = 1)  AND Num_Mix = 1 THEN 'Entertainment Pack'
				WHEN (cel.variety = 1 AND cel.style_culture = 1) AND Num_Mix = 2 THEN 'Entertainment Pack'
				WHEN Num_Mix > 0 THEN 'Entertainment Extra'
			END AS mix_pack -- Basic package has recently been split into the Entertainment and Entertainment Extra packs
		,CASE 
			WHEN cel.prem_sports = 2 AND cel.prem_movies = 2 THEN 'Movies & Sports' --'Top Tier'
			WHEN cel.prem_sports = 2 AND cel.prem_movies = 0 THEN 'Sports' --'Dual Sports'
			WHEN cel.prem_sports = 0 AND cel.prem_movies = 2 THEN 'Movies' --'Dual Movies'
			WHEN cel.prem_sports = 1 AND cel.prem_movies = 0 THEN 'Sports' --'Single Sports'
			WHEN cel.prem_sports = 0 AND cel.prem_movies = 1 THEN 'Movies' --'Single Movies'
			WHEN cel.prem_sports > 0 OR cel.prem_movies > 0  THEN 'Movies & Sports' --'Other Premiums'
			WHEN cel.prem_movies = 0 AND cel.prem_sports = 0 AND mix_pack = 'Entertainment Pack'  THEN 'Basic' --'Basic - Ent'
			WHEN cel.prem_movies = 0 AND cel.prem_sports = 0 AND mix_pack = 'Entertainment Extra' THEN 'Basic' --'Basic - Ent Extra'
			ELSE 'Basic' END 
	FROM #weekly_sample AS fbp
	LEFT JOIN /*sk_prod.*/ cust_entitlement_lookup AS cel ON fbp.current_short_description = cel.short_description
	WHERE fbp.cb_key_household IS NOT NULL
		AND fbp.cb_key_individual IS NOT NULL

	MESSAGE cast(now() as timestamp)||' | M11.1 SC3_scaling_weekly_sample insert. Rows:'||@@rowcount TO CLIENT
	
	COMMIT -- (^_^)
	DROP TABLE #weekly_sample
	TRUNCATE TABLE SC3I_Sky_base_segment_snapshots
	COMMIT -- (^_^)
	
		-- Inserting results into individual segment table
	INSERT INTO SC3I_Sky_base_segment_snapshots (
		account_number
		,profiling_date
		,HH_person_number
		,vespa_scaling_segment_id
		,expected_boxes
		)
	SELECT b.account_number
		,@profiling_thursday
		,d.HH_person_number
		,l_sc3i.scaling_segment_id
		, 1
	FROM SC3_scaling_weekly_sample AS b
	INNER JOIN V289_M08_SKY_HH_composition 				AS d 		ON b.account_number = d.account_number
	INNER JOIN vespa_analysts.SC3I_Segments_lookup_v1_1 AS l_sc3i 	ON b.isba_tv_region = l_sc3i.isba_tv_region
																	AND b.package = l_sc3i.package
																	AND d.person_head = l_sc3i.head_of_hhd
																	AND d.person_gender || ' ' || d.person_ageband = l_sc3i.age_band -- combine age and gender into a single attribute
																	AND CASE WHEN d.household_size > 7 THEN '8+'
																			ELSE cast(d.household_size AS VARCHAR(2)) END = l_sc3i.hh_size
	
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
	DROP TABLE #vespa_viewer_accounts
	DROP TABLE #vespa_viewer_individuals
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
	INNER JOIN SC3_scaling_weekly_sample AS l_sc3 ON p.account_number = l_sc3.account_number -- this join to bring in the old scaling segments
	INNER JOIN vespa_analysts.SC3I_Segments_lookup_v1_1 AS l_sc3i ON l_sc3.isba_tv_region = l_sc3i.isba_tv_region -- finally, match the attributes to give us the new SC3I segment IDs
		AND l_sc3.package = l_sc3i.package
		AND t.person_head = l_sc3i.head_of_hhd
		AND t.age_band = l_sc3i.age_band
		AND t.hhsize_capped = l_sc3i.hh_size
		AND t.viewed_tv = l_sc3i.viewed_tv
	WHERE t.viewed_tv IN ('Yes' ,'NV - Viewing HHD' )

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
	DROP TABLE #t3
	COMMIT -- (^_^)
END;-- of procedure "V289_M11_01_SC3_v1_1__do_weekly_segmentation"

COMMIT;
