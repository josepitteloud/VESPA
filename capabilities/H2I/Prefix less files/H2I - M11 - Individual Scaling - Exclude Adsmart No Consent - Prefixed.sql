/*

Could not execute statement.
Table '#scaling_box_level_viewing' not found
SQLCODE=-141, ODBC 3 State="42S02"
Line 4, column 1

--EXECUTE v289_m01_B_Multi_day_process_manager 1, '2013-09-20','2013-09-21', 5, 'Run 3 12/11 PM'        


Execute V289_M11_01_SC3_v1_1__do_weekly_segmentation '2013-09-19',3593,'2014-11-12'
*/

/* *************** PART A: PLACEHOLDER FOR VIRTUAL PANEL BALANCE ****************/

CREATE OR REPLACE PROCEDURE V289_M11_01_SC3_v1_1__do_weekly_segmentation @profiling_thursday DATE = NULL -- Day on which to do sky base profiling
	, @Scale_refresh_logging_ID BIGINT = NULL -- Might pass the log ID in as an argument if it's a big historical build, otherwise we'll make a new one
	, @batch_date DATETIME = now () -- Day on which build was kicked off
AS
BEGIN
	DECLARE @QA_catcher INT -- For control totals along the way
	DECLARE @tablespacename VARCHAR(40)

	EXECUTE logger_add_event @Scale_refresh_logging_ID , 3 , 'SC3: Profiling Sky UK base as of ' || DATEFORMAT (@profiling_thursday, 'yyyy-mm-dd') || '.'
	COMMIT

	DELETE 	FROM SC3_scaling_weekly_sample -- Clear out the processing tables and suchlike
	COMMIT

	IF @profiling_thursday IS NULL
	BEGIN
		EXECUTE vespa_analysts.Regulars_Get_report_end_date @profiling_thursday OUTPUT -- proc returns a Saturday
		SET @profiling_thursday = @profiling_thursday - 2 -- but we want a Thursday
	END
	COMMIT

	-- So this bit is not stable for the VIQ builds since we can't delete weights from there,
	-- but for dev builds within analytics this is required.
	DELETE
	FROM SC3_Sky_base_segment_snapshots
	WHERE profiling_date = @profiling_thursday
	COMMIT

	/**************** L01: ESTABLISH POPULATION ****************/
	 
	 MESSAGE cast(now() as timestamp)||' | Begining M11.01: ESTABLISH POPULATION' TO CLIENT
	
	SELECT account_number
		, cb_key_household
		, cb_key_individual
		, current_short_description
		, rank() OVER (PARTITION BY account_number ORDER BY effective_from_dt DESC, cb_row_id) AS rank
		, convert(BIT, 0) AS uk_standard_account
		, convert(VARCHAR(30), NULL) AS isba_tv_region
	INTO #weekly_sample
	FROM cust_subs_hist_V
	WHERE subscription_sub_type IN ('DTV Primary Viewing')
		AND status_code IN ('AC', 'AB', 'PC')
		AND effective_from_dt <= @profiling_thursday
		AND effective_to_dt > @profiling_thursday
		AND effective_from_dt <> effective_to_dt
		AND EFFECTIVE_FROM_DT IS NOT NULL
		AND cb_key_household > 0
		AND cb_key_household IS NOT NULL
		AND cb_key_individual IS NOT NULL
		AND account_number IS NOT NULL
		AND service_instance_id IS NOT NULL
	-- De-dupes accounts
	COMMIT
	DELETE	FROM #weekly_sample	WHERE rank > 1
	COMMIT

	SET @QA_catcher = - 1

	SELECT @QA_catcher = count(1)
	FROM #weekly_sample
	COMMIT

	EXECUTE logger_add_event @Scale_refresh_logging_ID, 3, 'L01: Midway 1/2 (Weekly sample)', coalesce(@QA_catcher, - 1)
	COMMIT

	-- Create indices
	CREATE UNIQUE INDEX fake_pk ON #weekly_sample (account_number)
	CREATE INDEX for_package_join ON #weekly_sample (current_short_description)
	COMMIT

	-- Take out ROIs (Republic of Ireland) and non-standard accounts as these are not currently in the scope of Vespa
	UPDATE #weekly_sample
	SET uk_standard_account = CASE 	WHEN b.acct_type = 'Standard'	AND b.account_number <> '?'	AND b.pty_country_code = 'GBR'	THEN 1	ELSE 0	END
		-- Insert SC3 TV regions
		, isba_tv_region = CASE 
			WHEN b.isba_tv_region = 'Border'				THEN 'NI, Scotland & Border'
			WHEN b.isba_tv_region = 'Central Scotland'		THEN 'NI, Scotland & Border'
			WHEN b.isba_tv_region = 'East Of England'		THEN 'Wales & Midlands'
			WHEN b.isba_tv_region = 'HTV Wales'				THEN 'Wales & Midlands'
			WHEN b.isba_tv_region = 'HTV West'				THEN 'South England'
			WHEN b.isba_tv_region = 'London'				THEN 'London'
			WHEN b.isba_tv_region = 'Meridian (exc. Channel Islands)'				THEN 'South England'
			WHEN b.isba_tv_region = 'Midlands'				THEN 'Wales & Midlands'
			WHEN b.isba_tv_region = 'North East'			THEN 'North England'
			WHEN b.isba_tv_region = 'North Scotland'		THEN 'NI, Scotland & Border'
			WHEN b.isba_tv_region = 'North West'			THEN 'North England'
			WHEN b.isba_tv_region = 'Not Defined'			THEN 'Not Defined'
			WHEN b.isba_tv_region = 'South West'			THEN 'South England'
			WHEN b.isba_tv_region = 'Ulster'				THEN 'NI, Scotland & Border'
			WHEN b.isba_tv_region = 'Yorkshire'				THEN 'North England'
															ELSE 'Not Defined'
			END
		, cb_key_individual = b.cb_key_individual
	FROM #weekly_sample AS a
	INNER JOIN cust_single_account_view_V AS b ON a.account_number = b.account_number

	COMMIT

	DELETE	FROM #weekly_sample
	WHERE uk_standard_account = 0
	COMMIT

	SET @QA_catcher = - 1

	SELECT @QA_catcher = count(1)
	FROM #weekly_sample

	COMMIT
	EXECUTE logger_add_event @Scale_refresh_logging_ID, 3, 'L01: Complete! (Population)', coalesce(@QA_catcher, - 1)
	COMMIT
		 
	MESSAGE cast(now() as timestamp)||' | Ending M11.01: ESTABLISH POPULATION' TO CLIENT

	/**************** L02: ASSIGN VARIABLES ****************/
	
	MESSAGE cast(now() as timestamp)||' | Begining M11.02: ASSIGN VARIABLES' TO CLIENT
	
	SELECT cv.cb_key_household
		, cv.cb_key_family
		, cv.cb_key_individual
		, min(cv.cb_row_id) AS cb_row_id
		, max(cv.h_household_composition) AS h_household_composition
		, max(pp.p_head_of_household) AS p_head_of_household
	INTO #cv_pp
	FROM EXPERIAN_CONSUMERVIEW_V cv
		, PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD_V pp
	WHERE cv.exp_cb_key_db_individual = pp.exp_cb_key_db_individual
		AND cv.cb_key_individual IS NOT NULL
	GROUP BY cv.cb_key_household
		, cv.cb_key_family
		, cv.cb_key_individual

	COMMIT

	CREATE LF INDEX idx1 ON #cv_pp (p_head_of_household)
	CREATE HG INDEX idx2 ON #cv_pp (cb_key_family)
	CREATE HG INDEX idx3 ON #cv_pp (cb_key_individual)

	SELECT cb_key_household
		, cb_row_id
		, rank() OVER (
			PARTITION BY cb_key_family ORDER BY p_head_of_household DESC
				, cb_row_id DESC
			) AS rank_fam
		, rank() OVER (
			PARTITION BY cb_key_household ORDER BY p_head_of_household DESC
				, cb_row_id DESC
			) AS rank_hhd
		, CASE 	WHEN h_household_composition = '00'			THEN 'A) Families'
			WHEN h_household_composition = '01'				THEN 'A) Families'
			WHEN h_household_composition = '02'				THEN 'A) Families'
			WHEN h_household_composition = '03'				THEN 'A) Families'
			WHEN h_household_composition = '04'				THEN 'B) Singles'
			WHEN h_household_composition = '05'				THEN 'B) Singles'
			WHEN h_household_composition = '06'				THEN 'C) Homesharers'
			WHEN h_household_composition = '07'				THEN 'C) Homesharers'
			WHEN h_household_composition = '08'				THEN 'C) Homesharers'
			WHEN h_household_composition = '09'				THEN 'A) Families'
			WHEN h_household_composition = '10'				THEN 'A) Families'
			WHEN h_household_composition = '11'				THEN 'C) Homesharers'
			WHEN h_household_composition = 'U'				THEN 'D) Unclassified HHComp'
															ELSE 'D) Unclassified HHComp'
				END AS h_household_composition
	INTO #cv_keys
	FROM #cv_pp
	WHERE cb_key_household IS NOT NULL
		AND cb_key_household <> 0

	COMMIT

	DELETE	FROM #cv_keys	WHERE rank_fam != 1	AND rank_hhd != 1
	COMMIT
	
	CREATE INDEX index_ac ON #cv_keys (cb_key_household)

	COMMIT

	SET @QA_catcher = - 1

	SELECT @QA_catcher = count(1)
	FROM #cv_keys

	COMMIT

	EXECUTE logger_add_event @Scale_refresh_logging_ID , 3, 'L02: Midway 1/8 (Consumerview Linkage)', coalesce(@QA_catcher, - 1)
	COMMIT

	-- Populate Package & ISBA TV Region
	INSERT INTO SC3_scaling_weekly_sample (
		account_number
		, cb_key_household
		, cb_key_individual
		, universe --scaling variables removed. Use later to set no_of_stbs
		, sky_base_universe -- Need to include this as they form part of a big index
		, vespa_universe -- Need to include this as they form part of a big index
		, isba_tv_region
		, hhcomposition
		, tenure
		, num_mix
		, mix_pack
		, package
		, boxtype
		, no_of_stbs
		, hd_subscription
		, pvr
		)
	SELECT fbp.account_number
		, fbp.cb_key_household
		, fbp.cb_key_individual
		, 'A) Single box HH' -- universe
		, 'Not adsmartable' -- sky_base_universe
		, 'Non-Vespa' -- Vespa Universe
		, fbp.isba_tv_region -- isba_tv_region
		, 'D)' -- hhcomposition
		, 'D) Unknown' -- tenure
		, cel.Variety + cel.Knowledge + cel.Kids + cel.Style_Culture + cel.Music + cel.News_Events AS num_mix
		, CASE 	WHEN Num_Mix IS NULL	OR Num_Mix = 0		THEN 'Entertainment Pack'
			WHEN (cel.variety = 1 OR cel.style_culture = 1)	AND Num_Mix = 1		THEN 'Entertainment Pack'
			WHEN (cel.variety = 1 AND cel.style_culture = 1) AND Num_Mix = 2	THEN 'Entertainment Pack'
			WHEN Num_Mix > 0								THEN 'Entertainment Extra'
			END AS mix_pack -- Basic package has recently been split into the Entertainment and Entertainment Extra packs
		, CASE 
			WHEN cel.prem_sports = 2 AND cel.prem_movies = 2 THEN 'Movies & Sports' --'Top Tier'
			WHEN cel.prem_sports = 2 AND cel.prem_movies = 0 THEN 'Sports' --'Dual Sports'
			WHEN cel.prem_sports = 0 AND cel.prem_movies = 2 THEN 'Movies' --'Dual Movies'
			WHEN cel.prem_sports = 1 AND cel.prem_movies = 0 THEN 'Sports' --'Single Sports'
			WHEN cel.prem_sports = 0 AND cel.prem_movies = 1 THEN 'Movies' --'Single Movies'
			WHEN cel.prem_sports > 0 OR cel.prem_movies > 0  THEN 'Movies & Sports' --'Other Premiums'
			WHEN cel.prem_movies = 0 AND cel.prem_sports = 0 AND mix_pack = 'Entertainment Pack' THEN 'Basic' --'Basic - Ent'
			WHEN cel.prem_movies = 0 AND cel.prem_sports = 0 AND mix_pack = 'Entertainment Extra' THEN 'Basic' --'Basic - Ent Extra'
				ELSE 'Basic' 	END -- 'Basic - Ent' END -- package
		, 'D) FDB & No_secondary_box' -- boxtype
		, 'Single' --no_of_stbs
		, 'No' --hd_subscription
		, 'No' --pvr
	FROM #weekly_sample AS fbp
	LEFT JOIN cust_entitlement_lookup_V AS cel ON fbp.current_short_description = cel.short_description
	WHERE fbp.cb_key_household IS NOT NULL
		AND fbp.cb_key_individual IS NOT NULL

	COMMIT

	DROP TABLE #weekly_sample

	COMMIT

	-- Populate sky_base_universe according to SQL code used to find adsmartable bozes in weekly reports
	SELECT account_number
		, CASE 	WHEN flag = 1 AND cust_viewing_data_capture_allowed = 'Y' THEN 'Adsmartable with consent'
				WHEN flag = 1 AND cust_viewing_data_capture_allowed <> 'Y' THEN 'Adsmartable but no consent'
			ELSE 'Not adsmartable'
			END AS sky_base_universe
	INTO #cv_sbu
	FROM (SELECT sav.account_number AS account_number
			, adsmart.flag
			, cust_viewing_data_capture_allowed
		FROM (SELECT DISTINCT account_number
				, cust_viewing_data_capture_allowed
			FROM CUST_SINGLE_ACCOUNT_VIEW_V
			WHERE CUST_ACTIVE_DTV = 1 -- this field implies -> prod_latest_dtv_status_code IN ('AC','AB','PC')
				AND pty_country_code = 'GBR'
			) AS sav
		LEFT JOIN (SELECT account_number
						, max(CASE 	WHEN x_pvr_type = 'PVR6' THEN 1
									WHEN x_pvr_type = 'PVR5' THEN 1
									WHEN x_pvr_type = 'PVR4' AND x_manufacturer = 'Samsung'	THEN 1
									WHEN x_pvr_type = 'PVR4' AND x_manufacturer = 'Pace'	THEN 1
									ELSE 0		END) AS flag
			FROM (SELECT * FROM (
					SELECT account_number
						, x_pvr_type
						, x_personal_storage_capacity
						, currency_code
						, x_manufacturer
						, rank() OVER (PARTITION BY service_instance_id ORDER BY ph_non_subs_link_sk DESC) active_flag
					FROM CUST_SET_TOP_BOX_V
					) AS base
				WHERE active_flag = 1
				) AS active_boxes
			WHERE currency_code = 'GBP'
			GROUP BY account_number
			) AS adsmart ON sav.account_number = adsmart.account_number
		) AS sub1

	COMMIT

	UPDATE SC3_scaling_weekly_sample
	SET stws.sky_base_universe = cv.sky_base_universe
	FROM SC3_scaling_weekly_sample AS stws
	INNER JOIN #cv_sbu AS cv ON stws.account_number = cv.account_number

	UPDATE SC3_scaling_weekly_sample
	SET vespa_universe = CASE 	WHEN sky_base_universe = 'Not adsmartable'				THEN 'Vespa not Adsmartable'
								WHEN sky_base_universe = 'Adsmartable with consent'		THEN 'Vespa adsmartable'
								WHEN sky_base_universe = 'Adsmartable but no consent'	THEN 'Vespa but no consent'
								ELSE 'Non-Vespa' 	END

	COMMIT

	SET @QA_catcher = - 1

	SELECT @QA_catcher = count(1)
	FROM SC3_scaling_weekly_sample
	WHERE sky_base_universe IS NOT NULL
		AND vespa_universe IS NOT NULL

	COMMIT
	EXECUTE logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 2a/8 (Accounts with no universe)', coalesce(@QA_catcher, - 1)
	COMMIT

	DELETE FROM SC3_scaling_weekly_sample
	WHERE sky_base_universe IS NULL
		OR vespa_universe IS NULL

	SET @QA_catcher = - 1

	SELECT @QA_catcher = count(1)
	FROM SC3_scaling_weekly_sample
	COMMIT

	EXECUTE logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 2/8 (Package & ISBA region)', coalesce(@QA_catcher, - 1)
	COMMIT

	-- HHcomposition
	UPDATE SC3_scaling_weekly_sample
	SET stws.hhcomposition = cv.h_household_composition
	FROM SC3_scaling_weekly_sample AS stws
	INNER JOIN #cv_keys AS cv ON stws.cb_key_household = cv.cb_key_household

	COMMIT

	DROP TABLE #cv_keys

	COMMIT

	SET @QA_catcher = - 1

	SELECT @QA_catcher = count(1)
	FROM SC3_scaling_weekly_sample
	WHERE left(hhcomposition, 2) <> 'D)'

	COMMIT

	EXECUTE logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 3/8 (HH composition)', coalesce(@QA_catcher, - 1)

	COMMIT

	-- Tenure
	-- Tenure has been grouped according to its relationship with viewing behaviour
	UPDATE SC3_scaling_weekly_sample t1
	SET tenure = CASE	WHEN datediff(day, acct_first_account_activation_dt, @profiling_thursday) <= 730	THEN 'A) 0-2 Years'
						WHEN datediff(day, acct_first_account_activation_dt, @profiling_thursday) <= 3650	THEN 'B) 3-10 Years'
						WHEN datediff(day, acct_first_account_activation_dt, @profiling_thursday) > 3650	THEN 'C) 10 Years+'
							ELSE 'D) Unknown'		END
	FROM cust_single_account_view_V AS sav
	WHERE t1.account_number = sav.account_number

	COMMIT

	SET @QA_catcher = - 1

	SELECT @QA_catcher = count(1)
	FROM SC3_scaling_weekly_sample
	WHERE tenure <> 'D) Unknown'

	-- Added SC3 line to remove Unknown tenure
	DELETE FROM SC3_scaling_weekly_sample	WHERE tenure = 'D) Unknown'
	DELETE FROM SC3_scaling_weekly_sample WHERE isba_tv_region = 'Not Defined'
	COMMIT

	EXECUTE logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 4/8 (Tenure)', coalesce(@QA_catcher, - 1)

	COMMIT

	-- Capture all active boxes for this week
	SELECT csh.service_instance_id
		, csh.account_number
		, subscription_sub_type
		, rank() OVER (PARTITION BY csh.service_instance_id ORDER BY csh.account_number, csh.cb_row_id DESC) AS rank
	INTO #accounts -- drop table #accounts
	FROM cust_subs_hist_V AS csh
	INNER JOIN SC3_scaling_weekly_sample AS ss ON csh.account_number = ss.account_number
	WHERE csh.subscription_sub_type IN ('DTV Primary Viewing', 'DTV Extra Subscription') --the DTV sub Type
		AND csh.status_code IN ('AC', 'AB', 'PC') --Active Status Codes
		AND csh.effective_from_dt <= @profiling_thursday
		AND csh.effective_to_dt > @profiling_thursday
		AND csh.effective_from_dt <> effective_to_dt

	-- De-dupe active boxes
	DELETE FROM #accounts	WHERE rank > 1
	COMMIT

	-- Create indices on list of boxes
	CREATE hg INDEX idx1 ON #accounts (service_instance_id)
	CREATE hg INDEX idx2 ON #accounts (account_number)
	COMMIT

	-- Identify HD & 1TB/2TB HD boxes
	SELECT stb.service_instance_id
		, SUM(CASE 	WHEN current_product_description LIKE '%HD%'	THEN 1
					ELSE 0 END) AS HD
		, SUM(CASE 	WHEN x_description IN ('Amstrad HD PVR6 (1TB)', 'Amstrad HD PVR6 (2TB)') THEN 1 
					ELSE 0 END) AS HD1TB
	INTO #hda -- drop table #hda
	FROM CUST_SET_TOP_BOX_V AS stb
	INNER JOIN #accounts AS acc ON stb.service_instance_id = acc.service_instance_id
	WHERE box_installed_dt <= @profiling_thursday
		AND box_replaced_dt > @profiling_thursday
		AND current_product_description LIKE '%HD%'
	GROUP BY stb.service_instance_id

	-- Create index on HD table
	COMMIT
	CREATE UNIQUE hg INDEX idx1 ON #hda (service_instance_id)
	COMMIT

	SET @QA_catcher = - 1

	SELECT @QA_catcher = count(1)
	FROM #hda
	COMMIT

	EXECUTE logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 5/8 (HD boxes)', coalesce(@QA_catcher, - 1)
	COMMIT
	-- Identify PVR boxes
	SELECT acc.account_number
		, MAX(CASE 	WHEN x_box_type LIKE '%Sky+%'	THEN 'Yes'
					ELSE 'No'	END) AS PVR
	INTO #pvra -- drop table #pvra
	FROM CUST_SET_TOP_BOX_V AS stb
	INNER JOIN #accounts AS acc ON stb.service_instance_id = acc.service_instance_id
	WHERE box_installed_dt <= @profiling_thursday
		AND box_replaced_dt > @profiling_thursday
	GROUP BY acc.account_number

	-- Create index on PVR table
	COMMIT
	CREATE hg INDEX pvidx1 ON #pvra (account_number)
	COMMIT

	-- PVR
	UPDATE SC3_scaling_weekly_sample
	SET stws.pvr = cv.pvr
	FROM SC3_scaling_weekly_sample AS stws
	INNER JOIN #pvra AS cv ON stws.account_number = cv.account_number

	SET @QA_catcher = - 1

	SELECT @QA_catcher = count(1)
	FROM #pvra

	COMMIT
	EXECUTE logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 6/8 (PVR boxes)', coalesce(@QA_catcher, - 1)
	COMMIT

	-- Set default value when account cannot be found
	UPDATE SC3_scaling_weekly_sample
	SET pvr = CASE WHEN sky_base_universe LIKE 'Adsmartable%'	THEN 'Yes'
				ELSE 'No'	END
	WHERE pvr IS NULL
	COMMIT

	--Further check to ensure that when PVR is No then the box is Not Adsmartable
	SET @QA_catcher = - 1

	SELECT @QA_catcher = count(1)
	FROM SC3_scaling_weekly_sample
	WHERE pvr = 'No'
		AND sky_base_universe LIKE 'Adsmartable%'

	COMMIT
	EXECUTE logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 6a/8 (Non-PVR boxes which are adsmartable)', coalesce(@QA_catcher, - 1)
	COMMIT

	-- Update PVR when PVR says 'No' and universe is an adsmartable one.
	UPDATE SC3_scaling_weekly_sample
	SET pvr = 'Yes'
	WHERE pvr = 'No'
		AND sky_base_universe LIKE 'Adsmartable%'

	COMMIT

	SELECT --acc.service_instance_id,
		acc.account_number
		, MAX(CASE 	WHEN csh.SUBSCRIPTION_SUB_TYPE = 'DTV Extra Subscription'	THEN 1
				ELSE 0	END) AS MR
		, MAX(CASE 	WHEN csh.SUBSCRIPTION_SUB_TYPE = 'DTV Sky+'					THEN 1
				ELSE 0	END) AS SP
		, MAX(CASE 	WHEN csh.SUBSCRIPTION_SUB_TYPE = 'DTV HD'					THEN 1
				ELSE 0	END) AS HD
		, MAX(CASE 	WHEN #hda.HD = 1											THEN 1
				ELSE 0	END) AS HDstb
		, MAX(CASE 	WHEN #hda.HD1TB = 1											THEN 1
				ELSE 0	END) AS HD1TBstb
	INTO #scaling_box_level_viewing
	FROM Cust_subs_hist_V AS csh
	INNER JOIN #accounts AS acc ON csh.service_instance_id = acc.service_instance_id --< Limits to your universe
	LEFT JOIN cust_entitlement_lookup_V AS cel ON csh.current_short_description = cel.short_description
	LEFT JOIN #hda ON csh.service_instance_id = #hda.service_instance_id --< Links to the HD Set Top Boxes
	WHERE csh.effective_FROM_dt <= @profiling_thursday
		AND csh.effective_to_dt > @profiling_thursday
		AND csh.status_code IN ('AC', 'AB', 'PC')
		AND csh.SUBSCRIPTION_SUB_TYPE IN ('DTV Primary Viewing', 'DTV Sky+', 'DTV Extra Subscription', 'DTV HD')
		AND csh.effective_FROM_dt <> csh.effective_to_dt
	GROUP BY acc.service_instance_id
		, acc.account_number

	COMMIT
	DROP TABLE #accounts
	DROP TABLE #hda
	COMMIT
	-- Identify boxtype of each box and whether it is a primary or a secondary box
	SELECT tgt.account_number
		, SUM(CASE 	WHEN MR = 1			THEN 1
				ELSE 0	END) AS mr_boxes
		, MAX(CASE 	WHEN MR = 0
					AND ((tgt.HD = 1	AND HD1TBstb = 1)
					OR (tgt.HD = 1		AND HDstb = 1))			THEN 4 -- HD ( inclusive of HD1TB)
					WHEN MR = 0	AND ((tgt.SP = 1	AND tgt.HD1TBstb = 1)
						OR (tgt.SP = 1	AND tgt.HDstb = 1))		THEN 3 -- HDx ( inclusive of HD1TB)
				WHEN MR = 0	AND tgt.SP = 1						THEN 2 -- Skyplus
				ELSE 1	END) AS pb -- FDB
		, MAX(CASE 
				WHEN MR = 1
					AND ((tgt.HD = 1	AND HD1TBstb = 1)	OR (tgt.HD = 1	AND HDstb = 1))
					THEN 4 -- HD ( inclusive of HD1TB)
				WHEN MR = 1	AND ((tgt.SP = 1	AND tgt.HD1TBstb = 1)	OR (tgt.SP = 1	AND tgt.HDstb = 1))
					THEN 3 -- HDx ( inclusive of HD1TB)
				WHEN MR = 1	AND tgt.SP = 1	THEN 2 -- Skyplus
					ELSE 1	END) AS sb -- FDB
		, convert(VARCHAR(20), NULL) AS universe
		, convert(VARCHAR(30), NULL) AS boxtype
	INTO #boxtype_ac -- drop table #boxtype_ac
	FROM #scaling_box_level_viewing AS tgt
	GROUP BY tgt.account_number

	-- Create indices on box-level boxtype temp table
	COMMIT
	CREATE UNIQUE INDEX idx_ac ON #boxtype_ac (account_number)
	DROP TABLE #scaling_box_level_viewing
	COMMIT

	SET @QA_catcher = - 1

	SELECT @QA_catcher = count(1)
	FROM #boxtype_ac

	COMMIT
	EXECUTE logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 6/8 (P/S boxes)', coalesce(@QA_catcher, - 1)
	COMMIT

	-- Build the combined flags
	UPDATE #boxtype_ac
	SET universe = CASE 
			WHEN mr_boxes = 0
				THEN 'A) Single box HH'
			ELSE 'B) Multiple box HH'
			END
		, boxtype = CASE 	WHEN mr_boxes = 0	AND pb = 3	AND sb = 1	THEN 'A) HDx & No_secondary_box'
							WHEN mr_boxes = 0	AND pb = 4	AND sb = 1	THEN 'B) HD & No_secondary_box'
							WHEN mr_boxes = 0	AND pb = 2	AND sb = 1	THEN 'C) Skyplus & No_secondary_box'
							WHEN mr_boxes = 0	AND pb = 1	AND sb = 1	THEN 'D) FDB & No_secondary_box'
							WHEN mr_boxes > 0	AND pb = 4	AND sb = 4	THEN 'E) HD & HD' -- If a hh has HD  then all boxes have HD (therefore no HD and HDx)
							WHEN mr_boxes > 0	AND (pb = 4	AND sb = 3	)	OR (pb = 3	AND sb = 4)	THEN 'E) HD & HD'
							WHEN mr_boxes > 0	AND (pb = 4	AND sb = 2)		OR (pb = 2	AND sb = 4)	THEN 'F) HD & Skyplus'
							WHEN mr_boxes > 0	AND (pb = 4	AND sb = 1)		OR (pb = 1	AND sb = 4)	THEN 'G) HD & FDB'
							WHEN mr_boxes > 0	AND pb = 3	AND sb = 3	THEN 'H) HDx & HDx'
							WHEN mr_boxes > 0	AND (pb = 3	AND sb = 2)	OR (pb = 2	AND sb = 3)		THEN 'I) HDx & Skyplus'
							WHEN mr_boxes > 0	AND (pb = 3	AND sb = 1)	OR (pb = 1	AND sb = 3)		THEN 'J) HDx & FDB'
							WHEN mr_boxes > 0	AND pb = 2	AND sb = 2	THEN 'K) Skyplus & Skyplus'
							WHEN mr_boxes > 0	AND (pb = 2	AND sb = 1)	OR (pb = 1	AND sb = 2)		THEN 'L) Skyplus & FDB'
							ELSE 'M) FDB & FDB'		END
	COMMIT

	CREATE TABLE #SC3_weird_sybase_update_workaround (
		account_number VARCHAR(20) PRIMARY KEY
		, cb_key_household BIGINT NOT NULL
		, cb_key_individual BIGINT NOT NULL
		, consumerview_cb_row_id BIGINT
		, universe VARCHAR(30) -- Single or multiple box household. Reused for no_of_stbs
		, sky_base_universe VARCHAR(30) -- Not adsmartable, Adsmartable with consent, Adsmartable but no consent household
		, vespa_universe VARCHAR(30) -- Non-Vespa, Not Adsmartable, Vespa with consent, vespa but no consent household
		, weighting_universe VARCHAR(30) -- Used when finding appropriate scaling segment - see note
		, isba_tv_region VARCHAR(30) -- Scaling variable 1 : Region
		, hhcomposition VARCHAR(2) DEFAULT 'D)' -- Scaling variable 2: Household composition
		, tenure VARCHAR(15) DEFAULT 'D) Unknown' -- Scaling variable 3: Tenure
		, num_mix INT
		, mix_pack VARCHAR(20)
		, package VARCHAR(20) -- Scaling variable 4: Package
		, boxtype VARCHAR(35) -- Old Scaling variable 5: Household boxtype split into no_of_stbs, hd_subscription and pvr.
		, no_of_stbs VARCHAR(15) -- Scaling variable 5: No of set top boxes
		, hd_subscription VARCHAR(5) -- Scaling variable 6: HD subscription
		, pvr VARCHAR(5) -- Scaling variable 7: Is the box pvr capable?
		, population_scaling_segment_id INT DEFAULT NULL -- segment scaling id for identifying segments
		, vespa_scaling_segment_id INT DEFAULT NULL -- segment scaling id for identifying segments
		, mr_boxes INT
		)

	--    ,complete_viewing                   TINYINT         DEFAULT 0           -- Flag for all accounts with complete viewing data
	CREATE INDEX for_segment_identification_temp1 ON #SC3_weird_sybase_update_workaround (isba_tv_region)
	CREATE INDEX for_segment_identification_temp2 ON #SC3_weird_sybase_update_workaround (hhcomposition)
	CREATE INDEX for_segment_identification_temp3 ON #SC3_weird_sybase_update_workaround (tenure)
	CREATE INDEX for_segment_identification_temp4 ON #SC3_weird_sybase_update_workaround (package)
	CREATE INDEX for_segment_identification_temp5 ON #SC3_weird_sybase_update_workaround (boxtype)
	CREATE INDEX consumerview_joining ON #SC3_weird_sybase_update_workaround (consumerview_cb_row_id)
	CREATE INDEX for_temping1 ON #SC3_weird_sybase_update_workaround (population_scaling_segment_id)
	CREATE INDEX for_temping2 ON #SC3_weird_sybase_update_workaround (vespa_scaling_segment_id)

	COMMIT

	INSERT INTO #SC3_weird_sybase_update_workaround (
		account_number
		, cb_key_household
		, cb_key_individual
		, consumerview_cb_row_id
		, universe
		, sky_base_universe
		, vespa_universe
		, isba_tv_region
		, hhcomposition
		, tenure
		, num_mix
		, mix_pack
		, package
		, boxtype
		, mr_boxes
		)
	SELECT sws.account_number
		, sws.cb_key_household
		, sws.cb_key_individual
		, sws.consumerview_cb_row_id
		, ac.universe
		, sky_base_universe
		, vespa_universe
		, sws.isba_tv_region
		, sws.hhcomposition
		, sws.tenure
		, sws.num_mix
		, sws.mix_pack
		, sws.package
		, ac.boxtype
		, ac.mr_boxes
	FROM SC3_scaling_weekly_sample AS sws
	INNER JOIN #boxtype_ac AS ac ON ac.account_number = sws.account_number
	WHERE sws.cb_key_household IS NOT NULL
		AND sws.cb_key_individual IS NOT NULL

	-- Update SC3 scaling variables in #SC3_weird_sybase_update_workaround according to Scaling 3.0 variables
	UPDATE #SC3_weird_sybase_update_workaround sws
	SET sws.pvr = ac.pvr
	FROM #pvra AS ac
	WHERE ac.account_number = sws.account_number

	COMMIT
	DROP TABLE #boxtype_ac
	COMMIT

	SET @QA_catcher = - 1

	SELECT @QA_catcher = count(1)
	FROM #SC3_weird_sybase_update_workaround

	COMMIT
	EXECUTE logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Complete! (Variables)', coalesce(@QA_catcher, - 1)

	COMMIT
	MESSAGE cast(now() as timestamp)||' | Ending M11.02: ASSIGN VARIABLES' TO CLIENT
	
	/**************** L03: ASSIGN SCALING SEGMENT ID ****************/
	MESSAGE cast(now() as timestamp)||' | Begining M11.03: ASSIGN SCALING SEGMENT ID ' TO CLIENT
	
	UPDATE #SC3_weird_sybase_update_workaround
	SET sky_base_universe = 'Not adsmartable'
	WHERE sky_base_universe IS NULL

	UPDATE #SC3_weird_sybase_update_workaround
	SET vespa_universe = 'Non-Vespa'
	WHERE sky_base_universe IS NULL

	UPDATE #SC3_weird_sybase_update_workaround
	SET weighting_universe = 'Not adsmartable'
	WHERE weighting_universe IS NULL

	-- Set default value when account cannot be found
	UPDATE #SC3_weird_sybase_update_workaround
	SET pvr = CASE 	WHEN sky_base_universe LIKE 'Adsmartable%'	THEN 'Yes'
			ELSE 'No'	END
	WHERE pvr IS NULL

	COMMIT

	-- Update PVR when PVR says 'No' and universe is an adsmartable one.
	UPDATE #SC3_weird_sybase_update_workaround
	SET pvr = 'Yes'
	WHERE pvr = 'No'
		AND sky_base_universe LIKE 'Adsmartable%'

	COMMIT

	UPDATE #SC3_weird_sybase_update_workaround
	SET no_of_stbs = CASE 	WHEN Universe LIKE '%Single%'		THEN 'Single'
							WHEN Universe LIKE '%Multiple%'		THEN 'Multiple'
							ELSE 'Single'	END

	UPDATE #SC3_weird_sybase_update_workaround
	SET hd_subscription = CASE 	WHEN boxtype LIKE 'B)%'	OR boxtype LIKE 'E)%'	OR boxtype LIKE 'F)%'	OR boxtype LIKE 'G)%'	THEN 'Yes'	
								ELSE 'No'			END
	COMMIT

	UPDATE #SC3_weird_sybase_update_workaround
	SET #SC3_weird_sybase_update_workaround.population_scaling_segment_ID = ssl.scaling_segment_ID
	FROM #SC3_weird_sybase_update_workaround
	INNER JOIN SC3_Segments_lookup_v1_1_V AS ssl ON trim(lower(#SC3_weird_sybase_update_workaround.sky_base_universe)) = trim(lower(ssl.sky_base_universe))
		AND left(#SC3_weird_sybase_update_workaround.hhcomposition, 2) = left(ssl.hhcomposition, 2)
		AND left(#SC3_weird_sybase_update_workaround.isba_tv_region, 20) = left(ssl.isba_tv_region, 20)
		AND #SC3_weird_sybase_update_workaround.Package = ssl.Package
		AND left(#SC3_weird_sybase_update_workaround.tenure, 2) = left(ssl.tenure, 2)
		AND #SC3_weird_sybase_update_workaround.no_of_stbs = ssl.no_of_stbs
		AND #SC3_weird_sybase_update_workaround.hd_subscription = ssl.hd_subscription
		AND #SC3_weird_sybase_update_workaround.pvr = ssl.pvr

	UPDATE #SC3_weird_sybase_update_workaround
	SET vespa_scaling_segment_id = population_scaling_segment_ID

	COMMIT

	SET @QA_catcher = - 1

	SELECT @QA_catcher = count(1)
	FROM #SC3_weird_sybase_update_workaround
	WHERE population_scaling_segment_ID IS NOT NULL
	COMMIT

	EXECUTE logger_add_event @Scale_refresh_logging_ID, 3, 'L03a: Midway (Population Segment lookup)', coalesce(@QA_catcher, - 1)
	COMMIT

	SET @QA_catcher = - 1

	SELECT @QA_catcher = count(1)
	FROM #SC3_weird_sybase_update_workaround
	WHERE vespa_scaling_segment_id IS NOT NULL

	COMMIT
	EXECUTE logger_add_event @Scale_refresh_logging_ID, 3, 'L03b: Midway (Weighting Segment lookup)', coalesce(@QA_catcher, - 1)

	COMMIT

	-- Okay, no throw all of that back into the weekly sample table, because that's where
	-- the build expects it to be, were it not for that weird bug in Sybase:
	DELETE	FROM SC3_scaling_weekly_sample

	COMMIT

	INSERT INTO SC3_scaling_weekly_sample
	SELECT *
	FROM #SC3_weird_sybase_update_workaround

	COMMIT
	DROP TABLE #SC3_weird_sybase_update_workaround
	COMMIT

	SET @QA_catcher = - 1

	SELECT @QA_catcher = count(1)
	FROM SC3_scaling_weekly_sample
	WHERE population_scaling_segment_ID IS NOT NULL
		AND vespa_scaling_segment_id IS NOT NULL

	COMMIT
	EXECUTE logger_add_event @Scale_refresh_logging_ID, 3, 'L03: Complete! (Segment ID assignment)', coalesce(@QA_catcher, - 1)
	COMMIT

	MESSAGE cast(now() as timestamp)||' | Ending M11.03: ASSIGN SCALING SEGMENT ID ' TO CLIENT
	/**************** L04: PUBLISHING INTO INTERFACE STRUCTURES ****************/
	MESSAGE cast(now() as timestamp)||' | Begining M11.04: PUBLISHING INTO INTERFACE STRUCTURES ' TO CLIENT
	
	INSERT INTO SC3_Sky_base_segment_snapshots
	SELECT account_number
		, @profiling_thursday
		, cb_key_household -- This guy still needs to be added to SC3_scaling_weekly_sample
		, population_scaling_segment_id
		, vespa_scaling_segment_id
		, mr_boxes + 1 -- Number of multiroom boxes plus 1 for the primary
	FROM SC3_scaling_weekly_sample
	WHERE population_scaling_segment_id IS NOT NULL
		AND vespa_scaling_segment_id IS NOT NULL -- still perhaps with the weird account from Eire?

	COMMIT

	SET @QA_catcher = - 1

	SELECT @QA_catcher = count(1)
	FROM SC3_Sky_base_segment_snapshots
	WHERE profiling_date = @profiling_thursday

	COMMIT

	EXECUTE logger_add_event @Scale_refresh_logging_ID, 3, 'L04: Complete! (Segments published)', coalesce(@QA_catcher, - 1)
	COMMIT

	EXECUTE logger_add_event @Scale_refresh_logging_ID, 3, 'SC3: base segmentation complete!'
	COMMIT
	END;-- of procedure "V289_M11_01_SC3_v1_1__do_weekly_segmentation"

COMMIT;

-- This section nominally decides which boxes are considered to be on the panel
-- for each day. There could be a bunch of influences here:
--   * Completeness of returned data in multiroom households
--   * Regularity of returned data for panel stability / box reliability
--   * Virtual panel balance decisions (using the wekly segmentation) - NYIP
-- The output is a table of account numbers and scaling segment IDs. Which is
-- the other reason why it depends on the segmentation build.
IF object_id('V289_M11_02_SC3_v1_1__prepare_panel_members') IS NOT NULL THEN
	DROP PROCEDURE V289_M11_02_SC3_v1_1__prepare_panel_members
	END

IF ;
	CREATE
		OR REPLACE PROCEDURE V289_M11_02_SC3_v1_1__prepare_panel_members @profiling_date DATE -- Thursday to use for scaling
		, @scaling_day DATE -- Day for which to do scaling
		, @batch_date DATETIME = now () -- Day on which build was kicked off
		, @Scale_refresh_logging_ID BIGINT = NULL -- Might pass the log ID in as an argument if it's a big historical build, otherwise we'll make a new one
		AS

BEGIN
	/**************** A00: CLEANING OUT ALL THE OLD STUFF ****************/
	DELETE
	FROM SC3_todays_panel_members

	COMMIT

	/**************** A01: ACCOUNTS REPORTING LAST WEEK ****************/
	-- This code block is more jury-rigged in than the others because the structure
	-- has to change a bit to accomodate appropriate modularisation. And it'll all
	-- change again later when Phase 2 stuff gets rolled in. And probably further to
	-- acommodate this overnight batching thing, because we won't have data returned
	-- up to a week in the future.
	--declare @profiling_date             date            -- The relevant Thursday of SAV flip etc
	DECLARE @QA_catcher INT -- For control totals along the way

	-- The weekly profiling is called in a different build, so we'll
	-- just grab the most recent one prior to the date we're scaling
	/*     select @profiling_date = max(profiling_date)
     from SC3_Sky_base_segment_snapshots
     where profiling_date <= @scaling_day
*/
	COMMIT

	EXECUTE logger_add_event @Scale_refresh_logging_ID
		, 3
		, 'SC3: Deciding panel members for ' || DATEFORMAT (
			@scaling_day
			, 'yyyy-mm-dd'
			) || ' using profiling of ' || DATEFORMAT (
			@profiling_date
			, 'yyyy-mm-dd'
			) || '.'

	COMMIT

	-- Prepare to catch the week's worth of logs:
	CREATE TABLE #raw_logs_dump_temp (
		account_number VARCHAR(20) NOT NULL
		, service_instance_id VARCHAR(30) NOT NULL
		)

	COMMIT

	-- In phase two, we don't have to worry about juggling things through the daily tables,
	-- so figuring out what's returned data is a lot easier.
	INSERT INTO #raw_logs_dump_temp
	SELECT DISTINCT account_number
		, service_instance_id
	FROM V289_viewing_data_view
	WHERE event_start_date_time_utc BETWEEN dateadd(hour, 6, @scaling_day)
			AND dateadd(hour, 30, @scaling_day)
		AND (
			panel_id = 12
			OR panel_id = 11
			)
		AND account_number IS NOT NULL
		AND service_instance_id IS NOT NULL

	COMMIT

	CREATE hg INDEX idx1 ON #raw_logs_dump_temp (account_number)

	CREATE hg INDEX idx2 ON #raw_logs_dump_temp (service_instance_id)

	CREATE TABLE #raw_logs_dump (
		account_number VARCHAR(20) NOT NULL
		, service_instance_id VARCHAR(30) NOT NULL
		)

	COMMIT

	INSERT INTO #raw_logs_dump
	SELECT DISTINCT account_number
		, service_instance_id
	FROM #raw_logs_dump_temp

	COMMIT

	CREATE INDEX some_key ON #raw_logs_dump (account_number)

	COMMIT

	SET @QA_catcher = - 1

	SELECT @QA_catcher = count(1)
	FROM #raw_logs_dump

	COMMIT

	EXECUTE logger_add_event @Scale_refresh_logging_ID
		, 3
		, 'A01: Midway 1/2 (Log extracts)'
		, coalesce(@QA_catcher, - 1)

	COMMIT

	SELECT account_number
		, count(DISTINCT service_instance_id) AS box_count
		, convert(TINYINT, NULL) AS expected_boxes
		, convert(INT, NULL) AS scaling_segment_id
	INTO #panel_options
	FROM #raw_logs_dump
	GROUP BY account_number

	COMMIT

	CREATE UNIQUE INDEX fake_pk ON #panel_options (account_number)

	DROP TABLE #raw_logs_dump

	COMMIT

	-- Getting this list of accounts isn't enough, we also want to know if all the boxes
	-- of the household have returned data.
	UPDATE #panel_options
	SET expected_boxes = sbss.expected_boxes
		, scaling_segment_id = sbss.vespa_scaling_segment_id
	FROM #panel_options
	INNER JOIN SC3_Sky_base_segment_snapshots AS sbss ON #panel_options.account_number = sbss.account_number
	WHERE sbss.profiling_date = @profiling_date

	COMMIT

	DELETE
	FROM SC3_todays_panel_members

	COMMIT

	-- First moving the unique account numbers in...
	INSERT INTO SC3_todays_panel_members (
		account_number
		, scaling_segment_id
		)
	SELECT account_number
		, scaling_segment_id
	FROM #panel_options
	WHERE expected_boxes >= box_count
		-- Might be more than we expect if NULL service_instance_ID's are distinct against
		-- populated ones (might get fixed later but for now the initial Phase 2 build
		-- doesn't populate them all yet)
		AND scaling_segment_id IS NOT NULL

	COMMIT

	DROP TABLE #panel_options

	COMMIT

	SET @QA_catcher = - 1

	SELECT @QA_catcher = count(1)
	FROM SC3_todays_panel_members

	COMMIT

	EXECUTE logger_add_event @Scale_refresh_logging_ID
		, 3
		, 'A01: Complete! (Panel members)'
		, coalesce(@QA_catcher, - 1)

	COMMIT

	EXECUTE logger_add_event @Scale_refresh_logging_ID
		, 3
		, 'SC3: panel members prepared!'

	COMMIT
END;-- of procedure "V289_M11_02_SC3_v1_1__prepare_panel_members"

COMMIT;

--------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------
--- Adds indivdual level data in some of the scaling tables for Skyview before the Rim Weighting is applied
IF object_id('V289_M11_03_SC3I_v1_1__add_individual_data') IS NOT NULL THEN
	DROP PROCEDURE V289_M11_03_SC3I_v1_1__add_individual_data
	END

IF ;
	CREATE PROCEDURE V289_M11_03_SC3I_v1_1__add_individual_data @profiling_thursday DATE -- Day on which to do sky base profiling
		, @batch_date DATETIME = now () -- Day on which build was kicked off
		, @Scale_refresh_logging_ID BIGINT = NULL -- Might pass the log ID in as an argument if it's a big historical build, otherwise we'll make a new one
	AS
	BEGIN
		DECLARE @QA_catcher INT -- For control totals along the way

		DELETE
		FROM SC3I_Sky_base_segment_snapshots
		WHERE profiling_date = @profiling_thursday

		COMMIT

		--- Skybase segments
		-- We can convert the segments from Scaling 3.0 into Skyview scaling segments
		INSERT INTO SC3I_Sky_base_segment_snapshots
		SELECT b.account_number
			, b.profiling_date
			, d.HH_person_number
			, l_sc3i.scaling_segment_id
			, l_sc3i.scaling_segment_id
			, b.expected_boxes
		FROM SC3_Sky_base_segment_snapshots b
		INNER JOIN V289_M08_SKY_HH_composition d ON b.account_number = d.account_number
		INNER JOIN SC3_Segments_lookup_v1_1_V AS l_sc3 ON b.population_scaling_segment_id = l_sc3.scaling_segment_id
		INNER JOIN SC3I_Segments_lookup_v1_1_V AS l_sc3i ON l_sc3.isba_tv_region = l_sc3i.isba_tv_region
			AND l_sc3.package = l_sc3i.package
			AND d.person_head = l_sc3i.head_of_hhd
			AND d.person_gender || ' ' || d.person_ageband = l_sc3i.age_band -- combine age and gender into a single attribute
			AND l_sc3i.viewed_tv = 'Y' -- most people watch TV and for SKy Base we can't differentiate between the viewers and non-viewers. Will deal with non-viewers later
		WHERE b.profiling_date = @profiling_thursday

		COMMIT

		/* IN this version we are not exlcuding any segments as this has already been taken care of in the segment definitions

--- We want to exclude some segments (and therefor accounts within these segments) from scaling to improve effective sample size
--- This will only effects segments which have low numbers of accounts
select distinct account_number
into #t1
from SC3I_Sky_base_segment_snapshots b inner join vespa_analysts.SC3I_Segments_lookup_v1_1 l on b.population_scaling_segment_id = l.scaling_segment_id
where (gender = 'U' and age_band <> '0-19') -- exclude U gender except for 0-19 (almost all 0-19 are U)
--        or hhcomposition = 'D) Unclassified HHComp' -- high numbers of zero vespa segemnts driving lower effective sample size
--        or l.sky_base_universe = 'Adsmartable but no consent' -- Oct test data has very few Adsmart No Consent on the panel so exclude
--        or (age_band = '20-24' and hhcomposition = 'B) Singles') -- Small segments
--        or (age_band = '65+' and hhcomposition = 'C) Homesharers') -- Small segments

commit

create hg index ind1 on #t1(account_number)
commit


-- Delete the excluded accounts
delete from SC3I_Sky_base_segment_snapshots
from SC3I_Sky_base_segment_snapshots b inner join #t1 t
on b.account_number = t.account_number
commit

*/
		SET @QA_catcher = - 1

		SELECT @QA_catcher = count(1)
		FROM SC3I_Sky_base_segment_snapshots

		COMMIT

		EXECUTE logger_add_event @Scale_refresh_logging_ID
			, 3
			, 'M11_03: Skybase Individuals'
			, coalesce(@QA_catcher, - 1)

		COMMIT

		DELETE
		FROM SC3I_Todays_panel_members

		COMMIT

		-- Ensure only accounts and individuals on Vespa extract is used
		SELECT account_number
			, hh_person_number
		INTO #t3
		FROM V289_M10_session_individuals -- changed from V289_M07_dp_data so only include accounts that make it through the whole H2I process
		GROUP BY account_number
			, hh_person_number

		COMMIT

		CREATE hg INDEX ind1 ON #t3 (account_number)

		CREATE lf INDEX ind2 ON #t3 (hh_person_number)

		COMMIT

		-- Vespa Viewers
		INSERT INTO SC3I_Todays_panel_members
		SELECT p.account_number
			, d.HH_person_number
			, l_sc3i.scaling_segment_id
		FROM SC3_Todays_panel_members AS p
		INNER JOIN V289_M08_SKY_HH_composition AS d ON p.account_number = d.account_number
		INNER JOIN SC3_Segments_lookup_v1_1_V 	AS l_sc3 	ON p.scaling_segment_id = l_sc3.scaling_segment_id
		INNER JOIN SC3I_Segments_lookup_v1_1_V 	AS l_sc3i 	ON l_sc3.isba_tv_region = l_sc3i.isba_tv_region
			AND l_sc3.package = l_sc3i.package
			AND d.person_head = l_sc3i.head_of_hhd
			AND d.person_gender || ' ' || d.person_ageband = l_sc3i.age_band -- combine age and gender into a single attribute
			AND l_sc3i.viewed_tv = 'Y' -- by definition all these guys watched tv .Will deal with non-viewers later
		INNER JOIN #t3 AS t ON p.account_number = t.account_number
			AND d.hh_person_number = t.hh_person_number

		COMMIT

		-- Vespa Non-Viewers
		-- Not sure we need this as won't be comparable to Barb non-viewers
		/* IN this version we are not exlcuding any segments as this has already been taken care of in the segment definitions

--- We want to exclude some segments (and therefor accounts within these segments) from scaling to improve effective sample size
--- This will only effects segments which have low numbers of accounts
select distinct account_number
into #t2
from SC3I_Todays_panel_members p inner join vespa_analysts.SC3I_Segments_lookup_v1_1 l on p.scaling_segment_id = l.scaling_segment_id
where (gender = 'U' and age_band <> '0-19') -- exclude U gender except for 0-19 (almost all 0-19 are U)
--        or hhcomposition = 'D) Unclassified HHComp' -- high numbers of zero vespa segemnts driving lower effective sample size
--        or l.sky_base_universe = 'Adsmartable but no consent' -- Oct test data has very few Adsmart No Consent on the panel so exclude
--        or (age_band = '20-24' and hhcomposition = 'B) Singles') -- Small segments
--        or (age_band = '65+' and hhcomposition = 'C) Homesharers') -- Small segments
commit

create hg index ind1 on #t2(account_number)
commit

-- Delete the excluded accounts
delete from SC3I_Todays_panel_members
from SC3I_Todays_panel_members p inner join #t2 t
on p.account_number = t.account_number
commit

*/
		SET @QA_catcher = - 1

		SELECT @QA_catcher = count(1)
		FROM SC3I_Sky_base_segment_snapshots

		COMMIT

		EXECUTE logger_add_event @Scale_refresh_logging_ID
			, 3
			, 'M11_03: Panel Individuals'
			, coalesce(@QA_catcher, - 1)

		COMMIT
	END;-- of procedure "V289_M11_03_SC3I_v1_1__add_individual_data"

	COMMIT;

	-----------------------------------------------------------------------------------------------------------------------------------------

		CREATE OR REPLACE PROCEDURE V289_M11_04_SC3I_v1_1__make_weights @scaling_day DATE -- Day for which to do scaling; this argument is mandatory
			, @batch_date DATETIME = now () -- Day on which build was kicked off
			, @Scale_refresh_logging_ID BIGINT = NULL -- Might pass the log ID in as an argument if it's a big historical build, otherwise we'll make a new one
		AS
		BEGIN
			-- Only need these if we can't get to execute as a Proc
			/*        declare @scaling_day  date
        declare @batch_date date
        declare @Scale_refresh_logging_ID bigint
        set @scaling_day = '2013-09-26'
        set @batch_date = '2014-07-10'
        set @Scale_refresh_logging_ID = 5
*/
			-- So by this point we're assuming that the Sky base segmentation is done
			-- (for a suitably recent item) and also that today's panel members have
			-- been established, and we're just going to go calculate these weights.
			DECLARE @cntr INT
			DECLARE @iteration INT
			DECLARE @cntr_var SMALLINT
			DECLARE @scaling_var VARCHAR(30)
			DECLARE @scaling_count SMALLINT
			DECLARE @convergence TINYINT
			DECLARE @sky_base DOUBLE
			DECLARE @vespa_panel DOUBLE
			DECLARE @sum_of_weights DOUBLE
			DECLARE @profiling_date DATE
			DECLARE @QA_catcher BIGINT

			COMMIT

			/**************** PART B01: GETTING TOTALS FOR EACH SEGMENT ****************/
			-- Figure out which profiling info we're using;
			SELECT @profiling_date = max(profiling_date)
			FROM SC3I_Sky_base_segment_snapshots
			WHERE profiling_date <= @scaling_day

			COMMIT

			-- Log the profiling date being used for the build
			EXECUTE logger_add_event @Scale_refresh_logging_ID
				, 3
				, 'SC3: Making weights for ' || DATEFORMAT (
					@scaling_day
					, 'yyyy-mm-dd'
					) || ' using profiling of ' || DATEFORMAT (
					@profiling_date
					, 'yyyy-mm-dd'
					) || '.'

			COMMIT

			-- First adding in the Sky base numbers
			DELETE
			FROM SC3I_weighting_working_table

			COMMIT

			INSERT INTO SC3I_weighting_working_table (
				scaling_segment_id
				, sky_base_accounts
				)
			SELECT population_scaling_segment_id
				, count(1)
			FROM SC3I_Sky_base_segment_snapshots
			WHERE profiling_date = @profiling_date
			GROUP BY population_scaling_segment_id

			COMMIT


			-- Now tack on the universe flags; a special case of things coming out of the lookup
			UPDATE SC3I_weighting_working_table
			SET sky_base_universe = sl.sky_base_universe
			FROM SC3I_weighting_working_table
			INNER JOIN SC3I_Segments_lookup_v1_1_V AS sl ON SC3I_weighting_working_table.scaling_segment_id = sl.scaling_segment_id

			COMMIT

			-- Mix in the Vespa panel counts as determined earlier
			SELECT scaling_segment_id
				, count(1) AS panel_members
			INTO #segment_distribs
			FROM SC3I_Todays_panel_members
			WHERE scaling_segment_id IS NOT NULL
			GROUP BY scaling_segment_id

			COMMIT

			CREATE UNIQUE INDEX fake_pk ON #segment_distribs (scaling_segment_id)

			COMMIT

			-- It defaults to 0, so we can just poke values in
			UPDATE SC3I_weighting_working_table
			SET vespa_panel = sd.panel_members
			FROM SC3I_weighting_working_table
			INNER JOIN #segment_distribs AS sd ON SC3I_weighting_working_table.scaling_segment_id = sd.scaling_segment_id

			-- And we're done! log the progress.
			COMMIT

			DROP TABLE #segment_distribs

			COMMIT

			SET @QA_catcher = - 1

			SELECT @QA_catcher = count(1)
			FROM SC3I_weighting_working_table

			COMMIT

			EXECUTE logger_add_event @Scale_refresh_logging_ID
				, 3
				, 'B01: Complete! (Segmentation totals)'
				, coalesce(@QA_catcher, - 1)

			COMMIT

			/**************** PART B02: ASSIGNING WEIGHTS TO EACH SEGMENT ****************/
			DELETE
			FROM SC3I_category_subtotals
			WHERE scaling_date = @scaling_day

			DELETE
			FROM SC3I_metrics
			WHERE scaling_date = @scaling_day

			COMMIT

			-- Rim-weighting is an iterative process that iterates through each of the scaling variables
			-- individually until the category sum of weights converge to the population category subtotals
			SET @cntr = 1
			SET @iteration = 0
			SET @cntr_var = 1
			SET @scaling_var = (
					SELECT scaling_variable
					FROM SC3I_Variables_lookup_v1_1_V
					WHERE id = @cntr
					)
			SET @scaling_count = (
					SELECT COUNT(scaling_variable)
					FROM SC3I_Variables_lookup_v1_1_V
					)

			-- The SC3I_weighting_working_table table contains subtotals and sum_of_weights for all segments represented by
			-- the sky base.
			-- Some segments are not represented by the vespa panel, these are allocated an arbitrary value of 0.000001
			-- to ensure convergence.
			-- arbitrary value to ensure convergence
			UPDATE SC3I_weighting_working_table
			SET vespa_panel = 0.000001
			WHERE vespa_panel = 0

			COMMIT

			-- Initialise working columns
			UPDATE SC3I_weighting_working_table
			SET sum_of_weights = vespa_panel

			COMMIT

			-- The iterative part.
			-- This works by choosing a particular scaling variable and then summing across the categories
			-- of that scaling variable for the sky base, the vespa panel and the sum of weights.
			-- A Category weight is calculated by dividing the sky base subtotal by the vespa panel subtotal
			-- for that category.
			-- This category weight is then applied back to the segments table and the process repeats until
			-- the sum_of_weights in the category table converges to the sky base subtotal.
			-- Category Convergence is defined as the category sum of weights being +/- 3 away from the sky
			-- base category subtotal within 100 iterations.
			-- Overall Convergence for that day occurs when each of the categories has converged, or the @convergence variable = 0
			-- The @convergence variable represents how many categories did not converge.
			-- If the number of iterations = 100 and the @convergence > 0 then this means that the Rim-weighting
			-- has not converged for this particular day.
			-- In this scenario, the person running the code should send the results of the SC3I_metrics for that
			-- week to analytics team for review. ## What exactly are we checking? can we automate any of it?
			WHILE @cntr <= @scaling_count
			BEGIN
				DELETE
				FROM SC3I_category_working_table

				SET @cntr_var = 1

				WHILE @cntr_var <= @scaling_count
				BEGIN
					SELECT @scaling_var = scaling_variable
					FROM SC3I_Variables_lookup_v1_1_V
					WHERE id = @cntr_var

					EXECUTE (
							'
                         INSERT INTO SC3I_category_working_table (sky_base_universe,profile,value,sky_base_accounts,vespa_panel,sum_of_weights)
                             SELECT  srs.sky_base_universe
                                    ,@scaling_var
                                    ,ssl.' 
							|| @scaling_var || 
							'
                                    ,SUM(srs.sky_base_accounts)
                                    ,SUM(srs.vespa_panel)
                                    ,SUM(srs.sum_of_weights)
                             FROM SC3I_weighting_working_table AS srs
                                     inner join SC3I_Segments_lookup_v1_1_V AS ssl ON srs.scaling_segment_id = ssl.scaling_segment_id
                             GROUP BY srs.sky_base_universe,ssl.' 
							|| @scaling_var || '
                             ORDER BY srs.sky_base_universe
                         '
							)

					SET @cntr_var = @cntr_var + 1
				END

				COMMIT

				UPDATE SC3I_category_working_table
				SET category_weight = sky_base_accounts / sum_of_weights
					, convergence_flag = CASE 
						WHEN abs(sky_base_accounts - sum_of_weights) < 3
							THEN 0
						ELSE 1
						END

				SELECT @convergence = SUM(convergence_flag)
				FROM SC3I_category_working_table

				SET @iteration = @iteration + 1

				SELECT @scaling_var = scaling_variable
				FROM SC3I_Variables_lookup_v1_1_V
				WHERE id = @cntr

				EXECUTE (
						'
             UPDATE SC3I_weighting_working_table
             SET  SC3I_weighting_working_table.category_weight = sc.category_weight
                 ,SC3I_weighting_working_table.sum_of_weights  = SC3I_weighting_working_table.sum_of_weights * sc.category_weight
             FROM SC3I_weighting_working_table
                     inner join SC3I_Segments_lookup_v1_1_V AS ssl ON SC3I_weighting_working_table.scaling_segment_id = ssl.scaling_segment_id
                     inner join SC3I_category_working_table AS sc ON sc.value = ssl.' 
						|| @scaling_var || '
                                                                      AND sc.sky_base_universe = ssl.sky_base_universe
             '
						)

				COMMIT

				IF @iteration = 100
					OR @convergence = 0
					SET @cntr = (@scaling_count + 1)
				ELSE IF @cntr = @scaling_count
					SET @cntr = 1
				ELSE
					SET @cntr = @cntr + 1
			END

			COMMIT

			-- This loop build took about 4 minutes. That's fine.
			-- Calculate segment weight and corresponding indices
			-- This section calculates the segment weight which is the weight that should be applied to viewing data
			-- A couple of indices are also calculated so that we can keep track of the performance of the rim-weighting
			SELECT @sky_base = SUM(sky_base_accounts)
			FROM SC3I_weighting_working_table

			SELECT @vespa_panel = SUM(vespa_panel)
			FROM SC3I_weighting_working_table

			SELECT @sum_of_weights = SUM(sum_of_weights)
			FROM SC3I_weighting_working_table

			UPDATE SC3I_weighting_working_table
			SET segment_weight = sum_of_weights / vespa_panel
				, indices_actual = 100 * (vespa_panel / @vespa_panel) / (sky_base_accounts / @sky_base)
				, indices_weighted = 100 * (sum_of_weights / @sum_of_weights) / (sky_base_accounts / @sky_base)

			COMMIT

			-- OK, now catch those cases where stuff diverged because segments weren't reperesented:
			UPDATE SC3I_weighting_working_table
			SET segment_weight = 0.000001
			WHERE vespa_panel = 0.000001

			COMMIT

			SET @QA_catcher = - 1

			SELECT @QA_catcher = count(1)
			FROM SC3I_weighting_working_table
			WHERE segment_weight >= 0.001 -- Ignore the placeholders here to guarantee convergence

			COMMIT

			EXECUTE logger_add_event @Scale_refresh_logging_ID
				, 3
				, 'B02: Midway (Iterations)'
				, coalesce(@QA_catcher, - 1)

			COMMIT

			-- Now push convergence details out to the tracking tables: the first one provides a convergence summary at a category level
			INSERT INTO SC3I_category_subtotals (
				scaling_date
				, sky_base_universe
				, PROFILE
				, value
				, sky_base_accounts
				, vespa_panel
				, category_weight
				, sum_of_weights
				, convergence
				)
			SELECT @scaling_day
				, sky_base_universe
				, PROFILE
				, value
				, sky_base_accounts
				, vespa_panel
				, category_weight
				, sum_of_weights
				, CASE 
					WHEN abs(sky_base_accounts - sum_of_weights) > 3
						THEN 1
					ELSE 0
					END
			FROM SC3I_category_working_table

			-- The SC3I_metrics table contains metrics for a particular scaling date. It shows whether the
			-- Rim-weighting process converged for that day and the number of iterations. It also shows the
			-- maximum and average weight for that day and counts for the sky base and the vespa panel.
			COMMIT

			-- Apparently it should be reviewed each week, but what are we looking for?
			INSERT INTO SC3I_metrics (
				scaling_date
				, iterations
				, convergence
				, max_weight
				, av_weight
				, sum_of_weights
				, sky_base
				, vespa_panel
				, non_scalable_accounts
				)
			SELECT @scaling_day
				, @iteration
				, @convergence
				, MAX(segment_weight)
				, sum(segment_weight * vespa_panel) / sum(vespa_panel) -- gives the average weight by account (just uising AVG would give it average by segment id)
				, SUM(segment_weight * vespa_panel) -- again need some math because this table has one record per segment id rather than being at acocunt level
				, @sky_base
				, sum(CASE 
						WHEN segment_weight >= 0.001
							THEN vespa_panel
						ELSE NULL
						END)
				, sum(CASE 
						WHEN segment_weight < 0.001
							THEN vespa_panel
						ELSE NULL
						END)
			FROM SC3I_weighting_working_table

			UPDATE SC3I_metrics
			SET sum_of_convergence = abs(sky_base - sum_of_weights)

			INSERT INTO SC3I_non_convergences (
				scaling_date
				, scaling_segment_id
				, difference
				)
			SELECT @scaling_day
				, scaling_segment_id
				, abs(sum_of_weights - sky_base_accounts)
			FROM SC3I_weighting_working_table
			WHERE abs((segment_weight * vespa_panel) - sky_base_accounts) > 3

			COMMIT

			EXECUTE logger_add_event @Scale_refresh_logging_ID
				, 3
				, 'B02: Complete (Calculate weights)'
				, coalesce(@QA_catcher, - 1)

			COMMIT

			/**************** PART B03: PUBLISHING WEIGHTS INTO INTERFACE STRUCTURES ****************/
			-- Here is where that bit of interface code goes, including extending the intervals
			-- in the Scaling midway tables (which now happens one day ata time). Maybe this guy
			-- wants to go into a new and different stored procedure?
			-- Heh, this deletion process clears out everything *after* the scaling day, meaning we
			-- have to start from the beginning doing this processing... I guess we'll just manage
			-- the historical build like this. (This is because we'd otherwise have to manage adding
			-- additional records to the interval table when we re-run a day and break an interval
			-- that already exists, and that whole process would be annoying to manage.)
			-- Except we'll only nuke everything if we *rebuild* a day that's not already there.
			IF (
					SELECT count(1)
					FROM SC3I_Weightings
					WHERE scaling_day = @scaling_day
					) > 0
			BEGIN
				DELETE
				FROM SC3I_Weightings
				WHERE scaling_day = @scaling_day

				DELETE
				FROM SC3I_Intervals
				WHERE reporting_starts = @scaling_day

				UPDATE SC3I_Intervals
				SET reporting_ends = dateadd(day, - 1, @scaling_day)
				WHERE reporting_ends >= @scaling_day
			END

			COMMIT

			-- Part 1: Update the Vespa midway scaling tables. In Vespa Analysts? May as well
			-- also keep this in VIQ_prod too.
			INSERT INTO SC3I_Weightings
			SELECT @scaling_day
				, scaling_segment_id
				, vespa_panel
				, sky_base_accounts
				, segment_weight
				, sum_of_weights
				, indices_actual
				, indices_weighted
				, CASE 
					WHEN abs(sky_base_accounts - sum_of_weights) > 3
						THEN 1
					ELSE 0
					END
			FROM SC3I_weighting_working_table

			-- Might have to check that the filter on segment_weight doesn't leave any orphaned
			-- accounts about the place...
			COMMIT

			SET @QA_catcher = - 1

			SELECT @QA_catcher = count(1)
			FROM SC3I_Weightings
			WHERE scaling_day = @scaling_day

			COMMIT

			EXECUTE logger_add_event @Scale_refresh_logging_ID
				, 3
				, 'B03: Midway 1/4 (Midway weights)'
				, coalesce(@QA_catcher, - 1)

			COMMIT

			-- First off extend the intervals that are already in the table:
	
			SET @QA_catcher = - 1

			SELECT @QA_catcher = count(1)
			FROM SC3I_Intervals
			WHERE reporting_ends = @scaling_day

			COMMIT

			EXECUTE logger_add_event @Scale_refresh_logging_ID
				, 3
				, 'B03: Midway 2/4 (Midway intervals)'
				, coalesce(@QA_catcher, - 1)

			COMMIT

			-- Part 2: Update the VIQ interface table (which needs the household key thing)
			IF (
					SELECT count(1)
					FROM V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING
					WHERE scaling_date = @scaling_day
					) > 0
			BEGIN
				DELETE
				FROM V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING
				WHERE scaling_date = @scaling_day
			END

			COMMIT

			INSERT INTO V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING
			SELECT ws.account_number
				, ws.HH_person_number
				, @scaling_day
				, wwt.segment_weight
				, @batch_date
			FROM SC3I_weighting_working_table AS wwt
			INNER JOIN SC3I_Sky_base_segment_snapshots AS ws -- need this table to get the cb_key_household items
				ON wwt.scaling_segment_id = ws.population_scaling_segment_id
			INNER JOIN SC3I_Todays_panel_members AS tpm ON ws.account_number = tpm.account_number -- Filter for today's panel only
				AND ws.profiling_date = @profiling_date

			COMMIT

			SET @QA_catcher = - 1

			SELECT @QA_catcher = count(1)
			FROM V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING
			WHERE scaling_date = @scaling_day

			COMMIT

			EXECUTE logger_add_event @Scale_refresh_logging_ID
				, 3
				, 'B03: Midway 3/4 (VIQ interface)'
				, coalesce(@QA_catcher, - 1)

			COMMIT

			EXECUTE logger_add_event @Scale_refresh_logging_ID
				, 3
				, 'B03: Complete! (Publish weights)'

			COMMIT

			EXECUTE logger_add_event @Scale_refresh_logging_ID
				, 3
				, 'SC3: Weights made for ' || DATEFORMAT (
					@scaling_day
					, 'yyyy-mm-dd'
					)

			COMMIT
		END;-- of procedure "V289_M11_04_SC3I_v1_1__make_weights"

		COMMIT;

		
		
		/* ******************************************************************************************************/
		/* ******************************************************************************************************/
		/* ******************************************************************************************************/		
		/* ******************************************************************************************************/
		
		
		
		CREATE OR REPLACE PROCEDURE V289_M11_04_SC3I_v1_1__make_weights_BARB @profiling_date DATE -- Thursday profilr date
				, @scaling_day DATE -- Day for which to do scaling; this argument is mandatory
				, @batch_date DATETIME = now () -- Day on which build was kicked off
				, @Scale_refresh_logging_ID BIGINT = NULL -- Might pass the log ID in as an argument if it's a big historical build, otherwise we'll make a new one
			AS
			BEGIN
				-- Only need these if we can't get to execute as a Proc
				/*        declare @scaling_day  date
        declare @batch_date date
        declare @Scale_refresh_logging_ID bigint
        set @scaling_day = '2013-09-26'
        set @batch_date = '2014-07-10'
        set @Scale_refresh_logging_ID = 5
*/
				-- So by this point we're assuming that the Sky base segmentation is done
				-- (for a suitably recent item) and also that today's panel members have
				-- been established, and we're just going to go calculate these weights.
				DECLARE @cntr INT
				DECLARE @iteration INT
				DECLARE @cntr_var SMALLINT
				DECLARE @scaling_var VARCHAR(30)
				DECLARE @scaling_count SMALLINT
				DECLARE @convergence TINYINT
				DECLARE @sky_base DOUBLE
				DECLARE @vespa_panel DOUBLE
				DECLARE @sum_of_weights DOUBLE
				--     declare @profiling_date date
				DECLARE @QA_catcher BIGINT

				COMMIT

				/**************** PART B01: GETTING TOTALS FOR EACH SEGMENT ****************/
				-- Figure out which profiling info we're using;
				/*     select @profiling_date = max(profiling_date)
     from SC3I_Sky_base_segment_snapshots
     where profiling_date <= @scaling_day

     commit
*/
				-- Log the profiling date being used for the build
				EXECUTE logger_add_event @Scale_refresh_logging_ID , 3, 'SC3: Making weights for ' || DATEFORMAT (@scaling_day, 'yyyy-mm-dd') || ' using profiling of ' || DATEFORMAT (@profiling_date, 'yyyy-mm-dd') || '.'
				COMMIT
				-- First adding in the Sky base numbers
				DELETE
				FROM SC3I_weighting_working_table
				COMMIT

				INSERT INTO SC3I_weighting_working_table (
					scaling_segment_id
					, sky_base_accounts
					)
				SELECT population_scaling_segment_id
					, count(1)
				FROM SC3I_Sky_base_segment_snapshots
				WHERE profiling_date = @profiling_date
				GROUP BY population_scaling_segment_id

				COMMIT

				 --MESSAGE cast(now() as timestamp)||' | PART B01.1 Completed ' TO CLIENT
				/* ** ************* update SC3I_weighting_working_table
-- Re-scale Sky base to Barb age/gender totals
-- Will only rescale to barb households that have any viewing data for the day being scaled
-- and NOT the barb base
*/
				-- Get individuals from Barb who have viewed tv
				SELECT household_number
					, person_number
				INTO #barb_viewers
				FROM skybarb_fullview
				WHERE DATE (start_time_of_session) = @scaling_day
				GROUP BY household_number
					, person_number

				COMMIT

				CREATE hg INDEX ind1 ON #barb_viewers (household_number)

				CREATE lf INDEX ind2 ON #barb_viewers (person_number)

				-- Get hhds that have some viewing
				SELECT household_number
				INTO #barb_hhd_viewers
				FROM #barb_viewers
				GROUP BY household_number

				COMMIT

				CREATE hg INDEX ind1 ON #barb_hhd_viewers (household_number)

				-- Get Barb individuals in Sky hhds
				SELECT h.house_id AS household_number
					, h.person AS person_number
					, h.age
					, CASE 	WHEN age <= 19 		THEN 'U'
							WHEN h.sex = 'Male' THEN 'M'
							WHEN h.sex = 'Female' THEN 'F'
							END AS gender
					, CASE 	WHEN age <= 19 THEN '0-19' 
							WHEN age BETWEEN 20 AND 24 	THEN '20-24'
							WHEN age BETWEEN 25 AND 34	THEN '25-34'
							WHEN age BETWEEN 35 AND 44  THEN '35-44'
							WHEN age BETWEEN 45 AND 64 	THEN '45-64'
							WHEN age >= 65 				THEN '65+'
							END AS ageband
					, CAST (h.head AS CHAR(1)) AS head_of_hhd
					, w.processing_weight / 10.0 AS processing_weight
				INTO #barb_inds_with_sky
				FROM skybarb h
				INNER JOIN barb_weights AS w ON h.house_id = w.household_number
					AND h.person = w.person_number
				
				--MESSAGE cast(now() as timestamp)||' | PART B01.2 Completed ' TO CLIENT
				
				-------------- Summaries Barb Data
				DELETE
				FROM V289_M11_04_Barb_weighted_population
				COMMIT

				INSERT INTO V289_M11_04_Barb_weighted_population
				SELECT (CASE WHEN ageband = '0-19'	THEN 'U'
								ELSE gender 	END) || ' ' || ageband AS gender_ageband
					, 'A' AS gender1
					, CASE WHEN v.household_number IS NULL THEN 'N' ELSE 'Y' END AS viewed_tv
					, i.head_of_hhd
					, sum(processing_weight)
				FROM #barb_inds_with_sky 		AS i
				LEFT JOIN #barb_viewers 		AS v ON i.household_number = v.household_number
					AND i.person_number = v.person_number
				GROUP BY gender_ageband
					, gender1
					, viewed_tv
					, i.head_of_hhd

				COMMIT
				DROP TABLE #barb_inds_with_sky
				COMMIT

				--MESSAGE cast(now() as timestamp)||' | PART B01.31 Completed ' TO CLIENT
				
				----
				-- Note that for the Skybase at this point there are no non-viewers of TV
				SELECT age_band
					, viewed_tv
					, head_of_hhd
					, cast(sum(sky_base_accounts) AS DOUBLE) AS age_gender_sky_base
				INTO #a1
				FROM SC3I_weighting_working_table 		AS w
				INNER JOIN SC3I_Segments_lookup_v1_1_V 	AS l ON w.scaling_segment_id = l.scaling_segment_id
				GROUP BY age_band
					, viewed_tv
					, head_of_hhd

				COMMIT
				CREATE lf INDEX ind1 ON #a1 (age_band)
				CREATE lf INDEX ind3 ON #a1 (viewed_tv)
				CREATE lf INDEX ind4 ON #a1 (head_of_hhd)
				COMMIT
				
				--MESSAGE cast(now() as timestamp)||' | PART B01.32 Completed ' TO CLIENT
				
				-- All Skybase has been set to tv viewers
				-- This will rescale them to Barb viewers by age gender group
				-- Do Head of HHD
				UPDATE SC3I_weighting_working_table w
				SET sky_base_accounts = sky_base_accounts * (barb_weight / age_gender_sky_base)
				FROM SC3I_Segments_lookup_v1_1_V 				AS l
					, V289_M11_04_Barb_weighted_population 		AS b
					, #a1 AS a
				WHERE w.scaling_segment_id = l.scaling_segment_id
					AND l.age_band = b.ageband
					AND l.age_band = a.age_band
					AND l.viewed_tv = 'Y'
					AND a.viewed_tv = 'Y'
					AND b.viewed_tv = 'Y'
					AND l.head_of_hhd = '1'
					AND a.head_of_hhd = '1'
					AND b.head_of_hhd = '1'

				COMMIT

				--MESSAGE cast(now() as timestamp)||' | PART B01.3 Completed ' TO CLIENT
				-- Do Non-Head of HHD
				UPDATE SC3I_weighting_working_table w
				SET sky_base_accounts = sky_base_accounts * (barb_weight / age_gender_sky_base)
				FROM SC3I_Segments_lookup_v1_1_V 			AS l
					, V289_M11_04_Barb_weighted_population 	AS b
					, #a1 AS a
				WHERE w.scaling_segment_id = l.scaling_segment_id
					AND l.age_band = b.ageband
					AND l.age_band = a.age_band
					AND l.viewed_tv = 'Y'
					AND a.viewed_tv = 'Y'
					AND b.viewed_tv = 'Y'
					AND l.head_of_hhd = '0'
					AND a.head_of_hhd = '0'
					AND b.head_of_hhd = '0'

				COMMIT
				DROP TABLE #a1
				COMMIT

				/***********************************************/
				-- Now tack on the universe flags; a special case of things coming out of the lookup
				UPDATE SC3I_weighting_working_table
				SET sky_base_universe = sl.sky_base_universe
				FROM SC3I_weighting_working_table
				INNER JOIN SC3I_Segments_lookup_v1_1_V AS sl ON SC3I_weighting_working_table.scaling_segment_id = sl.scaling_segment_id

				COMMIT

				-- Mix in the Vespa panel counts as determined earlier
				SELECT scaling_segment_id
					, count(1) AS panel_members
				INTO #segment_distribs
				FROM SC3I_Todays_panel_members
				WHERE scaling_segment_id IS NOT NULL
				GROUP BY scaling_segment_id

				COMMIT

				CREATE UNIQUE INDEX fake_pk ON #segment_distribs (scaling_segment_id)

				COMMIT

				-- It defaults to 0, so we can just poke values in
				UPDATE SC3I_weighting_working_table
				SET vespa_panel = sd.panel_members
				FROM SC3I_weighting_working_table
				INNER JOIN #segment_distribs AS sd ON SC3I_weighting_working_table.scaling_segment_id = sd.scaling_segment_id

				-- And we're done! log the progress.
				COMMIT
				DROP TABLE #segment_distribs
				COMMIT

				SET @QA_catcher = - 1

				SELECT @QA_catcher = count(1)
				FROM SC3I_weighting_working_table

				COMMIT
				EXECUTE logger_add_event @Scale_refresh_logging_ID, 3, 'B01: Complete! (Segmentation totals)', coalesce(@QA_catcher, - 1)
				COMMIT
				
				--MESSAGE cast(now() as timestamp)||' | PART B01.4 Completed ' TO CLIENT
				
				/* *************** ************************************************************/
				/* *************** PART B02: ASSIGNING WEIGHTS TO EACH SEGMENT ****************/
				/* *************** ************************************************************/
				
				DELETE FROM SC3I_category_subtotals
				WHERE scaling_date = @scaling_day

				DELETE FROM SC3I_metrics
				WHERE scaling_date = @scaling_day

				COMMIT

				-- Rim-weighting is an iterative process that iterates through each of the scaling variables
				-- individually until the category sum of weights converge to the population category subtotals
				SET @cntr = 1
				SET @iteration = 0
				SET @cntr_var = 1
				SET @scaling_var = (
						SELECT scaling_variable
						FROM SC3I_Variables_lookup_v1_1_V
						WHERE id = @cntr
						)
				SET @scaling_count = (
						SELECT COUNT(scaling_variable)
						FROM SC3I_Variables_lookup_v1_1_V
						)

				-- The SC3I_weighting_working_table table contains subtotals and sum_of_weights for all segments represented by
				-- the sky base.
				-- Some segments are not represented by the vespa panel, these are allocated an arbitrary value of 0.000001
				-- to ensure convergence.
				-- arbitrary value to ensure convergence
				UPDATE SC3I_weighting_working_table
				SET vespa_panel = 0.000001
				WHERE vespa_panel = 0

				COMMIT

				-- Initialise working columns
				UPDATE SC3I_weighting_working_table
				SET sum_of_weights = vespa_panel

				COMMIT

				-- The iterative part.
				-- This works by choosing a particular scaling variable and then summing across the categories
				-- of that scaling variable for the sky base, the vespa panel and the sum of weights.
				-- A Category weight is calculated by dividing the sky base subtotal by the vespa panel subtotal
				-- for that category.
				-- This category weight is then applied back to the segments table and the process repeats until
				-- the sum_of_weights in the category table converges to the sky base subtotal.
				-- Category Convergence is defined as the category sum of weights being +/- 3 away from the sky
				-- base category subtotal within 100 iterations.
				-- Overall Convergence for that day occurs when each of the categories has converged, or the @convergence variable = 0
				-- The @convergence variable represents how many categories did not converge.
				-- If the number of iterations = 100 and the @convergence > 0 then this means that the Rim-weighting
				-- has not converged for this particular day.
				-- In this scenario, the person running the code should send the results of the SC3I_metrics for that
				-- week to analytics team for review. ## What exactly are we checking? can we automate any of it?
			
				--MESSAGE cast(now() as timestamp)||' | PART B02.1 Completed ' TO CLIENT
			
				WHILE @cntr <= @scaling_count
				BEGIN
					DELETE
					FROM SC3I_category_working_table

					SET @cntr_var = 1

					WHILE @cntr_var <= @scaling_count
					BEGIN
						SELECT @scaling_var = scaling_variable
						FROM SC3I_Variables_lookup_v1_1_V
						WHERE id = @cntr_var

						EXECUTE (
								'
                         INSERT INTO SC3I_category_working_table (sky_base_universe,profile,value,sky_base_accounts,vespa_panel,sum_of_weights)
                             SELECT  srs.sky_base_universe
                                    ,@scaling_var
                                    ,ssl.' 
								|| @scaling_var || 
								'
                                    ,SUM(srs.sky_base_accounts)
                                    ,SUM(srs.vespa_panel)
                                    ,SUM(srs.sum_of_weights)
                             FROM SC3I_weighting_working_table AS srs
                                     inner join SC3I_Segments_lookup_v1_1_V AS ssl ON srs.scaling_segment_id = ssl.scaling_segment_id
                             GROUP BY srs.sky_base_universe,ssl.' 
								|| @scaling_var || '
                             ORDER BY srs.sky_base_universe
                         '
								)

						SET @cntr_var = @cntr_var + 1
					END

					COMMIT

					UPDATE SC3I_category_working_table
					SET category_weight = sky_base_accounts / sum_of_weights
						, convergence_flag = CASE 
							WHEN abs(sky_base_accounts - sum_of_weights) < 3
								THEN 0
							ELSE 1
							END

					SELECT @convergence = SUM(convergence_flag)
					FROM SC3I_category_working_table

					SET @iteration = @iteration + 1

					SELECT @scaling_var = scaling_variable
					FROM SC3I_Variables_lookup_v1_1_V
					WHERE id = @cntr

					EXECUTE  ('UPDATE SC3I_weighting_working_table
					SET  a.category_weight = sc.category_weight
						,a.sum_of_weights  = a.sum_of_weights * sc.category_weight
						FROM SC3I_weighting_working_table 	AS a
                     inner join SC3I_Segments_lookup_v1_1_V AS ssl 	ON a.scaling_segment_id = ssl.scaling_segment_id
                     inner join SC3I_category_working_table AS sc 	ON sc.value = ssl.' || @scaling_var || ' AND sc.sky_base_universe = ssl.sky_base_universe')

					COMMIT

					IF @iteration = 100 OR @convergence = 0
						SET @cntr = (@scaling_count + 1)
					ELSE IF @cntr = @scaling_count
						SET @cntr = 1
					ELSE
						SET @cntr = @cntr + 1
				END
			
			--MESSAGE cast(now() as timestamp)||' | PART B02.2  Completed ' TO CLIENT
			
			COMMIT

				-- This loop build took about 4 minutes. That's fine.
				-- Calculate segment weight and corresponding indices
				-- This section calculates the segment weight which is the weight that should be applied to viewing data
				-- A couple of indices are also calculated so that we can keep track of the performance of the rim-weighting
				SELECT @sky_base = SUM(sky_base_accounts)
				FROM SC3I_weighting_working_table

				SELECT @vespa_panel = SUM(vespa_panel)
				FROM SC3I_weighting_working_table

				SELECT @sum_of_weights = SUM(sum_of_weights)
				FROM SC3I_weighting_working_table

				UPDATE SC3I_weighting_working_table
				SET segment_weight = sum_of_weights / vespa_panel
					, indices_actual = 100 * (vespa_panel / @vespa_panel) / (sky_base_accounts / @sky_base)
					, indices_weighted = 100 * (sum_of_weights / @sum_of_weights) / (sky_base_accounts / @sky_base)

				COMMIT

				-- OK, now catch those cases where stuff diverged because segments weren't reperesented:
				UPDATE SC3I_weighting_working_table
				SET segment_weight = 0.000001
				WHERE vespa_panel = 0.000001

				COMMIT

				SET @QA_catcher = - 1

				SELECT @QA_catcher = count(1)
				FROM SC3I_weighting_working_table
				WHERE segment_weight >= 0.001 -- Ignore the placeholders here to guarantee convergence

				COMMIT

				EXECUTE logger_add_event @Scale_refresh_logging_ID
					, 3
					, 'B02: Midway (Iterations)'
					, coalesce(@QA_catcher, - 1)

				COMMIT
				
			--	MESSAGE cast(now() as timestamp)||' | PART B02.3  Completed ' TO CLIENT
				
				-- Now push convergence details out to the tracking tables: the first one provides a convergence summary at a category level
				INSERT INTO SC3I_category_subtotals (
					scaling_date
					, sky_base_universe
					, PROFILE
					, value
					, sky_base_accounts
					, vespa_panel
					, category_weight
					, sum_of_weights
					, convergence
					)
				SELECT @scaling_day
					, sky_base_universe
					, PROFILE
					, value
					, sky_base_accounts
					, vespa_panel
					, category_weight
					, sum_of_weights
					, CASE 
						WHEN abs(sky_base_accounts - sum_of_weights) > 3
							THEN 1
						ELSE 0
						END
				FROM SC3I_category_working_table

				-- The SC3I_metrics table contains metrics for a particular scaling date. It shows whether the
				-- Rim-weighting process converged for that day and the number of iterations. It also shows the
				-- maximum and average weight for that day and counts for the sky base and the vespa panel.
				COMMIT

				-- Apparently it should be reviewed each week, but what are we looking for?
				INSERT INTO SC3I_metrics (
					scaling_date
					, iterations
					, convergence
					, max_weight
					, av_weight
					, sum_of_weights
					, sky_base
					, vespa_panel
					, non_scalable_accounts
					)
				SELECT @scaling_day
					, @iteration
					, @convergence
					, MAX(segment_weight)
					, sum(segment_weight * vespa_panel) / sum(vespa_panel) -- gives the average weight by account (just uising AVG would give it average by segment id)
					, SUM(segment_weight * vespa_panel) -- again need some math because this table has one record per segment id rather than being at acocunt level
					, @sky_base
					, sum(CASE 
							WHEN segment_weight >= 0.001
								THEN vespa_panel
							ELSE NULL
							END)
					, sum(CASE 
							WHEN segment_weight < 0.001
								THEN vespa_panel
							ELSE NULL
							END)
				FROM SC3I_weighting_working_table
				
				--MESSAGE cast(now() as timestamp)||' | PART B02.4  Completed ' TO CLIENT	
				
				UPDATE SC3I_metrics
				SET sum_of_convergence = abs(sky_base - sum_of_weights)

				INSERT INTO SC3I_non_convergences (
					scaling_date
					, scaling_segment_id
					, difference
					)
				SELECT @scaling_day
					, scaling_segment_id
					, abs(sum_of_weights - sky_base_accounts)
				FROM SC3I_weighting_working_table
				WHERE abs((segment_weight * vespa_panel) - sky_base_accounts) > 3

				COMMIT

				EXECUTE logger_add_event @Scale_refresh_logging_ID
					, 3
					, 'B02: Complete (Calculate weights)'
					, coalesce(@QA_catcher, - 1)

				COMMIT

			--	MESSAGE cast(now() as timestamp)||' | PART B02.5  Completed ' TO CLIENT
				
				/**************** PART B03: PUBLISHING WEIGHTS INTO INTERFACE STRUCTURES ****************/
				-- Here is where that bit of interface code goes, including extending the intervals
				-- in the Scaling midway tables (which now happens one day ata time). Maybe this guy
				-- wants to go into a new and different stored procedure?
				-- Heh, this deletion process clears out everything *after* the scaling day, meaning we
				-- have to start from the beginning doing this processing... I guess we'll just manage
				-- the historical build like this. (This is because we'd otherwise have to manage adding
				-- additional records to the interval table when we re-run a day and break an interval
				-- that already exists, and that whole process would be annoying to manage.)
				-- Except we'll only nuke everything if we *rebuild* a day that's not already there.
				IF (
						SELECT count(1)
						FROM SC3I_Weightings
						WHERE scaling_day = @scaling_day
						) > 0
				BEGIN
					DELETE
					FROM SC3I_Weightings
					WHERE scaling_day = @scaling_day

					DELETE
					FROM SC3I_Intervals
					WHERE reporting_starts = @scaling_day

					UPDATE SC3I_Intervals
					SET reporting_ends = dateadd(day, - 1, @scaling_day)
					WHERE reporting_ends >= @scaling_day
				END

				COMMIT

				-- Part 1: Update the Vespa midway scaling tables. In Vespa Analysts? May as well
				-- also keep this in VIQ_prod too.
				INSERT INTO SC3I_Weightings
				SELECT @scaling_day
					, scaling_segment_id
					, vespa_panel
					, sky_base_accounts
					, segment_weight
					, sum_of_weights
					, indices_actual
					, indices_weighted
					, CASE 
						WHEN abs(sky_base_accounts - sum_of_weights) > 3
							THEN 1
						ELSE 0
						END
				FROM SC3I_weighting_working_table

				-- Might have to check that the filter on segment_weight doesn't leave any orphaned
				-- accounts about the place...
				COMMIT

				SET @QA_catcher = - 1

				SELECT @QA_catcher = count(1)
				FROM SC3I_Weightings
				WHERE scaling_day = @scaling_day

				COMMIT
				
				EXECUTE logger_add_event @Scale_refresh_logging_ID
					, 3
					, 'B03: Midway 1/4 (Midway weights)'
					, coalesce(@QA_catcher, - 1)
				
				
				COMMIT

				SET @QA_catcher = - 1

				SELECT @QA_catcher = count(1)
				FROM SC3I_Intervals
				WHERE reporting_ends = @scaling_day

				COMMIT

				EXECUTE logger_add_event @Scale_refresh_logging_ID
					, 3
					, 'B03: Midway 2/4 (Midway intervals)'
					, coalesce(@QA_catcher, - 1)

				COMMIT

				--MESSAGE cast(now() as timestamp)||' | PART B03.1  Completed ' TO CLIENT
				-- Part 2: Update the VIQ interface table (which needs the household key thing)
				IF (
						SELECT count(1)
						FROM V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING
						WHERE scaling_date = @scaling_day
						) > 0
				BEGIN
					DELETE
					FROM V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING
					WHERE scaling_date = @scaling_day
				END

				COMMIT

				INSERT INTO V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING
				SELECT ws.account_number
					, ws.HH_person_number
					, @scaling_day
					, wwt.segment_weight
					, @batch_date
				FROM SC3I_weighting_working_table AS wwt
				INNER JOIN SC3I_Sky_base_segment_snapshots AS ws ON wwt.scaling_segment_id = ws.population_scaling_segment_id -- need this table to get the cb_key_household items
				INNER JOIN SC3I_Todays_panel_members AS tpm ON ws.account_number = tpm.account_number -- Filter for today's panel only
					AND ws.hh_person_number = tpm.hh_person_number
					AND ws.profiling_date = @profiling_date

				COMMIT

				SET @QA_catcher = - 1

				SELECT @QA_catcher = count(1)
				FROM V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING
				WHERE scaling_date = @scaling_day

				COMMIT
				EXECUTE logger_add_event @Scale_refresh_logging_ID, 3, 'B03: Midway 3/4 (VIQ interface)', coalesce(@QA_catcher, - 1)
				COMMIT
				EXECUTE logger_add_event @Scale_refresh_logging_ID, 3, 'B03: Complete! (Publish weights)'COMMIT
				EXECUTE logger_add_event @Scale_refresh_logging_ID, 3, 'SC3: Weights made for ' || DATEFORMAT (@scaling_day, 'yyyy-mm-dd')
				--MESSAGE cast(now() as timestamp)||' | PART B03.2  Completed ' TO CLIENT
				
				COMMIT
			END;-- of procedure "V289_M11_04_SC3I_v1_1__make_weights_BARB"

			COMMIT;
