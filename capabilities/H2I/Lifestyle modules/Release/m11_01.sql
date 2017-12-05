CREATE OR REPLACE PROCEDURE ${SQLFILE_ARG001}.V289_M11_01_SC3_v1_1__do_weekly_segmentation_sv (
	@profiling_thursday DATE = NULL
	,@Scale_refresh_logging_ID BIGINT = NULL
	,@batch_date DATETIME = now()
	)
AS
BEGIN
	DECLARE @QA_catcher INTEGER
	DECLARE @tablespacename VARCHAR(40)

	COMMIT WORK

	DELETE
	FROM SC3_scaling_weekly_sample_sv

	COMMIT WORK

	IF @profiling_thursday IS NULL
	BEGIN
		SELECT @profiling_thursday = DATEFORMAT (
				(now() - datepart(weekday, "now" ())) - 2
				,'YYYY-MM-DD'
				)
	END

	COMMIT WORK

	DELETE
	FROM SC3_Sky_base_segment_snapshots_sv
	WHERE profiling_date = @profiling_thursday message convert(TIMESTAMP, now()) || ' | M11.1 - profiling_thursday :' || @profiling_thursday TO client

	COMMIT WORK

	SELECT account_number
		,cb_key_household
		,cb_key_individual
		,current_short_description
		,'rank' = rank() OVER (
			PARTITION BY account_number ORDER BY effective_from_dt DESC
				,cb_row_id ASC
			)
		,'uk_standard_account' = convert(BIT, 0)
		,'isba_tv_region' = convert(VARCHAR(30), NULL)
	INTO #weekly_sample
	FROM cust_subs_hist
	WHERE subscription_sub_type IN ('DTV Primary Viewing')
		AND status_code IN (
			'AC'
			,'AB'
			,'PC'
			)
		AND effective_from_dt <= @profiling_thursday
		AND effective_to_dt > @profiling_thursday
		AND effective_from_dt <> effective_to_dt
		AND EFFECTIVE_FROM_DT IS NOT NULL
		AND cb_key_household > 0
		AND cb_key_household IS NOT NULL
		AND cb_key_individual IS NOT NULL
		AND account_number IS NOT NULL
		AND service_instance_id IS NOT NULL message convert(TIMESTAMP, now()) || ' | M11.1 - weekly_sample rows:' || @@rowcount TO client

	COMMIT WORK

	DELETE
	FROM #weekly_sample
	WHERE rank > 1

	COMMIT WORK

	SET @QA_catcher = - 1

	SELECT @QA_catcher = count(1)
	FROM #weekly_sample

	COMMIT WORK

	COMMIT WORK

	CREATE UNIQUE INDEX fake_pk ON #weekly_sample (account_number)

	COMMIT WORK

	CREATE INDEX for_package_join ON #weekly_sample (current_short_description)

	COMMIT WORK

	UPDATE #weekly_sample AS a
	SET uk_standard_account = CASE 
			WHEN b.acct_type = 'Standard'
				AND b.account_number <> '?'
				AND b.pty_country_code = 'GBR'
				THEN 1
			ELSE 0
			END
		,isba_tv_region = CASE 
			WHEN b.isba_tv_region = 'Border'
				THEN 'NI, Scotland & Border'
			WHEN b.isba_tv_region = 'Central Scotland'
				THEN 'NI, Scotland & Border'
			WHEN b.isba_tv_region = 'East Of England'
				THEN 'Wales & Midlands'
			WHEN b.isba_tv_region = 'HTV Wales'
				THEN 'Wales & Midlands'
			WHEN b.isba_tv_region = 'HTV West'
				THEN 'South England'
			WHEN b.isba_tv_region = 'London'
				THEN 'London'
			WHEN b.isba_tv_region = 'Meridian (exc. Channel Islands)'
				THEN 'South England'
			WHEN b.isba_tv_region = 'Midlands'
				THEN 'Wales & Midlands'
			WHEN b.isba_tv_region = 'North East'
				THEN 'North England'
			WHEN b.isba_tv_region = 'North Scotland'
				THEN 'NI, Scotland & Border'
			WHEN b.isba_tv_region = 'North West'
				THEN 'North England'
			WHEN b.isba_tv_region = 'Not Defined'
				THEN 'Not Defined'
			WHEN b.isba_tv_region = 'South West'
				THEN 'South England'
			WHEN b.isba_tv_region = 'Ulster'
				THEN 'NI, Scotland & Border'
			WHEN b.isba_tv_region = 'Yorkshire'
				THEN 'North England'
			ELSE 'Not Defined'
			END
		,cb_key_individual = b.cb_key_individual
	FROM #weekly_sample AS a
	JOIN cust_single_account_view AS b ON a.account_number = b.account_number message convert(TIMESTAMP, now()) || ' | M11.1 - Update weekly_sample rows:' || @@rowcount TO client

	COMMIT WORK

	DELETE
	FROM #weekly_sample
	WHERE uk_standard_account = 0

	COMMIT WORK

	SET @QA_catcher = - 1

	COMMIT WORK

	SELECT @QA_catcher = count(1)
	FROM #weekly_sample

	COMMIT WORK

	COMMIT WORK

	SELECT cv.cb_key_household
		,cv.cb_key_family
		,cv.cb_key_individual
		,'cb_row_id' = min(cv.cb_row_id)
		,'h_household_composition' = max(cv.h_household_composition)
		,'p_head_of_household' = max(pp.p_head_of_household)
	INTO #cv_pp
	FROM EXPERIAN_CONSUMERVIEW AS cv
		,PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD AS pp
	WHERE cv.exp_cb_key_db_individual = pp.exp_cb_key_db_individual
		AND cv.cb_key_individual IS NOT NULL
	GROUP BY cv.cb_key_household
		,cv.cb_key_family
		,cv.cb_key_individual

	COMMIT WORK
	CREATE lf INDEX idx1 ON #cv_pp (p_head_of_household)
	CREATE hg INDEX idx2 ON #cv_pp (cb_key_family)
	CREATE hg INDEX idx3 ON #cv_pp (cb_key_individual)
	COMMIT WORK 
	message convert(TIMESTAMP, now()) || ' | M11.1 - cv_pp rows:' || @@rowcount TO client

	SELECT cb_key_household
		,cb_row_id
		,'rank_fam' = rank() OVER (
			PARTITION BY cb_key_family ORDER BY p_head_of_household DESC
				,cb_row_id DESC
			)
		,'rank_hhd' = rank() OVER (
			PARTITION BY cb_key_household ORDER BY p_head_of_household DESC
				,cb_row_id DESC
			)
		,'h_household_composition' = CASE 
			WHEN h_household_composition = '00'
				THEN 'A) Families'
			WHEN h_household_composition = '01'
				THEN 'A) Families'
			WHEN h_household_composition = '02'
				THEN 'A) Families'
			WHEN h_household_composition = '03'
				THEN 'A) Families'
			WHEN h_household_composition = '04'
				THEN 'B) Singles'
			WHEN h_household_composition = '05'
				THEN 'B) Singles'
			WHEN h_household_composition = '06'
				THEN 'C) Homesharers'
			WHEN h_household_composition = '07'
				THEN 'C) Homesharers'
			WHEN h_household_composition = '08'
				THEN 'C) Homesharers'
			WHEN h_household_composition = '09'
				THEN 'A) Families'
			WHEN h_household_composition = '10'
				THEN 'A) Families'
			WHEN h_household_composition = '11'
				THEN 'C) Homesharers'
			WHEN h_household_composition = 'U'
				THEN 'D) Unclassified HHComp'
			ELSE 'D) Unclassified HHComp'
			END
	INTO #cv_keys
	FROM #cv_pp
	WHERE cb_key_household IS NOT NULL
		AND cb_key_household <> 0

	COMMIT WORK

	DELETE
	FROM #cv_keys
	WHERE rank_fam <> 1
		AND rank_hhd <> 1

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M11.1 - cv_keys rows:' || @@rowcount TO client

	CREATE INDEX index_ac ON #cv_keys (cb_key_household)

	COMMIT WORK

	SET @QA_catcher = - 1

	COMMIT WORK

	SELECT @QA_catcher = count(1)
	FROM #cv_keys

	COMMIT WORK

	COMMIT WORK

	INSERT INTO SC3_scaling_weekly_sample_sv (
		account_number
		,cb_key_household
		,cb_key_individual
		,universe
		,sky_base_universe
		,vespa_universe
		,isba_tv_region
		,hhcomposition
		,tenure
		,num_mix
		,mix_pack
		,package
		,boxtype
		,no_of_stbs
		,hd_subscription
		,pvr
		)
	SELECT fbp.account_number
		,fbp.cb_key_household
		,fbp.cb_key_individual
		,'A) Single box HH'
		,'Not adsmartable'
		,'Non-Vespa'
		,fbp.isba_tv_region
		,'D)'
		,'D) Unknown'
		,'num_mix' = cel.Variety + cel.Knowledge + cel.Kids + cel.Style_Culture + cel.Music + cel.News_Events
		,'mix_pack' = CASE 
			WHEN Num_Mix IS NULL
				OR Num_Mix = 0
				THEN 'Entertainment Pack'
			WHEN (
					cel.variety = 1
					OR cel.style_culture = 1
					)
				AND Num_Mix = 1
				THEN 'Entertainment Pack'
			WHEN (
					cel.variety = 1
					AND cel.style_culture = 1
					)
				AND Num_Mix = 2
				THEN 'Entertainment Pack'
			WHEN Num_Mix > 0
				THEN 'Entertainment Extra'
			END
		,CASE 
			WHEN cel.prem_sports = 2
				AND cel.prem_movies = 2
				THEN 'Movies & Sports'
			WHEN cel.prem_sports = 2
				AND cel.prem_movies = 0
				THEN 'Sports'
			WHEN cel.prem_sports = 0
				AND cel.prem_movies = 2
				THEN 'Movies'
			WHEN cel.prem_sports = 1
				AND cel.prem_movies = 0
				THEN 'Sports'
			WHEN cel.prem_sports = 0
				AND cel.prem_movies = 1
				THEN 'Movies'
			WHEN cel.prem_sports > 0
				OR cel.prem_movies > 0
				THEN 'Movies & Sports'
			WHEN cel.prem_movies = 0
				AND cel.prem_sports = 0
				AND mix_pack = 'Entertainment Pack'
				THEN 'Basic'
			WHEN cel.prem_movies = 0
				AND cel.prem_sports = 0
				AND mix_pack = 'Entertainment Extra'
				THEN 'Basic'
			ELSE 'Basic'
			END
		,'D) FDB & No_secondary_box'
		,'Single'
		,'No'
		,'No'
	FROM #weekly_sample AS fbp
	LEFT OUTER JOIN cust_entitlement_lookup AS cel ON fbp.current_short_description = cel.short_description
	WHERE fbp.cb_key_household IS NOT NULL
		AND fbp.cb_key_individual IS NOT NULL message convert(TIMESTAMP, now()) || ' | M11.1 - SC3_scaling_weekly_sample_sv rows:' || @@rowcount TO client

	COMMIT WORK

	DROP TABLE #weekly_sample

	COMMIT WORK

	SELECT account_number
		,'sky_base_universe' = CASE 
			WHEN flag = 1
				AND cust_viewing_data_capture_allowed = 'Y'
				THEN 'Adsmartable with consent'
			WHEN flag = 1
				AND cust_viewing_data_capture_allowed <> 'Y'
				THEN 'Adsmartable but no consent'
			ELSE 'Not adsmartable'
			END
	INTO #cv_sbu
	FROM (
		SELECT 'account_number' = sav.account_number
			,adsmart.flag
			,cust_viewing_data_capture_allowed
		FROM (
			SELECT DISTINCT account_number
				,cust_viewing_data_capture_allowed
			FROM CUST_SINGLE_ACCOUNT_VIEW
			WHERE CUST_ACTIVE_DTV = 1
				AND pty_country_code = 'GBR'
			) AS sav
		LEFT OUTER JOIN (
			SELECT account_number
				,'flag' = max(CASE 
						WHEN x_pvr_type = 'PVR6'
							THEN 1
						WHEN x_pvr_type = 'PVR5'
							THEN 1
						WHEN x_pvr_type = 'PVR4'
							AND x_manufacturer = 'Samsung'
							THEN 1
						WHEN x_pvr_type = 'PVR4'
							AND x_manufacturer = 'Pace'
							THEN 1
						ELSE 0
						END)
			FROM (
				SELECT *
				FROM (
					SELECT account_number
						,x_pvr_type
						,x_personal_storage_capacity
						,currency_code
						,x_manufacturer
						,'active_flag' = rank() OVER (
							PARTITION BY service_instance_id ORDER BY ph_non_subs_link_sk DESC
							)
					FROM CUST_SET_TOP_BOX
					) AS base
				WHERE active_flag = 1
				) AS active_boxes
			WHERE currency_code = 'GBP'
			GROUP BY account_number
			) AS adsmart ON sav.account_number = adsmart.account_number
		) AS sub1

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M11.1 - cv_sbu rows:' || @@rowcount TO client

	UPDATE SC3_scaling_weekly_sample_sv AS stws
	SET stws.sky_base_universe = cv.sky_base_universe
	FROM SC3_scaling_weekly_sample_sv AS stws
	JOIN #cv_sbu AS cv ON stws.account_number = cv.account_number

	COMMIT WORK

	UPDATE SC3_scaling_weekly_sample_sv
	SET vespa_universe = CASE 
			WHEN sky_base_universe = 'Not adsmartable'
				THEN 'Vespa not Adsmartable'
			WHEN sky_base_universe = 'Adsmartable with consent'
				THEN 'Vespa adsmartable'
			WHEN sky_base_universe = 'Adsmartable but no consent'
				THEN 'Vespa but no consent'
			ELSE 'Non-Vespa'
			END

	COMMIT WORK

	DELETE
	FROM SC3_scaling_weekly_sample_sv
	WHERE (
			sky_base_universe IS NULL
			OR vespa_universe IS NULL
			)

	COMMIT WORK

	SET @QA_catcher = - 1

	COMMIT WORK

	SELECT @QA_catcher = count(1)
	FROM SC3_scaling_weekly_sample_sv

	UPDATE SC3_scaling_weekly_sample_sv AS stws
	SET stws.hhcomposition = cv.h_household_composition
	FROM SC3_scaling_weekly_sample_sv AS stws
	JOIN #cv_keys AS cv ON stws.cb_key_household = cv.cb_key_household

	COMMIT WORK

	DROP TABLE #cv_keys

	COMMIT WORK

	UPDATE SC3_scaling_weekly_sample_sv AS t1
	SET tenure = CASE 
			WHEN datediff(day, acct_first_account_activation_dt, @profiling_thursday) <= 730
				THEN 'A) 0-2 Years'
			WHEN datediff(day, acct_first_account_activation_dt, @profiling_thursday) <= 3650
				THEN 'B) 3-10 Years'
			WHEN datediff(day, acct_first_account_activation_dt, @profiling_thursday) > 3650
				THEN 'C) 10 Years+'
			ELSE 'D) Unknown'
			END
	FROM cust_single_account_view AS sav
	WHERE t1.account_number = sav.account_number

	COMMIT WORK

	DELETE
	FROM SC3_scaling_weekly_sample_sv
	WHERE tenure = 'D) Unknown'

	COMMIT WORK

	DELETE
	FROM SC3_scaling_weekly_sample_sv
	WHERE isba_tv_region = 'Not Defined'

	SELECT csh.service_instance_id
		,csh.account_number
		,subscription_sub_type
		,'rank' = rank() OVER (
			PARTITION BY csh.service_instance_id ORDER BY csh.account_number ASC
				,csh.cb_row_id DESC
			)
	INTO #accounts
	FROM cust_subs_hist AS csh
	JOIN SC3_scaling_weekly_sample_sv AS ss ON csh.account_number = ss.account_number
	WHERE csh.subscription_sub_type IN (
			'DTV Primary Viewing'
			,'DTV Extra Subscription'
			)
		AND csh.status_code IN (
			'AC'
			,'AB'
			,'PC'
			)
		AND csh.effective_from_dt <= @profiling_thursday
		AND csh.effective_to_dt > @profiling_thursday
		AND csh.effective_from_dt <> effective_to_dt

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M11.1 - accounts rows:' || @@rowcount TO client

	DELETE
	FROM #accounts
	WHERE rank > 1

	COMMIT WORK
	CREATE hg INDEX idx1 ON #accounts (service_instance_id)
	CREATE hg INDEX idx2 ON #accounts (account_number)
	COMMIT WORK

	SELECT stb.service_instance_id
		,'HD' = SUM(CASE 
				WHEN current_product_description LIKE '%HD%'
					THEN 1
				ELSE 0
				END)
		,'HD1TB' = SUM(CASE 
				WHEN x_description IN (
						'Amstrad HD PVR6 (1TB)'
						,'Amstrad HD PVR6 (2TB)'
						)
					THEN 1
				ELSE 0
				END)
	INTO #hda
	FROM CUST_SET_TOP_BOX AS stb
	JOIN #accounts AS acc ON stb.service_instance_id = acc.service_instance_id
	WHERE box_installed_dt <= @profiling_thursday
		AND box_replaced_dt > @profiling_thursday
		AND current_product_description LIKE '%HD%'
	GROUP BY stb.service_instance_id message convert(TIMESTAMP, now()) || ' | M11.1 - hda rows:' || @@rowcount TO client

	COMMIT WORK
	CREATE UNIQUE hg INDEX idx1 ON #hda (service_instance_id)
	COMMIT WORK

	SELECT acc.account_number
		,'PVR' = MAX(CASE 
				WHEN x_box_type LIKE '%Sky+%'
					THEN 'Yes'
				ELSE 'No'
				END)
	INTO #pvra
	FROM CUST_SET_TOP_BOX AS stb
	JOIN #accounts AS acc ON stb.service_instance_id = acc.service_instance_id
	WHERE box_installed_dt <= @profiling_thursday
		AND box_replaced_dt > @profiling_thursday
	GROUP BY acc.account_number message convert(TIMESTAMP, now()) || ' | M11.1 - pvra rows:' || @@rowcount TO client

	COMMIT WORK
	CREATE hg INDEX pvidx1 ON #pvra (account_number)
	COMMIT WORK

	UPDATE SC3_scaling_weekly_sample_sv AS stws
	SET stws.pvr = cv.pvr
	FROM SC3_scaling_weekly_sample_sv AS stws
	JOIN #pvra AS cv ON stws.account_number = cv.account_number

	COMMIT WORK

	UPDATE SC3_scaling_weekly_sample_sv
	SET pvr = CASE 
			WHEN sky_base_universe LIKE 'Adsmartable%'
				THEN 'Yes'
			ELSE 'No'
			END
	WHERE pvr IS NULL

	COMMIT WORK

	SET @QA_catcher = - 1

	COMMIT WORK

	SELECT @QA_catcher = count(1)
	FROM SC3_scaling_weekly_sample_sv
	WHERE pvr = 'No'
		AND sky_base_universe LIKE 'Adsmartable%'

	COMMIT WORK

	COMMIT WORK

	UPDATE SC3_scaling_weekly_sample_sv
	SET pvr = 'Yes'
	WHERE pvr = 'No'
		AND sky_base_universe LIKE 'Adsmartable%'

	COMMIT WORK

	SELECT acc.account_number
		,'MR' = MAX(CASE 
				WHEN csh.SUBSCRIPTION_SUB_TYPE = 'DTV Extra Subscription'
					THEN 1
				ELSE 0
				END)
		,'SP' = MAX(CASE 
				WHEN csh.SUBSCRIPTION_SUB_TYPE = 'DTV Sky+'
					THEN 1
				ELSE 0
				END)
		,'HD' = MAX(CASE 
				WHEN csh.SUBSCRIPTION_SUB_TYPE = 'DTV HD'
					THEN 1
				ELSE 0
				END)
		,'HDstb' = MAX(CASE 
				WHEN #hda.HD = 1
					THEN 1
				ELSE 0
				END)
		,'HD1TBstb' = MAX(CASE 
				WHEN #hda.HD1TB = 1
					THEN 1
				ELSE 0
				END)
	INTO #scaling_box_level_viewing
	FROM cust_subs_hist AS csh
	JOIN #accounts AS acc ON csh.service_instance_id = acc.service_instance_id
	LEFT OUTER JOIN cust_entitlement_lookup AS cel ON csh.current_short_description = cel.short_description
	LEFT OUTER JOIN #hda ON csh.service_instance_id = #hda.service_instance_id
	WHERE csh.effective_FROM_dt <= @profiling_thursday
		AND csh.effective_to_dt > @profiling_thursday
		AND csh.status_code IN (
			'AC'
			,'AB'
			,'PC'
			)
		AND csh.SUBSCRIPTION_SUB_TYPE IN (
			'DTV Primary Viewing'
			,'DTV Sky+'
			,'DTV Extra Subscription'
			,'DTV HD'
			)
		AND csh.effective_FROM_dt <> csh.effective_to_dt
	GROUP BY acc.service_instance_id
		,acc.account_number message convert(TIMESTAMP, now()) || ' | M11.1 - scaling_box_level_viewing rows:' || @@rowcount TO client

	COMMIT WORK

	DROP TABLE #accounts

	COMMIT WORK

	DROP TABLE #hda

	COMMIT WORK

	SELECT tgt.account_number
		,'mr_boxes' = SUM(CASE 
				WHEN MR = 1
					THEN 1
				ELSE 0
				END)
		,'pb' = MAX(CASE 
				WHEN MR = 0
					AND (
						(
							tgt.HD = 1
							AND HD1TBstb = 1
							)
						OR (
							tgt.HD = 1
							AND HDstb = 1
							)
						)
					THEN 4
				WHEN MR = 0
					AND (
						(
							tgt.SP = 1
							AND tgt.HD1TBstb = 1
							)
						OR (
							tgt.SP = 1
							AND tgt.HDstb = 1
							)
						)
					THEN 3
				WHEN MR = 0
					AND tgt.SP = 1
					THEN 2
				ELSE 1
				END)
		,'sb' = MAX(CASE 
				WHEN MR = 1
					AND (
						(
							tgt.HD = 1
							AND HD1TBstb = 1
							)
						OR (
							tgt.HD = 1
							AND HDstb = 1
							)
						)
					THEN 4
				WHEN MR = 1
					AND (
						(
							tgt.SP = 1
							AND tgt.HD1TBstb = 1
							)
						OR (
							tgt.SP = 1
							AND tgt.HDstb = 1
							)
						)
					THEN 3
				WHEN MR = 1
					AND tgt.SP = 1
					THEN 2
				ELSE 1
				END)
		,'universe' = convert(VARCHAR(20), NULL)
		,'boxtype' = convert(VARCHAR(30), NULL)
	INTO #boxtype_ac
	FROM #scaling_box_level_viewing AS tgt
	GROUP BY tgt.account_number message convert(TIMESTAMP, now()) || ' | M11.1 - boxtype_ac rows:' || @@rowcount TO client

	COMMIT WORK

	CREATE UNIQUE INDEX idx_ac ON #boxtype_ac (account_number)

	COMMIT WORK

	DROP TABLE #scaling_box_level_viewing

	COMMIT WORK

	SET @QA_catcher = - 1

	COMMIT WORK

	SELECT @QA_catcher = count(1)
	FROM #boxtype_ac

	COMMIT WORK

	COMMIT WORK

	UPDATE #boxtype_ac
	SET universe = CASE 
			WHEN mr_boxes = 0
				THEN 'A) Single box HH'
			ELSE 'B) Multiple box HH'
			END
		,boxtype = CASE 
			WHEN mr_boxes = 0
				AND pb = 3
				AND sb = 1
				THEN 'A) HDx & No_secondary_box'
			WHEN mr_boxes = 0
				AND pb = 4
				AND sb = 1
				THEN 'B) HD & No_secondary_box'
			WHEN mr_boxes = 0
				AND pb = 2
				AND sb = 1
				THEN 'C) Skyplus & No_secondary_box'
			WHEN mr_boxes = 0
				AND pb = 1
				AND sb = 1
				THEN 'D) FDB & No_secondary_box'
			WHEN mr_boxes > 0
				AND pb = 4
				AND sb = 4
				THEN 'E) HD & HD'
			WHEN mr_boxes > 0
				AND (
					pb = 4
					AND sb = 3
					)
				OR (
					pb = 3
					AND sb = 4
					)
				THEN 'E) HD & HD'
			WHEN mr_boxes > 0
				AND (
					pb = 4
					AND sb = 2
					)
				OR (
					pb = 2
					AND sb = 4
					)
				THEN 'F) HD & Skyplus'
			WHEN mr_boxes > 0
				AND (
					pb = 4
					AND sb = 1
					)
				OR (
					pb = 1
					AND sb = 4
					)
				THEN 'G) HD & FDB'
			WHEN mr_boxes > 0
				AND pb = 3
				AND sb = 3
				THEN 'H) HDx & HDx'
			WHEN mr_boxes > 0
				AND (
					pb = 3
					AND sb = 2
					)
				OR (
					pb = 2
					AND sb = 3
					)
				THEN 'I) HDx & Skyplus'
			WHEN mr_boxes > 0
				AND (
					pb = 3
					AND sb = 1
					)
				OR (
					pb = 1
					AND sb = 3
					)
				THEN 'J) HDx & FDB'
			WHEN mr_boxes > 0
				AND pb = 2
				AND sb = 2
				THEN 'K) Skyplus & Skyplus'
			WHEN mr_boxes > 0
				AND (
					pb = 2
					AND sb = 1
					)
				OR (
					pb = 1
					AND sb = 2
					)
				THEN 'L) Skyplus & FDB'
			ELSE 'M) FDB & FDB'
			END

	COMMIT WORK

	CREATE TABLE #SC3_weird_sybase_UPDATE_workaround (
		account_number VARCHAR(20) NOT NULL
		,cb_key_household BIGINT NOT NULL
		,cb_key_individual BIGINT NOT NULL
		,consumerview_cb_row_id BIGINT NULL
		,universe VARCHAR(30) NULL
		,sky_base_universe VARCHAR(30) NULL
		,vespa_universe VARCHAR(30) NULL
		,weighting_universe VARCHAR(30) NULL
		,isba_tv_region VARCHAR(30) NULL
		,hhcomposition VARCHAR(2) NOT NULL DEFAULT 'D)'
		,tenure VARCHAR(15) NOT NULL DEFAULT 'D) Unknown'
		,num_mix INTEGER NULL
		,mix_pack VARCHAR(20) NULL
		,package VARCHAR(20) NULL
		,boxtype VARCHAR(35) NULL
		,no_of_stbs VARCHAR(15) NULL
		,hd_subscription VARCHAR(5) NULL
		,pvr VARCHAR(5) NULL
		,population_scaling_segment_id INTEGER NULL DEFAULT NULL
		,vespa_scaling_segment_id INTEGER NULL DEFAULT NULL
		,mr_boxes INTEGER NULL
		,PRIMARY KEY (account_number)
		,
		)

	COMMIT WORK

	CREATE INDEX for_segment_identification_temp1 ON #SC3_weird_sybase_UPDATE_workaround (isba_tv_region)
	CREATE INDEX for_segment_identification_temp2 ON #SC3_weird_sybase_UPDATE_workaround (hhcomposition)
	CREATE INDEX for_segment_identification_temp3 ON #SC3_weird_sybase_UPDATE_workaround (tenure)
	CREATE INDEX for_segment_identification_temp4 ON #SC3_weird_sybase_UPDATE_workaround (package)
	CREATE INDEX for_segment_identification_temp5 ON #SC3_weird_sybase_UPDATE_workaround (boxtype)
	CREATE INDEX consumerview_joining ON #SC3_weird_sybase_UPDATE_workaround (consumerview_cb_row_id)
	CREATE INDEX for_temping1 ON #SC3_weird_sybase_UPDATE_workaround (population_scaling_segment_id)
	CREATE INDEX for_temping2 ON #SC3_weird_sybase_UPDATE_workaround (vespa_scaling_segment_id)
	COMMIT WORK

	INSERT INTO #SC3_weird_sybase_UPDATE_workaround (
		account_number
		,cb_key_household
		,cb_key_individual
		,consumerview_cb_row_id
		,universe
		,sky_base_universe
		,vespa_universe
		,isba_tv_region
		,hhcomposition
		,tenure
		,num_mix
		,mix_pack
		,package
		,boxtype
		,mr_boxes
		)
	SELECT sws.account_number
		,sws.cb_key_household
		,sws.cb_key_individual
		,sws.consumerview_cb_row_id
		,ac.universe
		,sky_base_universe
		,vespa_universe
		,sws.isba_tv_region
		,sws.hhcomposition
		,sws.tenure
		,sws.num_mix
		,sws.mix_pack
		,sws.package
		,ac.boxtype
		,ac.mr_boxes
	FROM SC3_scaling_weekly_sample_sv AS sws
	JOIN #boxtype_ac AS ac ON ac.account_number = sws.account_number
	WHERE sws.cb_key_household IS NOT NULL
		AND sws.cb_key_individual IS NOT NULL

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M11.1 - SC3_weird_sybase_UPDATE_workaround rows:' || @@rowcount TO client

	UPDATE #SC3_weird_sybase_UPDATE_workaround AS sws
	SET sws.pvr = ac.pvr
	FROM #pvra AS ac
	WHERE ac.account_number = sws.account_number

	COMMIT WORK

	DROP TABLE #boxtype_ac

	COMMIT WORK

	UPDATE #SC3_weird_sybase_UPDATE_workaround
	SET sky_base_universe = 'Not adsmartable'
	WHERE sky_base_universe IS NULL

	COMMIT WORK

	UPDATE #SC3_weird_sybase_UPDATE_workaround
	SET vespa_universe = 'Non-Vespa'
	WHERE sky_base_universe IS NULL

	COMMIT WORK

	UPDATE #SC3_weird_sybase_UPDATE_workaround
	SET weighting_universe = 'Not adsmartable'
	WHERE weighting_universe IS NULL

	COMMIT WORK

	UPDATE #SC3_weird_sybase_UPDATE_workaround
	SET pvr = CASE 
			WHEN sky_base_universe LIKE 'Adsmartable%'
				THEN 'Yes'
			ELSE 'No'
			END
	WHERE pvr IS NULL

	COMMIT WORK

	UPDATE #SC3_weird_sybase_UPDATE_workaround
	SET pvr = 'Yes'
	WHERE pvr = 'No'
		AND sky_base_universe LIKE 'Adsmartable%'

	COMMIT WORK

	UPDATE #SC3_weird_sybase_UPDATE_workaround
	SET no_of_stbs = CASE 
			WHEN Universe LIKE '%Single%'
				THEN 'Single'
			WHEN Universe LIKE '%Multiple%'
				THEN 'Multiple'
			ELSE 'Single'
			END

	COMMIT WORK

	UPDATE #SC3_weird_sybase_UPDATE_workaround
	SET hd_subscription = CASE 
			WHEN boxtype LIKE 'B)%'
				OR boxtype LIKE 'E)%'
				OR boxtype LIKE 'F)%'
				OR boxtype LIKE 'G)%'
				THEN 'Yes'
			ELSE 'No'
			END

	COMMIT WORK

	UPDATE #SC3_weird_sybase_UPDATE_workaround
	SET #SC3_weird_sybase_UPDATE_workaround.population_scaling_segment_ID = ssl.scaling_segment_ID
	FROM #SC3_weird_sybase_UPDATE_workaround
	JOIN vespa_analysts.SC3_Segments_lookup_v1_1 AS ssl ON trim(lower(#SC3_weird_sybase_UPDATE_workaround.sky_base_universe)) = trim(lower(ssl.sky_base_universe))
		AND left(#SC3_weird_sybase_UPDATE_workaround.hhcomposition, 2) = left(ssl.hhcomposition, 2)
		AND left(#SC3_weird_sybase_UPDATE_workaround.isba_tv_region, 20) = left(ssl.isba_tv_region, 20)
		AND #SC3_weird_sybase_UPDATE_workaround.Package = ssl.Package
		AND left(#SC3_weird_sybase_UPDATE_workaround.tenure, 2) = left(ssl.tenure, 2)
		AND #SC3_weird_sybase_UPDATE_workaround.no_of_stbs = ssl.no_of_stbs
		AND #SC3_weird_sybase_UPDATE_workaround.hd_subscription = ssl.hd_subscription
		AND #SC3_weird_sybase_UPDATE_workaround.pvr = ssl.pvr

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M11.1 - SC3_weird_sybase_UPDATE_workaround update:' || @@rowcount TO client

	UPDATE #SC3_weird_sybase_UPDATE_workaround
	SET vespa_scaling_segment_id = population_scaling_segment_ID

	COMMIT WORK

	DELETE
	FROM SC3_scaling_weekly_sample_sv

	COMMIT WORK

	INSERT INTO SC3_scaling_weekly_sample_sv
	SELECT *
	FROM #SC3_weird_sybase_UPDATE_workaround message convert(TIMESTAMP, now()) || ' | M11.1 - SC3_scaling_weekly_sample_sv insert:' || @@rowcount TO client

	COMMIT WORK

	DROP TABLE #SC3_weird_sybase_UPDATE_workaround

	INSERT INTO SC3_Sky_base_segment_snapshots_sv
	SELECT account_number
		,@profiling_thursday
		,cb_key_household
		,population_scaling_segment_id
		,vespa_scaling_segment_id
		,mr_boxes + 1
	FROM SC3_scaling_weekly_sample_sv
	WHERE population_scaling_segment_id IS NOT NULL
		AND vespa_scaling_segment_id IS NOT NULL 
		
		message convert(TIMESTAMP, now()) || ' | M11.1 - SC3_Sky_base_segment_snapshots_sv insert:' || @@rowcount TO client
END
GO
