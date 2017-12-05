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
CREATE OR replace PROCEDURE V289_M11_01_SC3_v1_1__do_weekly_segmentation_clean
	 @profiling_thursday DATE = NULL -- Day on which to do sky base profiling
	,@batch_date DATETIME = now () -- Day on which build was kicked off
	AS
BEGIN
	------- Base Tables preparation
	DELETE FROM SC3_scaling_weekly_sample
	TRUNCATE TABLE SC3_Sky_base_segment_snapshots
	COMMIT -- (^_^)

	-- Decide when we're doing the profiling, if it's not passed in as a parameter
	IF @profiling_thursday IS NULL
	BEGIN
		SELECT @profiling_thursday = DATEFORMAT ((now() - datepart(weekday, now())) - 2,'YYYY-MM-DD')
	END

	COMMIT -- (^_^)
	
	MESSAGE cast(now() as timestamp)||' | M11.1 Start ' TO CLIENT
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
		,CONVERT(VARCHAR(1), NULL)  AS data_capture_allowed
		,CONVERT(VARCHAR(15), NULL) AS tenure
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
		,data_capture_allowed = CASE WHEN CUST_ACTIVE_DTV = 1 AND pty_country_code = 'GBR' THEN b.cust_viewing_data_capture_allowed
			ELSE 'U' END
		, tenure = CASE 
					WHEN datediff(day, acct_first_account_activation_dt, @profiling_thursday) <= 730  THEN 'A) 0-2 Years'
					WHEN datediff(day, acct_first_account_activation_dt, @profiling_thursday) <= 3650 THEN 'B) 3-10 Years'
					WHEN datediff(day, acct_first_account_activation_dt, @profiling_thursday) > 3650  THEN 'C) 10 Years+'
					ELSE 'D) Unknown'
					END
	FROM #weekly_sample AS a
	INNER JOIN /*sk_prod.*/ cust_single_account_view AS b ON a.account_number = b.account_number

	MESSAGE cast(now() as timestamp)||' | M11.1 #weekly_sample update' TO CLIENT
	
	COMMIT -- (^_^)
	DELETE FROM #weekly_sample
	WHERE uk_standard_account = 0 
		OR tenure = 'D) Unknown' 
		OR isba_tv_region = 'Not Defined'
	COMMIT -- (^_^)

	/**************** L02: ASSIGN VARIABLES ****************/
	SELECT cv.cb_key_household
		,cv.cb_key_family
		,cv.cb_key_individual
		,min(cv.cb_row_id) AS cb_row_id
		,max(cv.h_household_composition) AS h_household_composition
		,max(pp.p_head_of_household) AS p_head_of_household
	INTO #cv_pp
	FROM /*sk_prod.*/ EXPERIAN_CONSUMERVIEW 		AS cv
	JOIN /*sk_prod.*/ PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD 	AS pp ON  cv.exp_cb_key_db_individual = pp.exp_cb_key_db_individual
	JOIN #weekly_sample	AS ws ON ws.cb_key_household =  cv.cb_key_household
	WHERE cv.cb_key_individual IS NOT NULL
	GROUP BY cv.cb_key_household
		,cv.cb_key_family
		,cv.cb_key_individual

	MESSAGE cast(now() as timestamp)||' | M11.1 #cv_pp creation. Rows:'||@@rowcount TO CLIENT
	
	COMMIT -- (^_^)
	CREATE LF INDEX idx1 ON #cv_pp (p_head_of_household)
	CREATE HG INDEX idx2 ON #cv_pp (cb_key_family)
	CREATE HG INDEX idx3 ON #cv_pp (cb_key_individual)
	COMMIT -- (^_^)

	SELECT cb_key_household
		,cb_row_id
		,RANK() OVER (PARTITION BY cb_key_family 	ORDER BY p_head_of_household DESC,cb_row_id DESC) 	AS RANK_fam
		,RANK() OVER (PARTITION BY cb_key_household ORDER BY p_head_of_household DESC,cb_row_id DESC) 	AS RANK_hhd
		,CASE 
			WHEN h_household_composition = '00'	THEN 'A) Families'
			WHEN h_household_composition = '01'	THEN 'A) Families'
			WHEN h_household_composition = '02'	THEN 'A) Families'
			WHEN h_household_composition = '03'	THEN 'A) Families'
			WHEN h_household_composition = '04'	THEN 'B) Singles'
			WHEN h_household_composition = '05'	THEN 'B) Singles'
			WHEN h_household_composition = '06'	THEN 'C) Homesharers'
			WHEN h_household_composition = '07'	THEN 'C) Homesharers'
			WHEN h_household_composition = '08'	THEN 'C) Homesharers'
			WHEN h_household_composition = '09'	THEN 'A) Families'
			WHEN h_household_composition = '10'	THEN 'A) Families'
			WHEN h_household_composition = '11'	THEN 'C) Homesharers'
			WHEN h_household_composition = 'U'	THEN 'D) Unclassified HHComp'
			ELSE 'D) Unclassified HHComp'		END AS h_household_composition
	INTO #cv_keys
	FROM #cv_pp
	WHERE cb_key_household IS NOT NULL
		AND cb_key_household <> 0

	MESSAGE cast(now() as timestamp)||' | M11.1 #cv_keys creation. Rows:'||@@rowcount TO CLIENT
	
	COMMIT -- (^_^)
	DELETE	FROM #cv_keys	
	WHERE RANK_fam != 1	AND RANK_hhd != 1

	COMMIT -- (^_^)
	CREATE INDEX index_ac ON #cv_keys (cb_key_household)
	COMMIT -- (^_^)

	-- Populate Package & ISBA TV Region
	INSERT INTO SC3_scaling_weekly_sample (
		account_number
		,cb_key_household
		,cb_key_individual
		,universe --scaling variables removed. Use later to set no_of_stbs
		,sky_base_universe -- Need to include this as they form part of a big index
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
		,'A) Single box HH' -- universe
		,'Not adsmartable' -- sky_base_universe
		,fbp.isba_tv_region -- isba_tv_region
		,'D)' -- hhcomposition
		,tenure
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
		,'D) FDB & No_secondary_box' -- boxtype
		,'Single' --no_of_stbs
		,'No' --hd_subscription
		,'No' --pvr
	FROM #weekly_sample AS fbp
	LEFT JOIN /*sk_prod.*/ cust_entitlement_lookup AS cel ON fbp.current_short_description = cel.short_description
	WHERE fbp.cb_key_household IS NOT NULL
		AND fbp.cb_key_individual IS NOT NULL

	MESSAGE cast(now() as timestamp)||' | M11.1 SC3_scaling_weekly_sample insert. Rows:'||@@rowcount TO CLIENT
	
	-- Populate sky_base_universe according to SQL code used to find adsmartable bozes in weekly reports
	SELECT account_number
		,CASE 	WHEN flag = 1 AND data_capture_allowed = 'Y'	THEN 'Adsmartable with consent'
				WHEN flag = 1	AND data_capture_allowed <> 'Y'THEN 'Adsmartable but no consent'
			ELSE 'Not adsmartable'
			END AS sky_base_universe
	INTO #cv_sbu
	FROM (SELECT sav.account_number AS account_number
			,adsmart.flag
			,data_capture_allowed
		FROM #weekly_sample AS sav
		LEFT JOIN (SELECT account_number 
						,max(CASE 	WHEN x_pvr_type = 'PVR6' THEN 1
									WHEN x_pvr_type = 'PVR5' THEN 1
									WHEN x_pvr_type = 'PVR4' AND x_manufacturer = 'Samsung' THEN 1
									WHEN x_pvr_type = 'PVR4' AND x_manufacturer = 'Pace' 	THEN 1
								ELSE 0 END) AS flag
				FROM 	(SELECT * FROM (SELECT account_number
										,x_pvr_type
										,x_manufacturer
										,RANK() OVER (PARTITION BY service_instance_id ORDER BY ph_non_subs_link_sk DESC) active_flag
								FROM /*sk_prod.*/ CUST_SET_TOP_BOX
								WHERE currency_code = 'GBP'
								) AS base
					WHERE active_flag = 1
					) AS active_boxes
				GROUP BY account_number
					) AS adsmart ON sav.account_number = adsmart.account_number
		) AS sub1

	MESSAGE cast(now() as timestamp)||' | M11.1 #cv_sbu   creation. Rows:'||@@rowcount TO CLIENT
	
	COMMIT -- (^_^)

	UPDATE SC3_scaling_weekly_sample
	SET stws.sky_base_universe = cv.sky_base_universe
	FROM SC3_scaling_weekly_sample AS stws
	INNER JOIN #cv_sbu AS cv ON stws.account_number = cv.account_number

	COMMIT -- (^_^)

	DELETE
	FROM SC3_scaling_weekly_sample
	WHERE sky_base_universe IS NULL 
	COMMIT -- (^_^)

	UPDATE SC3_scaling_weekly_sample
	SET stws.hhcomposition = cv.h_household_composition
	FROM SC3_scaling_weekly_sample AS stws
	INNER JOIN #cv_keys AS cv ON stws.cb_key_household = cv.cb_key_household

	COMMIT -- (^_^)
	DROP TABLE #cv_keys
	DROP TABLE #cv_sbu	
	DROP TABLE #weekly_sample
	COMMIT -- (^_^)
	COMMIT -- (^_^)

	-- Boxtype & Universe
	-- Capture all active boxes for this week
	SELECT csh.service_instance_id
		,csh.account_number
		,subscription_sub_type
		,RANK() OVER (PARTITION BY csh.service_instance_id ORDER BY csh.account_number ,csh.cb_row_id DESC) AS RANK
	INTO #accounts -- drop table #accounts
	FROM /*sk_prod.*/ cust_subs_hist AS csh
	INNER JOIN SC3_scaling_weekly_sample AS ss ON csh.account_number = ss.account_number
	WHERE csh.subscription_sub_type IN ('DTV Primary Viewing','DTV Extra Subscription') 
		AND csh.status_code IN ('AC','AB','PC') 
		AND csh.effective_from_dt <= @profiling_thursday
		AND csh.effective_to_dt > @profiling_thursday
		AND csh.effective_from_dt <> effective_to_dt

	MESSAGE cast(now() as timestamp)||' | M11.1 #accounts   creation. Rows:'||@@rowcount TO CLIENT
	
	COMMIT -- (^_^)
	DELETE FROM #accounts
	WHERE RANK > 1
	COMMIT -- (^_^)

	CREATE hg INDEX idx1 ON #accounts (service_instance_id)
	CREATE hg INDEX idx2 ON #accounts (account_number)
	COMMIT -- (^_^)

	-- Identify HD & 1TB/2TB HD boxes
	SELECT stb.service_instance_id
		,SUM(CASE WHEN current_product_description LIKE '%HD%' THEN 1
				ELSE 0 END) AS HD
		,SUM(CASE WHEN x_description IN ('Amstrad HD PVR6 (1TB)','Amstrad HD PVR6 (2TB)') THEN 1 ELSE 0 END) AS HD1TB
	INTO #hda 
	FROM /*sk_prod.*/ CUST_SET_TOP_BOX AS stb
	INNER JOIN #accounts AS acc ON stb.service_instance_id = acc.service_instance_id
	WHERE box_installed_dt <= @profiling_thursday
		AND box_replaced_dt > @profiling_thursday
		AND current_product_description LIKE '%HD%'
	GROUP BY stb.service_instance_id

	MESSAGE cast(now() as timestamp)||' | M11.1 #hda   creation. Rows:'||@@rowcount TO CLIENT
	
	-- Create index on HD table
	COMMIT -- (^_^)
	CREATE UNIQUE hg INDEX idx1 ON #hda (service_instance_id)
	COMMIT -- (^_^)

	-- Identify PVR boxes
	SELECT acc.account_number
		,MAX(CASE WHEN x_box_type LIKE '%Sky+%' THEN 'Yes'
				ELSE 'No' END) AS PVR
	INTO #pvra -- drop table #pvra
	FROM /*sk_prod.*/ CUST_SET_TOP_BOX AS stb
	INNER JOIN #accounts AS acc ON stb.service_instance_id = acc.service_instance_id
	WHERE box_installed_dt <= @profiling_thursday
		AND box_replaced_dt > @profiling_thursday
	GROUP BY acc.account_number

	MESSAGE cast(now() as timestamp)||' | M11.1 #pvra   creation. Rows:'||@@rowcount TO CLIENT
		
	COMMIT -- (^_^)
	CREATE hg INDEX pvidx1 ON #pvra (account_number)
	COMMIT -- (^_^)

	-- PVR
	UPDATE SC3_scaling_weekly_sample
	SET stws.pvr = cv.pvr
	FROM SC3_scaling_weekly_sample AS stws
	INNER JOIN #pvra AS cv ON stws.account_number = cv.account_number

	COMMIT -- (^_^)
	UPDATE SC3_scaling_weekly_sample
	SET pvr = 'Yes'
	WHERE ((pvr = 'No') OR pvr IS NULL) 
		AND sky_base_universe LIKE 'Adsmartable%'
	COMMIT -- (^_^)

	SELECT 
		acc.account_number
		,MAX(CASE WHEN csh.SUBSCRIPTION_SUB_TYPE = 'DTV Extra Subscription' THEN 1 ELSE 0 END) AS MR
		,MAX(CASE WHEN csh.SUBSCRIPTION_SUB_TYPE = 'DTV Sky+' 				THEN 1 ELSE 0 END) AS SP
		,MAX(CASE WHEN csh.SUBSCRIPTION_SUB_TYPE = 'DTV HD' 				THEN 1 ELSE 0 END) AS HD
		,MAX(CASE WHEN #hda.HD = 1 											THEN 1 ELSE 0 END) AS HDstb
		,MAX(CASE WHEN #hda.HD1TB = 1 										THEN 1 ELSE 0 END) AS HD1TBstb
	INTO #scaling_box_level_viewing
	FROM /*sk_prod.*/ cust_subs_hist AS csh
	INNER JOIN #accounts AS acc ON csh.service_instance_id = acc.service_instance_id --< Limits to your universe
	LEFT OUTER JOIN /*sk_prod.*/ cust_entitlement_lookup cel ON csh.current_short_description = cel.short_description
	LEFT OUTER JOIN #hda ON csh.service_instance_id = #hda.service_instance_id --< Links to the HD Set Top Boxes
	WHERE csh.effective_FROM_dt <= @profiling_thursday
		AND csh.effective_to_dt > @profiling_thursday
		AND csh.status_code IN ('AC','AB','PC')
		AND csh.SUBSCRIPTION_SUB_TYPE IN ('DTV Primary Viewing','DTV Sky+','DTV Extra Subscription','DTV HD')
		AND csh.effective_FROM_dt <> csh.effective_to_dt
	GROUP BY acc.service_instance_id
		,acc.account_number

	MESSAGE cast(now() as timestamp)||' | M11.1 #scaling_box_level_viewing   creation. Rows:'||@@rowcount TO CLIENT
	
	COMMIT -- (^_^)
	DROP TABLE #accounts
	DROP TABLE #hda
	COMMIT -- (^_^)

	-- Identify boxtype of each box and whether it is a primary or a secondary box
	SELECT tgt.account_number
		,SUM(CASE WHEN MR = 1 THEN 1 ELSE 0 END) AS mr_boxes
		,MAX(CASE 	WHEN MR = 0 AND ((tgt.HD = 1 AND HD1TBstb = 1) OR ( tgt.HD = 1 AND HDstb = 1)) THEN 4 -- HD ( inclusive of HD1TB)
					WHEN MR = 0 AND ((tgt.SP = 1 AND tgt.HD1TBstb = 1 ) OR (tgt.SP = 1 AND tgt.HDstb = 1 ))			THEN 3 -- HDx ( inclusive of HD1TB)
					WHEN MR = 0 AND tgt.SP = 1 THEN 2 -- Skyplus 
					ELSE 1 END) AS pb -- FDB
		,MAX(CASE 	WHEN MR = 1 AND ((tgt.HD = 1 AND HD1TBstb = 1) OR (tgt.HD = 1 AND HDstb = 1)) THEN 4 -- HD ( inclusive of HD1TB)
					WHEN MR = 1 AND ((tgt.SP = 1 AND tgt.HD1TBstb = 1) OR (tgt.SP = 1 AND tgt.HDstb = 1)) THEN 3 -- HDx ( inclusive of HD1TB)
					WHEN MR = 1 AND tgt.SP = 1 THEN 2 -- Skyplus
					ELSE 1 END) AS sb -- FDB
		,CONVERT(VARCHAR(20), NULL) AS universe
		,CONVERT(VARCHAR(30), NULL) AS boxtype
	INTO #boxtype_ac -- drop table #boxtype_ac
	FROM #scaling_box_level_viewing AS tgt
	GROUP BY tgt.account_number

	MESSAGE cast(now() as timestamp)||' | M11.1 #boxtype_ac   creation. Rows:'||@@rowcount TO CLIENT
	
	-- Create indices on box-level boxtype temp table
	COMMIT -- (^_^)
	CREATE UNIQUE INDEX idx_ac ON #boxtype_ac (account_number)
	DROP TABLE #scaling_box_level_viewing
	COMMIT -- (^_^)

	-- Build the combined flags
	UPDATE #boxtype_ac
	SET universe = CASE WHEN mr_boxes = 0 THEN 'A) Single box HH'
				ELSE 'B) Multiple box HH' END
		,boxtype = CASE WHEN mr_boxes = 0 AND pb = 3 AND sb = 1 THEN 'A) HDx & No_secondary_box'
						WHEN mr_boxes = 0 AND pb = 4 AND sb = 1 THEN 'B) HD & No_secondary_box'
						WHEN mr_boxes = 0 AND pb = 2 AND sb = 1 THEN 'C) Skyplus & No_secondary_box'
						WHEN mr_boxes = 0 AND pb = 1 AND sb = 1 THEN 'D) FDB & No_secondary_box'
						WHEN mr_boxes > 0 AND pb = 4 AND sb = 4 THEN 'E) HD & HD' -- If a hh has HD  then all boxes have HD (therefore no HD and HDx)
						WHEN mr_boxes > 0 AND (pb = 4 AND sb = 3) OR (pb = 3 AND sb = 4) THEN 'E) HD & HD'
						WHEN mr_boxes > 0 AND (pb = 4 AND sb = 2) OR (pb = 2 AND sb = 4) THEN 'F) HD & Skyplus'
						WHEN mr_boxes > 0 AND (pb = 4 AND sb = 1) OR (pb = 1 AND sb = 4) THEN 'G) HD & FDB'
						WHEN mr_boxes > 0 AND pb = 3 AND sb = 3 THEN 'H) HDx & HDx' WHEN mr_boxes > 0 AND (pb = 3 AND sb = 2 )
											OR (pb = 2 AND sb = 3) THEN 'I) HDx & Skyplus' 
						WHEN mr_boxes > 0 AND (pb = 3 AND sb = 1) OR (pb = 1 AND sb = 3) THEN 'J) HDx & FDB'
						WHEN mr_boxes > 0 AND pb = 2 AND sb = 2 THEN 'K) Skyplus & Skyplus'
						WHEN mr_boxes > 0 AND (pb = 2 AND sb = 1) OR (pb = 1 AND sb = 2) THEN 'L) Skyplus & FDB'
						ELSE 'M) FDB & FDB' END 
		COMMIT -- (^_^)

	CREATE TABLE #SC3_weird_sybase_update_workaround (
						account_number 					VARCHAR(20)  PRIMARY KEY  NOT NULL
						,cb_key_household 				BIGINT 		NOT NULL
						,cb_key_individual 				BIGINT 		NOT NULL
						,consumerview_cb_row_id 		BIGINT 		NULL
						,universe 						VARCHAR(30) NULL -- Single or multiple box household. Reused for no_of_stbs
						,sky_base_universe 				VARCHAR(30) NULL -- Not adsmartable, Adsmartable with consent, Adsmartable but no consent household
						,vespa_universe					VARCHAR(30) NULL -- NOT USED
						,weighting_universe				VARCHAR(30) NULL -- NOT USED
						,isba_tv_region 				VARCHAR(30) NULL -- Scaling variable 1 : Region
						,hhcomposition 					VARCHAR(2) 	DEFAULT 'D)' 			NOT NULL -- Scaling variable 2: Household composition
						,tenure 						VARCHAR(15) DEFAULT 'D) Unknown' 	NOT NULL -- Scaling variable 3: Tenure
						,num_mix 						INT NULL
						,mix_pack 						VARCHAR(20) NULL
						,package 						VARCHAR(20) NULL -- Scaling variable 4: Package
						,boxtype 						VARCHAR(35) NULL -- Old Scaling variable 5: Household boxtype split into no_of_stbs, hd_subscription and pvr.
						,no_of_stbs 					VARCHAR(15) NULL -- Scaling variable 5: No of set top boxes
						,hd_subscription 				VARCHAR(5) 	NULL -- Scaling variable 6: HD subscription
						,pvr 							VARCHAR(5) 	NULL -- Scaling variable 7: Is the box pvr capable?
						,population_scaling_segment_id 	INT DEFAULT NULL NULL -- segment scaling id for identifying segments
						,vespa_scaling_segment_id 		INT DEFAULT NULL NULL -- segment scaling id for identifying segments
						,mr_boxes 						INT 		NULL
						)

	COMMIT -- (^_^)
	CREATE INDEX for_segment_identification_temp1 ON #SC3_weird_sybase_update_workaround (isba_tv_region)
	CREATE INDEX for_segment_identification_temp2 ON #SC3_weird_sybase_update_workaround (hhcomposition)
	CREATE INDEX for_segment_identification_temp3 ON #SC3_weird_sybase_update_workaround (tenure)
	CREATE INDEX for_segment_identification_temp4 ON #SC3_weird_sybase_update_workaround (package)
	CREATE INDEX for_segment_identification_temp5 ON #SC3_weird_sybase_update_workaround (boxtype)
	CREATE INDEX consumerview_joining ON #SC3_weird_sybase_update_workaround (consumerview_cb_row_id)
	CREATE INDEX for_temping1 ON #SC3_weird_sybase_update_workaround (population_scaling_segment_id)
	CREATE INDEX for_temping2 ON #SC3_weird_sybase_update_workaround (vespa_scaling_segment_id)
	COMMIT -- (^_^)

	INSERT INTO #SC3_weird_sybase_update_workaround (
		account_number
		,cb_key_household
		,cb_key_individual
		,consumerview_cb_row_id
		,universe
		,sky_base_universe
		,isba_tv_region
		,hhcomposition
		,tenure
		,num_mix
		,mix_pack
		,package
		,boxtype
		,mr_boxes
		,pvr
		,no_of_stbs
		,hd_subscription
		)
	SELECT sws.account_number
		,sws.cb_key_household
		,sws.cb_key_individual
		,sws.consumerview_cb_row_id
		,ac.universe
		,sky_base_universe
		,sws.isba_tv_region
		,sws.hhcomposition
		,sws.tenure
		,sws.num_mix
		,sws.mix_pack
		,sws.package
		,ac.boxtype
		,ac.mr_boxes
		,sws.pvr
		,no_of_stbs = CASE 	WHEN ac.Universe LIKE '%Single%' 	THEN 'Single'
							WHEN ac.Universe LIKE '%Multiple%' THEN 'Multiple'
							ELSE 'Single' END
		,hd_subscription = CASE WHEN ac.boxtype LIKE 'B)%'
									OR ac.boxtype LIKE 'E)%'
									OR ac.boxtype LIKE 'F)%'
									OR ac.boxtype LIKE 'G)%' THEN 'Yes'
								ELSE 'No' END
	FROM SC3_scaling_weekly_sample AS sws
	INNER JOIN #boxtype_ac AS ac ON ac.account_number = sws.account_number
	WHERE sws.cb_key_household IS NOT NULL
		AND sws.cb_key_individual IS NOT NULL

	MESSAGE cast(now() as timestamp)||' | M11.1 #SC3_weird_sybase_update_workaround   creation. Rows:'||@@rowcount TO CLIENT
	
	COMMIT -- (^_^)
	DROP TABLE #boxtype_ac
	COMMIT -- (^_^)

	/**************** L03: ASSIGN SCALING SEGMENT ID ****************/
	UPDATE #SC3_weird_sybase_update_workaround
	SET sky_base_universe = 'Not adsmartable'
	WHERE sky_base_universe IS NULL

	COMMIT -- (^_^)

	UPDATE #SC3_weird_sybase_update_workaround
	SET #SC3_weird_sybase_update_workaround.population_scaling_segment_ID 	= ssl.scaling_segment_ID
	 ,  #SC3_weird_sybase_update_workaround.vespa_scaling_segment_id	 	= ssl.scaling_segment_ID
	FROM #SC3_weird_sybase_update_workaround
	INNER JOIN vespa_analysts.SC3_Segments_lookup_v1_1 AS ssl ON TRIM(lower(#SC3_weird_sybase_update_workaround.sky_base_universe)) = TRIM(lower(ssl.sky_base_universe))
		AND LEFT(#SC3_weird_sybase_update_workaround.hhcomposition, 2) = LEFT(ssl.hhcomposition, 2)
		AND LEFT(#SC3_weird_sybase_update_workaround.isba_tv_region, 20) = LEFT(ssl.isba_tv_region, 20)
		AND #SC3_weird_sybase_update_workaround.Package = ssl.Package
		AND LEFT(#SC3_weird_sybase_update_workaround.tenure, 2) = LEFT(ssl.tenure, 2)
		AND #SC3_weird_sybase_update_workaround.no_of_stbs = ssl.no_of_stbs
		AND #SC3_weird_sybase_update_workaround.hd_subscription = ssl.hd_subscription
		AND #SC3_weird_sybase_update_workaround.pvr = ssl.pvr

	COMMIT -- (^_^)

	DELETE
	FROM SC3_scaling_weekly_sample
	COMMIT -- (^_^)

	INSERT INTO SC3_scaling_weekly_sample
	SELECT *
	FROM #SC3_weird_sybase_update_workaround

	COMMIT -- (^_^)
	DROP TABLE #SC3_weird_sybase_update_workaround

	/**************** L04: PUBLISHING INTO INTERFACE STRUCTURES ****************/
	INSERT INTO SC3_Sky_base_segment_snapshots
	SELECT account_number
		,@profiling_thursday
		,cb_key_household -- This guy still needs to be added to SC3_scaling_weekly_sample
		,population_scaling_segment_id
		,vespa_scaling_segment_id
		,mr_boxes + 1 -- Number of multiroom boxes plus 1 for the primary
	FROM SC3_scaling_weekly_sample
	WHERE population_scaling_segment_id IS NOT NULL
		AND vespa_scaling_segment_id IS NOT NULL -- still perhaps with the weird account from Eire?

	MESSAGE cast(now() as timestamp)||' | M11.1 SC3_Sky_base_segment_snapshots   creation. Rows:'||@@rowcount TO CLIENT
	
	
	COMMIT -- (^_^)
END;-- of procedure "V289_M11_01_SC3_v1_1__do_weekly_segmentation"

COMMIT;
