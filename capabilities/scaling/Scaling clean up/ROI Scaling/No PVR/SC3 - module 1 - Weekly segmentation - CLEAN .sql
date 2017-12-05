CREATE OR REPLACE PROCEDURE SC3_ROI_do_weekly_segmentation_no_pvr
	 @profiling_thursday DATE = NULL -- Day on which to do sky base profiling
	,@batch_date DATETIME = now () -- Day on which build was kicked off
AS
BEGIN

	 MESSAGE cast(now() as timestamp)||' | @ SC3_ROI 1. Initialising Environment' TO CLIENT
	 
	DELETE FROM SC3_ROI_scaling_weekly_sample
	COMMIT

	if @profiling_thursday is null
	begin
	 execute vespa_analysts.Regulars_Get_report_end_date @profiling_thursday output  -- proc returns a Saturday
	 set @profiling_thursday = @profiling_thursday - 2                               -- but we want a Thursday
	end
	commit

	DELETE FROM SC3_ROI_Sky_base_segment_snapshots 
	commit

	MESSAGE cast(now() as timestamp)||' | @ SC3_ROI 1. Initialising Environment DONE' TO CLIENT
	
	/**************** L01: ESTABLISH POPULATION ****************/
	-- We need the segmentation over the whole Sky base so we can scale up
	-- Captures all active accounts in cust_subs_hist

	SELECT  account_number
		 ,cb_key_household
		 ,cb_key_individual
		 ,current_short_description
		 ,rank() over (PARTITION BY account_number ORDER BY effective_from_dt desc, cb_row_id) AS rank
		 ,convert(bit, 0)  AS ROI_account_flag
		 ,convert(VARCHAR(30), NULL) AS ROI_Region
		 ,convert(VARCHAR(1), NULL) AS cust_viewing_data_capture_allowed
		 ,convert(VARCHAR(15), NULL) AS tenure
	INTO #weekly_sample
	FROM /*sk_prod.*/cust_subs_hist
	WHERE subscription_sub_type IN ('DTV Primary Viewing')
		AND status_code IN ('AC','AB','PC')
		AND effective_from_dt    <= @profiling_thursday
		AND effective_to_dt      > @profiling_thursday
		AND effective_from_dt    <> effective_to_dt
		AND EFFECTIVE_FROM_DT    IS NOT NULL
		AND account_number       IS NOT NULL
		AND service_instance_id  IS NOT NULL

	MESSAGE cast(now() as timestamp)||' | @ SC3_ROI 1. #weekly_sample Created: '||@@rowcount  TO CLIENT
	-- De-dupes accounts
	COMMIT
	DELETE FROM #weekly_sample WHERE rank > 1
	COMMIT

	-- Create indices
	CREATE UNIQUE INDEX fake_pk ON #weekly_sample (account_number)
	CREATE INDEX for_package_JOIN ON #weekly_sample (current_short_description)
	COMMIT

	-- Take out ROIs (Republic of Ireland) AND non-standard accounts as these are not currently in the scope of Vespa
	UPDATE #weekly_sample
	SET a.ROI_account_flag = CASE WHEN  b.pty_country_code = 'IRL' AND b.fin_currency_code = 'EUR' THEN 1 
			ELSE 0 END
		,a.ROI_Region = CASE 	WHEN UPPER (b.cb_address_county) IN ('GALWAY','LEITRIM','MAYO','ROSCOMMON','SLIGO') THEN 'Connacht'
								WHEN UPPER (b.cb_address_county) IN ('CARLOW','KILDARE','KILKENNY','LAOIS','LONGFORD',
																	'LOUTH','MEATH','OFFALY','WESTMEATH','WEXFORD','WICKLOW') THEN 'Leinster'
								WHEN UPPER (b.cb_address_county) IN ('CLARE','CORK','KERRY','LIMERICK','TIPPERARY','WATERFORD') THEN 'Munster'
								WHEN UPPER (b.cb_address_county) IN ('DUBLIN') THEN 'Dublin'
								WHEN UPPER (b.cb_address_county) IN ('CAVAN','DONEGAL','MONAGHAN') THEN 'Ulster'
								WHEN UPPER(PTY_COUNTY_RAW) LIKE '%DUBLIN%' 		THEN 'Dublin'-- MAKE SURE WESTMEATH IS ABOVE MEATH IN THE HIERARCHY OTHERWISE WESTMEATH WILL GET SET TO MEATH!
								WHEN UPPER(PTY_COUNTY_RAW) LIKE '%WESTMEATH%' 	THEN 'Leinster'
								WHEN UPPER(PTY_COUNTY_RAW) LIKE '%CARLOW%' 		THEN 'Leinster'
								WHEN UPPER(PTY_COUNTY_RAW) LIKE '%CAVAN%' 		THEN 'Ulster'
								WHEN UPPER(PTY_COUNTY_RAW) LIKE '%CLARE%' 		THEN 'Munster'
								WHEN UPPER(PTY_COUNTY_RAW) LIKE '%CORK%' 		THEN 'Munster'
								WHEN UPPER(PTY_COUNTY_RAW) LIKE '%DONEGAL%' 	THEN 'Ulster'
								WHEN UPPER(PTY_COUNTY_RAW) LIKE '%GALWAY%' 		THEN 'Connacht'
								WHEN UPPER(PTY_COUNTY_RAW) LIKE '%KERRY%' 		THEN 'Munster'
								WHEN UPPER(PTY_COUNTY_RAW) LIKE '%KILDARE%' 	THEN 'Leinster'
								WHEN UPPER(PTY_COUNTY_RAW) LIKE '%KILKENNY%' 	THEN 'Leinster'
								WHEN UPPER(PTY_COUNTY_RAW) LIKE '%LAOIS%' 		THEN 'Leinster'
								WHEN UPPER(PTY_COUNTY_RAW) LIKE '%LEITRIM%' 	THEN 'Connacht'
								WHEN UPPER(PTY_COUNTY_RAW) LIKE '%LIMERICK%' 	THEN 'Munster'
								WHEN UPPER(PTY_COUNTY_RAW) LIKE '%LONGFORD%' 	THEN 'Leinster'
								WHEN UPPER(PTY_COUNTY_RAW) LIKE '%LOUTH%' 		THEN 'Leinster'
								WHEN UPPER(PTY_COUNTY_RAW) LIKE '%MAYO%' 		THEN 'Connacht'
								WHEN UPPER(PTY_COUNTY_RAW) LIKE '%MEATH%' 		THEN 'Leinster'
								WHEN UPPER(PTY_COUNTY_RAW) LIKE '%MONAGHAN%' 	THEN 'Ulster'
								WHEN UPPER(PTY_COUNTY_RAW) LIKE '%OFFALY%' 		THEN 'Leinster'
								WHEN UPPER(PTY_COUNTY_RAW) LIKE '%ROSCOMMON%' 	THEN 'Connacht'
								WHEN UPPER(PTY_COUNTY_RAW) LIKE '%SLIGO%' 		THEN 'Connacht'
								WHEN UPPER(PTY_COUNTY_RAW) LIKE '%TIPPERARY%' 	THEN 'Munster'
								WHEN UPPER(PTY_COUNTY_RAW) LIKE '%WATERFORD%' 	THEN 'Munster'
								WHEN UPPER(PTY_COUNTY_RAW) LIKE '%WEXFORD%' 	THEN 'Leinster'
								WHEN UPPER(PTY_COUNTY_RAW) LIKE '%WICKLOW%' 	THEN 'Leinster'
								WHEN PTY_COUNTY_RAW IS NULL AND UPPER(PTY_TOWN_RAW) LIKE '%DUBLIN%' THEN 'DUBLIN'
								
								ELSE 'Not Defined'
								END
		,a.cb_key_individual = b.cb_key_individual
		,a.cust_viewing_data_capture_allowed = b.cust_viewing_data_capture_allowed
		,a.tenure = CASE   WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) <=  1095 THEN 'A) 0-2 Years'
                         WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) <= 2920 THEN 'B) 3-10 Years'
                         WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) > 2920 THEN  'C) 10 Years+'
                         ELSE 'D) Unknown'
                  END
	FROM #weekly_sample AS a
	JOIN /*sk_prod.*/cust_single_account_view AS b ON a.account_number = b.account_number

	COMMIT
	DELETE FROM #weekly_sample WHERE ROI_account_flag=0
	DELETE FROM #weekly_sample WHERE ROI_Region = 'Not Defined'
	DELETE FROM #weekly_sample WHERE tenure = 'D) Unknown'
	COMMIT

	MESSAGE cast(now() as timestamp)||' | @ SC3_ROI 1. #weekly_sample Deduped' TO CLIENT
	
	/**************** L02: ASSIGN VARIABLES ****************/
	-- Populate Package & ISBA TV Region
	INSERT INTO SC3_ROI_scaling_weekly_sample (
			 account_number
			 ,cb_key_household
			 ,cb_key_individual
			 ,universe    --scaling variables removed. Use later to set no_of_stbs
			 ,sky_base_universe  -- Need to include this as they form part of a big index
			 ,vespa_universe  -- Need to include this as they form part of a big index
			 ,ROI_Region
			 ,tenure
			 ,num_mix
			 ,mix_pack
			 ,package
			 ,boxtype
			 ,no_of_stbs
			 ,hd_subscription
			 ,pvr
			)
	SELECT
		fbp.account_number
		,fbp.cb_key_household
		,fbp.cb_key_individual
		,'A) Single box HH' -- universe
		,'Not adsmartable'  -- sky_base_universe
		,'Non-Vespa'   -- Vespa Universe
		,fbp.ROI_Region -- ROI_Region
		,fbp.tenure
		,cel.Variety + cel.Knowledge + cel.Kids + cel.Style_Culture + cel.Music + cel.News_Events as num_mix
		,CASE
			WHEN Num_Mix IS NULL OR Num_Mix=0                           THEN 'Entertainment Pack'
			WHEN (cel.variety=1 OR cel.style_culture=1)  AND Num_Mix=1  THEN 'Entertainment Pack'
			WHEN (cel.variety=1 AND cel.style_culture=1) AND Num_Mix=2  THEN 'Entertainment Pack'
			WHEN Num_Mix > 0                                            THEN 'Entertainment Extra'
			END AS mix_pack -- Basic package has recently been split into the Entertainment AND Entertainment Extra packs
		,CASE
			WHEN cel.prem_sports = 2 AND cel.prem_movies = 2 THEN 'Movies & Sports' --'Top Tier'
			WHEN cel.prem_sports = 2 AND cel.prem_movies = 0 THEN 'Sports' --'Dual Sports'
			WHEN cel.prem_sports = 0 AND cel.prem_movies = 2 THEN 'Movies' --'Dual Movies'
			WHEN cel.prem_sports = 1 AND cel.prem_movies = 0 THEN 'Sports' --'Single Sports'
			WHEN cel.prem_sports = 0 AND cel.prem_movies = 1 THEN 'Movies' --'Single Movies'
			WHEN cel.prem_sports > 0 OR  cel.prem_movies > 0 THEN 'Movies & Sports' --'Other Premiums'
			WHEN cel.prem_movies = 0 AND cel.prem_sports = 0 AND mix_pack = 'Entertainment Pack'  THEN 'Basic' --'Basic - Ent'
			WHEN cel.prem_movies = 0 AND cel.prem_sports = 0 AND mix_pack = 'Entertainment Extra' THEN 'Basic' --'Basic - Ent Extra'
			ELSE 'Basic' END --                                                  'Basic - Ent' END -- package
		,'D) FDB & No_secondary_box' -- boxtype
		,'Single' --no_of_stbs
		,'No' --hd_subscription
		,'No' --pvr
	FROM #weekly_sample AS fbp
	left JOIN /*sk_prod.*/cust_entitlement_lookup AS cel
	 ON fbp.current_short_description = cel.short_description
	WHERE fbp.cb_key_household IS NOT NULL
	AND fbp.cb_key_individual IS NOT NULL

	MESSAGE cast(now() as timestamp)||' | @ SC3_ROI 1. SC3_ROI_scaling_weekly_sample. Inserted: '||@@rowcount  TO CLIENT

	-- Populate sky_base_universe according to SQL code used to find adsmartable bozes in weekly reports
	SELECT  sub1.account_number
		,CASE
			WHEN flag = 1 AND cust_viewing_data_capture_allowed = 'Y' then 'Adsmartable with consent'
			WHEN flag = 1 AND cust_viewing_data_capture_allowed <> 'Y' then 'Adsmartable but no consent'
			else 'Not adsmartable'
			end as sky_base_universe
	into  #cv_sbu
	FROM (SELECT  sav.account_number, adsmart.flag, cust_viewing_data_capture_allowed
		  FROM    (SELECT      distinct account_number, cust_viewing_data_capture_allowed FROM   #weekly_sample)		as sav
				left JOIN (SELECT  active_boxes.account_number
									,max(CASE   WHEN x_pvr_type ='PVR6'                                 THEN 1
												WHEN x_pvr_type ='PVR5'                                 THEN 1
												WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Samsung'  THEN 1
												WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Pace'     THEN 1
												ELSE 0 END) AS flag
							FROM    (SELECT  *
									FROM    (SELECT  a.account_number
													,x_pvr_type
													,x_personal_storage_capacity
													,currency_code
													,x_manufacturer
													,rank () over (partition by service_instance_id order by ph_non_subs_link_sk desc) active_flag
											FROM 	/*sk_prod.*/CUST_SET_TOP_BOX AS a 
											JOIN 	#weekly_sample AS b ON a.account_number = b.account_number
											)       as base
									WHERE   active_flag = 1
									)       as active_boxes
							WHERE   currency_code = 'EUR'
							group   by      account_number
							)       as adsmart on      sav.account_number = adsmart.account_number
		) as sub1
	commit
	
	MESSAGE cast(now() as timestamp)||' | @ SC3_ROI 1. #cv_sbu. Created: '||@@rowcount  TO CLIENT
	
	COMMIT
	DROP TABLE #weekly_sample
	COMMIT
	
	UPDATE SC3_ROI_scaling_weekly_sample
	SET stws.sky_base_universe = cv.sky_base_universe
	FROM SC3_ROI_scaling_weekly_sample AS stws
	JOIN #cv_sbu AS cv ON stws.account_number = cv.account_number

     -- Update vespa universe
    UPDATE SC3_ROI_scaling_weekly_sample
	SET vespa_universe = CASE 
			WHEN sky_base_universe = 'Not adsmartable' THEN 'Vespa not Adsmartable'
			WHEN sky_base_universe = 'Adsmartable with consent' THEN 'Vespa adsmartable'
			WHEN sky_base_universe = 'Adsmartable but no consent' THEN 'Vespa but no consent'
			ELSE 'Non-Vespa' END

	COMMIT

	MESSAGE cast(now() as timestamp)||' | @ SC3_ROI 1. SC3_ROI_scaling_weekly_sample universe. Updated: '||@@rowcount  TO CLIENT
	
	DELETE FROM SC3_ROI_scaling_weekly_sample
	WHERE sky_base_universe IS NULL
		OR vespa_universe IS NULL

	 -- Boxtype & Universe
     -- Boxtype is defined as the top two boxtypes held by a household ranked in the following order
     -- 1) HD, 2) HDx, 3) Skyplus, 4) FDB
     -- Capture all active boxes for this week
	SELECT csh.service_instance_id
		,csh.account_number
		,subscription_sub_type
		,RANK() OVER (PARTITION BY csh.service_instance_id ORDER BY csh.account_number,csh.cb_row_id DESC) AS rank
	INTO #accounts -- drop table #accounts
	FROM /*sk_prod.*/cust_subs_hist AS csh
	JOIN SC3_ROI_scaling_weekly_sample AS ss ON csh.account_number = ss.account_number
	WHERE csh.subscription_sub_type IN ('DTV Primary Viewing','DTV Extra Subscription') --the DTV sub Type
		AND csh.status_code IN ('AC','AB','PC') --Active Status Codes
		AND csh.effective_from_dt <= @profiling_thursday
		AND csh.effective_to_dt > @profiling_thursday
		AND csh.effective_from_dt <> effective_to_dt

	MESSAGE cast(now() as timestamp)||' | @ SC3_ROI 1. #accounts created: '||@@rowcount  TO CLIENT
	
	-- De-dupe active boxes
	DELETE FROM #accounts WHERE rank>1
	COMMIT

	-- Create indices on list of boxes
	CREATE hg INDEX idx1 ON #accounts(service_instance_id)
	CREATE hg INDEX idx2 ON #accounts(account_number)
	commit

	-- Identify HD & 1TB/2TB HD boxes
	SELECT  stb.service_instance_id
		,SUM(CASE WHEN current_product_description LIKE '%HD%' THEN 1 ELSE 0 END) AS HD
		,SUM(CASE WHEN x_description IN ('Amstrad HD PVR6 (1TB)', 'Amstrad HD PVR6 (2TB)') THEN 1 ELSE 0 END) AS HD1TB
	INTO #hda -- drop table #hda
	FROM /*sk_prod.*/CUST_SET_TOP_BOX AS stb INNER JOIN #accounts AS acc ON stb.service_instance_id = acc.service_instance_id
	WHERE box_installed_dt <= @profiling_thursday
		 AND box_replaced_dt   > @profiling_thursday
		 AND current_product_description like '%HD%'
	GROUP BY stb.service_instance_id

	MESSAGE cast(now() as timestamp)||' | @ SC3_ROI 1. #hda created: '||@@rowcount  TO CLIENT
	
	-- Create index on HD table
	COMMIT
	CREATE UNIQUE hg INDEX idx1 ON #hda(service_instance_id)
	COMMIT 

	-- Identify PVR boxes
	SELECT  acc.account_number
		,MAX(CASE WHEN x_box_type LIKE '%Sky+%' THEN 'Yes' ELSE 'No' END) AS PVR
	INTO #pvra -- drop table #pvra
	FROM /*sk_prod.*/CUST_SET_TOP_BOX AS stb INNER JOIN #accounts AS acc ON stb.service_instance_id = acc.service_instance_id
	WHERE box_installed_dt <= @profiling_thursday
		AND box_replaced_dt   > @profiling_thursday
	GROUP by acc.account_number

	MESSAGE cast(now() as timestamp)||' | @ SC3_ROI 1. #pvra created: '||@@rowcount  TO CLIENT
     -- Create index on PVR table
	COMMIT
	CREATE hg INDEX pvidx1 ON #pvra(account_number)
	commit

     -- PVR
	UPDATE SC3_ROI_scaling_weekly_sample
	SET stws.pvr = cv.pvr
	FROM SC3_ROI_scaling_weekly_sample AS stws
	JOIN #pvra AS cv ON stws.account_number = cv.account_number

	-- Set default value WHEN account cannot be found
	UPDATE SC3_ROI_scaling_weekly_sample
	SET pvr = CASE WHEN sky_base_universe like 'Adsmartable%' then 'Yes' else 'No' end
	WHERE pvr is null
	COMMIT

    -- Update PVR WHEN PVR says 'No' AND universe is an adsmartable one.
      update SC3_ROI_scaling_weekly_sample
         set pvr = 'Yes'
       WHERE pvr = 'No' AND sky_base_universe like 'Adsmartable%'
      commit

	SELECT  --acc.service_instance_id,
		acc.account_number
		,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Extra Subscription' THEN 1 ELSE 0  END) AS MR
		,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Sky+'               THEN 1 ELSE 0  END) AS SP
		,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV HD'                 THEN 1 ELSE 0  END) AS HD
		,MAX(CASE  WHEN #hda.HD = 1                                         THEN 1 ELSE 0  END) AS HDstb
		,MAX(CASE  WHEN #hda.HD1TB = 1                                      THEN 1 ELSE 0  END) AS HD1TBstb
	INTO #scaling_box_level_viewing
	FROM /*sk_prod.*/cust_subs_hist AS csh
	JOIN #accounts AS acc 								ON csh.service_instance_id = acc.service_instance_id --< Limits to your universe
	LEFT OUTER JOIN /*sk_prod.*/cust_entitlement_lookup cel ON csh.current_short_description = cel.short_description
	LEFT OUTER JOIN #hda 								ON csh.service_instance_id = #hda.service_instance_id --< Links to the HD Set Top Boxes
	WHERE csh.effective_FROM_dt <= @profiling_thursday
	AND csh.effective_to_dt    > @profiling_thursday
	AND csh.status_code IN  ('AC','AB','PC')
	AND csh.SUBSCRIPTION_SUB_TYPE IN ('DTV Primary Viewing','DTV Sky+', 'DTV Extra Subscription','DTV HD' )
	AND csh.effective_FROM_dt <> csh.effective_to_dt
	GROUP BY acc.service_instance_id ,acc.account_number
	
	MESSAGE cast(now() as timestamp)||' | @ SC3_ROI 1. #scaling_box_level_viewing created: '||@@rowcount  TO CLIENT

     commit
     drop table #accounts
     drop table #hda
     commit

     -- Identify boxtype of each box AND whether it is a primary or a secondary box
	SELECT  tgt.account_number
		,SUM(CASE WHEN MR=1 THEN 1 ELSE 0 END) AS mr_boxes
		,MAX(CASE WHEN MR=0 AND ((tgt.HD =1 AND HD1TBstb = 1) OR (tgt.HD =1 AND HDstb = 1))         THEN 4 -- HD ( inclusive of HD1TB)
				  WHEN MR=0 AND ((tgt.SP =1 AND tgt.HD1TBstb = 1) OR (tgt.SP =1 AND tgt.HDstb = 1)) THEN 3 -- HDx ( inclusive of HD1TB)
				  WHEN MR=0 AND tgt.SP =1                                                           THEN 2 -- Skyplus
				  ELSE                                                                              1 END) AS pb -- FDB
		,MAX(CASE WHEN MR=1 AND ((tgt.HD =1 AND HD1TBstb = 1) OR (tgt.HD =1 AND HDstb = 1))         THEN 4 -- HD ( inclusive of HD1TB)
				  WHEN MR=1 AND ((tgt.SP =1 AND tgt.HD1TBstb = 1) OR (tgt.SP =1 AND tgt.HDstb = 1)) THEN 3 -- HDx ( inclusive of HD1TB)
				  WHEN MR=1 AND tgt.SP =1                                                           THEN 2 -- Skyplus
				  ELSE                                                                              1 END) AS sb -- FDB
		 ,convert(varchar(20), null) as universe
		 ,convert(varchar(30), null) as boxtype
	INTO #boxtype_ac -- drop table #boxtype_ac
	FROM #scaling_box_level_viewing AS tgt
	GROUP BY tgt.account_number

	MESSAGE cast(now() as timestamp)||' | @ SC3_ROI 1. #boxtype_ac created: '||@@rowcount  TO CLIENT
	
     -- Create indices on box-level boxtype temp table
	COMMIT
	CREATE unique INDEX idx_ac ON #boxtype_ac(account_number)
	drop table #scaling_box_level_viewing
	commit

  
     -- Build the combined flags
     update #boxtype_ac
     set universe = CASE WHEN mr_boxes = 0 THEN 'A) Single box HH'
                              ELSE 'B) Multiple box HH' END
         ,boxtype  =
             CASE WHEN       mr_boxes = 0 AND  pb =  3 AND sb =  1   THEN  'A) HDx & No_secondary_box'
                  WHEN       mr_boxes = 0 AND  pb =  4 AND sb =  1   THEN  'B) HD & No_secondary_box'
                  WHEN       mr_boxes = 0 AND  pb =  2 AND sb =  1   THEN  'C) Skyplus & No_secondary_box'
                  WHEN       mr_boxes = 0 AND  pb =  1 AND sb =  1   THEN  'D) FDB & No_secondary_box'
                  WHEN       mr_boxes > 0 AND  pb =  4 AND sb =  4   THEN  'E) HD & HD' -- If a hh has HD  then all boxes have HD (therefore no HD AND HDx)
                  WHEN       mr_boxes > 0 AND (pb =  4 AND sb =  3) OR (pb =  3 AND sb =  4)  THEN  'E) HD & HD'
                  WHEN       mr_boxes > 0 AND (pb =  4 AND sb =  2) OR (pb =  2 AND sb =  4)  THEN  'F) HD & Skyplus'
                  WHEN       mr_boxes > 0 AND (pb =  4 AND sb =  1) OR (pb =  1 AND sb =  4)  THEN  'G) HD & FDB'
                  WHEN       mr_boxes > 0 AND  pb =  3 AND sb =  3                            THEN  'H) HDx & HDx'
                  WHEN       mr_boxes > 0 AND (pb =  3 AND sb =  2) OR (pb =  2 AND sb =  3)  THEN  'I) HDx & Skyplus'
                  WHEN       mr_boxes > 0 AND (pb =  3 AND sb =  1) OR (pb =  1 AND sb =  3)  THEN  'J) HDx & FDB'
                  WHEN       mr_boxes > 0 AND  pb =  2 AND sb =  2                            THEN  'K) Skyplus & Skyplus'
                  WHEN       mr_boxes > 0 AND (pb =  2 AND sb =  1) OR (pb =  1 AND sb =  2)  THEN  'L) Skyplus & FDB'
                             ELSE   'M) FDB & FDB' END

     commit

	
     /* Now building this differently; Sybase 15 didn't like it at all, even had trouble killing
     ** the thread after the query is cancelled. This weirdness has been replicated in other schemas
     ** AND on the QA server AND has been raised to Sybase. Apparently it's a known bug AND there's
     ** a patch coming in 15.4, but for the meantime this method remains commented out even though
     ** the workaround is amazingly ugly...pvr
     */
/*
     CREATE TABLE #SC3_weird_sybase_update_workaround (
          account_number                     VARCHAR(20)     primary key
         ,cb_key_household                   BIGINT          not null
         ,cb_key_individual                  BIGINT          not null
         ,consumerview_cb_row_id             BIGINT
         ,universe                           VARCHAR(30)                         -- Single or multiple box household. Reused for no_of_stbs
         ,sky_base_universe                  VARCHAR(30)                         -- Not adsmartable, Adsmartable with consent, Adsmartable but no consent household
         ,vespa_universe                     VARCHAR(30)                         -- Non-Vespa, Not Adsmartable, Vespa with consent, vespa but no consent household
         ,weighting_universe                 VARCHAR(30)                         -- Used WHEN finding appropriate scaling segment - see note
         ,ROI_Region                     VARCHAR(30)                         -- Scaling variable 1 : Region
         ,hhcomposition                      VARCHAR(2)      default 'D)'        -- Scaling variable 2: Household composition
         ,tenure                             VARCHAR(15)     DEFAULT 'D) Unknown'-- Scaling variable 3: Tenure
         ,num_mix                            INT
         ,mix_pack                           VARCHAR(20)
         ,package                            VARCHAR(20)                         -- Scaling variable 4: Package
         ,boxtype                            VARCHAR(35)                         -- Old Scaling variable 5: Household boxtype split into no_of_stbs, hd_subscription AND pvr.
         ,no_of_stbs                         VARCHAR(15)                         -- Scaling variable 5: No of set top boxes
         ,hd_subscription                    VARCHAR(5)                          -- Scaling variable 6: HD subscription
         ,pvr                                VARCHAR(5)                          -- Scaling variable 7: Is the box pvr capable?
         ,population_scaling_segment_id      INT             DEFAULT NULL        -- segment scaling id for identifying segments
         ,vespa_scaling_segment_id           INT             DEFAULT NULL        -- segment scaling id for identifying segments
         ,mr_boxes                           INT
     --    ,complete_viewing                   TINYINT         DEFAULT 0           -- Flag for all accounts with complete viewing data
     )

     CREATE INDEX for_segment_identification_temp1 ON #SC3_weird_sybase_update_workaround (ROI_Region)
     CREATE INDEX for_segment_identification_temp2 ON #SC3_weird_sybase_update_workaround (hhcomposition)
     CREATE INDEX for_segment_identification_temp3 ON #SC3_weird_sybase_update_workaround (tenure)
     CREATE INDEX for_segment_identification_temp4 ON #SC3_weird_sybase_update_workaround (package)
     CREATE INDEX for_segment_identification_temp5 ON #SC3_weird_sybase_update_workaround (boxtype)
     CREATE INDEX consumerview_JOINing ON #SC3_weird_sybase_update_workaround (consumerview_cb_row_id)
     CREATE INDEX for_temping1 ON #SC3_weird_sybase_update_workaround (population_scaling_segment_id)
     CREATE INDEX for_temping2 ON #SC3_weird_sybase_update_workaround (vespa_scaling_segment_id)
     COMMIT

     insert into #SC3_weird_sybase_update_workaround (
          account_number
         ,cb_key_household
         ,cb_key_individual
         ,consumerview_cb_row_id
         ,universe
         ,sky_base_universe
         ,vespa_universe
         ,ROI_Region
         ,hhcomposition
         ,tenure
         ,num_mix
         ,mix_pack
         ,package
         ,boxtype
         ,mr_boxes
     )
     SELECT
          sws.account_number
         ,sws.cb_key_household
         ,sws.cb_key_individual
         ,sws.consumerview_cb_row_id
         ,ac.universe
         ,sky_base_universe
         ,vespa_universe
         ,sws.ROI_Region
         ,sws.hhcomposition
         ,sws.tenure
         ,sws.num_mix
         ,sws.mix_pack
         ,sws.package
         ,ac.boxtype
         ,ac.mr_boxes
     FROM SC3_ROI_scaling_weekly_sample as sws
     inner JOIN #boxtype_ac AS ac
     on ac.account_number = sws.account_number
     WHERE sws.cb_key_household IS NOT NULL
       AND sws.cb_key_individual IS NOT NULL
*/
	UPDATE    SC3_ROI_scaling_weekly_sample
	SET   a.universe = ac.universe 
		, a.boxtype = ac.boxtype
		, a.mr_boxes = ac.mr_boxes
	FROM SC3_ROI_scaling_weekly_sample AS a 
	JOIN #boxtype_ac AS ac ON ac.account_number = a.account_number
    WHERE  a.cb_key_household  IS NOT NULL
       AND a.cb_key_individual IS NOT NULL
	
	MESSAGE cast(now() as timestamp)||' | @ SC3_ROI 1. SC3_ROI_scaling_weekly_sample updated: '||@@rowcount  TO CLIENT
    -- Update SC3 scaling variables according to Scaling 3.0 variables
	UPDATE SC3_ROI_scaling_weekly_sample sws
	SET sws.pvr = ac.pvr
	FROM #pvra AS ac
	WHERE ac.account_number = sws.account_number

     -- This data is eventually going to go back into the SC3_ROI_scaling_weekly_sample,
     -- but there's some weird Sybase bug at the moment that means that updates don't
     -- work. AND then the sessions can't be cancelled, for some bizarre reason.

	COMMIT
	DROP TABLE  #boxtype_ac
	COMMIT

    /**************** L03: ASSIGN SCALING SEGMENT ID ****************/

     -- The SC3_Segments_lookup table can be used to append a segment_id to
     -- the SC3_ROI_scaling_weekly_sample table by matching on universe AND each of the
     -- seven scaling variables (hhcomposition, ROI_Region, package, boxtype, tenure, no_of_stbs, hd_subscription AND pvr)
     -- Commented out code is for WHEN we were looking to create a proxy group using adsmartable accounts to mimic those adsmartable
     -- accounts that had not given viewing consent. Code is kept here jsut in CASE we need to revert back to this method.

     --Set default sky_base_universe, if, for some reason, it is null
    UPDATE SC3_ROI_scaling_weekly_sample
    SET   sky_base_universe = 'Not adsmartable'
    WHERE sky_base_universe IS NULL

    UPDATE SC3_ROI_scaling_weekly_sample
    SET   vespa_universe = 'Non-Vespa'
    WHERE sky_base_universe IS NULL 

    UPDATE SC3_ROI_scaling_weekly_sample
    SET   weighting_universe = 'Not adsmartable'
    WHERE weighting_universe IS NULL 

      -- Set default value WHEN account cannot be found
	UPDATE SC3_ROI_scaling_weekly_sample
	SET pvr = CASE WHEN sky_base_universe LIKE 'Adsmartable%' THEN 'Yes'
					ELSE 'No' END 
	WHERE pvr  IS NULL 
	COMMIT 

       -- Update PVR WHEN PVR says 'No' AND universe is an adsmartable one.
	UPDATE SC3_ROI_scaling_weekly_sample
	SET pvr = 'Yes'
	WHERE pvr = 'No' AND sky_base_universe LIKE 'Adsmartable%'
	COMMIT 

	UPDATE SC3_ROI_scaling_weekly_sample
	SET no_of_stbs = CASE	WHEN Universe like '%Single%' then 'Single'
							WHEN Universe like '%Multiple%' then 'Multiple'
							else 'Single' END

    UPDATE SC3_ROI_scaling_weekly_sample
	SET hd_subscription = CASE WHEN boxtype like 'B)%' or boxtype like 'E)%' or boxtype like 'F)%' or boxtype like 'G)%' then 'Yes'
										else 'No' end
    COMMIT 

    UPDATE SC3_ROI_scaling_weekly_sample
    SET a.population_scaling_segment_ID = ssl.scaling_segment_ID
		, a.vespa_scaling_segment_id = ssl.scaling_segment_ID
    FROM SC3_ROI_scaling_weekly_sample AS a
	JOIN /*vespa_analysts.*/SC3_ROI_Segments_lookup_no_pvr AS ssl ON trim(lower(a.sky_base_universe)) = trim(lower(ssl.sky_base_universe))
														 AND left(a.ROI_Region, 20) = left(ssl.ROI_Region, 20)
														 AND a.Package        = ssl.Package
														 AND left(a.tenure, 2)         = left(ssl.tenure, 2)
														 AND a.no_of_stbs     = ssl.no_of_stbs
														 AND a.hd_subscription = ssl.hd_subscription
--														 AND a.pvr            = ssl.pvr
	COMMIT
    
	
	MESSAGE cast(now() as timestamp)||' | @ SC3_ROI 1. SC3_ROI_scaling_weekly_sample 2 updated: '||@@rowcount  TO CLIENT
	
	-- Just checked one manual build, none of these are null, it should all work fine.
	-- Okay, no throw all of that back into the weekly sample table, because that's WHERE
    -- the build expects it to be, were it not for that weird bug in Sybase:

     /**************** L04: PUBLISHING INTO INTERFACE STRUCTURES ****************/

     -- First off we need the accounts AND their scaling segmentation IDs: generating
     -- some 10M such records a week, but we'd be able to cull them once we've finished
     -- the associated scaling builds. Only need to maintain it while we still have
     -- historic builds to do.

	INSERT INTO SC3_ROI_Sky_base_segment_snapshots
	SELECT
		account_number
		,@profiling_thursday
		,cb_key_household   -- This guy still needs to be added to SC3_ROI_scaling_weekly_sample
		,population_scaling_segment_id
		,vespa_scaling_segment_id
		,mr_boxes+1         -- Number of multiroom boxes plus 1 for the primary
	FROM SC3_ROI_scaling_weekly_sample
	WHERE population_scaling_segment_id is not null AND vespa_scaling_segment_id is not null -- still perhaps with the weird account FROM Eire?
	
	commit

	MESSAGE cast(now() as timestamp)||' | @ SC3_ROI 1. SC3_ROI_Sky_base_segment_snapshots insert: '||@@rowcount  TO CLIENT
	
end; -- of procedure "SC3_ROI_do_weekly_segmentation"
commit;
