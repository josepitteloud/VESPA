/*1	hhcomposition
2	package
3	isba_tv_region
4	tenure
5	no_of_stbs
6	hd_subscription
7	pvr
*/



CREATE OR REPLACE PROCEDURE SC3_v1_1__do_weekly_segmentation_SkyQ
	 @profiling_thursday DATE = NULL -- Day on which to do sky base profiling
	,@batch_date DATETIME = now () -- Day on which build was kicked off
AS
BEGIN

	MESSAGE cast(now() as timestamp)||' | @ SC3_ROI 1. Initialising Environment' TO CLIENT

		 
	DELETE FROM SC3_scaling_weekly_sample
	COMMIT

	if @profiling_thursday is null
	begin
	 execute vespa_analysts.Regulars_Get_report_end_date @profiling_thursday output  -- proc returns a Saturday
	 set @profiling_thursday = @profiling_thursday - 2                               -- but we want a Thursday
	end
	commit

	DELETE FROM SC3_Sky_base_segment_snapshots 
	commit

	MESSAGE cast(now() as timestamp)||' | @ SC3_ROI 1. Initialising Environment DONE' TO CLIENT

	/**************** L01: ESTABLISH POPULATION ****************/
	-- We need the segmentation over the whole Sky base so we can scale up
	-- Captures all active accounts in cust_subs_hist

	SELECT   account_number
		 ,cb_key_household
		 ,cb_key_individual
		 ,current_short_description
		 ,rank() over (PARTITION BY account_number ORDER BY effective_from_dt desc, cb_row_id) AS rank
		 ,convert(bit, 0)  AS uk_standard_account
		 ,convert(VARCHAR(30), NULL) AS isba_tv_region
		 ,convert(VARCHAR(1), NULL) AS cust_viewing_data_capture_allowed
		 ,convert(VARCHAR(15), NULL) AS tenure
	INTO #weekly_sample
	FROM cust_subs_hist
	WHERE subscription_sub_type IN ('DTV Primary Viewing')
		AND status_code IN ('AC','AB','PC')
		AND effective_from_dt    <= @profiling_thursday
		AND effective_to_dt      > @profiling_thursday
		AND effective_from_dt    <> effective_to_dt
		AND EFFECTIVE_FROM_DT    IS NOT NULL
		AND cb_key_household     > 0
		AND cb_key_household     IS NOT NULL
		AND cb_key_individual    IS NOT NULL
		AND account_number       IS NOT NULL
		AND service_instance_id  IS NOT NULL

	MESSAGE cast(now() as timestamp)||' | @ SC3_ROI 1. #weekly_sample_roi Created: '||@@rowcount  TO CLIENT

	-- De-dupes accounts
	COMMIT
	DELETE FROM #weekly_sample WHERE rank > 1
	COMMIT

	-- Create indices
	CREATE UNIQUE INDEX fake_pk ON #weekly_sample (account_number)
	CREATE INDEX for_package_JOIN ON #weekly_sample (current_short_description)
	COMMIT

	-- Take out ROIs (Republic of Ireland) and non-standard accounts as these are not currently in the scope of Vespa
	UPDATE #weekly_sample
	SET uk_standard_account = CASE 
			WHEN b.acct_type = 'Standard' AND b.account_number <> '?'  AND b.pty_country_code = 'GBR' THEN 1
			ELSE 0 END
		,isba_tv_region = CASE 	WHEN b.isba_tv_region = 'Border' THEN 'NI, Scotland & Border'
								WHEN b.isba_tv_region = 'Central Scotland' THEN 'NI, Scotland & Border'
								WHEN b.isba_tv_region = 'East Of England' THEN 'Wales & Midlands'
								WHEN b.isba_tv_region = 'HTV Wales' THEN 'Wales & Midlands'
								WHEN b.isba_tv_region = 'HTV West' THEN 'South England'
								WHEN b.isba_tv_region = 'London' THEN 'London'
								WHEN b.isba_tv_region = 'Meridian (exc. Channel Islands)' THEN 'South England'
								WHEN b.isba_tv_region = 'Midlands' THEN 'Wales & Midlands'
								WHEN b.isba_tv_region = 'North East' THEN 'North England'
								WHEN b.isba_tv_region = 'North Scotland' THEN 'NI, Scotland & Border'
								WHEN b.isba_tv_region = 'North West' THEN 'North England'
								WHEN b.isba_tv_region = 'Not Defined' THEN 'Not Defined'
								WHEN b.isba_tv_region = 'South West' THEN 'South England'
								WHEN b.isba_tv_region = 'Ulster' THEN 'NI, Scotland & Border'
								WHEN b.isba_tv_region = 'Yorkshire' THEN 'North England'
								ELSE 'Not Defined' END
		,cb_key_individual = b.cb_key_individual
		,a.cust_viewing_data_capture_allowed = b.cust_viewing_data_capture_allowed
		,tenure = CASE   WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) <=  730 THEN 'A) 0-2 Years'
                         WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) <= 3650 THEN 'B) 3-10 Years'
                         WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) > 3650 THEN  'C) 10 Years+'
                         ELSE 'D) Unknown'
                  END
	FROM #weekly_sample AS a
	INNER JOIN cust_single_account_view AS b ON a.account_number = b.account_number

	COMMIT
	DELETE FROM #weekly_sample WHERE uk_standard_account = 0
	DELETE FROM #weekly_sample WHERE isba_tv_region 	 = 'Not Defined'
	DELETE FROM #weekly_sample WHERE tenure 			 = 'D) Unknown'
	COMMIT

	/**************** L02: ASSIGN VARIABLES ****************/
	SELECT cv.cb_key_household
		,cv.cb_key_family
		,cv.cb_key_individual
		,min(cv.cb_row_id) AS cb_row_id
		,max(cv.h_household_composition) AS h_household_composition
		,max(pp.p_head_of_household) AS p_head_of_household
	INTO #cv_pp
	FROM EXPERIAN_CONSUMERVIEW AS cv
	JOIN PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD AS pp ON cv.exp_cb_key_db_individual = pp.exp_cb_key_db_individual
	JOIN  #weekly_sample AS a ON a.cb_key_household = cv.cb_key_household
	WHERE cv.cb_key_individual IS NOT NULL
	GROUP BY cv.cb_key_household
		,cv.cb_key_family
		,cv.cb_key_individual

	COMMIT
	CREATE LF INDEX idx1 ON #cv_pp (p_head_of_household)
	CREATE HG INDEX idx2 ON #cv_pp (cb_key_family)
	CREATE HG INDEX idx3 ON #cv_pp (cb_key_individual)
	COMMIT

	SELECT cb_key_household
		,cb_row_id
		,rank() OVER (PARTITION BY cb_key_family ORDER BY p_head_of_household DESC ,cb_row_id DESC) AS rank_fam
		,rank() OVER (PARTITION BY cb_key_household ORDER BY p_head_of_household DESC ,cb_row_id DESC) AS rank_hhd
		,CASE 
			WHEN h_household_composition = '00' THEN 'A) Families'
			WHEN h_household_composition = '01' THEN 'A) Families'
			WHEN h_household_composition = '02' THEN 'A) Families'
			WHEN h_household_composition = '03' THEN 'A) Families'
			WHEN h_household_composition = '04' THEN 'B) Singles'
			WHEN h_household_composition = '05' THEN 'B) Singles' 
			WHEN h_household_composition = '06' THEN 'C) Homesharers'
			WHEN h_household_composition = '07' THEN 'C) Homesharers'
			WHEN h_household_composition = '08' THEN 'C) Homesharers'
			WHEN h_household_composition = '09' THEN 'A) Families'
			WHEN h_household_composition = '10' THEN 'A) Families'
			WHEN h_household_composition = '11' THEN 'C) Homesharers'
			WHEN h_household_composition = 'U'  THEN 'D) Unclassified HHComp'
			ELSE 'D) Unclassified HHComp' 	 END AS h_household_composition
	INTO #cv_keys
	FROM #cv_pp
	WHERE cb_key_household IS NOT NULL
		AND cb_key_household <> 0

	DELETE FROM #cv_keys WHERE rank_fam != 1 AND rank_hhd != 1
    CREATE INDEX index_ac on #cv_keys (cb_key_household)
    COMMIT

     -- Populate Package & ISBA TV Region
	INSERT INTO SC3_scaling_weekly_sample (
			 account_number
			 ,cb_key_household
			 ,cb_key_individual
			 ,universe    --scaling variables removed. Use later to set no_of_stbs
			 ,sky_base_universe  -- Need to include this as they form part of a big index
			 ,vespa_universe  -- Need to include this as they form part of a big index
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
	SELECT
		fbp.account_number
		,fbp.cb_key_household
		,fbp.cb_key_individual
		,'A) Single box HH' -- universe
		,'Not adsmartable'  -- sky_base_universe
		,'Non-Vespa'   -- Vespa Universe
		,fbp.isba_tv_region -- isba_tv_region
		,'D)'  -- hhcomposition
		,fbp.tenure
		,cel.Variety + cel.Knowledge + cel.Kids + cel.Style_Culture + cel.Music + cel.News_Events as num_mix
		,CASE
			WHEN Num_Mix IS NULL OR Num_Mix=0                           THEN 'Entertainment Pack'
			WHEN (cel.variety=1 OR cel.style_culture=1)  AND Num_Mix=1  THEN 'Entertainment Pack'
			WHEN (cel.variety=1 AND cel.style_culture=1) AND Num_Mix=2  THEN 'Entertainment Pack'
			WHEN Num_Mix > 0                                            THEN 'Entertainment Extra'
			END AS mix_pack -- Basic package has recently been split into the Entertainment and Entertainment Extra packs
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
		,'0' --pvr
	FROM #weekly_sample AS fbp
	left JOIN cust_entitlement_lookup AS cel
	 ON fbp.current_short_description = cel.short_description
	WHERE fbp.cb_key_household IS NOT NULL
	AND fbp.cb_key_individual IS NOT NULL


	MESSAGE cast(now() as timestamp)||' | @ SC3_ROI 1. SC3_ROI_scaling_weekly_sample. Inserted: '||@@rowcount  TO CLIENT

	-- Populate sky_base_universe according to SQL code used to find adsmartable bozes in weekly reports
	/****************************************************************************************************
	****************************************************************************************************
	****************************************************************************************************
	****************************************************************************************************
	****************************************************************************************************
	****************************************************************************************************/
	
	SELECT  sub1.account_number
		,CASE
			WHEN flag = 1 and cust_viewing_data_capture_allowed = 'Y' then 'Adsmartable with consent'
			WHEN flag = 1 and cust_viewing_data_capture_allowed <> 'Y' then 'Adsmartable but no consent'
			else 'Not adsmartable'
			end as sky_base_universe
	into  #cv_sbu
	FROM (SELECT  sav.account_number, adsmart.flag, cust_viewing_data_capture_allowed
		  FROM    (SELECT      distinct account_number, cust_viewing_data_capture_allowed FROM   #weekly_sample)		as sav
				left JOIN (SELECT  account_number
									,max(CASE   
												WHEN x_pvr_type ='PVR8'                                 THEN 1
												WHEN x_pvr_type ='PVR7'                                 THEN 1
												WHEN x_pvr_type ='PVR6'                                 THEN 1
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
											FROM 	CUST_SET_TOP_BOX AS a 
											JOIN 	#weekly_sample AS b ON a.account_number = b.account_number
											)       as base
									where   active_flag = 1
									)       as active_boxes
							where   currency_code = 'GBP'
							group   by      active_boxes.account_number
							)       as adsmart on      sav.account_number = adsmart.account_number
		) as sub1
	commit
	
	/****************************************************************************************************
	****************************************************************************************************
	****************************************************************************************************
	****************************************************************************************************
	****************************************************************************************************
	*****************************************************************************************************/
	
	
	MESSAGE cast(now() as timestamp)||' | @ SC3_ROI 1. #cv_sbu_roi. Created: '||@@rowcount  TO CLIENT

	COMMIT
	DROP TABLE #weekly_sample
	COMMIT
	
	UPDATE SC3_scaling_weekly_sample
	SET stws.sky_base_universe = cv.sky_base_universe
	FROM SC3_scaling_weekly_sample AS stws
	JOIN #cv_sbu AS cv ON stws.account_number = cv.account_number

     -- Update vespa universe
    UPDATE SC3_scaling_weekly_sample
	SET vespa_universe = CASE 
			WHEN sky_base_universe = 'Not adsmartable' THEN 'Vespa not Adsmartable'
			WHEN sky_base_universe = 'Adsmartable with consent' THEN 'Vespa adsmartable'
			WHEN sky_base_universe = 'Adsmartable but no consent' THEN 'Vespa but no consent'
			ELSE 'Non-Vespa' END

	COMMIT

	MESSAGE cast(now() as timestamp)||' | @ SC3_ROI 1. SC3_ROI_scaling_weekly_sample universe. Updated: '||@@rowcount  TO CLIENT

	DELETE FROM SC3_scaling_weekly_sample
	WHERE sky_base_universe IS NULL
		OR vespa_universe IS NULL

	-- HHcomposition
	UPDATE SC3_scaling_weekly_sample
	SET stws.hhcomposition = cv.h_household_composition
	FROM SC3_scaling_weekly_sample AS stws
	JOIN #cv_keys AS cv ON stws.cb_key_household = cv.cb_key_household
	COMMIT

	DROP TABLE #cv_keys
	COMMIT
     -- Capture all active boxes for this week
	SELECT csh.service_instance_id
		,csh.account_number
		,subscription_sub_type
		,RANK() OVER (PARTITION BY csh.service_instance_id ORDER BY csh.account_number,csh.cb_row_id DESC) AS rank
	INTO #accounts -- drop table #accounts
	FROM cust_subs_hist AS csh
	JOIN SC3_scaling_weekly_sample AS ss ON csh.account_number = ss.account_number
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
/*	SELECT  stb.service_instance_id
		,SUM(CASE WHEN current_product_description LIKE '%HD%' THEN 1 ELSE 0 END) AS HD
		,SUM(CASE WHEN x_description IN ('Amstrad HD PVR6 (1TB)', 'Amstrad HD PVR6 (2TB)') THEN 1 ELSE 0 END) AS HD1TB
	INTO #hda -- drop table #hda
	FROM CUST_SET_TOP_BOX AS stb INNER JOIN #accounts AS acc ON stb.service_instance_id = acc.service_instance_id
	WHERE box_installed_dt <= @profiling_thursday
		 AND box_replaced_dt   > @profiling_thursday
		 AND current_product_description like '%HD%'
	GROUP BY stb.service_instance_id
*/
	MESSAGE cast(now() as timestamp)||' | @ SC3_ROI 1. #hda created: '||@@rowcount  TO CLIENT

	-- Create index on HD table
	/* COMMIT
	CREATE UNIQUE hg INDEX idx1 ON #hda(service_instance_id) 
	*/
	COMMIT 
	SELECT  --acc.service_instance_id,
		acc.account_number
		,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Extra Subscription' THEN 1 ELSE 0  END) AS MR
	/*	,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV HD'                 THEN 1 ELSE 0  END) AS HD
		,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Sky+'               THEN 1 ELSE 0  END) AS SP
		,MAX(CASE  WHEN #hda.HD = 1                                         THEN 1 ELSE 0  END) AS HDstb
		,MAX(CASE  WHEN #hda.HD1TB = 1                                      THEN 1 ELSE 0  END) AS HD1TBstb
		*/
	INTO #scaling_box_level_viewing
	FROM cust_subs_hist AS csh
	JOIN #accounts AS acc 								ON csh.service_instance_id = acc.service_instance_id --< Limits to your universe
	LEFT OUTER JOIN cust_entitlement_lookup cel ON csh.current_short_description = cel.short_description
	--LEFT OUTER JOIN #hda 								ON csh.service_instance_id = #hda.service_instance_id --< Links to the HD Set Top Boxes
	WHERE csh.effective_FROM_dt <= @profiling_thursday
	AND csh.effective_to_dt    > @profiling_thursday
	AND csh.status_code IN  ('AC','AB','PC')
	AND csh.SUBSCRIPTION_SUB_TYPE IN ('DTV Primary Viewing', 'DTV Extra Subscription')
	AND csh.effective_FROM_dt <> csh.effective_to_dt
	GROUP BY acc.service_instance_id ,acc.account_number

		MESSAGE cast(now() as timestamp)||' | @ SC3_ROI 1. #scaling_box_level_viewing created: '||@@rowcount  TO CLIENT

     commit
     drop table #accounts
  --   drop table #hda
     commit

     -- Identify boxtype of each box and whether it is a primary or a secondary box
	SELECT  tgt.account_number
		,SUM(CASE WHEN MR=1 THEN 1 ELSE 0 END) AS mr_boxes
		,convert(varchar(20), null) as universe
		/*,MAX(CASE WHEN MR=0 AND ((tgt.HD =1 AND HD1TBstb = 1) OR (tgt.HD =1 AND HDstb = 1))         THEN 4 -- HD ( inclusive of HD1TB)
				  WHEN MR=0 AND ((tgt.SP =1 AND tgt.HD1TBstb = 1) OR (tgt.SP =1 AND tgt.HDstb = 1)) THEN 3 -- HDx ( inclusive of HD1TB)
				  WHEN MR=0 AND tgt.SP =1                                                           THEN 2 -- Skyplus
				  ELSE                                                                              1 END) AS pb -- FDB
		,MAX(CASE WHEN MR=1 AND ((tgt.HD =1 AND HD1TBstb = 1) OR (tgt.HD =1 AND HDstb = 1))         THEN 4 -- HD ( inclusive of HD1TB)
				  WHEN MR=1 AND ((tgt.SP =1 AND tgt.HD1TBstb = 1) OR (tgt.SP =1 AND tgt.HDstb = 1)) THEN 3 -- HDx ( inclusive of HD1TB)
				  WHEN MR=1 AND tgt.SP =1                                                           THEN 2 -- Skyplus
				  ELSE                                                                              1 END) AS sb -- FDB
		,convert(varchar(30), null) as boxtype 	*/
	INTO #boxtype_ac -- drop table #boxtype_ac
	FROM #scaling_box_level_viewing AS tgt
	GROUP BY tgt.account_number

     -- Create indices on box-level boxtype temp table
	COMMIT
	CREATE unique INDEX idx_ac ON #boxtype_ac(account_number)
	drop table #scaling_box_level_viewing
	commit

  
     -- Build the combined flags
     update #boxtype_ac
     set universe = CASE WHEN mr_boxes = 0 THEN 'A) Single box HH'
                              ELSE 'B) Multiple box HH' END
     /*    ,boxtype  =
             CASE WHEN       mr_boxes = 0 AND  pb =  3 AND sb =  1   THEN  'A) HDx & No_secondary_box'
                  WHEN       mr_boxes = 0 AND  pb =  4 AND sb =  1   THEN  'B) HD & No_secondary_box'
                  WHEN       mr_boxes = 0 AND  pb =  2 AND sb =  1   THEN  'C) Skyplus & No_secondary_box'
                  WHEN       mr_boxes = 0 AND  pb =  1 AND sb =  1   THEN  'D) FDB & No_secondary_box'
                  WHEN       mr_boxes > 0 AND  pb =  4 AND sb =  4   THEN  'E) HD & HD' -- If a hh has HD  then all boxes have HD (therefore no HD and HDx)
                  WHEN       mr_boxes > 0 AND (pb =  4 AND sb =  3) OR (pb =  3 AND sb =  4)  THEN  'E) HD & HD'
                  WHEN       mr_boxes > 0 AND (pb =  4 AND sb =  2) OR (pb =  2 AND sb =  4)  THEN  'F) HD & Skyplus'
                  WHEN       mr_boxes > 0 AND (pb =  4 AND sb =  1) OR (pb =  1 AND sb =  4)  THEN  'G) HD & FDB'
                  WHEN       mr_boxes > 0 AND  pb =  3 AND sb =  3                            THEN  'H) HDx & HDx'
                  WHEN       mr_boxes > 0 AND (pb =  3 AND sb =  2) OR (pb =  2 AND sb =  3)  THEN  'I) HDx & Skyplus'
                  WHEN       mr_boxes > 0 AND (pb =  3 AND sb =  1) OR (pb =  1 AND sb =  3)  THEN  'J) HDx & FDB'
                  WHEN       mr_boxes > 0 AND  pb =  2 AND sb =  2                            THEN  'K) Skyplus & Skyplus'
                  WHEN       mr_boxes > 0 AND (pb =  2 AND sb =  1) OR (pb =  1 AND sb =  2)  THEN  'L) Skyplus & FDB'
                             ELSE   'M) FDB & FDB' END
*/


     commit

  
	UPDATE    SC3_scaling_weekly_sample
	SET   a.universe = ac.universe 
	--	, a.boxtype = ac.boxtype
		, a.mr_boxes = ac.mr_boxes
	FROM SC3_scaling_weekly_sample AS a 
	JOIN #boxtype_ac AS ac ON ac.account_number = a.account_number
    WHERE  a.cb_key_household  IS NOT NULL
       AND a.cb_key_individual IS NOT NULL

     -- This data is eventually going to go back into the SC3_scaling_weekly_sample,
     -- but there's some weird Sybase bug at the moment that means that updates don't
     -- work. And then the sessions can't be cancelled, for some bizarre reason.

	COMMIT
	DROP TABLE  #boxtype_ac
	COMMIT

    /**************** L03: ASSIGN SCALING SEGMENT ID ****************/

     -- The SC3_Segments_lookup table can be used to append a segment_id to
     -- the SC3_scaling_weekly_sample table by matching on universe and each of the
     -- seven scaling variables (hhcomposition, isba_tv_region, package, boxtype, tenure, no_of_stbs, hd_subscription and pvr)
     -- Commented out code is for WHEN we were looking to create a proxy group using adsmartable accounts to mimic those adsmartable
     -- accounts that had not given viewing consent. Code is kept here jsut in CASE we need to revert back to this method.

     --Set default sky_base_universe, if, for some reason, it is null
    UPDATE SC3_scaling_weekly_sample
    SET   sky_base_universe = 'Not adsmartable'
    WHERE sky_base_universe IS NULL

    UPDATE SC3_scaling_weekly_sample
    SET   vespa_universe = 'Non-Vespa'
    WHERE sky_base_universe IS NULL 

    UPDATE SC3_scaling_weekly_sample
    SET   weighting_universe = 'Not adsmartable'
    WHERE weighting_universe IS NULL 


	UPDATE SC3_scaling_weekly_sample
	SET no_of_stbs = CASE	WHEN Universe like '%Single%' then 'Single'
							WHEN Universe like '%Multiple%' then 'Multiple'
							else 'Single' END

	-- Defining HD status from SAV								
    UPDATE SC3_scaling_weekly_sample
	SET hd_subscription = CASE WHEN sav.prod_count_of_active_hd_subs  > 0  AND PROD_LATEST_HD_STATUS_CODE <> 'PC' then 'Yes'
										else 'No' end
	FROM  SC3_scaling_weekly_sample AS a 
	JOIN CUST_SINGLE_ACCOUNT_VIEW 	AS sav ON a.account_number = sav.account_number 
	WHERE sav.account_number <> '99999999999999'
		AND sav.account_number not like '%.%'
		AND sav.cust_active_dtv = 1
		AND sav.cust_primary_service_instance_id is not null
		AND sav.cb_key_household IS NOT NULL
		AND sav.account_number IS NOT NULL
    
	
	COMMIT 

	IF EXISTS(SELECT tname FROM syscatalog WHERE creator= user_name() AND UPPER(tname)=UPPER('SMG_Sky_q_box') 			AND UPPER(tabletype)='TABLE')
        DROP TABLE SMG_Sky_q_box
	------------- Adding Sky Q FLAG
	
		SELECT 		
			  account_number
			, MAX(CASE 	WHEN x_description = 'Sky Q' 		THEN 1
						WHEN x_description = 'Sky Q Silver' THEN 2
				  ELSE 0 END ) AS sky_q_box
		INTO  #Sky_q_box
		FROM CUST_set_top_box
		WHERE x_description IN ('Sky Q Silver', 'Sky Q')
			AND active_box_flag = 'Y'
			AND box_replaced_dt = '9999-09-09'
		GROUP BY account_number 
		
		COMMIT 
	
	UPDATE SC3_scaling_weekly_sample
	SET pvr = '1' 
		, no_of_stbs 	= CASE WHEN sky_q_box = 1 THEN 'Single' ELSE  'Multiple' END 
		, universe 		= CASE WHEN sky_q_box = 1 THEN 'Single' ELSE  'Multiple' END 
		, hd_subscription = 'Yes' 
		, 
    FROM SC3_scaling_weekly_sample As a     
	JOIN (SELECT  DISTINCT account_number, sky_q_box FROM  #Sky_q_box WHERE sky_q_box >= 1) AS b on a.account_number = b.account_number 
	---------------------------------------------
    
	UPDATE SC3_scaling_weekly_sample
    SET a.population_scaling_segment_ID = ssl.scaling_segment_ID
		, a.vespa_scaling_segment_id = ssl.scaling_segment_ID
    FROM SC3_scaling_weekly_sample AS a
	JOIN SC3_Segments_lookup_SKYQ  AS ssl ON trim(lower(a.sky_base_universe)) = trim(lower(ssl.sky_base_universe))
														 AND left(a.hhcomposition, 2)  = left(ssl.hhcomposition, 2)
														 AND left(a.isba_tv_region, 20) = left(ssl.isba_tv_region, 20)
														 AND a.Package        = ssl.Package
														 AND left(a.tenure, 2)         = left(ssl.tenure, 2)
														 AND a.no_of_stbs     = ssl.no_of_stbs
														 AND a.hd_subscription = ssl.hd_subscription
														 AND a.pvr            = ssl.pvr

	COMMIT
    -- Just checked one manual build, none of these are null, it should all work fine.
	-- Okay, no throw all of that back into the weekly sample table, because that's where
    -- the build expects it to be, were it not for that weird bug in Sybase:

     /**************** L04: PUBLISHING INTO INTERFACE STRUCTURES ****************/

     -- First off we need the accounts and their scaling segmentation IDs: generating
     -- some 10M such records a week, but we'd be able to cull them once we've finished
     -- the associated scaling builds. Only need to maintain it while we still have
     -- historic builds to do.

	INSERT INTO SC3_Sky_base_segment_snapshots
	SELECT
		account_number
		,@profiling_thursday
		,cb_key_household   -- This guy still needs to be added to SC3_scaling_weekly_sample
		,population_scaling_segment_id
		,vespa_scaling_segment_id
		,mr_boxes+1         -- Number of multiroom boxes plus 1 for the primary
	FROM SC3_scaling_weekly_sample
	where population_scaling_segment_id is not null and vespa_scaling_segment_id is not null -- still perhaps with the weird account FROM Eire?

	commit

end; -- of procedure "SC3_v1_1__do_weekly_segmentation"
commit;
