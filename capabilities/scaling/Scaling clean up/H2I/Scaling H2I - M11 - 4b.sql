CREATE OR replace PROCEDURE V289_M11_04_SC3I_v1_1__make_weights_BARB_clean
				@profiling_date DATE -- Thursday profilr date
				,@scaling_day DATE -- Day for which to do scaling; this argument is mandatory
				,@batch_date DATETIME = now () -- Day on which build was kicked off
AS

BEGIN
	/*
        -- Only need these if we can't get to execute as a Proc
                declare @scaling_day  date
        declare @batch_date date
        declare @Scale_refresh_logging_ID bigint
        set @scaling_day = '2013-09-26'
        set @batch_date = '2014-07-10'
        set @Scale_refresh_logging_ID = 5
                COMMIT
        */
	-- CREATE OR REPLACE VARIABLE @profiling_date             date  =       '2015-02-05';
	-- CREATE OR REPLACE VARIABLE @scaling_day                date =    '2015-02-06';
	-- CREATE OR REPLACE VARIABLE @batch_date                datetime = now();
	-- CREATE OR REPLACE VARIABLE @Scale_refresh_logging_ID  bigint = 4131;
	-- SET @profiling_date  =       '2015-02-05'
	-- SET @scaling_day =       '2015-02-06'
	-- SET      @Scale_refresh_logging_ID       = 4131
	-- COMMIT
	-- So by this point we're assuming that the Sky base segmentation is done
	-- (for a suitably recent item) and also that today's panel members have
	-- been established, and we're just going to go calculate these weights.

	-- CREATE OR REPLACE VARIABLE @cntr           INT;
	-- CREATE OR REPLACE VARIABLE @iteration      INT;
	-- CREATE OR REPLACE VARIABLE @cntr_var       SMALLINT;
	-- CREATE OR REPLACE VARIABLE @scaling_var    VARCHAR(30);
	-- CREATE OR REPLACE VARIABLE @scaling_count  SMALLINT;
	-- CREATE OR REPLACE VARIABLE @convergence    TINYINT;
	-- CREATE OR REPLACE VARIABLE @sky_base       DOUBLE;
	-- CREATE OR REPLACE VARIABLE @vespa_panel    DOUBLE;
	-- CREATE OR REPLACE VARIABLE @sum_of_weights DOUBLE;
	-- --     CREATE OR REPLACE VARIABLE @profiling_date date;

	MESSAGE cast(now() as timestamp)||' | M11.4 Start 'TO CLIENT
	
	DECLARE @cntr INT
	DECLARE @iteration INT
	DECLARE @cntr_var SMALLINT
	DECLARE @scaling_var VARCHAR(30)
	DECLARE @scaling_count SMALLINT
	DECLARE @convergence TINYINT
	DECLARE @sky_base DOUBLE
	DECLARE @vespa_panel DOUBLE
	DECLARE @sum_of_weights DOUBLE
	COMMIT -- (^_^)

	/**************** PART B01: GETTING TOTALS FOR EACH SEGMENT ****************/
	-- First adding in the Sky base numbers
	TRUNCATE TABLE SC3I_weighting_working_table

	MESSAGE cast(now() as timestamp)||' | M11.4 Preparation Done' TO CLIENT
	COMMIT -- (^_^)

	INSERT INTO SC3I_weighting_working_table (
		scaling_segment_id
		,sky_base_accounts
		)
	SELECT population_scaling_segment_id
		,count(1)
	FROM SC3I_Sky_base_segment_snapshots
	WHERE profiling_date = @profiling_date
	GROUP BY population_scaling_segment_id

	MESSAGE cast(now() as timestamp)||' | M11.1 #SC3I_weighting_working_table insert. Rows:'||@@rowcount TO CLIENT
	
	
	COMMIT -- (^_^)

	-- Rebase the Sky Base so that it matches EDM processes
	-- The easiest way to do this is to use the total wieghts derived in VIQ_VIEWING_DATA_SCALING
	-- Get Skybase size according to this process
	SELECT count(DISTINCT account_number) AS base_total
	INTO #base
	FROM SC3I_Sky_base_segment_snapshots
	WHERE profiling_date = @profiling_date

	-- Get Skybase size according to EDM
	SELECT max(adjusted_event_start_date_vespa) AS edm_latest_scaling_date
	INTO #latest_date
	FROM sk_prod.VIQ_VIEWING_DATA_SCALING

	COMMIT -- (^_^)

	SELECT CASE WHEN @scaling_day <= edm_latest_scaling_date THEN @scaling_day
			ELSE edm_latest_scaling_date
			END AS sky_base_universe_date
	INTO #use_date
	FROM #latest_date

	COMMIT -- (^_^)

	SELECT sum(calculated_scaling_weight) AS edm_total
	INTO #viq_scaling
	FROM sk_prod.VIQ_VIEWING_DATA_SCALING
	CROSS JOIN #use_date
	WHERE adjusted_event_start_date_vespa = sky_base_universe_date

	COMMIT -- (^_^)

	UPDATE SC3I_weighting_working_table AS w
	SET w.sky_base_accounts = w.sky_base_accounts * v.edm_total / b.base_total
	FROM #base b
		,#viq_scaling v

	COMMIT -- (^_^)
	
		MESSAGE cast(now() as timestamp)||' | M11.1 Rebase done'||@@rowcount TO CLIENT
	

	/**************** update SC3I_weighting_working_table
        -- Re-scale Sky base to Barb age/gender totals
        -- Will only rescale to barb households that have any viewing data for the day being scaled
        -- and NOT the barb base
        */
	-- Get individuals from Barb who have viewed tv for the processing day
	SELECT household_number
		,person_number
	INTO #barb_viewers
	FROM skybarb_fullview
	WHERE DATE (start_time_of_session) = @scaling_day
	GROUP BY household_number
		,person_number

	MESSAGE cast(now() as timestamp)||' | M11.1 #barb_viewers insert. Rows:'||@@rowcount TO CLIENT
	
	COMMIT -- (^_^)
	CREATE hg INDEX ind1 ON #barb_viewers (household_number)
	CREATE lf INDEX ind2 ON #barb_viewers (person_number)
	COMMIT -- (^_^)

	-- Now reduce that to the corresponding households that've registered some viewing
	SELECT household_number
	INTO #barb_hhd_viewers
	FROM #barb_viewers
	GROUP BY household_number

	MESSAGE cast(now() as timestamp)||' | M11.1 #barb_hhd_viewers insert. Rows:'||@@rowcount TO CLIENT
	
	COMMIT -- (^_^)
	CREATE hg INDEX ind1 ON #barb_hhd_viewers (household_number)
	COMMIT -- (^_^)

	-- Get details on individuals from Sky households in BARB (no date-dependency here)
	SELECT h.house_id AS household_number
		,h.person AS person_number
		,h.age
		,CASE 	WHEN age <= 19 THEN 'U'
				WHEN h.sex = 'Male' THEN 'M'
				WHEN h.sex = 'Female' THEN 'F'
			END AS gender
		,CASE 	WHEN age <= 11 THEN '0-11'
				WHEN age BETWEEN 12 AND 19 THEN '12-19'
				WHEN age BETWEEN 20 AND 24 THEN '20-24'
				WHEN age BETWEEN 25 AND 34 THEN '25-34'
				WHEN age BETWEEN 35 AND 44 THEN '35-44'
				WHEN age BETWEEN 45 AND 64 THEN '45-64'
				WHEN age >= 65 THEN '65+'
			END AS ageband
		,cast(h.head AS CHAR(1)) AS head_of_hhd
		,w.processing_weight AS processing_weight
	INTO #barb_inds_with_sky
	FROM skybarb AS h
	INNER JOIN barb_weights AS w ON h.house_id = w.household_number
		AND h.person = w.person_number

	COMMIT -- (^_^)

	MESSAGE cast(now() as timestamp)||' | M11.1 #barb_inds_with_sky insert. Rows:'||@@rowcount TO CLIENT
	
	-------------- Summarise Barb Data
	-- Define the default matrix of ageband, hhsize (and now viewed_tv) to deal with empty Barb segments
	SELECT hh_size
		,age_band
		,viewed_tv
		,head_of_hhd
		,1.0 AS default_weight
	INTO #default_m
	FROM (SELECT DISTINCT hh_size FROM vespa_analysts.SC3I_Segments_lookup_v1_1) 		AS a
	CROSS JOIN (SELECT DISTINCT age_band FROM vespa_analysts.SC3I_Segments_lookup_v1_1) AS b
	CROSS JOIN (SELECT DISTINCT viewed_tv FROM vespa_analysts.SC3I_Segments_lookup_v1_1)   AS c
	CROSS JOIN (SELECT DISTINCT head_of_hhd FROM vespa_analysts.SC3I_Segments_lookup_v1_1) AS d

	MESSAGE cast(now() as timestamp)||' | M11.1 #default_m insert. Rows:'||@@rowcount TO CLIENT
		
	COMMIT -- (^_^)

	-- Add BARB individual weights from the available segments, aggregated by gender-age/viewed-tv/hhsize attributes
	TRUNCATE TABLE V289_M11_04_Barb_weighted_population

	COMMIT -- (^_^)

	INSERT INTO V289_M11_04_Barb_weighted_population (
		ageband
		,viewed_tv
		,head_of_hhd
		,hh_size
		,barb_weight
		)
	SELECT (CASE WHEN ageband = '0-19' THEN 'U'
				ELSE gender
				END) || ' ' || ageband AS gender_ageband -- now adapted to be a gender-age attribute
			,CASE WHEN v.household_number IS NOT NULL THEN 'Yes'
			  WHEN v_hhd.household_number IS NOT NULL THEN 'NV - Viewing HHD'
			  ELSE 'NV - NonViewing HHD'
			END AS viewed_tv -- Derive viewer/non-viewer flag
		,i.head_of_hhd -- this has also been fixed                       as      head_of_hhd     -- '9'
		,z.hh_gr AS hh_size
		,sum(processing_weight) AS barb_weight
	FROM #barb_inds_with_sky AS i -- Sky individuals in BARB
	INNER JOIN (SELECT household_number
			,CASE WHEN hhsize < 8 THEN cast(hhsize AS VARCHAR(2))
				ELSE '8+'
				END AS hh_gr
		FROM (SELECT -- First calculate the household sizes
				household_number
				,count(1) AS hhsize
			FROM barb_weights
			GROUP BY household_number
			) AS w
		) AS z ON i.household_number = z.household_number
	LEFT JOIN #barb_viewers AS v ON i.household_number = v.household_number AND i.person_number = v.person_number
	LEFT JOIN #barb_hhd_viewers v_hhd ON i.household_number = v_hhd.household_number
	GROUP BY gender_ageband
		-- ,    gender
		,viewed_tv
		,head_of_hhd
		,hh_size

	MESSAGE cast(now() as timestamp)||' | M11.1 V289_M11_04_Barb_weighted_population insert. Rows:'||@@rowcount TO CLIENT
	
	COMMIT -- (^_^)

	-- Clean up
	DROP TABLE #barb_inds_with_sky

	COMMIT -- (^_^)

	-- Set the dummy variable here
	UPDATE V289_M11_04_Barb_weighted_population
	SET gender = 'A'

	COMMIT -- (^_^)

	-- Now add default weights for any segments that were missing in the above
	INSERT INTO V289_M11_04_Barb_weighted_population (
		ageband
		,gender
		,viewed_tv
		,head_of_hhd
		,hh_size
		,barb_weight
		)
	SELECT m.age_band
		,'A'
		-- ,    'Y'
		,m.viewed_tv
		,m.head_of_hhd --      '9'
		,m.hh_size
		,default_weight
	FROM #default_m AS m
	LEFT JOIN V289_M11_04_Barb_weighted_population AS b ON m.hh_size = b.hh_size
		AND m.age_band = b.ageband
		AND m.viewed_tv = b.viewed_tv
		AND m.head_of_hhd = b.head_of_hhd
	WHERE b.hh_size IS NULL

	COMMIT -- (^_^)

	MESSAGE cast(now() as timestamp)||' | M11.1 V289_M11_04_Barb_weighted_population default insert. Rows:'||@@rowcount TO CLIENT
	----
	-- Calculate SKy Base Total by Barb Segment
	SELECT l.age_band
		,l.head_of_hhd
		,l.hh_size
		,l.viewed_tv
		,sum(w.sky_base_accounts) AS tot_base_accounts
	INTO #base_totals
	FROM SC3I_weighting_working_table w
	INNER JOIN vespa_analysts.SC3i_Segments_lookup_v1_1 l ON w.scaling_segment_id = l.scaling_segment_id
	GROUP BY l.age_band
		,l.head_of_hhd
		,l.hh_size
		,l.viewed_tv

	MESSAGE cast(now() as timestamp)||' | M11.1 #base_totals insert. Rows:'||@@rowcount TO CLIENT
	COMMIT -- (^_^)

	UPDATE SC3I_weighting_working_table AS w
	SET w.sky_base_accounts = w.sky_base_accounts * barb_weight / cast(b.tot_base_accounts AS FLOAT)
	FROM vespa_analysts.SC3i_Segments_lookup_v1_1 AS l
	CROSS JOIN #base_totals b
	CROSS JOIN V289_M11_04_Barb_weighted_population p
	WHERE w.scaling_segment_id = l.scaling_segment_id
		AND l.age_band = b.age_band
		AND l.head_of_hhd = b.head_of_hhd
		AND l.hh_size = b.hh_size
		AND l.viewed_tv = b.viewed_tv
		AND l.age_band = p.ageband
		AND l.head_of_hhd = p.head_of_hhd
		AND l.hh_size = p.hh_size
		AND l.viewed_tv = p.viewed_tv

	MESSAGE cast(now() as timestamp)||' | M11.1 #SC3I_weighting_working_table update. Rows:'||@@rowcount TO CLIENT
	COMMIT -- (^_^)

	/***********************************************/
	-- Now tack on the universe flags; a special case of things coming out of the lookup
	UPDATE SC3I_weighting_working_table
	SET sky_base_universe = sl.sky_base_universe
	FROM SC3I_weighting_working_table
	INNER JOIN vespa_analysts.SC3I_Segments_lookup_v1_1 AS sl ON SC3I_weighting_working_table.scaling_segment_id = sl.scaling_segment_id


	COMMIT -- (^_^)

	-- Mix in the Vespa panel counts as determined earlier
	SELECT scaling_segment_id
		,count(1) AS panel_members
	INTO #segment_distribs
	FROM SC3I_Todays_panel_members
	WHERE scaling_segment_id IS NOT NULL
	GROUP BY scaling_segment_id

	MESSAGE cast(now() as timestamp)||' | M11.1 #segment_distribs creation. Rows:'||@@rowcount TO CLIENT

	COMMIT -- (^_^)
	CREATE UNIQUE INDEX fake_pk ON #segment_distribs (scaling_segment_id)
	COMMIT -- (^_^)

	-- It defaults to 0, so we can just poke values in
	UPDATE SC3I_weighting_working_table
	SET vespa_panel = sd.panel_members
	FROM SC3I_weighting_working_table
	INNER JOIN #segment_distribs AS sd ON SC3I_weighting_working_table.scaling_segment_id = sd.scaling_segment_id

	-- And we're done! log the progress.
	COMMIT -- (^_^)

	DROP TABLE #segment_distribs

	/**************** PART B02: ASSIGNING WEIGHTS TO EACH SEGMENT ****************/
	DELETE FROM SC3I_category_subtotals
	WHERE scaling_date = @scaling_day

	COMMIT -- (^_^)

	DELETE
	FROM SC3I_metrics
	WHERE scaling_date = @scaling_day

	COMMIT -- (^_^)

	MESSAGE cast(now() as timestamp)||' | M11.4 Deleting output tables' TO CLIENT
	-- Rim-weighting is an iterative process that iterates through each of the scaling variables
	-- individually until the category sum of weights converge to the population category subtotals
	SET @cntr = 1
	SET @iteration = 0
	SET @cntr_var = 1
	COMMIT -- (^_^)

	SET @scaling_var = (SELECT scaling_variable
						FROM vespa_analysts.SC3I_Variables_lookup_v1_1
						WHERE id = @cntr)
	COMMIT -- (^_^)

	SET @scaling_count = (SELECT COUNT(scaling_variable) FROM vespa_analysts.SC3I_Variables_lookup_v1_1)

	COMMIT -- (^_^)

	-- The SC3I_weighting_working_table table contains subtotals and sum_of_weights for all segments represented by
	-- the sky base.
	-- Some segments are not represented by the vespa panel, these are allocated an arbitrary value of 0.000001
	-- to ensure convergence.
	-- arbitrary value to ensure convergence
	UPDATE SC3I_weighting_working_table
	SET vespa_panel = 0.000001
	WHERE vespa_panel = 0

	COMMIT -- (^_^)

	-- Initialise working columns
	UPDATE SC3I_weighting_working_table
	SET sum_of_weights = vespa_panel

	COMMIT -- (^_^)

	MESSAGE cast(now() as timestamp)||' | M11.4 iteration parameters setup done' TO CLIENT
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
		TRUNCATE TABLE SC3I_category_working_table

		COMMIT

		SET @cntr_var = 1

		COMMIT

		WHILE @cntr_var <= @scaling_count
		BEGIN
			SELECT @scaling_var = scaling_variable
			FROM vespa_analysts.SC3I_Variables_lookup_v1_1
			WHERE id = @cntr_var

			COMMIT

			EXECUTE ('INSERT INTO SC3I_category_working_table(
										sky_base_universe
								,       profile
								,       value
								,       sky_base_accounts
								,       vespa_panel
								,       sum_of_weights)
					SELECT 
										srs.sky_base_universe
								,       @scaling_var
								,       ssl.' || @scaling_var || 
	'							,       SUM(srs.sky_base_accounts)
								,       SUM(srs.vespa_panel)
								,       SUM(srs.sum_of_weights)
					FROM SC3I_weighting_working_table AS srs
					inner join      vespa_analysts.SC3I_Segments_lookup_v1_1        AS      ssl             ON      srs.scaling_segment_id  =       ssl.scaling_segment_id
					GROUP BY
								srs.sky_base_universe
						,       @scaling_var
						,       ssl.' || @scaling_var)
			COMMIT
			SET @cntr_var = @cntr_var + 1
		END 
		COMMIT

		UPDATE SC3I_category_working_table
		SET category_weight = sky_base_accounts / sum_of_weights
			,convergence_flag = CASE WHEN abs(sky_base_accounts - sum_of_weights) < 3 THEN 0
										ELSE 1 END
		COMMIT
		SELECT @convergence = SUM(convergence_flag)
		FROM SC3I_category_working_table

		COMMIT
		SET @iteration = @iteration + 1
		COMMIT

		SELECT @scaling_var = scaling_variable
		FROM vespa_analysts.SC3I_Variables_lookup_v1_1
		WHERE id = @cntr

		COMMIT

		EXECUTE ('UPDATE  SC3I_weighting_working_table '||
				'SET a.category_weight = sc.category_weight '||
					',a.sum_of_weights = a.sum_of_weights * sc.category_weight '||
				'FROM SC3I_weighting_working_table 					 AS a '||
				'inner join vespa_analysts.SC3I_Segments_lookup_v1_1 AS ssl ON a.scaling_segment_id =  ssl.scaling_segment_id '||
				'inner join SC3I_category_working_table              AS sc  ON sc.value = ssl.' || @scaling_var || 
																'AND     sc.sky_base_universe = ssl.sky_base_universe'
																'AND     sc.profile          = @scaling_var')
		COMMIT

		IF (@iteration = 100 OR @convergence = 0 )
			SET @cntr = @scaling_count + 1
		ELSE IF @cntr = @scaling_count
			SET @cntr = 1
		ELSE
			SET @cntr = @cntr + 1

		COMMIT
	END -- of LOOP

	
	COMMIT -- (^_^)

	SELECT @sky_base = SUM(sky_base_accounts)
		,@vespa_panel = SUM(vespa_panel)
		,@sum_of_weights = SUM(sum_of_weights)
	FROM SC3I_weighting_working_table

	COMMIT -- (^_^)

	UPDATE SC3I_weighting_working_table
	SET segment_weight = sum_of_weights / vespa_panel
		,indices_actual = 100 * (vespa_panel / @vespa_panel) / (sky_base_accounts / @sky_base)
		,indices_weighted = 100 * (sum_of_weights / @sum_of_weights) / (sky_base_accounts / @sky_base)

	COMMIT -- (^_^)

	-- OK, now catch those cases where stuff diverged because segments weren't reperesented:
	UPDATE SC3I_weighting_working_table
	SET segment_weight = 0.000001
	WHERE vespa_panel = 0.000001

	COMMIT -- (^_^)

	-- Now push convergence details out to the tracking tables: the first one provides a convergence summary at a category level
	INSERT INTO SC3I_category_subtotals (
		scaling_date
		,sky_base_universe
		,PROFILE
		,value
		,sky_base_accounts
		,vespa_panel
		,category_weight
		,sum_of_weights
		,convergence
		)
	SELECT @scaling_day
		,sky_base_universe
		,PROFILE
		,value
		,sky_base_accounts
		,vespa_panel
		,category_weight
		,sum_of_weights
		,CASE WHEN abs(sky_base_accounts - sum_of_weights) > 3 THEN 1
			ELSE 0 END
	FROM SC3I_category_working_table

	COMMIT -- (^_^)

	-- The SC3I_metrics table contains metrics for a particular scaling date. It shows whether the
	-- Rim-weighting process converged for that day and the number of iterations. It also shows the
	-- maximum and average weight for that day and counts for the sky base and the vespa panel.
	COMMIT -- (^_^)

	-- Apparently it should be reviewed each week, but what are we looking for?
	INSERT INTO SC3I_metrics (
		scaling_date
		,iterations
		,convergence
		,max_weight
		,av_weight
		,sum_of_weights
		,sky_base
		,vespa_panel
		,non_scalable_accounts
		)
	SELECT @scaling_day
		,@iteration
		,@convergence
		,MAX(segment_weight)
		,sum(segment_weight * vespa_panel) / sum(vespa_panel) -- gives the average weight by account (just uising AVG would give it average by segment id)
		,SUM(segment_weight * vespa_panel) -- again need some math because this table has one record per segment id rather than being at acocunt level
		,@sky_base
		,sum(CASE WHEN segment_weight >= 0.001 THEN vespa_panel ELSE NULL END)
		,sum(CASE WHEN segment_weight < 0.001 THEN vespa_panel ELSE NULL END)
	FROM SC3I_weighting_working_table

	COMMIT -- (^_^)

	UPDATE SC3I_metrics
	SET sum_of_convergence = abs(sky_base - sum_of_weights)

	COMMIT -- (^_^)

	INSERT INTO SC3I_non_convergences (
		scaling_date
		,scaling_segment_id
		,difference
		)
	SELECT @scaling_day
		,scaling_segment_id
		,abs(sum_of_weights - sky_base_accounts)
	FROM SC3I_weighting_working_table
	WHERE abs((segment_weight * vespa_panel) - sky_base_accounts) > 3


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
	IF (SELECT count(1) FROM SC3I_Weightings WHERE scaling_day = @scaling_day) > 0
	BEGIN
		DELETE
		FROM SC3I_Weightings
		WHERE scaling_day = @scaling_day
	END

	COMMIT -- (^_^)

	-- Part 1: Update the Vespa midway scaling tables. In Vespa Analysts? May as well
	-- also keep this in VIQ_prod too.
	INSERT INTO SC3I_Weightings
	SELECT @scaling_day
		,scaling_segment_id
		,vespa_panel
		,sky_base_accounts
		,segment_weight
		,sum_of_weights
		,indices_actual
		,indices_weighted
		,CASE 
			WHEN abs(sky_base_accounts - sum_of_weights) > 3
				THEN 1
			ELSE 0
			END
	FROM SC3I_weighting_working_table

	COMMIT -- (^_^)

	-- Part 2: Update the VIQ interface table (which needs the household key thing)
	TRUNCATE TABLE V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING

	COMMIT -- (^_^)

	INSERT INTO V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING (
		account_number
		,HH_person_number
		,scaling_date
		,scaling_weighting
		,build_date
		)
	SELECT tpm.account_number
		,tpm.HH_person_number
		,@scaling_day
		,wwt.segment_weight
		,@batch_date
	FROM SC3I_Todays_panel_members AS tpm
	INNER JOIN SC3I_weighting_working_table AS wwt ON tpm.scaling_segment_id = wwt.scaling_segment_id


	COMMIT -- (^_^)
END;-- of procedure "V289_M11_04_SC3I_v1_1__make_weights_BARB"

COMMIT;
