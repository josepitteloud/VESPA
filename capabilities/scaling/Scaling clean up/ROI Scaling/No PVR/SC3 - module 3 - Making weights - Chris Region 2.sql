--WORKING version 
CREATE OR REPLACE PROCEDURE SC3_ROI_make_weights_no_pvr
	@scaling_day DATE -- Day for which to do scaling; this argument is mandatory
	,@batch_date DATETIME = now () -- Day on which build was kicked off
AS

BEGIN
	-- So by this point we're assuming that the Sky base segmentation is done
	-- (for a suitably recent item) and also that today's panel members have
	-- been established, and we're just going to go calculate these weights.
	
	MESSAGE cast(now() as timestamp)||' | SC3.3 START' TO CLIENT
	
	DECLARE @cntr INT
	DECLARE @panel_it INT
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
	DECLARE @bb_flg CHAR(1)
	COMMIT

	/**************** PART B01: GETTING TOTALS FOR EACH SEGMENT ****************/
	-- Figure out which profiling info we're using;
	SELECT @profiling_date = max(profiling_date)
	FROM SC3_ROI_Sky_base_segment_snapshots
	WHERE profiling_date <= @scaling_day

	COMMIT

	IF (SELECT count(1) FROM SC3_ROI_Weightings WHERE scaling_day = @scaling_day ) > 0
		BEGIN
			DELETE FROM SC3_ROI_Weightings 
			WHERE scaling_day = @scaling_day

			DELETE FROM SC3_ROI_Intervals
			WHERE reporting_starts = @scaling_day

			UPDATE SC3_ROI_Intervals
			SET reporting_ends = dateadd(day, - 1, @scaling_day)
			WHERE reporting_ends >= @scaling_day
		END
	
	DELETE FROM SC3_ROI_category_subtotals
	WHERE scaling_date = @scaling_day

	DELETE FROM SC3_ROI_metrics
	WHERE scaling_date = @scaling_day

	IF (SELECT count(1) FROM VESPA_HOUSEHOLD_WEIGHTING_ROI WHERE scaling_date = @scaling_day ) > 0
		BEGIN
			DELETE FROM VESPA_HOUSEHOLD_WEIGHTING_ROI
			WHERE scaling_date = @scaling_day
		END

	COMMIT

	COMMIT
	
	
	-- First adding in the Sky base numbers
	DELETE FROM SC3_ROI_weighting_working_table
	COMMIT

	INSERT INTO SC3_ROI_weighting_working_table (
				scaling_segment_id
				,sky_base_accounts
				,bb_flag
					)
	SELECT population_scaling_segment_id
		,count(1)
		,bb_flag
	FROM SC3_ROI_Sky_base_segment_snapshots
	WHERE profiling_date = @profiling_date
	GROUP BY population_scaling_segment_id
		,bb_flag
	
	MESSAGE cast(now() as timestamp)||' | SC3.3 SC3_ROI_weighting_working_table Insert. Rows:'||@@rowcount TO CLIENT
	COMMIT

	-- Now tack on the universe flags; a special case of things coming out of the lookup
	UPDATE SC3_ROI_weighting_working_table
	SET a.sky_base_universe = sl.sky_base_universe
	FROM SC3_ROI_weighting_working_table 	AS a 
	JOIN SC3_ROI_Segments_lookup_no_pvr_rC2 	AS sl ON a.scaling_segment_id = sl.scaling_segment_id

	COMMIT

	-- Mix in the Vespa panel counts as determined earlier
	SELECT scaling_segment_id
		,count(1) AS panel_members
		, bb_flag
	INTO #segment_distribs_roi
	FROM SC3_ROI_Todays_panel_members
	WHERE scaling_segment_id IS NOT NULL
	GROUP BY scaling_segment_id
		, bb_flag

	MESSAGE cast(now() as timestamp)||' | SC3.3 #segment_distribs_roi creation. Rows:'||@@rowcount TO CLIENT
	COMMIT
	CREATE UNIQUE INDEX fake_pk ON #segment_distribs_roi (scaling_segment_id, bb_flag)
	COMMIT

	-- It defaults to 0, so we can just poke values in
	UPDATE SC3_ROI_weighting_working_table
	SET vespa_panel = sd.panel_members
	FROM SC3_ROI_weighting_working_table 	AS a 
	INNER JOIN #segment_distribs_roi 		AS sd ON a.scaling_segment_id = sd.scaling_segment_id AND a.bb_flag = sd.bb_flag

	
	COMMIT
	DROP TABLE #segment_distribs_roi
	COMMIT

	/**************** PART B02: ASSIGNING WEIGHTS TO EACH SEGMENT ****************/


	-- Rim-weighting is an iterative process that iterates through each of the scaling variables
	-- individually until the category sum of weights converge to the population category subtotals
	SET @panel_it = 1
	WHILE @panel_it <=2
	BEGIN 
	IF @panel_it = 1 	SET @bb_flg = 'Y' ELSE SET @bb_flg = 'N' 
		SET @cntr = 1
		SET @iteration = 0
		SET @cntr_var = 1
		SET @scaling_var = ( SELECT scaling_variable FROM SC3_ROI_Variables_lookup WHERE id = @cntr )
		SET @scaling_count = ( SELECT COUNT(scaling_variable) FROM SC3_ROI_Variables_lookup )

		-- The SC3_ROI_weighting_working_table table contains subtotals and sum_of_weights for all segments represented by
		-- the sky base.
		-- Some segments are not represented by the vespa panel, these are allocated an arbitrary value of 0.000001
		-- to ensure convergence.
		-- arbitrary value to ensure convergence
		UPDATE SC3_ROI_weighting_working_table
		SET vespa_panel = 0.000001
		WHERE vespa_panel = 0

		COMMIT

		-- Initialise working columns
		UPDATE SC3_ROI_weighting_working_table
		SET sum_of_weights = vespa_panel

		COMMIT

		-- The iterative part.
		-- This works by choosing a particular scaling variable and then summing across the categories
		-- of that scaling variable for the sky base, the vespa panel and the sum of weights.
		-- A Category weight is calculated by dividing the sky base subtotal by the vespa panel subtotal
		
		MESSAGE cast(now() as timestamp)||' | SC3.3 Iteration Start. bb-flag:'||@bb_flg TO CLIENT
	WHILE @cntr <= @scaling_count
		BEGIN
			DELETE FROM SC3_ROI_category_working_table

			SET @cntr_var = 1

			WHILE @cntr_var <= @scaling_count
			BEGIN
				SELECT @scaling_var = scaling_variable
				FROM SC3_ROI_Variables_lookup
				WHERE id = @cntr_var

				EXECUTE ('INSERT INTO SC3_ROI_category_working_table (sky_base_universe,profile,value,sky_base_accounts,vespa_panel,sum_of_weights) '
								 ||' SELECT  srs.sky_base_universe '
									||' ,@scaling_var '
									||'     ,ssl.' || @scaling_var 
									||'     ,SUM(srs.sky_base_accounts)'
									||'     ,SUM(srs.vespa_panel)'
									||'     ,SUM(srs.sum_of_weights)'
								||' FROM SC3_ROI_weighting_working_table AS srs'
								||' JOIN SC3_ROI_Segments_lookup_no_pvr_rC2 AS ssl ON srs.scaling_segment_id = ssl.scaling_segment_id'
								||' WHERE srs.bb_flag = @bb_flg'
								||' GROUP BY srs.sky_base_universe,ssl.' || @scaling_var 
								||' ORDER BY srs.sky_base_universe'
						)

				SET @cntr_var = @cntr_var + 1
			END

			COMMIT

			UPDATE SC3_ROI_category_working_table
			SET category_weight = sky_base_accounts / sum_of_weights
				,convergence_flag = CASE  	WHEN abs(sky_base_accounts - sum_of_weights) < 3 THEN 0 
											ELSE 1 END

			SELECT @convergence = SUM(convergence_flag)
			FROM SC3_ROI_category_working_table
			

			SET @iteration = @iteration + 1

			SELECT @scaling_var = scaling_variable
			FROM SC3_ROI_Variables_lookup
			WHERE id = @cntr

			EXECUTE ('UPDATE SC3_ROI_weighting_working_table '
					||' SET  SC3_ROI_weighting_working_table.category_weight = sc.category_weight '
					||' ,SC3_ROI_weighting_working_table.sum_of_weights  = SC3_ROI_weighting_working_table.sum_of_weights * sc.category_weight '
					||' FROM SC3_ROI_weighting_working_table '
					||' JOIN SC3_ROI_Segments_lookup_no_pvr_rC2 AS ssl ON SC3_ROI_weighting_working_table.scaling_segment_id = ssl.scaling_segment_id '
					||' JOIN SC3_ROI_category_working_table AS sc ON sc.value = ssl.' || @scaling_var 
					||'        AND sc.sky_base_universe = ssl.sky_base_universe'
					||' WHERE SC3_ROI_weighting_working_table.bb_flag = @bb_flg'
					)

			COMMIT

			IF @iteration = 100 --OR @convergence = 0
				SET @cntr = (@scaling_count + 1)
				ELSE 
					IF @cntr = @scaling_count 
					SET @cntr = 1
					ELSE SET @cntr = @cntr + 1
		END
		MESSAGE cast(now() as timestamp)||' | SC3.3 Iteration END. bb-flag:'||@bb_flg TO CLIENT
		COMMIT

		-- This loop build took about 4 minutes. That's fine.
		-- Calculate segment weight and corresponding indices
		-- This section calculates the segment weight which is the weight that should be applied to viewing data
		-- A couple of indices are also calculated so that we can keep track of the performance of the rim-weighting
		SELECT @sky_base = SUM(sky_base_accounts)
		FROM SC3_ROI_weighting_working_table
		WHERE bb_flag = @bb_flg

		SELECT @vespa_panel = SUM(vespa_panel)
		FROM SC3_ROI_weighting_working_table
		WHERE bb_flag = @bb_flg
		
		SELECT @sum_of_weights = SUM(sum_of_weights)
		FROM SC3_ROI_weighting_working_table
		WHERE bb_flag = @bb_flg

		UPDATE SC3_ROI_weighting_working_table
		SET segment_weight = sum_of_weights / vespa_panel
			,indices_actual = 100 * (vespa_panel / @vespa_panel) / (sky_base_accounts / @sky_base)
			,indices_weighted = 100 * (sum_of_weights / @sum_of_weights) / (sky_base_accounts / @sky_base)
		WHERE bb_flag = @bb_flg
		COMMIT

		-- OK, now catch those cases where stuff diverged because segments weren't reperesented:
		UPDATE SC3_ROI_weighting_working_table
		SET segment_weight = 0.000001
		WHERE vespa_panel = 0.000001
		AND bb_flag = @bb_flg

		COMMIT

		-- Now push convergence details out to the tracking tables: the first one provides a convergence summary at a category level
		INSERT INTO SC3_ROI_category_subtotals (
			scaling_date
			,sky_base_universe
			,PROFILE
			,value
			,sky_base_accounts
			,vespa_panel
			,category_weight
			,sum_of_weights
			,convergence
			,bb_flag
			)
		SELECT @scaling_day
			,sky_base_universe
			,PROFILE
			,value
			,sky_base_accounts
			,vespa_panel
			,category_weight
			,sum_of_weights
			,CASE 	WHEN abs(sky_base_accounts - sum_of_weights) > 3 THEN 1
					ELSE 0 END
			,bb_flag = @bb_flg
		FROM SC3_ROI_category_working_table
		
		
		
		MESSAGE cast(now() as timestamp)||' | SC3.3 SC3_ROI_category_subtotals insert: '||@@rowcount TO CLIENT
		-- The SC3_ROI_metrics table contains metrics for a particular scaling date. It shows whether the
		-- Rim-weighting process converged for that day and the number of iterations. It also shows the
		-- maximum and average weight for that day and counts for the sky base and the vespa panel.
		COMMIT

		-- Apparently it should be reviewed each week, but what are we looking for?
		INSERT INTO SC3_ROI_metrics (
			scaling_date
			,iterations
			,convergence
			,max_weight
			,av_weight
			,sum_of_weights
			,sky_base
			,vespa_panel
			,non_scalable_accounts
			,bb_flag
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
			,@bb_flg
		FROM SC3_ROI_weighting_working_table
		WHERE bb_flag = @bb_flg

		MESSAGE cast(now() as timestamp)||' | SC3.3 SC3_ROI_metrics insert: '||@@rowcount TO CLIENT
		
		UPDATE SC3_ROI_metrics
		SET sum_of_convergence = abs(sky_base - sum_of_weights)

		INSERT INTO SC3_ROI_non_convergences (
			scaling_date
			,scaling_segment_id
			,difference
			,bb_flag
			)
		SELECT @scaling_day
			,scaling_segment_id
			,abs(sum_of_weights - sky_base_accounts)
			,@bb_flg
		FROM SC3_ROI_weighting_working_table
		WHERE abs((segment_weight * vespa_panel) - sky_base_accounts) > 3
			

		MESSAGE cast(now() as timestamp)||' | SC3.3 SC3_ROI_non_convergences insert: '||@@rowcount TO CLIENT
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


		COMMIT

		-- Part 1: Update the Vespa midway scaling tables. In Vespa Analysts? May as well
		-- also keep this in VIQ_prod too.
		INSERT INTO SC3_ROI_Weightings
		SELECT @scaling_day
			,scaling_segment_id
			,vespa_panel
			,sky_base_accounts
			,segment_weight
			,sum_of_weights
			,indices_actual
			,indices_weighted
			,CASE  	WHEN abs(sky_base_accounts - sum_of_weights) > 3 THEN 1
					ELSE 0 END
			,@bb_flg
		FROM SC3_ROI_weighting_working_table
		WHERE bb_flag = @bb_flg

		MESSAGE cast(now() as timestamp)||' | SC3.3 SC3_ROI_Weightings insert: '||@@rowcount TO CLIENT
		-- Might have to check that the filter on segment_weight doesn't leave any orphaned
		-- accounts about the place...
		COMMIT


		-- First off extend the intervals that are already in the table:
		UPDATE SC3_ROI_Intervals
		SET reporting_ends = @scaling_day
		FROM SC3_ROI_Intervals
		JOIN SC3_ROI_Todays_panel_members AS tpm ON SC3_ROI_Intervals.account_number = tpm.account_number
			AND SC3_ROI_Intervals.scaling_segment_ID = tpm.scaling_segment_ID
		WHERE reporting_ends = @scaling_day - 1

		-- Next step is adding in all the new intervals that don't appear
		-- as extensions on existing intervals. First off, isolate the
		-- intervals that got extended
		SELECT DISTINCT account_number
		INTO #included_accounts_roi
		FROM SC3_ROI_Intervals
		WHERE reporting_ends = @scaling_day

		COMMIT
		CREATE UNIQUE INDEX fake_pk ON #included_accounts_roi (account_number)
		COMMIT

		-- Now having figured out what already went in, we can throw in the rest:
		INSERT INTO SC3_ROI_Intervals (
			account_number
			,reporting_starts
			,reporting_ends
			,scaling_segment_ID
			,bb_flag
			)
		SELECT tpm.account_number
			,@scaling_day
			,@scaling_day
			,tpm.scaling_segment_ID
			,@bb_flg
		FROM SC3_ROI_Todays_panel_members AS tpm
		LEFT JOIN #included_accounts_roi AS ia ON tpm.account_number = ia.account_number
		WHERE ia.account_number IS NULL -- we don't want to add things already in the intervals table
		AND bb_flag = @bb_flg
		
		MESSAGE cast(now() as timestamp)||' | SC3.3 SC3_ROI_Intervals insert: '||@@rowcount TO CLIENT
		COMMIT

		DROP TABLE #included_accounts_roi

		COMMIT

		-- Part 2: Update the VIQ interface table (which needs the household key thing)

		INSERT INTO VESPA_HOUSEHOLD_WEIGHTING_ROI
		SELECT ws.account_number
			,ws.cb_key_household
			,@scaling_day
			,wwt.segment_weight
			,@batch_date
		FROM SC3_ROI_weighting_working_table AS wwt
		JOIN SC3_ROI_Sky_base_segment_snapshots AS ws 	ON wwt.scaling_segment_id = ws.population_scaling_segment_id AND wwt.bb_flag = ws.bb_flag
		JOIN SC3_ROI_Todays_panel_members AS tpm 		ON ws.account_number = tpm.account_number -- Filter for today's panel only
			AND ws.profiling_date = @profiling_date
		WHERE wwt.bb_flag = @bb_flg

		MESSAGE cast(now() as timestamp)||' | SC3.3 VESPA_HOUSEHOLD_WEIGHTING_ROI insert: '||@@rowcount TO CLIENT
		COMMIT
		SET @panel_it = 1 + @panel_it 
END  --- of panel type loop  bb_flag: Y/N
END;-- of procedure "SC3_ROI_make_weights "

COMMIT;
