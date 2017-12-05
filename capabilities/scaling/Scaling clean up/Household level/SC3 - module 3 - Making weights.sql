CREATE OR REPLACE PROCEDURE SC3_v1_1__make_weights 
	@scaling_day DATE -- Day for which to do scaling; this argument is mandatory
	,@batch_date DATETIME = now () -- Day on which build was kicked off
AS

BEGIN
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
	FROM SC3_Sky_base_segment_snapshots
	WHERE profiling_date <= @scaling_day

	COMMIT

	-- First adding in the Sky base numbers
	DELETE FROM SC3_weighting_working_table
	COMMIT

	INSERT INTO SC3_weighting_working_table (
				scaling_segment_id
				,sky_base_accounts
					)
	SELECT population_scaling_segment_id
		,count(1)
	FROM SC3_Sky_base_segment_snapshots
	WHERE profiling_date = @profiling_date
	GROUP BY population_scaling_segment_id

	COMMIT

	-- Now tack on the universe flags; a special case of things coming out of the lookup
	UPDATE SC3_weighting_working_table
	SET sky_base_universe = sl.sky_base_universe
	FROM SC3_weighting_working_table
	JOIN vespa_analysts.SC3_Segments_lookup_v1_1 AS sl ON SC3_weighting_working_table.scaling_segment_id = sl.scaling_segment_id

	COMMIT

	-- Mix in the Vespa panel counts as determined earlier
	SELECT scaling_segment_id
		,count(1) AS panel_members
	INTO #segment_distribs
	FROM SC3_Todays_panel_members
	WHERE scaling_segment_id IS NOT NULL
	GROUP BY scaling_segment_id

	COMMIT
	CREATE UNIQUE INDEX fake_pk ON #segment_distribs (scaling_segment_id)
	COMMIT

	-- It defaults to 0, so we can just poke values in
	UPDATE SC3_weighting_working_table
	SET vespa_panel = sd.panel_members
	FROM SC3_weighting_working_table
	INNER JOIN #segment_distribs AS sd ON SC3_weighting_working_table.scaling_segment_id = sd.scaling_segment_id

	-- And we're done! log the progress.
	COMMIT

	DROP TABLE #segment_distribs

	COMMIT

	/**************** PART B02: ASSIGNING WEIGHTS TO EACH SEGMENT ****************/
	DELETE FROM SC3_category_subtotals
	WHERE scaling_date = @scaling_day

	DELETE FROM SC3_metrics
	WHERE scaling_date = @scaling_day

	COMMIT

	-- Rim-weighting is an iterative process that iterates through each of the scaling variables
	-- individually until the category sum of weights converge to the population category subtotals
	SET @cntr = 1
	SET @iteration = 0
	SET @cntr_var = 1
	--      SET @scaling_var    = (SELECT scaling_variable FROM vespa_analysts.SC2_Variables_lookup_v2_1 WHERE id = @cntr)
	SET @scaling_var = ( SELECT scaling_variable FROM SC3_Variables_lookup_v1_1 WHERE id = @cntr )
	SET @scaling_count = ( SELECT COUNT(scaling_variable) FROM SC3_Variables_lookup_v1_1 )

	-- The SC3_weighting_working_table table contains subtotals and sum_of_weights for all segments represented by
	-- the sky base.
	-- Some segments are not represented by the vespa panel, these are allocated an arbitrary value of 0.000001
	-- to ensure convergence.
	-- arbitrary value to ensure convergence
	UPDATE SC3_weighting_working_table
	SET vespa_panel = 0.000001
	WHERE vespa_panel = 0

	COMMIT

	-- Initialise working columns
	UPDATE SC3_weighting_working_table
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
	-- In this scenario, the person running the code should send the results of the SC3_metrics for that
	-- week to analytics team for review. ## What exactly are we checking? can we automate any of it?
	WHILE @cntr <= @scaling_count
	BEGIN
		DELETE FROM SC3_category_working_table

		SET @cntr_var = 1

		WHILE @cntr_var <= @scaling_count
		BEGIN
			SELECT @scaling_var = scaling_variable
			FROM vespa_analysts.SC3_Variables_lookup_v1_1
			WHERE id = @cntr_var

			EXECUTE ('INSERT INTO SC3_category_working_table (sky_base_universe,profile,value,sky_base_accounts,vespa_panel,sum_of_weights) '
                             ||' SELECT  srs.sky_base_universe '
								||' ,@scaling_var '
                                ||'     ,ssl.' || @scaling_var 
                                ||'     ,SUM(srs.sky_base_accounts)'
                                ||'     ,SUM(srs.vespa_panel)'
                                ||'     ,SUM(srs.sum_of_weights)'
                            ||' FROM SC3_weighting_working_table AS srs'
                            ||' JOIN SC3_Segments_lookup_v1_1 AS ssl ON srs.scaling_segment_id = ssl.scaling_segment_id'
                            ||' GROUP BY srs.sky_base_universe,ssl.' || @scaling_var 
                            ||' ORDER BY srs.sky_base_universe'
					)

			SET @cntr_var = @cntr_var + 1
		END

		COMMIT

		UPDATE SC3_category_working_table
		SET category_weight = sky_base_accounts / sum_of_weights
			,convergence_flag = CASE  	WHEN abs(sky_base_accounts - sum_of_weights) < 3 THEN 0 
										ELSE 1 END

		SELECT @convergence = SUM(convergence_flag)
		FROM SC3_category_working_table

		SET @iteration = @iteration + 1

		SELECT @scaling_var = scaling_variable
		FROM SC3_Variables_lookup_v1_1
		WHERE id = @cntr

		EXECUTE ('UPDATE SC3_weighting_working_table '
				||' SET  SC3_weighting_working_table.category_weight = sc.category_weight '
                ||' ,SC3_weighting_working_table.sum_of_weights  = SC3_weighting_working_table.sum_of_weights * sc.category_weight '
				||' FROM SC3_weighting_working_table '
                ||' JOIN SC3_Segments_lookup_v1_1 AS ssl ON SC3_weighting_working_table.scaling_segment_id = ssl.scaling_segment_id '
                ||' JOIN SC3_category_working_table AS sc ON sc.value = ssl.' || @scaling_var 
                ||'  AND sc.sky_base_universe = ssl.sky_base_universe'
				)

		COMMIT

		IF @iteration = 100 OR @convergence = 0
			SET @cntr = (@scaling_count + 1)
			ELSE 
				IF @cntr = @scaling_count 
				SET @cntr = 1
				ELSE SET @cntr = @cntr + 1
	END

	COMMIT

	-- This loop build took about 4 minutes. That's fine.
	-- Calculate segment weight and corresponding indices
	-- This section calculates the segment weight which is the weight that should be applied to viewing data
	-- A couple of indices are also calculated so that we can keep track of the performance of the rim-weighting
	SELECT @sky_base = SUM(sky_base_accounts)
	FROM SC3_weighting_working_table

	SELECT @vespa_panel = SUM(vespa_panel)
	FROM SC3_weighting_working_table

	SELECT @sum_of_weights = SUM(sum_of_weights)
	FROM SC3_weighting_working_table

	UPDATE SC3_weighting_working_table
	SET segment_weight = sum_of_weights / vespa_panel
		,indices_actual = 100 * (vespa_panel / @vespa_panel) / (sky_base_accounts / @sky_base)
		,indices_weighted = 100 * (sum_of_weights / @sum_of_weights) / (sky_base_accounts / @sky_base)

	COMMIT

	-- OK, now catch those cases where stuff diverged because segments weren't reperesented:
	UPDATE SC3_weighting_working_table
	SET segment_weight = 0.000001
	WHERE vespa_panel = 0.000001

	COMMIT

	-- Now push convergence details out to the tracking tables: the first one provides a convergence summary at a category level
	INSERT INTO SC3_category_subtotals (
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
		,CASE 	WHEN abs(sky_base_accounts - sum_of_weights) > 3 THEN 1
				ELSE 0 END
	FROM SC3_category_working_table

	-- The SC3_metrics table contains metrics for a particular scaling date. It shows whether the
	-- Rim-weighting process converged for that day and the number of iterations. It also shows the
	-- maximum and average weight for that day and counts for the sky base and the vespa panel.
	COMMIT

	-- Apparently it should be reviewed each week, but what are we looking for?
	INSERT INTO SC3_metrics (
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
	FROM SC3_weighting_working_table

	UPDATE SC3_metrics
	SET sum_of_convergence = abs(sky_base - sum_of_weights)

	INSERT INTO SC3_non_convergences (
		scaling_date
		,scaling_segment_id
		,difference
		)
	SELECT @scaling_day
		,scaling_segment_id
		,abs(sum_of_weights - sky_base_accounts)
	FROM SC3_weighting_working_table
	WHERE abs((segment_weight * vespa_panel) - sky_base_accounts) > 3

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
	IF (SELECT count(1) FROM SC3_Weightings WHERE scaling_day = @scaling_day ) > 0
	BEGIN
		DELETE FROM SC3_Weightings 
		WHERE scaling_day = @scaling_day

		DELETE FROM SC3_Intervals
		WHERE reporting_starts = @scaling_day

		UPDATE SC3_Intervals
		SET reporting_ends = dateadd(day, - 1, @scaling_day)
		WHERE reporting_ends >= @scaling_day
	END

	COMMIT

	-- Part 1: Update the Vespa midway scaling tables. In Vespa Analysts? May as well
	-- also keep this in VIQ_prod too.
	INSERT INTO SC3_Weightings
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
	FROM SC3_weighting_working_table

	-- Might have to check that the filter on segment_weight doesn't leave any orphaned
	-- accounts about the place...
	COMMIT


	-- First off extend the intervals that are already in the table:
	UPDATE SC3_Intervals
	SET reporting_ends = @scaling_day
	FROM SC3_Intervals
	JOIN SC3_Todays_panel_members AS tpm ON SC3_Intervals.account_number = tpm.account_number
		AND SC3_Intervals.scaling_segment_ID = tpm.scaling_segment_ID
	WHERE reporting_ends = @scaling_day - 1

	-- Next step is adding in all the new intervals that don't appear
	-- as extensions on existing intervals. First off, isolate the
	-- intervals that got extended
	SELECT account_number
	INTO #included_accounts
	FROM SC3_Intervals
	WHERE reporting_ends = @scaling_day

	COMMIT
	CREATE UNIQUE INDEX fake_pk ON #included_accounts (account_number)
	COMMIT

	-- Now having figured out what already went in, we can throw in the rest:
	INSERT INTO SC3_Intervals (
		account_number
		,reporting_starts
		,reporting_ends
		,scaling_segment_ID
		)
	SELECT tpm.account_number
		,@scaling_day
		,@scaling_day
		,tpm.scaling_segment_ID
	FROM SC3_Todays_panel_members AS tpm
	LEFT JOIN #included_accounts AS ia ON tpm.account_number = ia.account_number
	WHERE ia.account_number IS NULL -- we don't want to add things already in the intervals table

	COMMIT

	DROP TABLE #included_accounts

	COMMIT

	-- Part 2: Update the VIQ interface table (which needs the household key thing)
	IF (SELECT count(1) FROM VESPA_HOUSEHOLD_WEIGHTING WHERE scaling_date = @scaling_day ) > 0
	BEGIN
		DELETE FROM VESPA_HOUSEHOLD_WEIGHTING
		WHERE scaling_date = @scaling_day
	END

	COMMIT

	INSERT INTO VESPA_HOUSEHOLD_WEIGHTING
	SELECT ws.account_number
		,ws.cb_key_household
		,@scaling_day
		,wwt.segment_weight
		,@batch_date
	FROM SC3_weighting_working_table AS wwt
	JOIN SC3_Sky_base_segment_snapshots AS ws 	ON wwt.scaling_segment_id = ws.population_scaling_segment_id
	JOIN SC3_Todays_panel_members AS tpm 		ON ws.account_number = tpm.account_number -- Filter for today's panel only
		AND ws.profiling_date = @profiling_date

	COMMIT

END;-- of procedure "SC3_v1_1__make_weights"

COMMIT;
