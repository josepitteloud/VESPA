CREATE OR REPLACE PROCEDURE V289_M11_04_SC3I_v1_1__make_weights_sv_BARB_sv (
	@profiling_date DATE
	,@scaling_day DATE
	,@batch_date DATETIME = now()
	,@Scale_refresh_logging_ID BIGINT = NULL
	)
AS
BEGIN
	DECLARE @cntr INTEGER
	DECLARE @iteration INTEGER
	DECLARE @cntr_var SMALLINT
	DECLARE @scaling_var VARCHAR(30)
	DECLARE @scaling_count SMALLINT
	DECLARE @convergence TINYINT
	DECLARE @sky_base DOUBLE
	DECLARE @vespa_panel DOUBLE
	DECLARE @sum_of_weights DOUBLE
	DECLARE @QA_catcher BIGINT

	COMMIT WORK

	COMMIT WORK

	TRUNCATE TABLE jsk01.SC3I_weighting_working_table_sv

	COMMIT WORK

	INSERT INTO SC3I_weighting_working_table_sv (
		scaling_segment_id
		,sky_base_accounts
		)
	SELECT population_scaling_segment_id
		,count(1)
	FROM SC3I_Sky_base_segment_snapshots_sv
	WHERE profiling_date = @profiling_date
	GROUP BY population_scaling_segment_id

	COMMIT WORK

	SELECT 'base_total' = count(DISTINCT account_number)
	INTO #base
	FROM SC3I_Sky_base_segment_snapshots_sv
	WHERE profiling_date = @profiling_date

	SELECT 'edm_latest_scaling_date' = max(adjusted_event_start_date_vespa)
	INTO #latest_date
	FROM sk_prod.VIQ_VIEWING_DATA_SCALING

	COMMIT WORK

	SELECT 'sky_base_universe_date' = CASE 
			WHEN @scaling_day <= edm_latest_scaling_date
				THEN @scaling_day
			ELSE edm_latest_scaling_date
			END
	INTO #use_date
	FROM #latest_date

	COMMIT WORK

	SELECT 'edm_total' = sum(calculated_scaling_weight)
	INTO #viq_scaling
	FROM sk_prod.VIQ_VIEWING_DATA_SCALING
	CROSS JOIN #use_date
	WHERE adjusted_event_start_date_vespa = sky_base_universe_date

	COMMIT WORK

	UPDATE SC3I_weighting_working_table_sv AS w
	SET w.sky_base_accounts = w.sky_base_accounts * v.edm_total / b.base_total
	FROM #base AS b
		,#viq_scaling AS v

	COMMIT WORK

	SELECT household_number
		,person_number
	INTO #barb_viewers
	FROM skybarb_sv_fullview_sv
	WHERE DATE (start_time_of_session) = @scaling_day
	GROUP BY household_number
		,person_number

	COMMIT WORK
	CREATE hg INDEX ind1 ON #barb_viewers (household_number)
	CREATE lf INDEX ind2 ON #barb_viewers (person_number)
	COMMIT WORK

	SELECT household_number
	INTO #barb_hhd_viewers
	FROM #barb_viewers
	GROUP BY household_number

	COMMIT WORK
	CREATE hg INDEX ind1 ON #barb_hhd_viewers (household_number)
	COMMIT WORK

	SELECT 'household_number' = h.house_id
		,'person_number' = h.person
		,h.age
		,'gender' = CASE 
			WHEN age <= 19
				THEN 'U'
			WHEN h.sex = 'Male'
				THEN 'M'
			WHEN h.sex = 'Female'
				THEN 'F'
			END
		,'ageband' = CASE 
			WHEN age <= 19
				THEN '0-19'
			WHEN age BETWEEN 20
					AND 44
				THEN '20-44'
			WHEN age >= 45
				THEN '45+'
			END
		,'head_of_hhd' = convert(CHAR(1), h.head)
		,'processing_weight' = w.processing_weight
	INTO #barb_inds_with_sky
	FROM skybarb_sv AS h
	JOIN barb_weights_sv AS w ON h.house_id = w.household_number
		AND h.person = w.person_number

	COMMIT WORK

	SELECT hh_size
		,age_band
		,viewed_tv
		,head_of_hhd
		,'default_weight' = 1.0
	INTO #default_m
	FROM (SELECT DISTINCT hh_size FROM SC3I_Segments_Lookup_vkuba) AS a
	CROSS JOIN (SELECT DISTINCT age_band FROM SC3I_Segments_Lookup_vkuba) AS b
	CROSS JOIN (SELECT DISTINCT viewed_tv FROM SC3I_Segments_Lookup_vkuba) AS c
	CROSS JOIN (SELECT DISTINCT head_of_hhd FROM SC3I_Segments_Lookup_vkuba) AS d

	TRUNCATE TABLE jsk01.V289_M11_04_Barb_weighted_population_sv

	COMMIT WORK

	INSERT INTO V289_M11_04_Barb_weighted_population_sv (
		ageband
		,viewed_tv
		,head_of_hhd
		,hh_size
		,barb_weight
		)
	SELECT 'gender_ageband' = (
			CASE 
				WHEN ageband = '0-19'
					THEN 'U'
				ELSE gender
				END
			) || ' ' || ageband
		,'viewed_tv' = CASE 
			WHEN v.household_number IS NOT NULL
				THEN 'Yes'
			WHEN v_hhd.household_number IS NOT NULL
				THEN 'NV - Viewing HHD'
			ELSE 'NV - NonViewing HHD'
			END
		,i.head_of_hhd
		,'hh_size' = z.hh_gr
		,'barb_weight' = sum(processing_weight)
	FROM #barb_inds_with_sky AS i
	JOIN (
		SELECT household_number
			,'hh_gr' = CASE 
				WHEN hhsize < 5
					THEN convert(VARCHAR(2), hhsize)
				ELSE '5+'
				END
		FROM (
			SELECT household_number
				,'hhsize' = count(1)
			FROM barb_weights_sv
			GROUP BY household_number
			) AS w
		) AS z ON i.household_number = z.household_number
	LEFT OUTER JOIN #barb_viewers AS v ON i.household_number = v.household_number
		AND i.person_number = v.person_number
	LEFT OUTER JOIN #barb_hhd_viewers AS v_hhd ON i.household_number = v_hhd.household_number
	GROUP BY gender_ageband
		,viewed_tv
		,head_of_hhd
		,hh_size

	COMMIT WORK

	DROP TABLE #barb_inds_with_sky

	UPDATE V289_M11_04_Barb_weighted_population_sv
	SET gender = 'A'

	COMMIT WORK

	INSERT INTO V289_M11_04_Barb_weighted_population_sv (
		ageband
		,gender
		,viewed_tv
		,head_of_hhd
		,hh_size
		,barb_weight
		)
	SELECT m.age_band
		,'A'
		,m.viewed_tv
		,m.head_of_hhd
		,m.hh_size
		,default_weight
	FROM #default_m AS m
	LEFT OUTER JOIN V289_M11_04_Barb_weighted_population_sv AS b ON m.hh_size = b.hh_size
		AND m.age_band = b.ageband
		AND m.viewed_tv = b.viewed_tv
		AND m.head_of_hhd = b.head_of_hhd
	WHERE b.hh_size IS NULL

	COMMIT WORK

	SELECT l.age_band
		,l.head_of_hhd
		,l.hh_size
		,l.viewed_tv
		,'tot_base_accounts' = sum(w.sky_base_accounts) 
	INTO #base_totals
	FROM SC3I_weighting_working_table_sv AS w
	JOIN SC3I_Segments_Lookup_vkuba AS l ON w.scaling_segment_id = l.scaling_segment_id
	GROUP BY l.age_band
		,l.head_of_hhd
		,l.hh_size
		,l.viewed_tv

	COMMIT WORK

	UPDATE SC3I_weighting_working_table_sv AS w
	SET w.sky_base_accounts = w.sky_base_accounts * barb_weight / convert(REAL, b.tot_base_accounts)
	FROM SC3I_Segments_Lookup_vkuba AS l
	CROSS JOIN #base_totals AS b
	CROSS JOIN V289_M11_04_Barb_weighted_population_sv AS p
	WHERE w.scaling_segment_id = l.scaling_segment_id
		AND l.age_band = b.age_band
		AND l.head_of_hhd = b.head_of_hhd
		AND l.hh_size = b.hh_size
		AND l.viewed_tv = b.viewed_tv
		AND l.age_band = p.ageband
		AND l.head_of_hhd = p.head_of_hhd
		AND l.hh_size = p.hh_size
		AND l.viewed_tv = p.viewed_tv

	COMMIT WORK

	UPDATE SC3I_weighting_working_table_sv
	SET sky_base_universe = sl.sky_base_universe
	FROM SC3I_weighting_working_table_sv
	JOIN SC3I_Segments_Lookup_vkuba AS sl ON SC3I_weighting_working_table_sv.scaling_segment_id = sl.scaling_segment_id

	COMMIT WORK

	SELECT scaling_segment_id
		,'panel_members' = count(1)
	INTO #segment_distribs
	FROM SC3I_Todays_panel_members_sv
	WHERE scaling_segment_id IS NOT NULL
	GROUP BY scaling_segment_id

	COMMIT WORK

	CREATE UNIQUE INDEX fake_pk ON #segment_distribs (scaling_segment_id)

	COMMIT WORK

	UPDATE SC3I_weighting_working_table_sv
	SET vespa_panel = sd.panel_members
	FROM SC3I_weighting_working_table_sv
	JOIN #segment_distribs AS sd ON SC3I_weighting_working_table_sv.scaling_segment_id = sd.scaling_segment_id

	COMMIT WORK

	DROP TABLE #segment_distribs

	COMMIT WORK

	DELETE
	FROM SC3I_category_subtotals_sv
	WHERE scaling_date = @scaling_day

	COMMIT WORK

	DELETE
	FROM SC3I_metrics_sv
	WHERE scaling_date = @scaling_day

	COMMIT WORK

	SET @cntr = 1
	SET @iteration = 0
	SET @cntr_var = 1

	COMMIT WORK

	SET @scaling_var = (
			SELECT scaling_variable
			FROM vespa_analysts.SC3I_Variables_lookup_v1_1
			WHERE id = @cntr
			)
	SET @scaling_count = (
			SELECT COUNT(scaling_variable)
			FROM vespa_analysts.SC3I_Variables_lookup_v1_1
			)

	COMMIT WORK

	UPDATE SC3I_weighting_working_table_sv
	SET vespa_panel = .000001
	WHERE vespa_panel = 0

	COMMIT WORK

	UPDATE SC3I_weighting_working_table_sv
	SET sum_of_weights = vespa_panel

	COMMIT WORK

	WHILE @cntr <= @scaling_count
	BEGIN
		TRUNCATE TABLE jsk01.SC3I_category_working_table_sv

		COMMIT WORK

		SET @cntr_var = 1

		COMMIT WORK

		WHILE @cntr_var <= @scaling_count
		BEGIN
			SELECT @scaling_var = scaling_variable
			FROM vespa_analysts.SC3I_Variables_lookup_v1_1
			WHERE id = @cntr_var

			MESSAGE cast(now() as timestamp)||' | Iterating over: '||@scaling_var  TO CLIENT
	
			COMMIT WORK

			EXECUTE (
					'INSERT INTO SC3I_category_working_table_sv '||
					'(sky_base_universe, profile,       value,       sky_base_accounts,       vespa_panel,       sum_of_weights) '||
					'SELECT srs.sky_base_universe,       @scaling_var,       ssl.' || @scaling_var || 
					',       SUM(srs.sky_base_accounts),       SUM(srs.vespa_panel),       SUM(srs.sum_of_weights) '||
					' FROM SC3I_weighting_working_table_sv AS srs '||
					' inner join      SC3I_Segments_Lookup_vkuba        AS      ssl             ON      srs.scaling_segment_id  =       ssl.scaling_segment_id '||
					' GROUP BY srs.sky_base_universe,       @scaling_var,       ssl.' || @scaling_var 
					)

			
			
			COMMIT WORK

			SET @cntr_var = @cntr_var + 1

			COMMIT WORK
		END

		COMMIT WORK

		UPDATE SC3I_category_working_table_sv
		SET category_weight = sky_base_accounts / sum_of_weights
			,convergence_flag = CASE 
				WHEN abs(sky_base_accounts - sum_of_weights) < 3
					THEN 0
				ELSE 1
				END

		MESSAGE cast(now() as timestamp)||' | Updating SC3I_category_working_table_sv '  TO CLIENT
		
		COMMIT WORK

		SELECT @convergence = SUM(convergence_flag)
		FROM SC3I_category_working_table_sv

		COMMIT WORK

		SET @iteration = @iteration + 1

		COMMIT WORK

		SELECT @scaling_var = scaling_variable
		FROM vespa_analysts.SC3I_Variables_lookup_v1_1
		WHERE id = @cntr

		COMMIt WORK

		EXECUTE(
				'UPDATE SC3I_weighting_working_table_sv '||
				'SET a.category_weight = sc.category_weight '|| 
					',a.sum_of_weights = a.sum_of_weights*sc.category_weight '||
				'FROM SC3I_weighting_working_table_sv AS a '||
				'inner join SC3I_Segments_Lookup_vkuba AS ssl ON a.scaling_segment_id = ssl.scaling_segment_id '||
				'inner join SC3I_category_working_table_sv AS sc ON sc.value = ssl.'||@scaling_var||
				' AND sc.sky_base_universe = ssl.sky_base_universe AND sc.profile = @scaling_var'
				)

		COMMIT WORK

		IF (
				@iteration = 100
				OR @convergence = 0
				)
			SET @cntr = @scaling_count + 1
		ELSE IF @cntr = @scaling_count
			SET @cntr = 1
		ELSE
			SET @cntr = @cntr + 1

		COMMIT WORK
	END

	MESSAGE cast(now() as timestamp)||' | End of iterations: '||@scaling_var  TO CLIENT
	
	COMMIT WORK

	SELECT @sky_base = SUM(sky_base_accounts)
		,@vespa_panel = SUM(vespa_panel)
		,@sum_of_weights = SUM(sum_of_weights)
	FROM SC3I_weighting_working_table_sv

	COMMIT WORK

	UPDATE SC3I_weighting_working_table_sv
	SET segment_weight = sum_of_weights / vespa_panel
		,indices_actual = 100 * (vespa_panel / @vespa_panel) / (sky_base_accounts / @sky_base)
		,indices_weighted = 100 * (sum_of_weights / @sum_of_weights) / (sky_base_accounts / @sky_base)

	MESSAGE cast(now() as timestamp)||' | Update 2 '  TO CLIENT
	COMMIT WORK

	UPDATE SC3I_weighting_working_table_sv
	SET segment_weight = .000001
	WHERE vespa_panel = .000001

	COMMIT WORK
	MESSAGE cast(now() as timestamp)||' | Update 3 '  TO CLIENT
	INSERT INTO SC3I_category_subtotals_sv (
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
		,CASE 
			WHEN abs(sky_base_accounts - sum_of_weights) > 3
				THEN 1
			ELSE 0
			END
	FROM SC3I_category_working_table_sv

	MESSAGE cast(now() as timestamp)||' | Inserting SC3I_category_working_table_sv' TO CLIENT
	
	COMMIT WORK

	INSERT INTO SC3I_metrics_sv (
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
		,sum(segment_weight * vespa_panel) / sum(vespa_panel)
		,SUM(segment_weight * vespa_panel)
		,@sky_base
		,sum(CASE 
				WHEN segment_weight >= .001
					THEN vespa_panel
				ELSE NULL
				END)
		,sum(CASE 
				WHEN segment_weight < .001
					THEN vespa_panel
				ELSE NULL
				END)
	FROM SC3I_weighting_working_table_sv

	MESSAGE cast(now() as timestamp)||' | Insert metrics'  TO CLIENT
	COMMIT WORK

	UPDATE SC3I_metrics_sv
	SET sum_of_convergence = abs(sky_base - sum_of_weights)

	COMMIT WORK

	INSERT INTO SC3I_non_convergences_sv (
		scaling_date
		,scaling_segment_id
		,difference
		)
	SELECT @scaling_day
		,scaling_segment_id
		,abs(sum_of_weights - sky_base_accounts)
	FROM SC3I_weighting_working_table_sv
	WHERE abs((segment_weight * vespa_panel) - sky_base_accounts) > 3

	COMMIT WORK

	MESSAGE cast(now() as timestamp)||' | Insert convergence'  TO CLIENT

	IF (
			SELECT count(1)
			FROM SC3I_Weightings_sv
			WHERE scaling_day = @scaling_day
			) > 0
	BEGIN
		DELETE
		FROM SC3I_Weightings_sv
		WHERE scaling_day = @scaling_day

		DELETE
		FROM SC3I_Intervals_sv
		WHERE reporting_starts = @scaling_day

		UPDATE SC3I_Intervals_sv
		SET reporting_ends = dateadd(day, - 1, @scaling_day)
		WHERE reporting_ends >= @scaling_day
	END

	COMMIT WORK

	INSERT INTO SC3I_Weightings_sv
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
	FROM SC3I_weighting_working_table_sv

	MESSAGE cast(now() as timestamp)||' | Update '  TO CLIENT
	COMMIT WORK

	SELECT 
		scaling_segment_id = b.new_seg
	, sky_base_accounts = SUM(a.sky_base_accounts)
	, vespa_panel 		= SUM(a.vespa_panel)
	, sum_of_weights   	= SUM(CASE WHEN a.vespa_panel < 0.001 THEN 0.000001 ELSE a.sum_of_weights END)
	, segment_weight    = CASE WHEN sum_of_weights < 0.001 THEN 0.00001 ELSE sum_of_weights  / vespa_panel END
	INTO SC3I_weighting_working_table_2_sv 
	FROM SC3I_weighting_working_table_sv 	AS a 
	JOIN SC3I_Segments_Lookup_vkuba	AS b ON a.scaling_segment_id = b.scaling_segment_id
	GROUP BY scaling_segment_id
	
	COMMIT 
	
	SELECT @scaling_day AS scaling_day
		,scaling_segment_id
		,vespa_panel
		,sky_base_accounts
		,segment_weight
		,sum_of_weights
		,CONVERGENCE = CASE WHEN abs(sky_base_accounts - sum_of_weights) > 3 THEN 1 ELSE 0 END
	INTO SC3I_Weightings_2_sv
	FROM SC3I_weighting_working_table_2_sv

	
	------------------------------ FINAL OUTPUT
	TRUNCATE TABLE jsk01.V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING_sv

	COMMIT WORK

	INSERT INTO V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING_sv (
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
	FROM SC3I_Todays_panel_members_sv AS tpm
	JOIN SC3I_Segments_Lookup_vkuba	AS b ON tpm.scaling_segment_id = b.scaling_segment_id
	JOIN SC3I_weighting_working_table_2_sv AS wwt ON b.new_seg = wwt.scaling_segment_id

	COMMIT WORK
END
