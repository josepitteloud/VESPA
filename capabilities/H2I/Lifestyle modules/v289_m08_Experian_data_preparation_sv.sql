create or replace procedure ${SQLFILE_ARG001}.v289_m08_Experian_data_preparation_sv
AS
BEGIN
	message convert(TIMESTAMP, now()) || ' | Begining M08.0 - Initialising Environment' TO client

	DECLARE @last_update_dt DATE
	DECLARE @event_date DATE
	DECLARE @female_probability DECIMAL(8, 6) -- approx this percentage of head of hhds selected will be female
	COMMIT WORK

	SELECT @last_update_dt = max(updated_on)
	FROM V289_M08_SKY_HH_view_sv -- '2000-01-01' commit

	COMMIT WORK

	SELECT @event_date = max(date_of_activity_db1)
	FROM barb_weights_sv

	SET @female_probability = .8

	COMMIT WORK

	IF ( datediff(day, @last_update_dt, today()) > 6 OR @last_update_dt IS NULL )
	BEGIN
		
		message convert(TIMESTAMP, now()) || ' | @ M08.0: Initialising Environment DONE' TO client message convert(TIMESTAMP, now()) || ' | Begining M08.1 - Account extraction from SAV' TO client

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = '${SQLFILE_ARG001}'
					AND upper(tname) = upper('V289_M08_SKY_HH_view_sv')
					AND tabletype = 'TABLE'
				)
			TRUNCATE TABLE ${SQLFILE_ARG001}.V289_M08_SKY_HH_view_sv

		COMMIT WORK

		INSERT INTO V289_M08_SKY_HH_view_sv (
			account_number
			,cb_key_household
			,cb_address_line_1
			,panel_flag
			)
		SELECT DISTINCT sav.account_number
			,sav.cb_key_household
			,sav.cb_address_line_1
			,'panel_flag' = CASE 
				WHEN viq.account_number IS NOT NULL
					THEN 1
				ELSE 0
				END
		INTO V289_M08_SKY_HH_view_sv
		FROM cust_subs_hist AS csh
		JOIN CUST_SINGLE_ACCOUNT_VIEW AS sav ON sav.account_number = csh.account_number
		LEFT OUTER JOIN (
			SELECT DISTINCT account_number
			FROM VIQ_VIEWING_DATA_SCALING
			WHERE adjusted_event_start_date_vespa = @event_date
			) AS viq ON sav.account_number = viq.account_number
		WHERE subscription_sub_type IN ('DTV Primary Viewing')
			AND csh.status_code IN (
				'AC'
				,'AB'
				,'PC'
				)
			AND csh.effective_from_dt <= @event_date
			AND csh.effective_to_dt >= @event_date
			AND csh.effective_from_dt <> csh.effective_to_dt
			AND csh.EFFECTIVE_FROM_DT IS NOT NULL
			AND csh.cb_key_household > 0
			AND csh.cb_key_household IS NOT NULL
			AND csh.cb_key_individual IS NOT NULL
			AND csh.account_number IS NOT NULL
			AND csh.service_instance_id IS NOT NULL

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M08.1 TABLE V289_M08_SKY_HH_view_sv Populated' TO client
			--------------------------------------------------------------------------------------------------
			-- M08.2 - Experian HH Info Extraction (1st round - Only hh_key and address line matching accounts)
			--------------------------------------------------------------------------------------------------
			message convert(TIMESTAMP, now()) || ' | Begining M08.2 - Experian HH Info Extraction' TO client

		SELECT account_number
			,vh.cb_key_household
			,vh.cb_address_line_1
			,'HH_composition' = COUNT(DISTINCT ex.cb_key_db_person) + MAX(convert(INTEGER, h_number_of_children_in_household_2011))
			,'Children_count' = MAX(convert(INTEGER, h_number_of_children_in_household_2011))
			,'h_0_4_flag' = MAX(CASE h_presence_of_child_aged_0_4_2011
					WHEN '1'
						THEN 1
					ELSE 0
					END)
			,'h_5_11_flag' = MAX(CASE h_presence_of_child_aged_12_17_2011
					WHEN '1'
						THEN 1
					ELSE 0
					END)
			,'h_12_17_flag' = MAX(CASE h_presence_of_child_aged_5_11_2011
					WHEN '1'
						THEN 1
					ELSE 0
					END)
		INTO #t1
		FROM V289_M08_SKY_HH_view_sv AS vh
		JOIN EXPERIAN_CONSUMERVIEW AS ex ON ex.cb_key_household = vh.cb_key_household
			AND ex.cb_address_line_1 = vh.cb_address_line_1
		GROUP BY account_number
			,vh.cb_key_household
			,vh.cb_address_line_1

		COMMIT WORK

		CREATE hg INDEX idhh ON #t1 (cb_key_household)

		COMMIT WORK

		CREATE hg INDEX idac ON #t1 (account_number)

		COMMIT WORK

		CREATE hg INDEX idal ON #t1 (cb_address_line_1)

		COMMIT WORK

		COMMIT WORK

		---------------------   Table Update
		UPDATE V289_M08_SKY_HH_view_sv AS a
		SET a.Children_count = b.Children_count
			,a.HH_composition = b.HH_composition
			,a.non_matching_flag = 1
			,a.h_0_4_flag = COALESCE(b.h_0_4_flag, 0)
			,a.h_5_11_flag = COALESCE(b.h_5_11_flag, 0)
			,a.h_12_17_flag = COALESCE(b.h_12_17_flag, 0)
		FROM V289_M08_SKY_HH_view_sv AS a
		JOIN #t1 AS b ON a.account_number = b.account_number
			AND a.cb_key_household = b.cb_key_household
			AND a.cb_address_line_1 = b.cb_address_line_1

		COMMIT WORK

		-- Clean up
		DROP TABLE #t1

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M08.2 1st round finished ' TO client
			----------------------------------------------------------------------------------------------------------
			-- M08.3 - Experian HH Info Extraction (2nd round - Non-matching address line accounts AND hh > 10 people)
			----------------------------------------------------------------------------------------------------------
			message convert(TIMESTAMP, now()) || ' | Begining M08.3 - Experian HH Info Extraction (2nd round)' TO client

		SELECT vh.account_number
			,vh.cb_key_household
			,vh.cb_address_line_1
			,'linex' = ex.cb_address_line_1
			,'HH_composition' = COUNT(DISTINCT ex.cb_key_db_person) + MAX(convert(INTEGER, h_number_of_children_in_household_2011))
			,'Children_count' = MAX(convert(INTEGER, h_number_of_children_in_household_2011))
			,'rank_1' = RANK() OVER (
				PARTITION BY vh.cb_key_household ORDER BY HH_composition DESC
				)
			,'h_0_4_flag' = MAX(CASE h_presence_of_child_aged_0_4_2011
					WHEN '1'
						THEN 1
					ELSE 0
					END)
			,'h_5_11_flag' = MAX(CASE h_presence_of_child_aged_12_17_2011
					WHEN '1'
						THEN 1
					ELSE 0
					END)
			,'h_12_17_flag' = MAX(CASE h_presence_of_child_aged_5_11_2011
					WHEN '1'
						THEN 1
					ELSE 0
					END)
		INTO #t2
		FROM V289_M08_SKY_HH_view_sv AS vh
		JOIN EXPERIAN_CONSUMERVIEW AS ex ON ex.cb_key_household = vh.cb_key_household
		WHERE (vh.non_matching_flag = 0)
		GROUP BY vh.account_number
			,vh.cb_key_household
			,vh.cb_address_line_1
			,linex
		HAVING HH_composition <= 10

		COMMIT WORK

		CREATE hg INDEX idhh ON #t2 (cb_key_household)

		COMMIT WORK

		CREATE hg INDEX idac ON #t2 (account_number)

		COMMIT WORK

		CREATE hg INDEX idal ON #t2 (cb_address_line_1)

		COMMIT WORK

		COMMIT WORK

		---------------------   Table Update
		UPDATE V289_M08_SKY_HH_view_sv AS a
		SET a.Children_count = b.Children_count
			,a.HH_composition = b.HH_composition
			,a.cb_address_line_1 = b.linex
			,a.non_matching_flag = 1
			,a.edited_add_flag = 1
			,a.h_0_4_flag = COALESCE(b.h_0_4_flag, 0)
			,a.h_5_11_flag = COALESCE(b.h_5_11_flag, 0)
			,a.h_12_17_flag = COALESCE(b.h_12_17_flag, 0)
		FROM V289_M08_SKY_HH_view_sv AS a
		JOIN #t2 AS b ON a.account_number = b.account_number
			AND a.cb_key_household = b.cb_key_household
			AND a.cb_address_line_1 = b.cb_address_line_1
			AND rank_1 = 1

		COMMIT WORK

		-- Clean up
		DROP TABLE #t2

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M08.3 2nd round finished ' TO client
			-----------------------------------------------------------------------------------------
			-- M08.4 - Experian HH Info Extraction (3nd round - Non-matching address line accounts A)
			-----------------------------------------------------------------------------------------
			message convert(TIMESTAMP, now()) || ' | Begining M08.4 - Experian HH Info Extraction (3nd round)' TO client

		SELECT vh.account_number
			,vh.cb_key_household
			,vh.cb_address_line_1
			,'linex' = ex.cb_address_line_1
			,'HH_composition' = COUNT(DISTINCT ex.cb_key_db_person) + MAX(convert(INTEGER, h_number_of_children_in_household_2011))
			,'Children_count' = MAX(convert(INTEGER, h_number_of_children_in_household_2011))
			,'rank_1' = RANK() OVER (
				PARTITION BY vh.cb_key_household ORDER BY HH_composition ASC
				)
			,'h_0_4_flag' = MAX(CASE h_presence_of_child_aged_0_4_2011
					WHEN '1'
						THEN 1
					ELSE 0
					END)
			,'h_5_11_flag' = MAX(CASE h_presence_of_child_aged_12_17_2011
					WHEN '1'
						THEN 1
					ELSE 0
					END)
			,'h_12_17_flag' = MAX(CASE h_presence_of_child_aged_5_11_2011
					WHEN '1'
						THEN 1
					ELSE 0
					END)
		INTO #t3
		FROM V289_M08_SKY_HH_view_sv AS vh
		JOIN EXPERIAN_CONSUMERVIEW AS ex ON ex.cb_key_household = vh.cb_key_household
		WHERE (vh.non_matching_flag = 0)
		GROUP BY vh.account_number
			,vh.cb_key_household
			,vh.cb_address_line_1
			,linex

		COMMIT WORK

		CREATE hg INDEX idhh ON #t3 (cb_key_household)

		COMMIT WORK

		CREATE hg INDEX idac ON #t3 (account_number)

		COMMIT WORK

		CREATE hg INDEX idal ON #t3 (cb_address_line_1)

		COMMIT WORK

		COMMIT WORK

		---------------------   Table Update
		UPDATE V289_M08_SKY_HH_view_sv AS a
		SET a.Children_count = b.Children_count
			,a.HH_composition = b.HH_composition
			,a.cb_address_line_1 = b.linex
			,a.non_matching_flag = 1
			,a.edited_add_flag = 1
			,a.h_0_4_flag = COALESCE(b.h_0_4_flag, 0)
			,a.h_5_11_flag = COALESCE(b.h_5_11_flag, 0)
			,a.h_12_17_flag = COALESCE(b.h_12_17_flag, 0)
		FROM V289_M08_SKY_HH_view_sv AS a
		JOIN #t3 AS b ON a.account_number = b.account_number
			AND a.cb_key_household = b.cb_key_household
			AND a.cb_address_line_1 = b.cb_address_line_1
			AND rank_1 = 1

		COMMIT WORK

		-- Clean up
		DROP TABLE #t3

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M08.4 3rd round finished ' TO client
			--------------------------------------
			-- M08.5 - Individual TABLE POPULATION
			--------------------------------------
			message convert(TIMESTAMP, now()) || ' | Begining M08.5 - Individual TABLE POPULATION' TO client

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = '${SQLFILE_ARG001}'
					AND upper(tname) = upper('V289_M08_SKY_HH_composition_sv')
					AND tabletype = 'TABLE'
				)
			TRUNCATE TABLE ${SQLFILE_ARG001}.V289_M08_SKY_HH_composition_sv

		INSERT INTO V289_M08_SKY_HH_composition_sv (
			account_number
			,cb_key_household
			,exp_cb_key_db_person
			,cb_address_line_1
			,cb_key_db_person
			,person_age
			,person_ageband
			,HH_person_number
			,person_gender
			,person_income
			,demographic_ID
			,panel_flag
			)
		SELECT vh.account_number
			,vh.cb_key_household
			,ex.exp_cb_key_db_person
			,vh.cb_address_line_1
			,ex.cb_key_db_person
			,'person_age' = ex.p_actual_age
			,'person_ageband' = CASE 
				WHEN person_age <= 19
					THEN '0-19'
				WHEN person_age BETWEEN 20
						AND 44
					THEN '20-44'
				WHEN person_age >= 45
					THEN '45+'
				END
			,'HH_person_number' = RANK() OVER (
				PARTITION BY vh.account_number ORDER BY person_age ASC
					,p_gender ASC
					,ex.cb_key_db_person ASC
				)
			,'person_gender' = CASE 
				WHEN ex.p_gender = '0'
					THEN 'M'
				WHEN ex.p_gender = '1'
					THEN 'F'
				ELSE 'U'
				END
			,'person_income' = ex.p_personal_income_value
			,'demographic_ID' = CASE 
				WHEN p_gender = '0'
					AND p_actual_age <= 19
					THEN 7
				WHEN p_gender = '0'
					AND p_actual_age BETWEEN 20
						AND 24
					THEN 6
				WHEN p_gender = '0'
					AND p_actual_age BETWEEN 25
						AND 34
					THEN 5
				WHEN p_gender = '0'
					AND p_actual_age BETWEEN 35
						AND 44
					THEN 4
				WHEN p_gender = '0'
					AND p_actual_age BETWEEN 45
						AND 64
					THEN 3
				WHEN p_gender = '0'
					AND p_actual_age >= 65
					THEN 2
						---------- FEMALES
				WHEN p_gender = '1'
					AND p_actual_age <= 19
					THEN 14
				WHEN p_gender = '1'
					AND p_actual_age BETWEEN 20
						AND 24
					THEN 13
				WHEN p_gender = '1'
					AND p_actual_age BETWEEN 25
						AND 34
					THEN 12
				WHEN p_gender = '1'
					AND p_actual_age BETWEEN 35
						AND 44
					THEN 11
				WHEN p_gender = '1'
					AND p_actual_age BETWEEN 45
						AND 64
					THEN 10
				WHEN p_gender = '1'
					AND p_actual_age >= 65
					THEN 9
						---------- UNDEFINED GENDER
				WHEN p_gender = 'U'
					AND p_actual_age <= 19
					THEN 15
				WHEN p_gender = 'U'
					AND p_actual_age BETWEEN 20
						AND 24
					THEN 16
				WHEN p_gender = 'U'
					AND p_actual_age BETWEEN 25
						AND 34
					THEN 17
				WHEN p_gender = 'U'
					AND p_actual_age BETWEEN 35
						AND 44
					THEN 18
				WHEN p_gender = 'U'
					AND p_actual_age BETWEEN 45
						AND 64
					THEN 19
				WHEN p_gender = 'U'
					AND p_actual_age >= 65
					THEN 20
						---------- UNDEFINED AGE
				WHEN p_gender = '1'
					AND p_actual_age IS NULL
					THEN 21
				WHEN p_gender = '0'
					AND p_actual_age IS NULL
					THEN 22
						---------- UNDIFINED ALL
				WHEN p_gender = 'U'
					AND p_actual_age IS NULL
					THEN 23
				ELSE 0
				END
			,panel_flag
		FROM V289_M08_SKY_HH_view_sv AS vh
		JOIN EXPERIAN_CONSUMERVIEW AS ex ON ex.cb_key_household = vh.cb_key_household
			AND ex.cb_address_line_1 = vh.cb_address_line_1

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M08.5 Individual table populated' TO client
			--------------------------------
			-- M08.6 - Add Head of Household
			--------------------------------
			message convert(TIMESTAMP, now()) || ' | Begining M08.6 - Add Head of Household' TO client

		--------        Get Experian Head of Household
		UPDATE V289_M08_SKY_HH_composition_sv AS s
		SET exp_person_head = p_head_of_household
		FROM sk_prod.PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD AS e
		WHERE s.exp_cb_key_db_person = e.exp_cb_key_db_person

		COMMIT WORK

		---------------------------------------------------------------------------------
		--- Based upon Experian Head of hhd select a single head of hhd for each hhd
		--- A hhd is defined by cb_key_household and cb_address_line_1
		--- Experian generally assigns BOTH a male and a female as head of hhd
		--- We need to select ONE. This is done based upon highest personal income by gender
		--- Then selecting a random male or female based upon a probability assigned to female
		---------------------------------------------------------------------------------
		-- Identify highest personal income from indiviuals in a hhd who are head of hhd - by gender
		-- Also assign a probabity for selecting either experian head of hhd based upon gender
		SELECT account_number
			,exp_cb_key_db_person
			,cb_key_household
			,cb_address_line_1
			,'rank_1' = rank() OVER (
				PARTITION BY account_number
				,cb_key_household
				,cb_address_line_1
				,person_gender ORDER BY person_income DESC
					,exp_cb_key_db_person DESC
				)
			,'probability' = CASE 
				WHEN person_gender = 'F'
					THEN @female_probability
				ELSE 1 - @female_probability
				END
		INTO #a1_1
		FROM V289_M08_SKY_HH_composition_sv
		WHERE exp_person_head = 1

		COMMIT WORK

		CREATE hg INDEX ind0 ON #a1_1 (account_number)

		CREATE hg INDEX ind1 ON #a1_1 (exp_cb_key_db_person)

		CREATE hg INDEX ind2 ON #a1_1 (cb_key_household)

		CREATE hg INDEX ind3 ON #a1_1 (cb_address_line_1)

		COMMIT WORK

		SELECT account_number
			,cb_key_household
			,cb_address_line_1
			,'tot_probability' = sum(probability)
		INTO #a1_2
		FROM #a1_1
		WHERE rank_1 = 1
		GROUP BY account_number
			,cb_key_household
			,cb_address_line_1

		COMMIT WORK
		CREATE hg INDEX ind0 ON #a1_2 (account_number)
		CREATE hg INDEX ind1 ON #a1_2 (cb_key_household)
		CREATE hg INDEX ind2 ON #a1_2 (cb_address_line_1)
		COMMIT WORK

		-- For each experian head of household calculate bounds to apply probability to
		SELECT #a1_1.exp_cb_key_db_person
			,#a1_1.account_number
			,#a1_1.cb_key_household
			,#a1_1.cb_address_line_1
			,rank_1
			,'low_limit' = CASE 
				WHEN probability / tot_probability > .5
					THEN 0.0
				ELSE convert(DECIMAL(8, 6), 1 - probability / tot_probability)
				END
			,'high_limit' = CASE 
				WHEN probability / tot_probability > .5
					THEN convert(DECIMAL(8, 6), probability / tot_probability)
				ELSE 1.0
				END
		INTO #a1_3
		FROM #a1_1
		JOIN #a1_2 ON #a1_1.cb_key_household = #a1_2.cb_key_household
			AND #a1_1.cb_address_line_1 = #a1_2.cb_address_line_1
			AND #a1_1.account_number = #a1_2.account_number
		WHERE rank_1 = 1

		COMMIT WORK
		CREATE hg INDEX ind1 ON #a1_3 (exp_cb_key_db_person)
		CREATE hg INDEX ind2 ON #a1_3 (cb_key_household)
		CREATE hg INDEX ind3 ON #a1_3 (cb_address_line_1)
		CREATE hg INDEX ind4 ON #a1_3 (account_number)
		COMMIT WORK

		-- Generate a random number per hhd
		SELECT DISTINCT cb_key_household
			,cb_address_line_1
			,'random_number' = .000001
		INTO #r1
		FROM V289_M08_SKY_HH_composition_sv

		COMMIT WORK

		UPDATE #r1
		SET random_number = RAND(cb_key_household + DATEPART(us, GETDATE()))

		COMMIT WORK

		CREATE hg INDEX ind1 ON #r1 (cb_key_household)

		CREATE hg INDEX ind2 ON #r1 (cb_address_line_1)

		COMMIT WORK

		-- Assign a single individual in each hhd as head of hhd based upon above
		UPDATE V289_M08_SKY_HH_composition_sv AS e
		SET person_head = '1'
		FROM #a1_3 AS a
			,#r1 AS r
		WHERE e.exp_cb_key_db_person = a.exp_cb_key_db_person
			AND a.rank_1 = 1
			AND e.cb_key_household = r.cb_key_household
			AND e.cb_address_line_1 = r.cb_address_line_1
			AND random_number >= low_limit
			AND random_number < high_limit

		COMMIT WORK

		DROP TABLE #a1_1

		DROP TABLE #a1_2

		DROP TABLE #a1_3

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M08.6 Head of household added where Experian head exists' TO client

		--- Not all hhds have a defined head of hhd from Experian. So will assign highest personal income in these cases
		-- First count number of heads of hhd as per our definition for each hhd
		SELECT account_number
			,cb_key_household
			,cb_address_line_1
			,'head_count' = sum(CASE 
					WHEN person_head = '1'
						THEN 1
					ELSE 0
					END)
		INTO #b1
		FROM V289_M08_SKY_HH_composition_sv
		GROUP BY account_number
			,cb_key_household
			,cb_address_line_1

		COMMIT WORK

		CREATE hg INDEX ind1 ON #b1 (cb_key_household)

		CREATE hg INDEX ind2 ON #b1 (cb_address_line_1)

		CREATE lf INDEX ind3 ON #b1 (head_count)

		CREATE hg INDEX ind4 ON #b1 (account_number)

		COMMIT WORK

		-- Those hhds where above is zero need to be allocated individual with highest income by gender
		SELECT p.exp_cb_key_db_person
			,p.account_number
			,p.cb_key_household
			,p.cb_address_line_1
			,'rank_1' = rank() OVER (
				PARTITION BY p.account_number
				,p.cb_key_household
				,p.cb_address_line_1
				,person_gender ORDER BY p.person_income DESC
					,p.exp_cb_key_db_person DESC
				)
			,'probability' = CASE 
				WHEN person_gender = 'F'
					THEN @female_probability
				ELSE 1 - @female_probability
				END
		INTO #b1_1
		FROM V289_M08_SKY_HH_composition_sv AS p
		JOIN #b1 AS b ON p.cb_key_household = b.cb_key_household
			AND p.cb_address_line_1 = b.cb_address_line_1
		WHERE b.head_count = 0

		COMMIT WORK

		CREATE hg INDEX ind1 ON #b1_1 (exp_cb_key_db_person)

		COMMIT WORK

		-- Calulate total probabilty by hhd
		SELECT account_number
			,cb_key_household
			,cb_address_line_1
			,'tot_probability' = sum(probability)
		INTO #b1_2
		FROM #b1_1
		WHERE rank_1 = 1
		GROUP BY account_number
			,cb_key_household
			,cb_address_line_1

		COMMIT WORK

		CREATE hg INDEX ind1 ON #b1_2 (cb_key_household)

		CREATE hg INDEX ind2 ON #b1_2 (cb_address_line_1)

		CREATE hg INDEX ind3 ON #b1_2 (account_number)

		COMMIT WORK

		-- Calculate lower and upper bounds for each potential head of hhd individual
		SELECT #b1_1.exp_cb_key_db_person
			,#b1_1.account_number
			,#b1_1.cb_key_household
			,#b1_1.cb_address_line_1
			,rank_1
			,'low_limit' = CASE 
				WHEN probability / tot_probability > .5
					THEN 0.0
				ELSE convert(DECIMAL(8, 6), 1 - probability / tot_probability)
				END
			,'high_limit' = CASE 
				WHEN probability / tot_probability > .5
					THEN convert(DECIMAL(8, 6), probability / tot_probability)
				ELSE 1.0
				END
		INTO #b1_3
		FROM #b1_1
		JOIN #b1_2 ON #b1_1.cb_key_household = #b1_2.cb_key_household
			AND #b1_1.cb_address_line_1 = #b1_2.cb_address_line_1
			AND #b1_1.account_number = #b1_2.account_number
		WHERE rank_1 = 1

		COMMIT WORK

		CREATE hg INDEX ind1 ON #b1_3 (exp_cb_key_db_person)

		CREATE hg INDEX ind2 ON #b1_3 (cb_key_household)

		CREATE hg INDEX ind3 ON #b1_3 (cb_address_line_1)

		CREATE hg INDEX ind4 ON #b1_3 (account_number)

		COMMIT WORK

		-- Assign individual as head of hhd
		UPDATE V289_M08_SKY_HH_composition_sv AS e
		SET person_head = '1'
		FROM #b1_3 AS b
			,#r1 AS r
		WHERE e.exp_cb_key_db_person = b.exp_cb_key_db_person
			AND b.rank_1 = 1
			AND e.cb_key_household = r.cb_key_household
			AND e.cb_address_line_1 = r.cb_address_line_1
			AND random_number >= low_limit
			AND random_number < high_limit

		COMMIT WORK

		DROP TABLE #r1

		DROP TABLE #b1

		DROP TABLE #b1_1

		DROP TABLE #b1_2

		DROP TABLE #b1_3

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M08.6 Head of household added' TO client
			----------------------------------
			-- M08.7 - Add Individual Children
			----------------------------------
			message convert(TIMESTAMP, now()) || ' | Begining M08.7 - Add Individual Children' TO client

		-- Experian tables do not have individual data for children less than 17
		---- Need to append rows for these
		--- They cannot be head of hhd either so can be run after that code
		-- Will need to add a row for each child, these multiple rows in this table will enable
		-- the right number of individuals to be added to the data
		SELECT 'number_of_kids' = 1
			,'unique_row' = 1
		INTO #PIV_append_kids_rows

		COMMIT WORK

		CREATE lf INDEX ind1 ON #PIV_append_kids_rows (number_of_kids)

		COMMIT WORK

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			2
			,2
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			2
			,3
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			3
			,4
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			3
			,5
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			3
			,6
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			4
			,7
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			4
			,8
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			4
			,9
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			4
			,10
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			5
			,11
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			5
			,12
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			5
			,13
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			5
			,14
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			5
			,15
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			6
			,16
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			6
			,17
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			6
			,18
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			6
			,19
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			6
			,20
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			6
			,21
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			7
			,22
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			7
			,23
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			7
			,24
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			7
			,25
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			7
			,26
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			7
			,27
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			7
			,28
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			8
			,29
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			8
			,30
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			8
			,31
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			8
			,32
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			8
			,33
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			8
			,34
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			8
			,35
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			8
			,36
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			9
			,37
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			9
			,38
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			9
			,39
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			9
			,40
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			9
			,41
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			9
			,42
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			9
			,43
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			9
			,44
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			9
			,45
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			10
			,46
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			10
			,47
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			10
			,48
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			10
			,49
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			10
			,50
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			10
			,51
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			10
			,52
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			10
			,53
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			10
			,54
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			10
			,55
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			11
			,56
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			11
			,57
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			11
			,58
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			11
			,59
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			11
			,60
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			11
			,61
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			11
			,62
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			11
			,63
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			11
			,64
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			11
			,65
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			11
			,66
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			12
			,67
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			12
			,68
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			12
			,69
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			12
			,70
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			12
			,71
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			12
			,72
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			12
			,73
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			12
			,74
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			12
			,75
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			12
			,76
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			12
			,77
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			12
			,78
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			13
			,79
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			13
			,80
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			13
			,81
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			13
			,82
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			13
			,83
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			13
			,84
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			13
			,85
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			13
			,86
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			13
			,87
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			13
			,88
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			13
			,89
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			13
			,90
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			13
			,91
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			14
			,92
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			14
			,93
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			14
			,94
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			14
			,95
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			14
			,96
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			14
			,97
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			14
			,98
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			14
			,99
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			14
			,100
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			14
			,101
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			14
			,102
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			14
			,103
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			14
			,104
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			14
			,105
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			15
			,106
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			15
			,107
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			15
			,108
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			15
			,109
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			15
			,110
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			15
			,111
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			15
			,112
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			15
			,113
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			15
			,114
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			15
			,115
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			15
			,116
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			15
			,117
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			15
			,118
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			15
			,119
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			15
			,120
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			16
			,121
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			16
			,122
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			16
			,123
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			16
			,124
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			16
			,125
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			16
			,126
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			16
			,127
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			16
			,128
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			16
			,129
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			16
			,130
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			16
			,131
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			16
			,132
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			16
			,133
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			16
			,134
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			16
			,135
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			16
			,136
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			17
			,137
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			17
			,138
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			17
			,139
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			17
			,140
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			17
			,141
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			17
			,142
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			17
			,143
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			17
			,144
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			17
			,145
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			17
			,146
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			17
			,147
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			17
			,148
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			17
			,149
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			17
			,150
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			17
			,151
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			17
			,152
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			17
			,153
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			18
			,154
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			18
			,155
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			18
			,156
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			18
			,157
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			18
			,158
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			18
			,159
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			18
			,160
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			18
			,161
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			18
			,162
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			18
			,163
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			18
			,164
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			18
			,165
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			18
			,166
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			18
			,167
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			18
			,168
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			18
			,169
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			18
			,170
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			18
			,171
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			19
			,172
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			19
			,173
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			19
			,174
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			19
			,175
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			19
			,176
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			19
			,177
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			19
			,178
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			19
			,179
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			19
			,180
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			19
			,181
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			19
			,182
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			19
			,183
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			19
			,184
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			19
			,185
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			19
			,186
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			19
			,187
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			19
			,188
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			19
			,189
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			19
			,190
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			20
			,191
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			20
			,192
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			20
			,193
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			20
			,194
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			20
			,195
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			20
			,196
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			20
			,197
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			20
			,198
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			20
			,199
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			20
			,200
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			20
			,201
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			20
			,202
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			20
			,203
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			20
			,204
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			20
			,205
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			20
			,206
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			20
			,207
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			20
			,208
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			20
			,209
			)

		INSERT INTO #PIV_append_kids_rows
		VALUES (
			20
			,210
			)

		COMMIT WORK

		INSERT INTO V289_M08_SKY_HH_composition_sv (
			account_number
			,cb_key_household
			,cb_address_line_1
			,person_gender
			,person_ageband
			,demographic_ID
			,randd
			,panel_flag
			)
		SELECT hh.account_number
			,hh.cb_key_household
			,hh.cb_address_line_1
			,'U'
			,CASE 
				WHEN hh.h_12_17_flag = 0
					AND (
						hh.h_0_4_flag = 1
						OR hh.h_5_11_flag = 1
						)
					THEN '0-11'
				WHEN hh.h_12_17_flag = 1
					AND (
						hh.h_0_4_flag = 0
						AND hh.h_5_11_flag = 0
						)
					THEN '12-19'
				WHEN hh.children_count = 2
					AND hh.h_12_17_flag = 1
					AND (
						hh.h_0_4_flag = 1
						OR hh.h_5_11_flag = 1
						)
					AND unique_row = 2
					THEN '0-11'
				WHEN hh.children_count = 2
					AND hh.h_12_17_flag = 1
					AND (
						hh.h_0_4_flag = 1
						OR hh.h_5_11_flag = 1
						)
					AND unique_row = 3
					THEN '12-19'
				WHEN hh.children_count = 3
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 4
					THEN '0-11'
				WHEN hh.children_count = 3
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 5
					THEN '12-19'
				WHEN hh.children_count = 3
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 6
					THEN '0-11'
				WHEN hh.children_count = 3
					AND hh.h_12_17_flag = 1
					AND (
						hh.h_0_4_flag = 1
						OR hh.h_5_11_flag = 1
						)
					AND unique_row = 4
					THEN '0-11'
				WHEN hh.children_count = 3
					AND hh.h_12_17_flag = 1
					AND (
						hh.h_0_4_flag = 1
						OR hh.h_5_11_flag = 1
						)
					AND unique_row = 5
					THEN '12-19'
				WHEN hh.children_count = 4
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 7
					THEN '0-11'
				WHEN hh.children_count = 4
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 8
					THEN '12-19'
				WHEN hh.children_count = 4
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 9
					THEN '0-11'
				WHEN hh.children_count = 4
					AND hh.h_12_17_flag = 1
					AND (
						hh.h_0_4_flag = 1
						OR hh.h_5_11_flag = 1
						)
					AND unique_row = 7
					THEN '0-11'
				WHEN hh.children_count = 4
					AND hh.h_12_17_flag = 1
					AND (
						hh.h_0_4_flag = 1
						OR hh.h_5_11_flag = 1
						)
					AND unique_row = 8
					THEN '12-19'
				WHEN hh.children_count = 5
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 10
					THEN '0-11'
				WHEN hh.children_count = 5
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 11
					THEN '12-19'
				WHEN hh.children_count = 5
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 12
					THEN '0-11'
				WHEN hh.children_count = 5
					AND hh.h_12_17_flag = 1
					AND (
						hh.h_0_4_flag = 1
						OR hh.h_5_11_flag = 1
						)
					AND unique_row = 10
					THEN '0-11'
				WHEN hh.children_count = 5
					AND hh.h_12_17_flag = 1
					AND (
						hh.h_0_4_flag = 1
						OR hh.h_5_11_flag = 1
						)
					AND unique_row = 11
					THEN '12-19'
				WHEN hh.children_count = 6
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 16
					THEN '0-11'
				WHEN hh.children_count = 6
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 17
					THEN '12-19'
				WHEN hh.children_count = 6
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 18
					THEN '0-11'
				WHEN hh.children_count = 6
					AND hh.h_12_17_flag = 1
					AND (
						hh.h_0_4_flag = 1
						OR hh.h_5_11_flag = 1
						)
					AND unique_row = 16
					THEN '0-11'
				WHEN hh.children_count = 6
					AND hh.h_12_17_flag = 1
					AND (
						hh.h_0_4_flag = 1
						OR hh.h_5_11_flag = 1
						)
					AND unique_row = 17
					THEN '12-19'
				WHEN hh.children_count = 7
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 22
					THEN '0-11'
				WHEN hh.children_count = 7
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 23
					THEN '12-19'
				WHEN hh.children_count = 7
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 24
					THEN '0-11'
				WHEN hh.children_count = 7
					AND hh.h_12_17_flag = 1
					AND (
						hh.h_0_4_flag = 1
						OR hh.h_5_11_flag = 1
						)
					AND unique_row = 22
					THEN '0-11'
				WHEN hh.children_count = 7
					AND hh.h_12_17_flag = 1
					AND (
						hh.h_0_4_flag = 1
						OR hh.h_5_11_flag = 1
						)
					AND unique_row = 23
					THEN '12-19'
				WHEN hh.children_count = 8
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 29
					THEN '0-11'
				WHEN hh.children_count = 8
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 30
					THEN '12-19'
				WHEN hh.children_count = 8
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 31
					THEN '0-11'
				WHEN hh.children_count = 8
					AND hh.h_12_17_flag = 1
					AND (
						hh.h_0_4_flag = 1
						OR hh.h_5_11_flag = 1
						)
					AND unique_row = 29
					THEN '0-11'
				WHEN hh.children_count = 8
					AND hh.h_12_17_flag = 1
					AND (
						hh.h_0_4_flag = 1
						OR hh.h_5_11_flag = 1
						)
					AND unique_row = 30
					THEN '12-19'
				WHEN hh.children_count = 9
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 37
					THEN '0-11'
				WHEN hh.children_count = 9
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 38
					THEN '12-19'
				WHEN hh.children_count = 9
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 39
					THEN '0-11'
				WHEN hh.children_count = 9
					AND hh.h_12_17_flag = 1
					AND (
						hh.h_0_4_flag = 1
						OR hh.h_5_11_flag = 1
						)
					AND unique_row = 37
					THEN '0-11'
				WHEN hh.children_count = 9
					AND hh.h_12_17_flag = 1
					AND (
						hh.h_0_4_flag = 1
						OR hh.h_5_11_flag = 1
						)
					AND unique_row = 38
					THEN '12-19'
				WHEN hh.children_count = 10
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 46
					THEN '0-11'
				WHEN hh.children_count = 10
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 47
					THEN '12-19'
				WHEN hh.children_count = 10
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 48
					THEN '0-11'
				WHEN hh.children_count = 10
					AND hh.h_12_17_flag = 1
					AND (
						hh.h_0_4_flag = 1
						OR hh.h_5_11_flag = 1
						)
					AND unique_row = 46
					THEN '0-11'
				WHEN hh.children_count = 10
					AND hh.h_12_17_flag = 1
					AND (
						hh.h_0_4_flag = 1
						OR hh.h_5_11_flag = 1
						)
					AND unique_row = 47
					THEN '12-19'
				WHEN hh.children_count = 11
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 56
					THEN '0-11'
				WHEN hh.children_count = 11
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 57
					THEN '12-19'
				WHEN hh.children_count = 11
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 58
					THEN '0-11'
				WHEN hh.children_count = 11
					AND hh.h_12_17_flag = 1
					AND (
						hh.h_0_4_flag = 1
						OR hh.h_5_11_flag = 1
						)
					AND unique_row = 56
					THEN '0-11'
				WHEN hh.children_count = 11
					AND hh.h_12_17_flag = 1
					AND (
						hh.h_0_4_flag = 1
						OR hh.h_5_11_flag = 1
						)
					AND unique_row = 57
					THEN '12-19'
				WHEN hh.children_count = 12
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 67
					THEN '0-11'
				WHEN hh.children_count = 12
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 68
					THEN '12-19'
				WHEN hh.children_count = 12
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 69
					THEN '0-11'
				WHEN hh.children_count = 12
					AND hh.h_12_17_flag = 1
					AND (
						hh.h_0_4_flag = 1
						OR hh.h_5_11_flag = 1
						)
					AND unique_row = 67
					THEN '0-11'
				WHEN hh.children_count = 12
					AND hh.h_12_17_flag = 1
					AND (
						hh.h_0_4_flag = 1
						OR hh.h_5_11_flag = 1
						)
					AND unique_row = 68
					THEN '12-19'
				WHEN hh.children_count = 13
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 79
					THEN '0-11'
				WHEN hh.children_count = 13
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 80
					THEN '12-19'
				WHEN hh.children_count = 13
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 81
					THEN '0-11'
				WHEN hh.children_count = 13
					AND hh.h_12_17_flag = 1
					AND (
						hh.h_0_4_flag = 1
						OR hh.h_5_11_flag = 1
						)
					AND unique_row = 79
					THEN '0-11'
				WHEN hh.children_count = 13
					AND hh.h_12_17_flag = 1
					AND (
						hh.h_0_4_flag = 1
						OR hh.h_5_11_flag = 1
						)
					AND unique_row = 80
					THEN '12-19'
				WHEN hh.children_count = 14
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 92
					THEN '0-11'
				WHEN hh.children_count = 14
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 93
					THEN '12-19'
				WHEN hh.children_count = 14
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 94
					THEN '0-11'
				WHEN hh.children_count = 14
					AND hh.h_12_17_flag = 1
					AND (
						hh.h_0_4_flag = 1
						OR hh.h_5_11_flag = 1
						)
					AND unique_row = 92
					THEN '0-11'
				WHEN hh.children_count = 14
					AND hh.h_12_17_flag = 1
					AND (
						hh.h_0_4_flag = 1
						OR hh.h_5_11_flag = 1
						)
					AND unique_row = 93
					THEN '12-19'
				WHEN hh.children_count = 15
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 106
					THEN '0-11'
				WHEN hh.children_count = 15
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 107
					THEN '12-19'
				WHEN hh.children_count = 15
					AND hh.h_12_17_flag = 1
					AND hh.h_0_4_flag = 1
					AND hh.h_5_11_flag = 1
					AND unique_row = 108
					THEN '0-11'
				WHEN hh.children_count = 15
					AND hh.h_12_17_flag = 1
					AND (
						hh.h_0_4_flag = 1
						OR hh.h_5_11_flag = 1
						)
					AND unique_row = 106
					THEN '0-11'
				WHEN hh.children_count = 15
					AND hh.h_12_17_flag = 1
					AND (
						hh.h_0_4_flag = 1
						OR hh.h_5_11_flag = 1
						)
					AND unique_row = 107
					THEN '12-19'
				ELSE 'XX'
				END
			,15
			,'randd' = RAND(cb_key_household + unique_row + DATEPART(us, getdate()))
			,panel_flag
		FROM V289_M08_SKY_HH_view_sv AS hh
		JOIN #PIV_append_kids_rows AS k ON hh.children_count = k.number_of_kids

		-- Clean up
		DROP TABLE #PIV_append_kids_rows

		COMMIT WORK

		CREATE TABLE #tkid (
			demo VARCHAR(5) NULL
			,low DECIMAL(8, 1) NULL
			,upp DECIMAL(8, 1) NULL
			,
			)

		INSERT INTO #tkid
		VALUES (
			'0-11'
			,0
			,.3
			)
			,(
			'12-19'
			,.3
			,1
			)

		COMMIT WORK

		UPDATE V289_M08_SKY_HH_composition_sv AS a
		SET person_ageband = demo
		FROM V289_M08_SKY_HH_composition_sv AS a
		JOIN #tkid AS b ON randd <= upp
			AND randd > low
		WHERE person_ageband = 'XX'

		---- There are a small number of 0-19 in the Experian data (these were 18-19 in Experian data)
		--- These will have a gender. But because they are a small number distort the scaling
		--- Change the gender of these to U
		UPDATE V289_M08_SKY_HH_composition_sv
		SET person_gender = 'U'
		WHERE person_ageband IN (
				'0-11'
				,'12-19'
				)

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M08.7 kids data added' TO client
			--------------------------------
			-- M08.8 - Final Tidying of Data
			--------------------------------
			message convert(TIMESTAMP, now()) || ' | Begining M08.8 - Final Tidying of Data' TO client

		-- Everyone with the same account_number gets a unique number
		SELECT row_id
			,'rank1' = RANK() OVER (
				PARTITION BY account_number ORDER BY person_head DESC
					,row_id ASC
				)
		INTO #a4
		FROM V289_M08_SKY_HH_composition_sv
		GROUP BY account_number
			,person_head
			,row_id

		COMMIT WORK

		CREATE hg INDEX ind1 ON #a4 (row_id)

		COMMIT WORK

		UPDATE V289_M08_SKY_HH_composition_sv AS h
		SET HH_person_number = rank1
		FROM #a4 AS r
		WHERE h.row_id = r.row_id

		COMMIT WORK

		-- Clean up
		DROP TABLE #a4

		-- Calculate household size and delete any > 15
		SELECT account_number
			,'hhd_count' = count(1)
		INTO #a5
		FROM V289_M08_SKY_HH_composition_sv
		GROUP BY account_number

		COMMIT WORK

		UPDATE V289_M08_SKY_HH_composition_sv AS c
		SET household_size = hhd_count
		FROM #a5 AS a
		WHERE c.account_number = a.account_number

		COMMIT WORK

		DELETE
		FROM V289_M08_SKY_HH_composition_sv
		WHERE household_size > 15

		COMMIT WORK

		-- Clean up
		DROP TABLE #a5 message convert(TIMESTAMP, now()) || ' | @ M08.8: Final Tidying of Data DONE' TO client
			-------------------------------------------------------------------------------
			-- M08.9 - Remove incomplete households (Undefined gender for ageband NOT 0-19)
			-------------------------------------------------------------------------------
			message convert(TIMESTAMP, now()) || ' | @ M08.9: Removing incomplete households' TO client

		-- First, identify the households/accounts to remove
		SELECT account_number
		INTO #INCOMPLETE_HHS
		FROM V289_M08_SKY_HH_composition_sv
		WHERE person_gender = 'U'
			AND person_ageband NOT IN (
				'0-11'
				,'12-19'
				,'XX'
				)
		GROUP BY account_number

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M08.9: Incomplete households identified for removal : ' || @@rowcount TO client

		CREATE UNIQUE hg INDEX IDX1 ON #INCOMPLETE_HHS (account_number)

		COMMIT WORK

		-- Now delete the appropriate records corresponding to those incomplete accounts
		DELETE
		FROM V289_M08_SKY_HH_composition_sv AS BAS
		FROM V289_M08_SKY_HH_composition_sv AS BAS
		LEFT OUTER JOIN #INCOMPLETE_HHS AS INC ON BAS.account_number = INC.account_number
		WHERE INC.ACCOUNT_NUMBER IS NOT NULL

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M08.9: Removing incomplete households...DONE. Rows removed : ' || @@rowcount TO client

		DROP TABLE #INCOMPLETE_HHS

		COMMIT WORK

		--------------------------------------------------------------
		-------------------------------         KUBA agenband recode for 0-19
		--------------------------------------------------------------
		UPDATE V289_M08_SKY_HH_composition_sv
		SET person_ageband = '0-19'
		WHERE person_ageband IN (
				'0-11'
				,'12-19'
				)

		COMMIT WORK
			---------------------------------------------------------------------------------------------
	END
	ELSE
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M08.0: Data still valid, last update was less than a week ago' TO client message convert(TIMESTAMP, now()) || ' | @ M08.0: Initialising Environment DONE' TO client
	END message convert(TIMESTAMP, now()) || ' | M08.8 Process completed' TO client
END;
GO 
commit;
