CREATE PROCEDURE jsk01.v289_m06_DP_data_extraction_sv (
	@event_date DATE = NULL
	,@sample_proportion SMALLINT = 100
	)
AS
BEGIN
	message convert(TIMESTAMP, now()) || ' | Begining M06.0 - Initialising Environment' TO client

	DECLARE @dp_tname VARCHAR(50)
	DECLARE @query VARCHAR(3000)
	DECLARE @from_dt INTEGER
	DECLARE @to_dt INTEGER

	SET @dp_tname = 'SK_PROD.VESPA_DP_PROG_VIEWED_'

	SELECT @from_dt = convert(INTEGER, (
				DATEFORMAT (
					@Event_date
					,'YYYYMMDD'
					) || '00'
				))

	SELECT @to_dt = convert(INTEGER, (
				DATEFORMAT (
					@Event_date
					,'YYYYMMDD'
					) || '23'
				))

	IF @Event_date IS NULL
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M06.0: You need to provide a Date for extraction !!!' TO client
	END
	ELSE
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M06.0: Initialising Environment DONE' TO client message convert(TIMESTAMP, now()) || ' | Begining M06.1 - Composing Table Name' TO client

		SET @dp_tname = @dp_tname || datepart(year, @Event_date) || right(('00' || convert(VARCHAR(2), datepart(month, @event_date))), 2) message convert(TIMESTAMP, now()) || ' | @ M06.1: Composing Table Name DONE: ' || @dp_tname TO client message convert(TIMESTAMP, now()) || ' | Resetting panel_flag = 1 to all panel accounts' TO client

		UPDATE V289_M08_SKY_HH_composition_sv
		SET panel_flag = 0

		COMMIT WORK

		UPDATE V289_M08_SKY_HH_composition_sv AS a
		SET panel_flag = 1
		FROM V289_M08_SKY_HH_composition_sv AS a
		JOIN (
			SELECT DISTINCT account_number
			FROM VIQ_VIEWING_DATA_SCALING
			WHERE adjusted_event_start_date_vespa = @event_date
			) AS viq ON a.account_number = viq.account_number
		JOIN H2I_accounts_M06 AS skv ON viq.account_number = skv.account_number

		IF @sample_proportion < 100
		BEGIN
			message convert(TIMESTAMP, now()) || ' | Begining M06.2 - Trimming Sample' TO client

			COMMIT WORK

			SELECT account_number
				,'random' = convert(REAL, account_number)
			INTO #aclist
			FROM V289_M08_SKY_HH_composition_sv
			WHERE panel_flag = 1
			GROUP BY account_number

			COMMIT WORK

			UPDATE #aclist
			SET random = rand(convert(REAL, account_number) + datepart(us, getdate()))

			COMMIT WORK

			SELECT DISTINCT account_number
			INTO #sample
			FROM (
				SELECT *
					,'therow' = row_number() OVER (
						ORDER BY random ASC
						)
				FROM #aclist
				) AS base
			WHERE therow <= (
					SELECT (count(1) * @sample_proportion) / 100
					FROM #aclist
					)

			COMMIT WORK

			UPDATE V289_M08_SKY_HH_composition_sv
			SET PANEL_FLAG = 0
			WHERE NOT account_number = ANY (
					SELECT account_number
					FROM #sample
					) message convert(TIMESTAMP, now()) || ' | @ M06.2: Trimming Sample DONE: ' || @@rowcount TO client
		END message convert(TIMESTAMP, now()) || ' | Begining M06.3 - Data Extraction' TO client

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = user_name()
					AND upper(tname) = upper('v289_M06_dp_raw_data_sv')
					AND tabletype = 'TABLE'
				)
			TRUNCATE TABLE jsk01.v289_M06_dp_raw_data_sv

		COMMIT WORK

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = user_name()
					AND upper(tname) = upper('v289_m06_pseudo')
					AND tabletype = 'TABLE'
				)
			DROP TABLE v289_m06_pseudo

		COMMIT WORK

		SELECT account_number
			,'household_key' = min(cb_key_household)
		INTO #account_household_keys
		FROM V289_M08_SKY_HH_composition_sv
		WHERE PANEL_FLAG = 1
		GROUP BY account_number

		COMMIT WORK

		CREATE UNIQUE hg INDEX idx1 ON #account_household_keys (account_number)

		COMMIT WORK

		SET @query = 'select  pk_viewing_prog_instance_fact'
				|| ',viewing_event_id'
				|| ',dk_event_start_datehour_dim'
				|| ',dk_event_end_datehour_dim'
				|| ',dk_broadcast_start_Datehour_dim'
				|| ',dk_instance_start_datehour_dim'
				|| ',duration'
				|| ',case when genre_description in (''Undefined'',''Unknown'') then ''Unknown'' else genre_description end as genre_description'
				|| ',service_key'
				|| ',c.household_key'
				|| ',event_start_date_time_utc'
				|| ',event_end_date_time_utc'
				|| ',a.account_number'
				|| ',subscriber_id'
				|| ',service_instance_id'
				|| ',programme_name'
				|| ',capping_end_Date_time_utc'
				|| ',broadcast_start_date_time_utc'
				|| ',broadcast_end_date_time_utc'
				|| ',instance_start_date_time_utc'
				|| ',instance_end_date_time_utc'
				|| ',dk_barb_min_start_datehour_dim'
				|| ',dk_barb_min_start_time_dim'
				|| ',dk_barb_min_end_datehour_dim'
				|| ',dk_barb_min_end_time_dim'
				|| ',barb_min_start_date_time_utc'
				|| ',barb_min_end_date_time_utc'
				|| ',live_recorded'
				|| ' into\x09v289_m06_pseudo'
				|| ' from    ' || @dp_tname || ' as a '
				|| 'inner join  #account_household_keys   as c on a.account_number = c.account_number '
				|| 'where '
				|| 'dk_event_start_datehour_dim between ' || @from_dt || ' and ' || @to_dt
				|| ' and service_key is not null'
				||' AND event_start_date_time_utc IS NOT NULL ' 
				||' AND event_end_date_time_utc IS NOT NULL ' 
				||' AND instance_start_date_time_utc IS NOT NULL ' 
				||' AND instance_end_date_time_utc IS NOT NULL ' 
		 
		EXECUTE (@query)

		COMMIT WORK

		CREATE hg INDEX key1 ON v289_m06_pseudo (pk_viewing_prog_instance_fact)
		CREATE hg INDEX hg0 ON v289_m06_pseudo (viewing_event_id)
		CREATE hg INDEX hg1 ON v289_m06_pseudo (dk_event_start_datehour_dim)
		CREATE hg INDEX hg2 ON v289_m06_pseudo (dk_broadcast_start_datehour_dim)
		CREATE hg INDEX hg3 ON v289_m06_pseudo (dk_instance_start_datehour_dim)
		CREATE hg INDEX hg5 ON v289_m06_pseudo (service_key)
		CREATE hg INDEX hg6 ON v289_m06_pseudo (account_number)
		CREATE hg INDEX hg7 ON v289_m06_pseudo (subscriber_id)
		CREATE hg INDEX hg8 ON v289_m06_pseudo (programme_name)
		CREATE hg INDEX hg9 ON v289_m06_pseudo (dk_barb_min_start_datehour_dim)
		CREATE hg INDEX hg10 ON v289_m06_pseudo (dk_barb_min_start_time_dim)
		CREATE hg INDEX hg11 ON v289_m06_pseudo (dk_barb_min_end_datehour_dim)
		CREATE hg INDEX hg12 ON v289_m06_pseudo (dk_barb_min_end_time_dim)
		CREATE hg INDEX hg13 ON v289_m06_pseudo (barb_min_start_date_time_utc)
		CREATE hg INDEX hg14 ON v289_m06_pseudo (barb_min_end_date_time_utc)
		CREATE lf INDEX lf1 ON v289_m06_pseudo (genre_description)
		CREATE lf INDEX lf2 ON v289_m06_pseudo (live_recorded)
		CREATE dttm INDEX dttm1 ON v289_m06_pseudo (barb_min_start_date_time_utc)
		CREATE dttm INDEX dttm2 ON v289_m06_pseudo (barb_min_end_date_time_utc)

		COMMIT WORK

		DROP TABLE #account_household_keys

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M06.3: Data Extraction !!!!!SHIELD AGAINST DUPLICATED PKS!!!!!!' TO client

		SELECT pk_viewing_prog_instance_fact
		INTO #templist
		FROM v289_m06_pseudo
		GROUP BY pk_viewing_prog_instance_fact
		HAVING count(1) > 1

		COMMIT WORK

		CREATE UNIQUE hg INDEX idx1 ON #templist (pk_viewing_prog_instance_fact)

		COMMIT WORK

		DELETE
		FROM v289_m06_pseudo AS a
		FROM #templist AS b
		WHERE a.pk_viewing_prog_instance_fact = b.pk_viewing_prog_instance_fact

		COMMIT WORK

		DROP TABLE #templist

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M06.3: Data Extraction !!!!!SHIELD AGAINST DUPLICATED PKS!!!!!! DONE. ROWS DELETED:' || @@rowcount TO client

		INSERT INTO v289_M06_dp_raw_data_sv (
			pk_viewing_prog_instance_fact
			,dth_event_id
			,dk_event_start_datehour_dim
			,dk_event_end_datehour_dim
			,dk_broadcast_start_Datehour_dim
			,dk_instance_start_datehour_dim
			,duration
			,genre_description
			,service_key
			,cb_key_household
			,event_start_date_time_utc
			,event_end_date_time_utc
			,account_number
			,subscriber_id
			,service_instance_id
			,programme_name
			,capping_end_Date_time_utc
			,broadcast_start_date_time_utc
			,broadcast_end_date_time_utc
			,instance_start_date_time_utc
			,instance_end_date_time_utc
			,dk_barb_min_start_datehour_dim
			,dk_barb_min_start_time_dim
			,dk_barb_min_end_datehour_dim
			,dk_barb_min_end_time_dim
			,barb_min_start_date_time_utc
			,barb_min_end_date_time_utc
			,live_recorded
			)
		SELECT *
		FROM v289_m06_pseudo message convert(TIMESTAMP, now()) || ' | @ M06.3: Data Extraction DONE ROWS:' || @@rowcount TO client

		COMMIT WORK

		DROP TABLE v289_m06_pseudo

		COMMIT WORK
	END message convert(TIMESTAMP, now()) || ' | M06 Finished' TO client
END
