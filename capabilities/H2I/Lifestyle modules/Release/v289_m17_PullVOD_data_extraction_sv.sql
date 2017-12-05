create or replace procedure ${SQLFILE_ARG001}.v289_m17_PullVOD_data_extraction_sv (@event_date DATE = NULL)
AS
BEGIN
	message convert(TIMESTAMP, now()) || ' | Begining M17.0 - Initialising Environment' TO client

	DECLARE @dp_tname VARCHAR(50)
	DECLARE @query VARCHAR(3000)
	DECLARE @from_dt INTEGER
	DECLARE @to_dt INTEGER
	DECLARE @default_date TIMESTAMP
	DECLARE @pull_vod VARCHAR(10)

	SET @dp_tname = 'VESPA_STREAM_VOD_VIEWING_PROG_FACT_'
	SET @default_date = '1970-01-01 00:00:00'
	SET @pull_vod = 'On Demand'

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
		message convert(TIMESTAMP, now()) || ' | @ M17.0: You need to provide a Date for extraction !!!' TO client
	END
	ELSE
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M17.0: Initialising Environment DONE' TO client message convert(TIMESTAMP, now()) || ' | Begining M17.1 - Composing Table Name' TO client

		SET @dp_tname = @dp_tname || datepart(year, @Event_date) || right(('00' || convert(VARCHAR(2), datepart(month, @event_date))), 2) message convert(TIMESTAMP, now()) || ' | @ M17.1: Composing Table Name DONE: ' || @dp_tname TO client message convert(TIMESTAMP, now()) || ' | Begining M17.2 - Data Extraction' TO client

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = '${SQLFILE_ARG001}'
					AND upper(tname) = upper('v289_M17_vod_raw_data_sv')
					AND tabletype = 'TABLE'
				)
			TRUNCATE TABLE v289_M17_vod_raw_data_sv

		COMMIT WORK

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = '${SQLFILE_ARG001}'
					AND upper(tname) = upper('v289_m17_pseudo')
					AND tabletype = 'TABLE'
				)
			DROP TABLE v289_m17_pseudo

		COMMIT WORK

		SELECT account_number
			,'household_key' = min(cb_key_household)
		INTO #account_household_keys
		FROM V289_M08_SKY_HH_composition_sv
		WHERE PANEL_FLAG = 1
		GROUP BY account_number

		COMMIT WORK

		CREATE UNIQUE hg INDEX idx1 ON #account_household_keys (account_number)

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | M17.2 - Data Extraction preparation done' TO client

		SET @query = 'if  exists(  select tname from syscatalog where lower(creator) = ''sk_prod'' and upper(tname) = upper(''' || @dp_tname || ''')  ) ' || 'select  pk_viewing_programme_instance_fact as pk_viewing_prog_instance_fact' || ',dk_event_start_datehour as dk_event_start_datehour_dim' || ',dk_event_start_time' || ',dk_event_end_datehour as dk_event_end_datehour_dim' || ',dk_event_end_time' || ',dk_broadcast_start_Datehour as dk_broadcast_start_Datehour_dim' || ',dk_broadcast_start_time' || ',dk_instance_start_datehour as dk_instance_start_datehour_dim' || ',dk_instance_start_time' || ',duration' || ',case when programme_genre in (''Undefined'',''Unknown'') then ''Unknown'' else programme_genre end as genre_description' || ',cast(null as integer) as service_key' || ',c.household_key' || ',cast(NULL as timestamp) as event_start_date_time_utc' || ',cast(NULL as timestamp) as event_end_date_time_utc' || ',cast(a.account_number as varchar(15)) as account_number' || ',99 as subscriber_id' || 
			',cast(null as varchar(1)) as service_instance_id' || ',programme_name' || ',cast(NULL as timestamp) as capping_end_Date_time_utc' || ',dk_capped_event_end_time_datehour_dim' || ',dk_capped_event_end_time_dim' || ',cast(NULL as timestamp) as broadcast_start_date_time_utc' || ',cast(NULL as timestamp) as broadcast_end_date_time_utc' || ',cast(NULL as timestamp) as instance_start_date_time_utc' || ',cast(NULL as timestamp) as instance_end_date_time_utc' || ',dk_broadcast_end_Datehour as dk_broadcast_end_Datehour_dim' || ',dk_broadcast_end_time' || ',dk_instance_end_datehour as dk_instance_end_datehour_dim' || ',dk_instance_end_time' || ',prog_dim_provider_id as provider_id' || ',-1 as provider_id_number ' || ',dk_barb_min_end_datehour ' || ',dk_barb_min_end_time ' || ',dk_barb_min_start_datehour ' || ',dk_barb_min_start_time ' || ',cast(NULL as timestamp) as barb_min_start_date_time_utc ' || ',cast(NULL as timestamp) as barb_min_end_date_time_utc ' || 'into   v289_m17_pseudo ' || 'from   ' || @dp_tname || ' as a ' || 
			'inner join  #account_household_keys  as c ' || 'on cast(a.account_number as varchar(15)) = c.account_number ' || 'where  dk_event_start_datehour_dim between ' || @from_dt || ' and ' || @to_dt || ' ' || 'and    event_sub_type = ''On Demand'' ' || 'and    prog_dim_provider_id is not null'

		EXECUTE (@query)

		COMMIT WORK

		IF NOT EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = '${SQLFILE_ARG001}'
					AND upper(tname) = upper('v289_m17_pseudo')
					AND tabletype = 'TABLE'
				)
			GOTO fatality

		CREATE hg INDEX key1 ON v289_m17_pseudo (pk_viewing_prog_instance_fact)

		CREATE hg INDEX hg1 ON v289_m17_pseudo (dk_event_start_datehour_dim)

		CREATE hg INDEX hg2 ON v289_m17_pseudo (dk_broadcast_start_datehour_dim)

		CREATE hg INDEX hg3 ON v289_m17_pseudo (dk_instance_start_datehour_dim)

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | Begining M17.2 - Data Extraction Completed' TO client message convert(TIMESTAMP, now()) || ' | Begining M17.2 - PATCH FOR PROGRAMME_GENRE' TO client

		UPDATE v289_m17_pseudo
		SET genre_description = CASE lower(trim(genre_description))
				WHEN '(unknown)'
					THEN 'Unknown'
				WHEN 'entertainment'
					THEN 'Entertainment'
				WHEN 'kids'
					THEN 'Children'
				WHEN 'movies'
					THEN 'Movies'
				WHEN 'music'
					THEN 'Music & Radio'
				WHEN 'news'
					THEN 'News & Documentaries'
				WHEN 'sports'
					THEN 'Sports'
				ELSE 'Unknown'
				END

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | Begining M17.2 - PATCH FOR PROGRAMME_GENRE DONE' TO client

		UPDATE v289_m17_pseudo AS p
		SET event_start_date_time_utc = dateadd(ss, convert(INTEGER, substring(utc_time, 5, 2)), dateadd(mi, convert(INTEGER, substring(utc_time, 3, 2)), dateadd(hh, convert(INTEGER, substring(utc_time, 1, 2)), convert(TIMESTAMP, utc_day_date))))
		FROM VIQ_DATE AS d
			,VIQ_TIME AS t
		WHERE p.dk_event_start_datehour_dim = d.pk_datehour_dim
			AND p.dk_event_start_time = convert(INTEGER, t.pk_time_dim)

		COMMIT WORK

		UPDATE v289_m17_pseudo AS p
		SET event_end_date_time_utc = dateadd(ss, convert(INTEGER, substring(utc_time, 5, 2)), dateadd(mi, convert(INTEGER, substring(utc_time, 3, 2)), dateadd(hh, convert(INTEGER, substring(utc_time, 1, 2)), convert(TIMESTAMP, utc_day_date))))
		FROM VIQ_DATE AS d
			,VIQ_TIME AS t
		WHERE p.dk_event_end_datehour_dim = d.pk_datehour_dim
			AND p.dk_event_end_time = convert(INTEGER, t.pk_time_dim)

		COMMIT WORK

		UPDATE v289_m17_pseudo AS p
		SET capping_end_Date_time_utc = dateadd(ss, convert(INTEGER, substring(utc_time, 5, 2)), dateadd(mi, convert(INTEGER, substring(utc_time, 3, 2)), dateadd(hh, convert(INTEGER, substring(utc_time, 1, 2)), convert(TIMESTAMP, utc_day_date))))
		FROM VIQ_DATE AS d
			,VIQ_TIME AS t
		WHERE p.dk_capped_event_end_time_datehour_dim = d.pk_datehour_dim
			AND p.dk_capped_event_end_time_dim = convert(INTEGER, t.pk_time_dim)

		COMMIT WORK

		UPDATE v289_m17_pseudo AS p
		SET capping_end_Date_time_utc = event_end_date_time_utc
		WHERE p.capping_end_Date_time_utc IS NULL

		UPDATE v289_m17_pseudo AS p
		SET instance_start_date_time_utc = dateadd(ss, convert(INTEGER, substring(utc_time, 5, 2)), dateadd(mi, convert(INTEGER, substring(utc_time, 3, 2)), dateadd(hh, convert(INTEGER, substring(utc_time, 1, 2)), convert(TIMESTAMP, utc_day_date))))
		FROM VIQ_DATE AS d
			,VIQ_TIME AS t
		WHERE p.dk_instance_start_datehour_dim = d.pk_datehour_dim
			AND p.dk_instance_start_time = convert(INTEGER, t.pk_time_dim)

		COMMIT WORK

		UPDATE v289_m17_pseudo AS p
		SET instance_end_date_time_utc = dateadd(ss, convert(INTEGER, substring(utc_time, 5, 2)), dateadd(mi, convert(INTEGER, substring(utc_time, 3, 2)), dateadd(hh, convert(INTEGER, substring(utc_time, 1, 2)), convert(TIMESTAMP, utc_day_date))))
		FROM VIQ_DATE AS d
			,VIQ_TIME AS t
		WHERE p.dk_instance_end_datehour_dim = d.pk_datehour_dim
			AND p.dk_instance_end_time = convert(INTEGER, t.pk_time_dim)

		COMMIT WORK

		UPDATE v289_m17_pseudo AS p
		SET duration = datediff(ss, event_start_date_time_utc, CASE 
					WHEN event_end_date_time_utc <= capping_end_Date_time_utc
						THEN event_end_date_time_utc
					ELSE capping_end_Date_time_utc
					END)

		COMMIT WORK

		UPDATE v289_m17_pseudo AS p
		SET barb_min_start_date_time_utc = CASE 
				WHEN duration < 31
					THEN NULL
				ELSE CASE 
						WHEN second(event_start_date_time_utc) < 31
							THEN datefloor(mi, event_start_date_time_utc)
						ELSE dateceiling(mi, event_start_date_time_utc)
						END
				END

		COMMIT WORK

		UPDATE v289_m17_pseudo AS p
		SET barb_min_end_date_time_utc = CASE 
				WHEN duration < 31
					THEN NULL
				ELSE CASE 
						WHEN second(capping_end_Date_time_utc) < 31
							THEN dateadd(mi, - 1, datefloor(mi, capping_end_Date_time_utc))
						ELSE datefloor(mi, capping_end_Date_time_utc)
						END
				END

		COMMIT WORK

		UPDATE v289_m17_pseudo AS p
		SET barb_min_start_date_time_utc = NULL
		WHERE (
				barb_min_end_date_time_utc IS NULL
				OR p.barb_min_start_date_time_utc > p.barb_min_end_date_time_utc
				)

		COMMIT WORK

		UPDATE v289_m17_pseudo AS p
		SET barb_min_end_date_time_utc = NULL
		WHERE (
				barb_min_start_date_time_utc IS NULL
				OR p.barb_min_start_date_time_utc > p.barb_min_end_date_time_utc
				)

		COMMIT WORK

		UPDATE v289_m17_pseudo AS m17
		SET m17.service_key = ska.service_key
		FROM vespa_analysts.channel_map_prod_service_key_attributes AS ska
		WHERE m17.provider_id = ska.provider_id
			AND m17.event_start_date_time_utc BETWEEN ska.effective_from
				AND ska.effective_to

		COMMIT WORK

		CREATE hg INDEX hg5 ON v289_m17_pseudo (service_key)

		CREATE hg INDEX hg6 ON v289_m17_pseudo (account_number)

		CREATE hg INDEX hg7 ON v289_m17_pseudo (subscriber_id)

		CREATE hg INDEX hg8 ON v289_m17_pseudo (programme_name)

		CREATE lf INDEX lf1 ON v289_m17_pseudo (genre_description)

		COMMIT WORK

		DROP TABLE #account_household_keys

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M17.2: Data Extraction !!!!!SHIELD AGAINST DUPLICATED PKS!!!!!!' TO client

		SELECT pk_viewing_prog_instance_fact
		INTO #templist
		FROM v289_m17_pseudo
		GROUP BY pk_viewing_prog_instance_fact
		HAVING count(1) > 1

		COMMIT WORK

		CREATE UNIQUE hg INDEX idx1 ON #templist (pk_viewing_prog_instance_fact)

		COMMIT WORK

		DELETE
		FROM v289_m17_pseudo AS a
		FROM #templist AS b
		WHERE a.pk_viewing_prog_instance_fact = b.pk_viewing_prog_instance_fact

		COMMIT WORK

		DROP TABLE #templist

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M17.2: Data Extraction !!!!!SHIELD AGAINST DUPLICATED PKS!!!!!! DONE. ROWS DELETED:' || @@rowcount TO client

		INSERT INTO v289_M17_vod_raw_data_sv (
			pk_viewing_prog_instance_fact
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
			,provider_id
			,provider_id_number
			,barb_min_start_date_time_utc
			,barb_min_end_date_time_utc
			)
		SELECT pk_viewing_prog_instance_fact
			,dk_event_start_datehour_dim
			,dk_event_end_datehour_dim
			,dk_broadcast_start_Datehour_dim
			,dk_instance_start_datehour_dim
			,duration
			,genre_description
			,service_key
			,household_key
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
			,provider_id
			,provider_id_number
			,barb_min_start_date_time_utc
			,barb_min_end_date_time_utc
		FROM v289_m17_pseudo

		fatality: message convert(TIMESTAMP, now()) || ' | @ M17.2: Data Extraction DONE ROWS:' || @@rowcount TO client

		COMMIT WORK

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = '${SQLFILE_ARG001}'
					AND upper(tname) = upper('v289_m17_pseudo')
					AND tabletype = 'TABLE'
				)
			DROP TABLE v289_m17_pseudo

		COMMIT WORK
	END message convert(TIMESTAMP, now()) || ' | M17 Finished' TO client
END;
GO 
commit;
