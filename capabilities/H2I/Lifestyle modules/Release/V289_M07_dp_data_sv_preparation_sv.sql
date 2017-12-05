create or replace procedure ${SQLFILE_ARG001}.V289_M07_dp_data_sv_preparation_sv
AS
BEGIN
	message convert(TIMESTAMP, now()) || ' | Begining  M07.0 - Initialising Environment' TO client

	IF EXISTS (
			SELECT TOP 1 *
			FROM v289_M06_dp_raw_data_sv
			)
	BEGIN
		TRUNCATE TABLE ${SQLFILE_ARG001}.V289_M07_dp_data_sv

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M07.0: Initialising Environment DONE' TO client message convert(TIMESTAMP, now()) || ' | Begining  M07.1 - Compacting Data at Event level' TO client

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = '${SQLFILE_ARG001}'
					AND upper(tname) = upper('V289_M07_dp_data_sv_tempshelf')
					AND tabletype = 'TABLE'
				)
			DROP TABLE V289_M07_dp_data_sv_tempshelf

		COMMIT WORK

		SELECT *
		INTO V289_M07_dp_data_sv_tempshelf
		FROM V289_M07_dp_data_sv

		COMMIT WORK

		INSERT INTO V289_M07_dp_data_sv_tempshelf (
			account_number
			,subscriber_id
			,event_id
			,event_start_utc
			,event_end_utc
			,event_start_dim
			,event_end_dim
			,event_duration_seg
			,service_key
			,barb_min_start_date_time_utc
			,barb_min_end_date_time_utc
			,provider_id
			,provider_id_number
			,viewing_type_flag
			,programme_genre
			)
		SELECT base.*
			,'provider_id' = NULL
			,'provider_id_number' = - 1
			,'viewing_type_flag' = 0
			,'programme_genre' = lookup.genre_description
		FROM (
			SELECT account_number
				,subscriber_id
				,'event_id' = min(pk_viewing_prog_instance_fact)
				,'event_start_utc' = event_start_date_time_utc
				,'event_end_utc' = CASE 
					WHEN min(capping_end_Date_time_utc) IS NOT NULL
						THEN min(capping_end_Date_time_utc)
					ELSE event_end_date_time_utc
					END
				,'dk_event_start_dim' = min(dk_event_start_datehour_dim)
				,'dk_event_end_dim' = min(dk_event_end_datehour_dim)
				,'duration' = datediff(ss, event_start_utc, event_end_utc)
				,'service_key' = min(service_key)
				,'barb_min_event_start' = min(CASE 
						WHEN barb_min_start_date_time_utc IS NOT NULL
							THEN barb_min_start_date_time_utc
						ELSE '2999-12-31 00:00:00'
						END)
				,'barb_min_event_end' = max(CASE 
						WHEN barb_min_end_date_time_utc IS NOT NULL
							THEN barb_min_end_date_time_utc
						ELSE '1970-01-01 00:00:00'
						END)
			FROM v289_M06_dp_raw_data_sv
			GROUP BY account_number
				,subscriber_id
				,event_start_date_time_utc
				,event_end_date_time_utc
			) AS base
		JOIN v289_M06_dp_raw_data_sv AS lookup ON base.event_id = lookup.pk_viewing_prog_instance_fact

		COMMIT WORK

		INSERT INTO V289_M07_dp_data_sv_tempshelf (
			account_number
			,subscriber_id
			,event_id
			,event_start_utc
			,event_end_utc
			,event_start_dim
			,event_end_dim
			,event_duration_seg
			,service_key
			,barb_min_start_date_time_utc
			,barb_min_end_date_time_utc
			,provider_id
			,provider_id_number
			,viewing_type_flag
			,programme_genre
			)
		SELECT base.*
			,'provider_id' = NULL
			,'provider_id_number' = - 1
			,'viewing_type_flag' = 1
			,'programme_genre' = lookup.genre_description
		FROM (
			SELECT account_number
				,subscriber_id
				,'event_id' = min(pk_viewing_prog_instance_fact)
				,'event_start_utc' = event_start_date_time_utc
				,'event_end_utc' = CASE 
					WHEN min(capping_end_Date_time_utc) IS NOT NULL
						THEN min(capping_end_Date_time_utc)
					ELSE event_end_date_time_utc
					END
				,'dk_event_start_dim' = min(dk_event_start_datehour_dim)
				,'dk_event_end_dim' = min(dk_event_end_datehour_dim)
				,'duration' = datediff(ss, event_start_utc, event_end_utc)
				,'service_key' = min(service_key)
				,'barb_min_event_start' = min(CASE 
						WHEN barb_min_start_date_time_utc IS NOT NULL
							THEN barb_min_start_date_time_utc
						ELSE '2999-12-31 00:00:00'
						END)
				,'barb_min_event_end' = max(CASE 
						WHEN barb_min_end_date_time_utc IS NOT NULL
							THEN barb_min_end_date_time_utc
						ELSE '1970-01-01 00:00:00'
						END)
			FROM v289_M17_vod_raw_data_sv
			GROUP BY account_number
				,subscriber_id
				,event_start_date_time_utc
				,event_end_date_time_utc
			) AS base
		JOIN v289_M17_vod_raw_data_sv AS lookup ON base.event_id = lookup.pk_viewing_prog_instance_fact

		COMMIT WORK

		CREATE hg INDEX hg1 ON V289_M07_dp_data_sv_tempshelf (account_number)
		CREATE hg INDEX hg2 ON V289_M07_dp_data_sv_tempshelf (subscriber_id)
		CREATE hg INDEX hg3 ON V289_M07_dp_data_sv_tempshelf (event_id)
		CREATE hg INDEX hg4 ON V289_M07_dp_data_sv_tempshelf (channel_pack)
		CREATE hg INDEX hg5 ON V289_M07_dp_data_sv_tempshelf (programme_genre)
		CREATE hg INDEX hg6 ON V289_M07_dp_data_sv_tempshelf (session_daypart)
		CREATE dttm INDEX dttm1 ON V289_M07_dp_data_sv_tempshelf (event_start_utc)
		CREATE dttm INDEX dttm2 ON V289_M07_dp_data_sv_tempshelf (event_end_utc)

		COMMIT WORK 
		message convert(TIMESTAMP, now()) || ' | @ M07.1: Compacting Data at Event level DONE' TO client 
		message convert(TIMESTAMP, now()) || ' | Begining  M07.2 - Appending Dimensions' TO client

		UPDATE V289_M07_dp_data_sv_tempshelf
		SET session_daypart = CASE 
				WHEN convert(TIME, event_start_utc) BETWEEN '00:00:00.000'
						AND '05:59:59.000'
					THEN 'night'
				WHEN convert(TIME, event_start_utc) BETWEEN '06:00:00.000'
						AND '08:59:59.000'
					THEN 'breakfast'
				WHEN convert(TIME, event_start_utc) BETWEEN '09:00:00.000'
						AND '11:59:59.000'
					THEN 'morning'
				WHEN convert(TIME, event_start_utc) BETWEEN '12:00:00.000'
						AND '14:59:59.000'
					THEN 'lunch'
				WHEN convert(TIME, event_start_utc) BETWEEN '15:00:00.000'
						AND '17:59:59.000'
					THEN 'early prime'
				WHEN convert(TIME, event_start_utc) BETWEEN '18:00:00.000'
						AND '20:59:59.000'
					THEN 'prime'
				WHEN convert(TIME, event_start_utc) BETWEEN '21:00:00.000'
						AND '23:59:59.000'
					THEN 'late night'
				END

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M07.2: Appending Session_Daypart DONE' TO client

		
		
		UPDATE V289_M07_dp_data_sv_tempshelf AS dpdata
		SET channel_pack = cm.channel_pack
		FROM v289_M06_dp_raw_data_sv AS dpraw
		JOIN vespa_Analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES AS cm ON dpraw.service_key = cm.service_key
			AND convert(DATE, dpraw.event_Start_date_time_utc) BETWEEN cm.effective_from
				AND cm.effective_to
		WHERE dpraw.pk_viewing_prog_instance_fact = dpdata.event_id

		COMMIT WORK

		UPDATE V289_M07_dp_data_sv_tempshelf
		SET channel_pack = 'Other'
		WHERE viewing_type_flag = 1

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M07.2: Appending Channel_Pack DONE' TO client

		UPDATE V289_M07_dp_data_sv_tempshelf AS dpdata
		SET hhsize = base.household_size
		FROM V289_M08_SKY_HH_composition_sv AS base
		WHERE base.account_number = dpdata.account_number
			AND panel_flag = 1

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M07.2: Appending HHSize DONE' TO client

		UPDATE V289_M07_dp_data_sv_tempshelf
		SET programme_genre = 'Unknown'
		WHERE (
				programme_genre IS NULL
				OR programme_genre = 'DUMMY'
				)

		UPDATE V289_M07_dp_data_sv_tempshelf AS dpdata
		SET dpdata.segment_id = seglookup.segment_id
		FROM V289_PIV_Grouped_Segments_desc_sv AS seglookup
		WHERE seglookup.daypart = dpdata.session_daypart
			AND seglookup.genre = dpdata.programme_genre
			AND seglookup.channel_pack = dpdata.channel_pack

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M07.2: Appending Segment_ID DONE' TO client message convert(TIMESTAMP, now()) || ' | @ M07.2: Appending Dimensions DONE' TO client message convert(TIMESTAMP, now()) || ' | Begining  M07.3 - Flagging Overlapping Events' TO client

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = '${SQLFILE_ARG001}'
					AND upper(tname) = upper('v289_m07_events_overlap')
					AND tabletype = 'TABLE'
				)
			DROP TABLE v289_m07_events_overlap

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | Begining  M07.3 - Flagging Overlapping Events - Checkpoint A' TO client

		SELECT account_number
			,subscriber_id
			,event_id
			,event_start_utc
			,event_end_utc
		INTO #side_a
		FROM V289_M07_dp_data_sv_tempshelf

		COMMIT WORK

		CREATE hg INDEX hg1 ON #side_a (account_number)

		CREATE hg INDEX hg2 ON #side_a (subscriber_id)

		CREATE hg INDEX hg3 ON #side_a (event_id)

		CREATE dttm INDEX dttm1 ON #side_a (event_start_utc)

		CREATE dttm INDEX dttm2 ON #side_a (event_end_utc)

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | Begining  M07.3 - Flagging Overlapping Events - Checkpoint B' TO client

		SELECT account_number
			,event_id
			,event_start_utc
			,event_end_utc
		INTO #side_b
		FROM V289_M07_dp_data_sv_tempshelf

		COMMIT WORK

		CREATE hg INDEX hg1 ON #side_b (account_number)

		CREATE hg INDEX hg3 ON #side_b (event_id)

		CREATE dttm INDEX dttm1 ON #side_b (event_start_utc)

		CREATE dttm INDEX dttm2 ON #side_b (event_end_utc)

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | Begining  M07.3 - Flagging Overlapping Events - Checkpoint C' TO client

		SELECT side_a.*
			,'event_start_b' = side_b.event_start_utc
			,'event_end_b' = side_b.event_end_utc
			,'event_index' = dense_rank() OVER (
				PARTITION BY side_a.account_number ORDER BY side_a.event_id ASC
				)
		INTO v289_m07_events_overlap
		FROM #side_a AS side_a
		JOIN #side_b AS side_b ON side_a.account_number = side_b.account_number
			AND (
				(
					side_a.event_start_utc > side_b.event_Start_utc
					AND side_a.event_start_utc < side_b.event_end_utc
					)
				OR (
					side_a.event_end_utc > side_b.event_Start_utc
					AND side_a.event_end_utc < side_b.event_end_utc
					)
				OR (
					side_b.event_Start_utc > side_a.event_start_utc
					AND side_b.event_Start_utc < side_a.event_end_utc
					)
				OR (
					side_b.event_end_utc > side_a.event_Start_utc
					AND side_b.event_end_utc < side_a.event_end_utc
					)
				)

		COMMIT WORK

		DROP TABLE #side_a

		COMMIT WORK

		DROP TABLE #side_b

		COMMIT WORK

		CREATE hg INDEX hg1 ON v289_m07_events_overlap (account_number)

		CREATE hg INDEX hg2 ON v289_m07_events_overlap (subscriber_id)

		CREATE hg INDEX hg3 ON v289_m07_events_overlap (event_id)

		CREATE dttm INDEX dttm1 ON v289_m07_events_overlap (event_start_utc)

		CREATE dttm INDEX dttm2 ON v289_m07_events_overlap (event_end_utc)

		CREATE dttm INDEX dttm3 ON v289_m07_events_overlap (event_start_b)

		CREATE dttm INDEX dttm4 ON v289_m07_events_overlap (event_end_b)

		COMMIT WORK

		GRANT SELECT
			ON v289_m07_events_overlap
			TO vespa_group_low_security

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M07.3: Flagging Overlapping Events DONE' TO client

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = '${SQLFILE_ARG001}'
					AND upper(tname) = upper('v289_m07_overlaps_chunks')
					AND tabletype = 'TABLE'
				)
			DROP TABLE v289_m07_overlaps_chunks

		COMMIT WORK

		SELECT *
			,'chunk_end' = min(chunk_start) OVER (
				PARTITION BY account_number
				,event_id ORDER BY chunk_start ASC rows BETWEEN 1 following
						AND 1 following
				)
		INTO v289_m07_overlaps_chunks
		FROM (
			SELECT DISTINCT *
			FROM (
				SELECT account_number
					,event_id
					,'chunk_start' = event_start_utc
					,event_index
					,'theflag' = 1
				FROM v289_m07_events_overlap
				
				UNION ALL
				
				SELECT account_number
					,event_id
					,'chunk_start' = event_end_utc
					,event_index
					,'theflag' = 0
				FROM v289_m07_events_overlap
				
				UNION ALL
				
				SELECT account_number
					,event_id
					,'chunk_start' = event_start_b
					,event_index
					,'theflag' = 0
				FROM v289_m07_events_overlap
				WHERE event_start_b > event_start_utc
				
				UNION ALL
				
				SELECT account_number
					,event_id
					,'chunk_start' = event_end_b
					,event_index
					,'theflag' = 0
				FROM v289_m07_events_overlap
				WHERE event_end_b <= event_end_utc
				) AS base
			) AS base2

		COMMIT WORK

		CREATE hg INDEX hg1 ON v289_m07_overlaps_chunks (account_number)

		CREATE hg INDEX hg2 ON v289_m07_overlaps_chunks (event_id)

		CREATE dttm INDEX dttm1 ON v289_m07_overlaps_chunks (chunk_start)

		CREATE dttm INDEX dttm2 ON v289_m07_overlaps_chunks (chunk_end)

		COMMIT WORK

		GRANT SELECT
			ON v289_m07_overlaps_chunks
			TO vespa_group_low_security

		COMMIT WORK

		DROP TABLE v289_m07_events_overlap

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M07.3: Breaking Overlapping Events into Chunks DONE' TO client

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = '${SQLFILE_ARG001}'
					AND upper(tname) = upper('v289_m07_overlap_batches_sv')
					AND tabletype = 'TABLE'
				)
			DROP TABLE ${SQLFILE_ARG001}.v289_m07_overlap_batches_sv

		COMMIT WORK

		SELECT side_a.*
			,'thebatch' = dense_rank() OVER (
				PARTITION BY side_a.account_number ORDER BY side_a.chunk_start ASC
				)
		INTO v289_m07_overlap_batches_sv
		FROM v289_m07_overlaps_chunks AS side_a
		JOIN (
			SELECT DISTINCT account_number
				,event_id
				,chunk_start
			FROM v289_m07_overlaps_chunks
			WHERE theflag = 1
			) AS side_b ON side_a.account_number = side_b.account_number
			AND side_a.event_id = side_b.event_id
		WHERE side_a.chunk_end IS NOT NULL
			AND side_b.chunk_start <= side_a.chunk_start
			AND side_a.chunk_start <> side_a.chunk_end

		COMMIT WORK

		CREATE hg INDEX hg1 ON ${SQLFILE_ARG001}.v289_m07_overlap_batches_sv (account_number)

		CREATE hg INDEX hg2 ON ${SQLFILE_ARG001}.v289_m07_overlap_batches_sv (event_id)

		CREATE dttm INDEX dttm1 ON ${SQLFILE_ARG001}.v289_m07_overlap_batches_sv (chunk_start)

		CREATE dttm INDEX dttm2 ON ${SQLFILE_ARG001}.v289_m07_overlap_batches_sv (chunk_end)

		CREATE lf INDEX lf1 ON ${SQLFILE_ARG001}.v289_m07_overlap_batches_sv (thebatch)

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.v289_m07_overlap_batches_sv
			TO vespa_group_low_security

		COMMIT WORK

		DROP TABLE v289_m07_overlaps_chunks

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M07.3: Assembling batches of overlaps DONE' TO client message convert(TIMESTAMP, now()) || ' | Begining  M07.4 - Returning Results' TO client

		SELECT event_id
			,thebatch
		INTO #longest_batch
		FROM (
			SELECT event_id
				,thebatch
				,'dur_rank' = dense_rank() OVER (
					PARTITION BY event_id ORDER BY datediff(ss, chunk_start, chunk_end) DESC
						,chunk_start ASC
					)
			FROM v289_m07_overlap_batches_sv
			) AS long_batch
		WHERE dur_rank = 1

		COMMIT WORK

		CREATE hg INDEX i1 ON #longest_batch (event_id)

		CREATE hg INDEX i2 ON #longest_batch (thebatch)

		COMMIT WORK

		INSERT INTO V289_M07_dp_data_sv (
			account_number
			,subscriber_id
			,event_id
			,event_Start_utc
			,event_end_utc
			,chunk_start
			,chunk_end
			,event_duration_seg
			,chunk_duration_seg
			,programme_genre
			,session_daypart
			,hhsize
			,viewer_hhsize
			,channel_pack
			,segment_id
			,Overlap_batch
			,session_size
			,event_start_dim
			,event_end_dim
			,service_key
			,provider_id
			,provider_id_number
			,viewing_type_flag
			,barb_min_start_date_time_utc
			,barb_min_end_date_time_utc
			)
		SELECT dpdata.account_number
			,dpdata.subscriber_id
			,dpdata.event_id
			,dpdata.event_start_utc
			,dpdata.event_end_utc
			,overlap.chunk_start
			,overlap.Chunk_end
			,dpdata.event_duration_seg
			,'chunk_duration_seg' = CASE 
				WHEN overlap.chunk_start IS NOT NULL
					THEN datediff(second, overlap.chunk_start, overlap.chunk_end)
				ELSE NULL
				END
			,dpdata.programme_genre
			,dpdata.session_daypart
			,dpdata.hhsize
			,0
			,dpdata.channel_pack
			,dpdata.segment_id
			,overlap.thebatch
			,'session_size' = 0
			,dpdata.event_start_dim
			,dpdata.event_end_dim
			,dpdata.service_key
			,dpdata.provider_id
			,dpdata.provider_id_number
			,dpdata.viewing_type_flag
			,CASE 
				WHEN overlap.event_id IS NULL
					THEN CASE 
							WHEN barb_min_start_date_time_utc <> '2999-12-31 00:00:00'
								THEN barb_min_start_date_time_utc
							ELSE NULL
							END
				WHEN overlap.event_id IS NOT NULL
					THEN CASE 
							WHEN l.thebatch IS NULL
								THEN NULL
							ELSE CASE 
									WHEN barb_min_start_date_time_utc <> '2999-12-31 00:00:00'
										THEN barb_min_start_date_time_utc
									ELSE NULL
									END
							END
				END
			,CASE 
				WHEN overlap.event_id IS NULL
					THEN CASE 
							WHEN barb_min_end_date_time_utc <> '1970-01-01 00:00:00'
								THEN barb_min_end_date_time_utc
							ELSE NULL
							END
				WHEN overlap.event_id IS NOT NULL
					THEN CASE 
							WHEN l.thebatch IS NULL
								THEN NULL
							ELSE CASE 
									WHEN barb_min_end_date_time_utc <> '1970-01-01 00:00:00'
										THEN barb_min_end_date_time_utc
									ELSE NULL
									END
							END
				END
		FROM V289_M07_dp_data_sv_tempshelf AS dpdata
		LEFT OUTER JOIN v289_m07_overlap_batches_sv AS overlap ON dpdata.account_number = overlap.account_number
			AND dpdata.event_id = overlap.event_id
		LEFT OUTER JOIN #longest_batch AS l ON overlap.event_id = l.event_id
			AND overlap.thebatch = l.thebatch

		COMMIT WORK

		DROP TABLE V289_M07_dp_data_sv_tempshelf

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M07.4: Output table V289_M07_dp_data_sv DONE' TO client
	END
	ELSE
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M07.0: Missing DP Viewing Data to prepare( v289_M06_dp_raw_data_sv empty)!!!' TO client
	END message convert(TIMESTAMP, now()) || ' | M07 Finished' TO client
END;
GO 
commit;
