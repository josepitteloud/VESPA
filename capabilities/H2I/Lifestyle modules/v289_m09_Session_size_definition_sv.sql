create or replace procedure ${SQLFILE_ARG001}.v289_m09_Session_size_definition_sv
AS
BEGIN
	message convert(TIMESTAMP, now()) || ' | Begining M09.0 - Initialising Environment' TO client

	DECLARE @account VARCHAR(20)
	DECLARE @subs DECIMAL(10)
	DECLARE @iter TINYINT
	DECLARE @cont TINYINT
	DECLARE @event BIGINT
	DECLARE @length DECIMAL(7, 6)
	DECLARE @random REAL
	DECLARE @s_size TINYINT
	DECLARE @adj_hh TINYINT
	DECLARE @hh_size TINYINT
	DECLARE @segment TINYINT
	DECLARE @batch TINYINT
	DECLARE @row_id INTEGER
	DECLARE @event_id BIGINT
	DECLARE @maxi TINYINT

	COMMIT WORK

	SELECT 'overlap_size' = count(event_id)
		,account_number
		,Overlap_batch
	INTO #tmp1
	FROM V289_M07_dp_data_sv
	WHERE Overlap_batch IS NOT NULL
	GROUP BY Overlap_batch
		,account_number

	COMMIT WORK

	CREATE hg INDEX tmp1_idx_1 ON #tmp1 (account_number)

	CREATE lf INDEX tmp1_idx_2 ON #tmp1 (overlap_batch)

	COMMIT WORK

	IF EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = 'TEMP_EVENT'
			)
		DROP TABLE ${SQLFILE_ARG001}temp_event

	SELECT event_ID
		,dt.account_number
		,dt.subscriber_id
		,'event_dt' = convert(DATE, event_start_utc)
		,'new_hh_size' = CASE 
			WHEN hhsize > viewer_hhsize
				THEN viewer_hhsize
			ELSE hhsize
			END
		,'hhsize_' = CASE 
			WHEN new_hh_size > 8
				THEN 8
			ELSE new_hh_size
			END
		,'segment_ID' = COALESCE(dt.segment_ID, 2)
		,'random1' = RAND(dt.event_id + DATEPART(us, GETDATE()))
		,'overlap' = ov.overlap_size
		,'overlap_batch' = COALESCE(dt.overlap_batch, 0)
		,'box_rank' = dense_rank() OVER (
			PARTITION BY dt.account_number
			,dt.Overlap_batch ORDER BY subscriber_id ASC
				,event_end_utc DESC
			)
		,'session_size' = convert(TINYINT, 0)
	INTO temp_event
	FROM V289_M07_dp_data_sv AS dt
	LEFT OUTER JOIN #tmp1 AS ov ON ov.account_number = dt.account_number
		AND ov.Overlap_batch = dt.overlap_batch
	WHERE hhsize_ > 0
		AND session_size = 0 message convert(TIMESTAMP, now()) || ' | @ M09.1: temp_Event Table created: ' || @@rowcount TO client

	COMMIT WORK

	CREATE hg INDEX ide1 ON ${SQLFILE_ARG001}temp_event (event_ID)

	CREATE lf INDEX ide2 ON ${SQLFILE_ARG001}temp_event (overlap_batch)

	CREATE lf INDEX ide3 ON ${SQLFILE_ARG001}temp_event (segment_ID)

	CREATE lf INDEX ide4 ON ${SQLFILE_ARG001}temp_event (hhsize_)

	COMMIT WORK

	DROP TABLE #tmp1

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M09.3: Multi Box events started ' || @@rowcount TO client

	IF EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = 'EVENTS_1_BOX'
			)
		DROP TABLE ${SQLFILE_ARG001}events_1_box

	SELECT *
		,'row_id' = row_number() OVER (
			ORDER BY subscriber_id ASC
			)
		,'ev_proc_flag' = convert(BIT, 0)
		,'adj_hh' = hhsize_ - overlap + 1
		,'length_1' = convert(DECIMAL(7, 6), 0)
	INTO events_1_box
	FROM temp_event
	WHERE session_size = 0
		AND hhsize_ IS NOT NULL
		AND box_rank = 1
		AND overlap IS NOT NULL
	ORDER BY account_number ASC
		,subscriber_id ASC
		,overlap_batch ASC message convert(TIMESTAMP, now()) || ' | @ M09.3: Multi Box primary box table populated: ' || @@rowcount TO client

	COMMIT WORK

	CREATE hg INDEX idxe1 ON ${SQLFILE_ARG001}events_1_box (event_ID)

	CREATE lf INDEX id1 ON ${SQLFILE_ARG001}events_1_box (overlap_batch)

	CREATE hg INDEX id2 ON ${SQLFILE_ARG001}events_1_box (subscriber_id)

	CREATE lf INDEX box ON ${SQLFILE_ARG001}events_1_box (box_rank)

	CREATE hg INDEX box1 ON ${SQLFILE_ARG001}events_1_box (length_1)

	CREATE lf INDEX box2 ON ${SQLFILE_ARG001}events_1_box (adj_hh)

	COMMIT WORK

	UPDATE events_1_box
	SET adj_hh = 1
	WHERE adj_hh < 1

	UPDATE events_1_box AS ev
	SET length_1 = upper_limit
		,random1 = random1 * upper_limit
	FROM events_1_box AS ev
	JOIN v289_sessionsize_matrix_sv_sv_default AS mx ON mx.segment_ID = ev.segment_id
		AND ev.hhsize_ = mx.viewing_size
		AND ev.adj_hh = mx.session_size

	COMMIT WORK

	SELECT ev.event_ID
		,ev.overlap_batch
		,ev.segment_id
		,ev.hhsize_
		,ev.event_dt
		,ev1.adj_hh
		,ev1.random1
	INTO #tmp1
	FROM temp_event AS ev
	JOIN events_1_box AS ev1 ON ev.event_ID = ev1.event_ID
		AND ev.overlap_batch = ev1.overlap_batch

	COMMIT WORK

	CREATE hg INDEX tmp1_idx_1 ON #tmp1 (event_ID)

	CREATE lf INDEX tmp1_idx_2 ON #tmp1 (overlap_batch)

	COMMIT WORK

	SELECT tmp.*
		,'mx_session_size' = mx.session_size
	INTO #tmp2
	FROM #tmp1 AS tmp
	JOIN v289_sessionsize_matrix_sv_sv_default AS mx ON mx.segment_ID = tmp.segment_id
		AND tmp.hhsize_ = mx.viewing_size
		AND tmp.random1 > mx.lower_limit
		AND tmp.random1 <= mx.upper_limit

	COMMIT WORK

	CREATE hg INDEX tmp2_idx_1 ON #tmp2 (event_ID)

	CREATE lf INDEX tmp2_idx_2 ON #tmp2 (overlap_batch)

	COMMIT WORK

	SELECT tmp.*
		,'sm_session_size' = sm.session_size
		,'ev_session_size' = COALESCE(sm_session_size, mx_session_size)
	INTO #tmp3
	FROM #tmp2 AS tmp
	LEFT OUTER JOIN v289_sessionsize_matrix_sv_sv AS sm ON sm.segment_ID = tmp.segment_id
		AND tmp.hhsize_ = sm.viewing_size
		AND tmp.adj_hh >= sm.session_size
		AND tmp.random1 > sm.lower_limit
		AND tmp.random1 <= sm.upper_limit
		AND tmp.event_dt = sm.thedate

	COMMIT WORK

	CREATE hg INDEX tmp3_idx_1 ON #tmp3 (event_ID)

	CREATE lf INDEX tmp3_idx_2 ON #tmp3 (overlap_batch)

	COMMIT WORK

	UPDATE temp_event AS ev
	SET ev.session_size = tmp.ev_session_size
	FROM temp_event AS ev
	JOIN #tmp3 AS tmp ON ev.event_ID = tmp.event_ID
		AND ev.overlap_batch = tmp.overlap_batch
		AND ev.segment_id = tmp.segment_id
		AND ev.hhsize_ = tmp.hhsize_
		AND ev.event_dt = tmp.event_dt

	DROP TABLE #tmp1

	DROP TABLE #tmp2

	DROP TABLE #tmp3

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M09.3: Multi Box primary box events updated: ' || @@rowcount TO client

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M09.4: Multi Box Other boxes loop started ' TO client

	SET @cont = 2
	SET @maxi = (
			SELECT MAX(box_rank) + 1
			FROM temp_event
			WHERE overlap IS NOT NULL
			)
	SET @maxi = CASE 
			WHEN @maxi > 15
				THEN 15
			ELSE @maxi
			END

	WHILE @cont <= @maxi
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M09.4: Multi Box start box #: ' || @cont TO client

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = '${SQLFILE_ARG001}'
					AND tabletype = 'TABLE'
					AND upper(tname) = 'EVENTS_1_BOX'
				)
			TRUNCATE TABLE ${SQLFILE_ARG001}events_1_box

		COMMIT WORK

		SELECT Overlap_batch
			,account_number
			,'s_size' = SUM(session_size)
			,'boxes' = COUNT(subscriber_id)
		INTO #tmp1
		FROM temp_event
		GROUP BY Overlap_batch
			,account_number

		COMMIT WORK

		CREATE hg INDEX tmp1_idx_1 ON #tmp1 (account_number)

		CREATE lf INDEX tmp2_idx_2 ON #tmp1 (overlap_batch)

		COMMIT WORK

		INSERT INTO events_1_box
		SELECT te.*
			,'row_id' = row_number() OVER (
				ORDER BY subscriber_id ASC
				)
			,'ev_proc_flag' = convert(BIT, 0)
			,'adj_hh' = te.hhsize_ - v.s_size - (v.boxes - @cont)
			,'length_1' = convert(DECIMAL(7, 6), 0)
		FROM temp_event AS te
		JOIN #tmp1 AS v ON v.Overlap_batch = te.Overlap_batch
			AND v.account_number = te.account_number
		WHERE te.session_size = 0
			AND te.hhsize_ IS NOT NULL
			AND te.box_rank = @cont
			AND te.overlap IS NOT NULL
		ORDER BY te.account_number ASC
			,te.subscriber_id ASC
			,te.overlap_batch ASC

		COMMIT WORK

		DROP TABLE #tmp1

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M09.4: Multi Box events_1_box table populated: ' || @@rowcount TO client

		UPDATE events_1_box
		SET adj_hh = 1
		WHERE adj_hh < 1

		UPDATE events_1_box AS ev
		SET length_1 = upper_limit
			,random1 = random1 * upper_limit
		FROM events_1_box AS ev
		JOIN v289_sessionsize_matrix_sv_sv_default AS mx ON mx.segment_ID = ev.segment_id
			AND ev.hhsize_ = mx.viewing_size
			AND ev.adj_hh = mx.session_size

		SELECT ev.event_ID
			,ev.overlap_batch
			,ev.segment_id
			,ev.hhsize_
			,ev.event_dt
			,ev1.adj_hh
			,ev1.random1
		INTO #tmp1
		FROM temp_event AS ev
		JOIN events_1_box AS ev1 ON ev.event_ID = ev1.event_ID
			AND ev.overlap_batch = ev1.overlap_batch

		COMMIT WORK

		CREATE hg INDEX tmp1_idx_1 ON #tmp1 (event_ID)

		CREATE lf INDEX tmp1_idx_2 ON #tmp1 (overlap_batch)

		COMMIT WORK

		SELECT tmp.*
			,'mx_session_size' = mx.session_size
		INTO #tmp2
		FROM #tmp1 AS tmp
		JOIN v289_sessionsize_matrix_sv_sv_default AS mx ON mx.segment_ID = tmp.segment_id
			AND tmp.hhsize_ = mx.viewing_size
			AND tmp.random1 > mx.lower_limit
			AND tmp.random1 <= mx.upper_limit

		COMMIT WORK

		CREATE hg INDEX tmp2_idx_1 ON #tmp2 (event_ID)

		CREATE lf INDEX tmp2_idx_2 ON #tmp2 (overlap_batch)

		COMMIT WORK

		SELECT tmp.*
			,'sm_session_size' = sm.session_size
			,'ev_session_size' = COALESCE(sm_session_size, mx_session_size)
		INTO #tmp3
		FROM #tmp2 AS tmp
		LEFT OUTER JOIN v289_sessionsize_matrix_sv_sv AS sm ON sm.segment_ID = tmp.segment_id
			AND tmp.hhsize_ = sm.viewing_size
			AND tmp.adj_hh >= sm.session_size
			AND tmp.random1 > sm.lower_limit
			AND tmp.random1 <= sm.upper_limit
			AND tmp.event_dt = sm.thedate

		COMMIT WORK

		CREATE hg INDEX tmp3_idx_1 ON #tmp3 (event_ID)

		CREATE lf INDEX tmp3_idx_2 ON #tmp3 (overlap_batch)

		COMMIT WORK

		UPDATE temp_event AS ev
		SET ev.session_size = tmp.ev_session_size
		FROM temp_event AS ev
		JOIN #tmp3 AS tmp ON ev.event_ID = tmp.event_ID
			AND ev.overlap_batch = tmp.overlap_batch
			AND ev.segment_id = tmp.segment_id
			AND ev.hhsize_ = tmp.hhsize_
			AND ev.event_dt = tmp.event_dt

		DROP TABLE #tmp1

		DROP TABLE #tmp2

		DROP TABLE #tmp3

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M09.3: Multi Box box#: ' || @cont || '  events updated: ' || @@rowcount TO client

		COMMIT WORK

		SET @cont = @cont + 1
	END

	UPDATE V289_M07_dp_data_sv AS dt
	SET dt.session_size = te.session_size
	FROM V289_M07_dp_data_sv AS dt
	JOIN temp_event AS te ON te.event_id = dt.event_id
		AND te.overlap_batch = dt.overlap_batch
	WHERE overlap IS NOT NULL message convert(TIMESTAMP, now()) || ' | @ M09.4: Multi Box events updated: ' || @@rowcount TO client

	IF EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = 'OVERLAPPING_EV'
			)
		DROP TABLE ${SQLFILE_ARG001}overlapping_ev

	SELECT 'thedate' = DATE (base.event_Start_utc)
		,base.segment_id
		,'viewing_size' = base.viewer_hhsize
		,base.session_size
		,'overl_minutes' = SUM(event_duration_seg) / 60
		,totals.tot_uk_hhwatched
		,'prop' = convert(REAL, overl_minutes) / tot_uk_hhwatched
	INTO overlapping_ev
	FROM V289_M07_dp_data_sv AS base
	JOIN (
		SELECT 'thedate' = DATE (event_Start_utc)
			,segment_id
			,'viewing_size' = viewer_hhsize
			,'tot_uk_hhwatched' = sum(event_duration_seg) / 60
		FROM V289_M07_dp_data_sv
		GROUP BY thedate
			,segment_id
			,viewing_size
		) AS totals ON thedate = totals.thedate
		AND base.segment_id = totals.segment_id
		AND viewing_size = totals.viewing_size
	WHERE totals.tot_uk_hhwatched > 0
		AND session_size > 0
	GROUP BY thedate
		,base.segment_id
		,viewing_size
		,base.session_size
		,tot_uk_hhwatched

	COMMIT WORK

	CREATE lf INDEX lfx ON ${SQLFILE_ARG001}overlapping_ev (segment_id)

	CREATE lf INDEX lfx2 ON ${SQLFILE_ARG001}overlapping_ev (viewing_size)

	COMMIT WORK

	IF EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = 'NEW_MATRIX'
			)
		DROP TABLE ${SQLFILE_ARG001}new_matrix

	SELECT mx.thedate
		,mx.segment_id
		,mx.viewing_size
		,mx.session_size
		,'new_min' = SUM(uk_hours_watched) * (1 - COALESCE(prop, 0))
		,'new_total' = SUM(new_min) OVER (
			PARTITION BY mx.thedate
			,mx.segment_id
			,mx.viewing_size
			)
		,'proportion' = new_min / new_total
		,'Lower_Limit' = convert(REAL, 0)
		,'Upper_limit' = convert(REAL, 0)
	INTO new_matrix
	FROM v289_sessionsize_matrix_sv_sv AS mx
	LEFT OUTER JOIN overlapping_ev AS b ON mx.thedate = b.thedate
		AND mx.segment_id = b.segment_id
		AND mx.viewing_size = b.viewing_size
		AND mx.session_size = b.session_size
	WHERE prop <> 1
	GROUP BY mx.thedate
		,mx.segment_id
		,mx.viewing_size
		,mx.session_size
		,prop

	SELECT *
		,'Low' = coalesce((
				SUM(proportion) OVER (
					PARTITION BY thedate
					,segment_ID
					,viewing_size ORDER BY session_size ASC rows BETWEEN unbounded preceding
							AND 1 preceding
					)
				), 0)
		,'Up' = SUM(proportion) OVER (
			PARTITION BY thedate
			,segment_ID
			,viewing_size ORDER BY session_size ASC rows BETWEEN unbounded preceding
					AND CURRENT row
			)
	INTO #ttt
	FROM new_matrix

	UPDATE new_matrix AS a
	SET lower_limit = low
		,upper_limit = up
	FROM new_matrix AS a
	JOIN #ttt AS b ON a.segment_id = b.segment_id
		AND a.thedate = b.thedate
		AND a.session_size = b.session_size
		AND a.viewing_size = b.viewing_size

	UPDATE temp_event AS ev
	SET ev.session_size = COALESCE(sm.session_size, mx.session_size)
	FROM temp_event AS ev
	LEFT OUTER JOIN new_matrix AS sm ON sm.segment_ID = ev.segment_id
		AND ev.hhsize_ = sm.viewing_size
		AND random1 > sm.lower_limit
		AND random1 <= sm.upper_limit
		AND ev.event_dt = sm.thedate
	JOIN v289_sessionsize_matrix_sv_sv_default AS mx ON mx.segment_ID = ev.segment_id
		AND ev.hhsize_ = mx.viewing_size
		AND random1 > mx.lower_limit
		AND random1 <= mx.upper_limit
	WHERE (
			Overlap_batch = 0
			OR overlap = 1
			) message convert(TIMESTAMP, now()) || ' | @ M09.2: Single Box events done: ' || @@rowcount TO client

	UPDATE V289_M07_dp_data_sv AS dt
	SET dt.session_size = te.session_size
	FROM V289_M07_dp_data_sv AS dt
	JOIN temp_event AS te ON te.event_id = dt.event_id
	WHERE te.overlap_batch = 0 message convert(TIMESTAMP, now()) || ' | @ M09.4: Single Box events updated: ' || @@rowcount TO client

	COMMIT WORK
END;
GO 
commit;
