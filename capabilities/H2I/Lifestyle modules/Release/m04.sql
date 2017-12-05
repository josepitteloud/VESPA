CREATE OR REPLACE PROCEDURE ${SQLFILE_ARG001}.v289_m04_barb_data_preparation_sv (@processing_date DATE = NULL)
AS
BEGIN
	message convert(TIMESTAMP, now ()) || ' | Begining M04.0 - Initialising Environment' TO client

	DECLARE @a INTEGER

	SELECT @a = count (1)
	FROM barb_weights_sv

	IF @a > 0
	BEGIN
		message convert(TIMESTAMP, now ()) || ' | @ M04.0: Initialising Environment DONE' TO client message convert(TIMESTAMP, now ()) || ' | Begining M04.1 - Preparing transient tables' TO client

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = '${SQLFILE_ARG001}'
					AND upper (tname) = upper ('skybarb_sv')
					AND tabletype = 'TABLE'
				)
			DROP TABLE skybarb_sv

		COMMIT WORK

		SELECT 'house_id' = demo.household_number
			,'person' = demo.person_number
			,'age' = datediff (
				yy
				,demo.date_of_birth
				,@processing_date
				)
			,'sex' = CASE 
				WHEN demo.sex_code = 1
					THEN 'Male'
				WHEN demo.sex_code = 2
					THEN 'Female'
				ELSE 'Unknown'
				END
			,'head' = CASE 
				WHEN demo.household_status IN (
						4
						,2
						)
					THEN 1
				ELSE 0
				END
			,'digital_hh' = s4
		INTO skybarb_sv
		FROM BARB_INDV_PANELMEM_DET AS demo
		JOIN (
			SELECT whole.household_number
				,'s1' = min (analogue_terrestrial)
				,'s2' = min (digital_terrestrial)
				,'s3' = min (analogue_stallite)
				,'s4' = min (digital_satellite)
				,'s5' = min (analogue_cable)
				,'s6' = min (digital_cable)
			FROM BARB_PANEL_DEMOGR_TV_CHAR AS whole
			JOIN (
				SELECT DISTINCT household_number
				FROM BARB_PANEL_DEMOGR_TV_CHAR
				WHERE @processing_date BETWEEN date_valid_from
						AND date_valid_to
					AND (
						reception_capability_code_1 = 2
						OR reception_capability_code_2 = 2
						OR reception_capability_code_3 = 2
						OR reception_capability_code_4 = 2
						OR reception_capability_code_5 = 2
						OR reception_capability_code_6 = 2
						OR reception_capability_code_7 = 2
						OR reception_capability_code_8 = 2
						OR reception_capability_code_9 = 2
						OR reception_capability_code_10 = 2
						)
				) AS skycap ON whole.household_number = skycap.household_number
			GROUP BY whole.household_number
			) AS barb_sky_panelists ON demo.household_number = barb_sky_panelists.household_number
		JOIN barb_weights_sv AS b ON demo.household_number = b.household_number
			AND demo.person_number = b.person_number
		WHERE @processing_date BETWEEN demo.date_valid_from
				AND demo.date_valid_to
			AND demo.person_membership_status = 0

		COMMIT WORK

		CREATE hg INDEX hg1 ON skybarb_sv (house_id)

		CREATE lf INDEX lf1 ON skybarb_sv (person)

		COMMIT WORK

		GRANT SELECT
			ON skybarb_sv
			TO vespa_group_low_security

		COMMIT WORK message convert(TIMESTAMP, now ()) || ' | @ M04.1: Preparing transient tables DONE' TO client message convert(TIMESTAMP, now ()) || ' | Begining M04.2 - Final BARB Data Preparation' TO client

		IF EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = '${SQLFILE_ARG001}'
					AND upper (tname) = upper ('skybarb_sv_fullview_sv')
					AND tabletype = 'TABLE'
				)
			DROP TABLE skybarb_sv_fullview_sv

		COMMIT WORK

		SELECT mega.*
			,z.sex
			,'ageband' = CASE 
				WHEN z.age BETWEEN 0
						AND 19
					THEN '0-19'
				WHEN z.age BETWEEN 20
						AND 44
					THEN '20-44'
				WHEN z.age >= 45
					THEN '45+'
				END
		INTO skybarb_sv_fullview_sv
		FROM (
			SELECT 'hhsize' = barbskyhhsize.thesize
				,barbskyhhsize.hh_weight
				,base.*
			FROM (
				SELECT viewing.household_number
					,viewing.pvf_pv2
					,'session_id' = dense_rank () OVER (
						PARTITION BY convert(DATE, viewing.local_start_time_of_session)
						,viewing.household_number ORDER BY viewing.set_number ASC
							,viewing.local_start_time_of_session ASC
						)
					,'event_id' = dense_rank () OVER (
						PARTITION BY viewing.household_number ORDER BY viewing.local_tv_event_start_date_time || '-' || viewing.set_number ASC
						)
					,set_number
					,viewing.programme_name
					,'start_time_of_session' = local_start_time_of_session
					,'end_time_of_session' = local_end_time_of_session
					,'instance_start' = local_tv_instance_start_date_time
					,'instance_end' = local_tv_instance_end_date_time
					,'event_Start' = local_tv_event_start_date_time
					,duration_of_session
					,db1_station_code
					,'session_start_date_time' = CASE 
						WHEN local_start_time_of_recording IS NULL
							THEN local_start_time_of_session
						ELSE local_start_time_of_recording
						END
					,'session_end_date_time' = CASE 
						WHEN local_start_time_of_recording IS NULL
							THEN local_end_time_of_session
						ELSE dateadd (
								mi
								,Duration_of_session
								,local_start_time_of_recording
								)
						END
					,'session_daypart' = CASE 
						WHEN convert(TIME, local_start_time_of_session) BETWEEN '00:00:00.000'
								AND '05:59:59.000'
							THEN 'night'
						WHEN convert(TIME, local_start_time_of_session) BETWEEN '06:00:00.000'
								AND '08:59:59.000'
							THEN 'breakfast'
						WHEN convert(TIME, local_start_time_of_session) BETWEEN '09:00:00.000'
								AND '11:59:59.000'
							THEN 'morning'
						WHEN convert(TIME, local_start_time_of_session) BETWEEN '12:00:00.000'
								AND '14:59:59.000'
							THEN 'lunch'
						WHEN convert(TIME, local_start_time_of_session) BETWEEN '15:00:00.000'
								AND '17:59:59.000'
							THEN 'early prime'
						WHEN convert(TIME, local_start_time_of_session) BETWEEN '18:00:00.000'
								AND '20:59:59.000'
							THEN 'prime'
						WHEN convert(TIME, local_start_time_of_session) BETWEEN '21:00:00.000'
								AND '23:59:59.000'
							THEN 'late night'
						END
					,'service_key' = coalesce (
						viewing.service_key
						,181818
						)
					,'channel_pack' = coalesce (
						viewing.channel_pack
						,'Unknown'
						)
					,viewing.channel_name
					,'programme_genre' = coalesce (
						viewing.genre_description
						,'Unknown'
						)
					,weights.person_number
					,'processing_weight' = weights.processing_weight
					,'person_1' = CASE 
						WHEN person_1_viewing = 1
							AND person_number = 1
							THEN processing_weight * duration_of_session
						ELSE 0
						END
					,'person_2' = CASE 
						WHEN person_2_viewing = 1
							AND person_number = 2
							THEN processing_weight * duration_of_session
						ELSE 0
						END
					,'person_3' = CASE 
						WHEN person_3_viewing = 1
							AND person_number = 3
							THEN processing_weight * duration_of_session
						ELSE 0
						END
					,'person_4' = CASE 
						WHEN person_4_viewing = 1
							AND person_number = 4
							THEN processing_weight * duration_of_session
						ELSE 0
						END
					,'person_5' = CASE 
						WHEN person_5_viewing = 1
							AND person_number = 5
							THEN processing_weight * duration_of_session
						ELSE 0
						END
					,'person_6' = CASE 
						WHEN person_6_viewing = 1
							AND person_number = 6
							THEN processing_weight * duration_of_session
						ELSE 0
						END
					,'person_7' = CASE 
						WHEN person_7_viewing = 1
							AND person_number = 7
							THEN processing_weight * duration_of_session
						ELSE 0
						END
					,'person_8' = CASE 
						WHEN person_8_viewing = 1
							AND person_number = 8
							THEN processing_weight * duration_of_session
						ELSE 0
						END
					,'person_9' = CASE 
						WHEN person_9_viewing = 1
							AND person_number = 9
							THEN processing_weight * duration_of_session
						ELSE 0
						END
					,'person_10' = CASE 
						WHEN person_10_viewing = 1
							AND person_number = 10
							THEN processing_weight * duration_of_session
						ELSE 0
						END
					,'person_11' = CASE 
						WHEN person_11_viewing = 1
							AND person_number = 11
							THEN processing_weight * duration_of_session
						ELSE 0
						END
					,'person_12' = CASE 
						WHEN person_12_viewing = 1
							AND person_number = 12
							THEN processing_weight * duration_of_session
						ELSE 0
						END
					,'person_13' = CASE 
						WHEN person_13_viewing = 1
							AND person_number = 13
							THEN processing_weight * duration_of_session
						ELSE 0
						END
					,'person_14' = CASE 
						WHEN person_14_viewing = 1
							AND person_number = 14
							THEN processing_weight * duration_of_session
						ELSE 0
						END
					,'person_15' = CASE 
						WHEN person_15_viewing = 1
							AND person_number = 15
							THEN processing_weight * duration_of_session
						ELSE 0
						END
					,'person_16' = CASE 
						WHEN person_16_viewing = 1
							AND person_number = 16
							THEN processing_weight * duration_of_session
						ELSE 0
						END
					,'theflag' = person_1 + person_2 + person_3 + person_4 + person_5 + person_6 + person_7 + person_8 + person_9 + person_10 + person_11 + person_12 + person_13 + person_14 + person_15 + person_16
					,broadcast_start_date_time_local
					,broadcast_end_date_time_local
					,'progwatch_duration' = barb_instance_duration
					,'progscaled_duration' = progwatch_duration * processing_weight
					,viewing.viewing_platform
					,viewing.sky_stb_viewing
				FROM barb_daily_ind_prog_viewed AS viewing
				JOIN barb_weights_sv AS weights ON viewing.household_number = weights.household_number
				LEFT OUTER JOIN (
					SELECT s.service_key
						,l.programme_genre
						,s.EFFECTIVE_FROM
						,s.EFFECTIVE_TO
					FROM vespa_Analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES AS s
					JOIN V289_M04_Channel_Genre_Lookup AS l ON s.channel_genre = l.channel_genre
					) AS ska ON viewing.service_key = ska.service_key
					AND viewing.local_start_time_of_session BETWEEN ska.EFFECTIVE_FROM
						AND ska.EFFECTIVE_TO
				WHERE viewing.sky_stb_viewing = 'Y'
					AND viewing.viewing_platform = 4
					AND viewing.panel_or_guest_flag = 'Panel'
					AND convert(DATE, viewing.local_start_time_of_session) BETWEEN @processing_date - 29
						AND @processing_date
				) AS base
			JOIN (
				SELECT a.house_id
					,'thesize' = count (DISTINCT a.person)
					,'hh_weight' = sum (a.head * b.processing_weight)
				FROM skybarb_sv AS a
				LEFT OUTER JOIN barb_weights_sv AS b ON a.house_id = b.household_number
					AND a.person = b.person_number
				GROUP BY a.house_id
				HAVING hh_weight > 0
				) AS barbskyhhsize ON base.household_number = barbskyhhsize.house_id
			WHERE base.theflag > 0
			) AS mega
		JOIN skybarb_sv AS z ON mega.household_number = z.house_id
			AND mega.person_number = z.person

		COMMIT WORK

		CREATE hg INDEX hg1 ON skybarb_sv_fullview_sv (service_key)
		CREATE hg INDEX hg2 ON skybarb_sv_fullview_sv (household_number)
		CREATE hg INDEX hg3 ON skybarb_sv_fullview_sv (session_daypart)
		CREATE lf INDEX lf1 ON skybarb_sv_fullview_sv (channel_pack)
		CREATE lf INDEX lf2 ON skybarb_sv_fullview_sv (programme_genre)
		CREATE dttm INDEX dt1 ON skybarb_sv_fullview_sv (start_time_of_session)
		CREATE dttm INDEX dt2 ON skybarb_sv_fullview_sv (end_time_of_session)
		CREATE dttm INDEX dt3 ON skybarb_sv_fullview_sv (session_start_date_time)
		CREATE dttm INDEX dt4 ON skybarb_sv_fullview_sv (session_end_date_time)
		COMMIT WORK

		GRANT SELECT ON skybarb_sv_fullview_sv TO vespa_group_low_security

		COMMIT WORK message convert(TIMESTAMP, now ()) || ' | @ M04.1: Final BARB Data Preparation DONE' TO client
	END
	ELSE
	BEGIN
		message convert(TIMESTAMP, now ()) || ' | @ M04.0: Missing Data on base tables for Data Preparation Stage!!!' TO client
	END message convert(TIMESTAMP, now ()) || ' | M04 Finished' TO client

END;
GO
