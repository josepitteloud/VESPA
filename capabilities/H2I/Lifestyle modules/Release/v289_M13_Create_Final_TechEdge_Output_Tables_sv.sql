create or replace procedure ${SQLFILE_ARG001}.v289_M13_Create_Final_TechEdge_Output_Tables_sv (@proc_date DATE = NULL , @fresh_start BIT = 0 )
AS
BEGIN
	message convert(TIMESTAMP, now()) || ' | Begining M13.0 - Initialising Environment' TO client

	DECLARE @person_loop INTEGER
	DECLARE @sql_text VARCHAR(10000)
	COMMIT WORK

	IF @fresh_start = 1  AND EXISTS(SELECT tname FROM syscatalog WHERE creator = '${SQLFILE_ARG001}' and tabletype = 'TABLE' and upper(tname) = UPPER ('V289_M13_individual_viewing_working_table_SV'))  
		DROP TABLE V289_M13_individual_viewing_working_table_SV
		
	IF NOT EXISTS(SELECT tname FROM syscatalog WHERE creator = '${SQLFILE_ARG001}' and tabletype = 'TABLE' and upper(tname) = UPPER ('V289_M13_individual_viewing_working_table_SV'))  
		BEGIN 
			create table V289_M13_individual_viewing_working_table_SV (
						event_id							bigint				  	null
						,pk_viewing_prog_instance_fact  	bigint				  	null
						,overlap_batch				  		int						null
						,account_number				 		varchar(20)			 	null
						,subscriber_id				  		numeric(10)			 	null
						,service_key						int						null
						,parent_service_key			 		int						null
						,HD_flag							int						null
						,event_start_date_time  			timestamp			   	null
						,event_end_date_time				timestamp			   	null
						,barb_min_start_date_time_utc   	timestamp	   			null
						,barb_min_end_date_time_utc		 	timestamp	   			null
						,event_start_date_BARB  			timestamp			   	null
						,barb_min_start_date				timestamp			   	null
						,live_recorded				  		varchar(8)			  	not	null
						,viewing_type_flag			  		int			 			not null default 0
						,person_1						   	smallint				null	default 0
						,person_2						   	smallint				null	default 0
						,person_3						   	smallint				null	default 0
						,person_4						   	smallint				null	default 0
						,person_5						   	smallint				null	default 0
						,person_6						   	smallint				null	default 0
						,person_7						   	smallint				null	default 0
						,person_8						   	smallint				null	default 0
						,person_9						   	smallint				null	default 0
						,person_10						  	smallint				null	default 0
						,person_11						  	smallint				null	default 0
						,person_12						  	smallint				null	default 0
						,person_13						  	smallint				null	default 0
						,person_14						  	smallint				null	default 0
						,person_15						  	smallint				null	default 0
						,person_16	 						smallint				null	default 0
						,keeper								BIT						not NULL default 0 
						,duration_min						INTEGER					Null 	default null 
						
						)
			commit -- ; --(^_^)

			create hg index hg1 on V289_M13_individual_viewing_working_table_SV(event_id)     
			create hg index hg2 on V289_M13_individual_viewing_working_table_SV(overlap_batch)
			create hg index hg3 on V289_M13_individual_viewing_working_table_SV(account_number)
			create hg index hg4 on V289_M13_individual_viewing_working_table_SV(subscriber_id) 
			create hg index hg5 on V289_M13_individual_viewing_working_table_SV(service_key)  
			create hg index hg6 on V289_M13_individual_viewing_working_table_SV(parent_service_key)   
			create hg index hg7 on V289_M13_individual_viewing_working_table_SV(event_start_date_time)
			create hg index hg8 on V289_M13_individual_viewing_working_table_SV(event_end_date_time)  
			create hg index hg9 on V289_M13_individual_viewing_working_table_SV(barb_min_start_date_time_utc) 
			create hg index hg10 on V289_M13_individual_viewing_working_table_SV(barb_min_end_date_time_utc)  
			create hg index hg11 on V289_M13_individual_viewing_working_table_SV(pk_viewing_prog_instance_fact)       
			create hg index hg12 on V289_M13_individual_viewing_working_table_SV(event_start_date_BARB)       
			create hg index hg13 on V289_M13_individual_viewing_working_table_SV(barb_min_start_date) 
			create lf index lf1 on V289_M13_individual_viewing_working_table_SV(HD_flag)      
			create lf index lf2 on V289_M13_individual_viewing_working_table_SV(live_recorded)
			commit -- ; --(^_^)
		END 

	COMMIT WORK 
	message convert(TIMESTAMP, now()) || ' | @ M13.0: Initialising Environment DONE' TO client
	message convert(TIMESTAMP, now()) || ' | Begining M13.1 - Create individual viewing working table...' TO client

	DELETE V289_M13_individual_viewing_working_table_SV 
	WHERE ((DATE (event_start_date_time) <> @proc_date) OR (keeper <> 1 ))

	COMMIT WORK

	INSERT INTO V289_M13_individual_viewing_working_table_SV (
		event_id
		,pk_viewing_prog_instance_fact
		,overlap_batch
		,account_number
		,subscriber_id
		,service_key
		,parent_service_key
		,HD_flag
		,event_start_date_time
		,event_end_date_time
		,barb_min_start_date_time_utc
		,barb_min_end_date_time_utc
		,event_start_date_BARB
		,barb_min_start_date
		,live_recorded
		,viewing_type_flag
		)
	SELECT dp_raw.dth_event_id
		,dp.event_id
		,dp.overlap_batch
		,dp.account_number
		,dp.subscriber_id
		,dp_raw.service_key
		,CASE WHEN cmap.[format] =  'HD'    then    cmap.parent_service_key ELSE dp_raw.service_key END
		,CASE cmap.format 	WHEN 'HD' THEN 1 
							WHEN '3D' THEN 1  
							ELSE 0 END
		,dp.event_Start_utc
		,dp.event_end_utc
		,dp.barb_min_start_date_time_utc
		,dp.barb_min_end_date_time_utc
		,CASE 
			WHEN datepart(hour, dp.event_Start_utc) < 2
				THEN dateadd(dd, - 1, DATE (dp.event_Start_utc))
			ELSE DATE (dp.event_Start_utc)
			END
		,CASE 
			WHEN datepart(hour, dp.barb_min_start_date_time_utc) < 2
				THEN dateadd(dd, - 1, DATE (dp.barb_min_start_date_time_utc))
			ELSE DATE (dp.barb_min_start_date_time_utc)
			END
		,dp_raw.live_recorded
		,dp.viewing_type_flag
	FROM v289_M06_dp_raw_data_sv AS dp_raw
	JOIN V289_M07_dp_data_sv AS dp ON dp_raw.pk_viewing_prog_instance_fact = dp.event_id
	JOIN vespa_Analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES AS cmap ON dp_raw.service_key = cmap.service_key
	WHERE dp.barb_min_start_date_time_utc IS NOT NULL
		AND dp.event_Start_utc 					BETWEEN cmap.effective_from AND cmap.effective_to
		AND dp_raw.event_Start_date_time_utc 	BETWEEN cmap.effective_from AND cmap.effective_to

	COMMIT WORK

	INSERT INTO V289_M13_individual_viewing_working_table_SV (
		event_id
		,pk_viewing_prog_instance_fact
		,overlap_batch
		,account_number
		,subscriber_id
		,service_key
		,parent_service_key
		,HD_flag
		,event_start_date_time
		,event_end_date_time
		,barb_min_start_date_time_utc
		,barb_min_end_date_time_utc
		,event_start_date_BARB
		,barb_min_start_date
		,live_recorded
		,viewing_type_flag
		, keeper
		)
	SELECT dp_raw.dth_event_id
		,dp.event_id
		,dp.overlap_batch
		,dp.account_number
		,dp.subscriber_id
		,dp_raw.service_key
		,       CASE WHEN cmap.[format] =    'HD'    then    cmap.parent_service_key ELSE dp_raw.service_key END
		,CASE cmap.format
			WHEN 'HD'
				THEN 1
			WHEN '3D'
				THEN 1
			ELSE 0
			END
		,dp.event_Start_utc
		,dp.event_end_utc
		,dp.barb_min_start_date_time_utc
		,dp.barb_min_end_date_time_utc
		,CASE 
			WHEN datepart(hour, dp.event_Start_utc) < 2
				THEN dateadd(dd, - 1, DATE (dp.event_Start_utc))
			ELSE DATE (dp.event_Start_utc)
			END
		,CASE 
			WHEN datepart(hour, dp.barb_min_start_date_time_utc) < 2
				THEN dateadd(dd, - 1, DATE (dp.barb_min_start_date_time_utc))
			ELSE DATE (dp.barb_min_start_date_time_utc)
			END
		,'RECORDED'
		,dp.viewing_type_flag
		, 0
	FROM v289_M17_vod_raw_data_sv AS dp_raw
	JOIN V289_M07_dp_data_sv AS dp ON dp_raw.pk_viewing_prog_instance_fact = dp.event_id
	JOIN vespa_Analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES AS cmap ON dp_raw.service_key = cmap.service_key
	WHERE dp.barb_min_start_date_time_utc IS NOT NULL
		AND dp.event_Start_utc BETWEEN cmap.effective_from
			AND cmap.effective_to

	DELETE FROM V289_M13_individual_viewing_working_table_SV
	WHERE DATEDIFF(dd,DATE (barb_min_start_date), @proc_date) > 28
		
	COMMIT WORK message convert(TIMESTAMP, now()) || ' | Begining M13.1 - Iterate over individuals and add their viewing...' TO client

	COMMIT WORK

	SELECT account_number
		,hh_person_number
	INTO #scale_accs
	FROM V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING_sv
	WHERE scaling_date = @proc_date

	COMMIT WORK

	CREATE hg INDEX ind1 ON #scale_accs (account_number)

	CREATE hg INDEX ind2 ON #scale_accs (hh_person_number)

	COMMIT WORK

	SET @person_loop = 1

	COMMIT WORK

	WHILE @person_loop <= 16
	BEGIN
		set     @sql_text =     'update  V289_M13_individual_viewing_working_table m13'
								||' set             person_' || @person_loop || ' = 1'
								||' from    V289_M10_session_individuals m10'
								||' inner join #scale_accs m11 on m10.account_number = m11.account_number'
								||' and  m10.hh_person_number = m11.hh_person_number'
								||' where'
								||' m13.pk_viewing_prog_instance_fact = m10.event_id'
								||' and m13.overlap_batch = m10.overlap_batch'
								||' and m10.hh_person_number = ' || @person_loop
					commit
					execute (@sql_text)
					commit

					-- now update when overlap_batch is null (i.e. the event is not overlapping another in same hhd which is most of them)
					set     @sql_text =     'update  V289_M13_individual_viewing_working_table m13'
											||' set             person_' || @person_loop || ' = 1'
											||'	from    V289_M10_session_individuals m10'
											||' inner join #scale_accs m11 on m10.account_number = m11.account_number'
											||' and  m10.hh_person_number = m11.hh_person_number'
											||' where'
											||' m13.pk_viewing_prog_instance_fact = m10.event_id'
											||' and     m13.overlap_batch is null'
											||' and     m10.overlap_batch is null'
											||' and     m10.hh_person_number = ' || @person_loop
					commit

					execute (@sql_text)
					commit
		COMMIT WORK
	END

		--- DUPLICATING 1st set od events
		INSERT INTO #midnight_working (event_id							
			,pk_viewing_prog_instance_fact,overlap_batch				  	
			,account_number,subscriber_id,event_start_date_time  		
			,event_end_date_time	,barb_min_start_date_time_utc  ,barb_min_end_date_time_utc	 	
			,event_start_date_BARB,barb_min_start_date,event_rank, split_type, MA_flag, live_recorded)			
		SELECT event_id							
			,pk_viewing_prog_instance_fact  
			,overlap_batch				  	
			,account_number				 	
			,subscriber_id				  	
			,event_start_date_time  		
			,event_end_date_time	
			,barb_min_start_date_time_utc  
			,barb_min_end_date_time_utc	 	
			,event_start_date_BARB  	
			,barb_min_start_date		
			,2 		---- 2nd event
			, split_type
			, MA_flag
			, live_recorded
		FROM #midnight_working
		COMMIT
		MESSAGE cast(now() as timestamp)||' | Begining M13.2 - #midnight_working inserting duplicates, rows: '||@@rowcount TO CLIENT	
		
		UPDATE #midnight_working 
		SET event_start_date_time = CASE WHEN event_rank = 1 THEN event_start_date_time 
									ELSE  DATEADD(dd, DATEDIFF(dd,'2000-01-01', event_start_date_time)+1, '2000-01-01')
									END 
			,event_end_date_time = CASE  WHEN event_rank = 1 THEN DATEADD(SECOND, -1, DATEADD(dd, DATEDIFF(dd,'2000-01-01', event_start_date_time)+1, '2000-01-01'))
									ELSE event_end_date_time 
									END 
			, duration = DATEDIFF (MINUTE, CASE WHEN event_rank = 1 THEN event_start_date_time 
									ELSE  DATEADD(dd, DATEDIFF(dd,'2000-01-01', event_start_date_time)+1, '2000-01-01')
									END, 
									CASE  WHEN event_rank = 1 THEN DATEADD(minute, -1, DATEADD(dd, DATEDIFF(dd,'2000-01-01', event_start_date_time)+1, '2000-01-01'))
									ELSE event_end_date_time 
									END)
			, proc_flag = 1
		WHERE split_type  in (1,3)
		
		MESSAGE cast(now() as timestamp)||' | Begining M13.2 - #midnight_working first update, rows: '||@@rowcount TO CLIENT	
		
UPDATE #midnight_working 
	SET event_start_date_time = event_end_date_time
WHERE event_end_date_time < event_start_date_time  AND event_rank <> 1
 AND proc_flag = 1

UPDATE #midnight_working 
	SET event_end_date_time = event_start_date_time
WHERE event_end_date_time < event_start_date_time  AND event_rank = 1
AND proc_flag = 1

------------------ Updating Broadcast times (BARB Minuted attributed times) 
---------- RULES:
------	1st event (event_rank = 1): START time same as original / END TIME 	when is LIVE and STB_BROADCAST_START_TIME = STB_EVENT_START_TIME then (STB_BROADCAST_START_TIME + duration in minutes)
------																		when is LIVE and the 1st attributed minute happens the next day then STB_BROADCAST_START_TIME (rare case)
------																		when is LIVE ELSE 23:59:00 of the same day
------																		when is NOT LIVE (RECORDED) and the the MA flag = 1 AND the duration [minutes] = 0 (events started just after 23:59:00) then STB_BROADCAST_START_TIME + duration === STB_BROADCAST_START_TIME
------																		when is NOT LIVE (RECORDED) and the the MA flag = 1 AND the duration [minutes] <> 0 then STB_BROADCAST_START_TIME + duration + 1 
------																		when is NOT LIVE (RECORDED) and the the MA flag = 0 AND the duration [minutes] = 0  then STB_BROADCAST_START_TIME + duration 
------  NOT 1st event (event_rank <> 1): End time same as original / START TIME When is LIVE then  00:00:00 next DAY
------																			when not LIVE AND MA Flag = 1 	STB_BROADCAST_END_TIME - duration 
------																			when not LIVE AND MA Flag = 0 	STB_BROADCAST_END_TIME - duration-1
				
		UPDATE #midnight_working 
		SET barb_min_start_date_time_utc = CASE WHEN event_rank = 1 THEN barb_min_start_date_time_utc 
												WHEN live_recorded = 'LIVE' AND event_rank <> 1 AND DATE(barb_min_end_date_time_utc) = DATE(event_end_date_time)  THEN DATEADD(MINUTE, DATEDIFF(MINUTE, '2000-01-01',event_start_date_time),'2000-01-01') 
												WHEN MA_flag = 1 AND  event_rank <> 1 AND live_recorded <> 'LIVE' AND split_type = 1 THEN DATEADD (minute, -duration +1, barb_min_end_date_time_utc) 	---------------- Already rounded - barb_min_end_date_time_utc is rounded 
												WHEN MA_flag = 1 THEN DATEADD (minute, -duration, barb_min_end_date_time_utc) 	---------------- Already rounded - barb_min_end_date_time_utc is rounded 
												ELSE DATEADD (minute, -duration-1 , barb_min_end_date_time_utc)					---------------- Already rounded - barb_min_end_date_time_utc is rounded 
												END
			,barb_min_end_date_time_utc =  CASE WHEN event_rank <> 1 THEN barb_min_end_date_time_utc 
												WHEN live_recorded = 'LIVE'   AND barb_min_start_date_time_utc = event_end_date_time THEN  DATEADD (minute, duration, barb_min_start_date_time_utc) --  rounded barb_min_start_date_time_utc
												WHEN live_recorded = 'LIVE'   THEN CASE WHEN barb_min_start_date_time_utc > DATEADD(minute, -1, DATEADD(dd, DATEDIFF(dd,'2000-01-01', event_end_date_time)+1, '2000-01-01')) THEN barb_min_start_date_time_utc -- Already rounded 
																				ELSE DATEADD(minute, -1, DATEADD(dd, DATEDIFF(dd,'2000-01-01', event_end_date_time)+1, '2000-01-01')) END ------ Rounded: 23:59:00
												WHEN MA_flag = 1 AND duration = 0 THEN DATEADD (minute, duration, barb_min_start_date_time_utc) 	---------------- Rounded 
												WHEN MA_flag = 1 THEN DATEADD (minute, duration+1, barb_min_start_date_time_utc)					---------------- Rounded 
												ELSE DATEADD (minute, duration, barb_min_start_date_time_utc)
												END
			, proc_flag = 4 
		WHERE split_type in (1,3) 
		AND proc_flag = 1 
		
		UPDATE #midnight_working 
			SET barb_min_start_date_time_utc = barb_min_end_date_time_utc
		WHERE barb_min_end_date_time_utc < barb_min_start_date_time_utc  AND event_rank <> 1
		 AND proc_flag = 4

		UPDATE #midnight_working 
			SET barb_min_end_date_time_utc = barb_min_start_date_time_utc
		WHERE barb_min_end_date_time_utc < barb_min_start_date_time_utc  AND event_rank = 1
		AND proc_flag = 4
				
		UPDATE #midnight_working  
		SET barb_min_start_date_time_utc = barb_min_end_date_time_utc
		WHERE proc_flag = 4 AND split_type = 1 AND barb_min_start_date_time_utc > barb_min_end_date_time_utc AND DATEPART(MINUTE, barb_min_end_date_time_utc) = 59 AND DATEPART(HOUR, barb_min_end_date_time_utc) = 23

		UPDATE #midnight_working  
		SET barb_min_start_date_time_utc =  DATEADD(MINUTE, 1, barb_min_start_date_time_utc)
		WHERE proc_flag = 4 AND split_type = 1 AND DATEPART(MINUTE, barb_min_start_date_time_utc) = 59 AND DATEPART(HOUR, barb_min_start_date_time_utc) = 23
			AND event_rank = 2 AND MA_flag = 0 
			AND pk_viewing_prog_instance_fact IN (SELECT pk_viewing_prog_instance_fact FROM #midnight_working 
											WHERE split_type = 1 AND DATEPART(MINUTE, barb_min_start_date_time_utc) = 0 AND DATEPART(HOUR, barb_min_start_date_time_utc) = 0
											AND event_rank = 1 AND MA_flag = 0 AND barb_min_start_date_time_utc = barb_min_end_date_time_utc) 
				
		MESSAGE cast(now() as timestamp)||' | Begining M13.2 - #midnight_working second update, rows: '||@@rowcount TO CLIENT	
		
		INSERT INTO #midnight_working (event_id							
			,pk_viewing_prog_instance_fact,overlap_batch				  	
			,account_number,subscriber_id,event_start_date_time  		
			,event_end_date_time	,barb_min_start_date_time_utc  ,barb_min_end_date_time_utc	 	
			,event_start_date_BARB,barb_min_start_date,event_rank, split_type, MA_flag, live_recorded)			
		SELECT event_id							
			,pk_viewing_prog_instance_fact  
			,overlap_batch				  	
			,account_number				 	
			,subscriber_id				  	
			,event_start_date_time  		
			,event_end_date_time	
			,barb_min_start_date_time_utc  
			,barb_min_end_date_time_utc	 	
			,event_start_date_BARB  	
			,barb_min_start_date		
			,3 		---- 3rd  event
			, split_type
			, MA_flag
			, live_recorded
		FROM #midnight_working
		WHERE DATE (barb_min_start_date_time_utc) <> DATE (barb_min_end_date_time_utc)
		AND split_type = 3
		AND proc_flag = 4 and DATE(barb_min_start_date_time_utc) <> DATE(barb_min_end_date_time_utc)
		COMMIT	
		
		MESSAGE cast(now() as timestamp)||' | Begining M13.2 - #midnight_working resinserting duplicates (type 3), rows: '||@@rowcount TO CLIENT	
		-- Splitting events where broadcast spanning happen
		UPDATE  #midnight_working
		SET barb_min_start_date_time_utc = CASE WHEN (split_type = 2 AND event_rank = 1) 			-- the 1st chunk from type 2
										OR  (split_type = 3 AND event_rank <>3) 		-- the 1st event of the type 3 
											THEN barb_min_start_date_time_utc 
										ELSE  DATEADD(dd, DATEDIFF(dd,'2000-01-01', barb_min_start_date_time_utc)+1, '2000-01-01')
										END 	
								
			,barb_min_end_date_time_utc = CASE  WHEN (split_type = 2 AND event_rank = 1) 			-- the 1st chunk from type 2
										OR  (split_type = 3 AND event_rank <>3) 		-- the 1st event of the type 3 
										THEN DATEADD(minute, -1, DATEADD(dd, DATEDIFF(dd,'2000-01-01', barb_min_start_date_time_utc)+1, '2000-01-01'))
										ELSE barb_min_end_date_time_utc 
										END 
			,duration = DATEDIFF ( MINUTE, CASE WHEN (split_type = 2 AND event_rank = 1) 			-- the 1st chunk from type 2
										OR  (split_type = 3 AND event_rank <>3) 		-- the 1st event of the type 3 
											THEN barb_min_start_date_time_utc 
										ELSE  DATEADD(dd, DATEDIFF(dd,'2000-01-01', barb_min_start_date_time_utc)+1, '2000-01-01')
										END ,
										CASE  WHEN (split_type = 2 AND event_rank = 1) 			-- the 1st chunk from type 2
												OR  (split_type = 3 AND event_rank <>3) 		-- the 1st event of the type 3 
										THEN DATEADD(minute, -1, DATEADD(dd, DATEDIFF(dd,'2000-01-01', barb_min_start_date_time_utc)+1, '2000-01-01'))
										ELSE barb_min_end_date_time_utc 
										END )
			,proc_flag = 6 								
		WHERE DATE(barb_min_start_date_time_utc) <> DATE(barb_min_end_date_time_utc)
			AND split_type  <> 1
		COMMIT
		
				
		UPDATE #midnight_working  
		SET a.barb_min_start_date_time_utc = b.barb_min_end_date_time_utc
		FROM #midnight_working  AS a 
		JOIN (SELECT pk_viewing_prog_instance_fact,event_id, event_rank, split_type , barb_min_start_date_time_utc, barb_min_end_date_time_utc
				FROM #midnight_working  AS b
				WHERE b.split_type = 3 AND b.event_rank = 1)  as b ON a.pk_viewing_prog_instance_fact = b.pk_viewing_prog_instance_fact AND a.event_id= b.event_id
		WHERE a.split_type = 3 AND a.event_rank = 2 
				AND b.barb_min_end_date_time_utc            >       a.barb_min_start_date_time_utc
				AND DATE(b.barb_min_end_date_time_utc)      = DATE (a.barb_min_end_date_time_utc)
				AND DATE(b.barb_min_start_date_time_utc)    = DATE (a.barb_min_start_date_time_utc)



		
		
		
		
		
		
		
		MESSAGE cast(now() as timestamp)||' | Begining M13.2 - #midnight_working third update, rows: '||@@rowcount TO CLIENT	
		------------------ Updating event times
		UPDATE  #midnight_working	
		SET 	event_start_date_time = CASE WHEN event_rank = 1  OR  (split_type = 3 AND event_rank <>3)  THEN event_start_date_time
										ELSE DATEADD (minute, -duration, event_end_date_time)
										END
				,event_end_date_time =  CASE WHEN event_rank = 1 OR  (split_type = 3 AND event_rank <>3) THEN DATEADD (minute, duration, event_start_date_time)
										ELSE event_end_date_time 
										END
			, proc_flag = CASE WHEN split_type  = 1 THEN 7 ELSE 1 END	
		WHERE proc_flag = 6
				
				
		UPDATE V289_M13_individual_viewing_working_table_SV
		SET	a.event_end_date_time = b.event_end_date_time
			,a.barb_min_end_date_time_utc = b.barb_min_end_date_time_utc	
			,a.duration_min = b.duration
		FROM V289_M13_individual_viewing_working_table_SV AS a 
		JOIN #midnight_working AS b ON a.event_id = b.event_id 
									AND a.pk_viewing_prog_instance_fact = b.pk_viewing_prog_instance_fact
									AND COALESCE(a.overlap_batch,0)	= COALESCE(b.overlap_batch ,0)
									AND a.account_number = b.account_number 
									
		WHERE event_rank = 1  
			AND DATE (a.event_start_date_time) = @proc_date 
		
		MESSAGE cast(now() as timestamp)||' | Begining M13.2 - V289_M13_individual_viewing_working_table_SV update, rows: '||@@rowcount TO CLIENT	
		
		INSERT INTO V289_M13_individual_viewing_working_table_SV
		SELECT   a.event_id
                , b.pk_viewing_prog_instance_fact
                , b.overlap_batch
                , b.account_number
                , b.subscriber_id
                , b.service_key
                , b.parent_service_key
                , b.HD_flag
                , a.event_start_date_time
                , a.event_end_date_time
                , a.barb_min_start_date_time_utc
                , a.barb_min_end_date_time_utc
                , b.event_start_date_BARB
                , b.barb_min_start_date
                , b.live_recorded
                , b.viewing_type_flag
				, b.person_1	,b.person_2					
				, b.person_3	,b.person_4						   	
				, b.person_5	,b.person_6	
				, b.person_7	,b.person_8	
				, b.person_9	,b.person_10	
				, b.person_11	,b.person_12	
				, b.person_13	,b.person_14	
				, b.person_15	,b.person_16	
				, keeper = CASE WHEN DATE(a.event_start_date_time) = @proc_date  THEN 0 ELSE 1 END 				
				, a.duration					
		FROM #midnight_working as a 
		JOIN V289_M13_individual_viewing_working_table_SV AS b ON a.event_id = b.event_id 
									AND a.pk_viewing_prog_instance_fact = b.pk_viewing_prog_instance_fact
									AND COALESCE(a.overlap_batch,0)	= COALESCE(b.overlap_batch ,0)
									AND a.account_number = b.account_number 
		WHERE event_rank <> 1
		COMMIT 	
		
		DROP TABLE #midnight_working
		COMMIT
		
  		MESSAGE cast(now() as timestamp)||' | Begining M13.2 - V289_M13_individual_viewing_working_table_SV insert , rows: '||@@rowcount TO CLIENT		
	COMMIT WORK message convert(TIMESTAMP, now()) || ' | Begining M13.2 - Update final individual viewing output table...' TO client

	COMMIT WORK

	TRUNCATE TABLE V289_M13_individual_viewing_live_vosdal_sv

	COMMIT WORK

	INSERT INTO V289_M13_individual_viewing_live_vosdal_sv (
		SUBSCRIBER_ID
		,ACCOUNT_NUMBER
		,STB_BROADCAST_START_TIME
		,STB_BROADCAST_END_TIME
		,STB_EVENT_START_TIME
		,TIMESHIFT
		,service_key
		,Platform_flag
		,Original_Service_key
		,AdSmart_flag
		,DTH_VIEWING_EVENT_ID
		,person_1
		,person_2
		,person_3
		,person_4
		,person_5
		,person_6
		,person_7
		,person_8
		,person_9
		,person_10
		,person_11
		,person_12
		,person_13
		,person_14
		,person_15
		,person_16
		)
	SELECT m13.subscriber_id
		,m13.account_number
		,barb_min_start_date_time_utc
		,barb_min_end_date_time_utc
		,event_start_date_time
		,'TIMESHIFT' = CASE 
			WHEN live_recorded = 'LIVE'
				THEN 0
			ELSE (
					(
						CASE 
							WHEN (event_start_date_BARB - barb_min_start_date) < 0
								THEN 0
							ELSE (event_start_date_BARB - barb_min_start_date)
							END
						) + 1
					)
			END
		,parent_service_key
		,HD_flag
		,service_key
		,0
		,pk_viewing_prog_instance_fact
		,person_1
		,person_2
		,person_3
		,person_4
		,person_5
		,person_6
		,person_7
		,person_8
		,person_9
		,person_10
		,person_11
		,person_12
		,person_13
		,person_14
		,person_15
		,person_16
	FROM V289_M13_individual_viewing_working_table_SV AS m13
	WHERE person_1 + person_2 + person_3 + person_4 + person_5 + person_6 + person_7 + person_8 + person_9 + person_10 + person_11 + person_12 + person_13 + person_14 + person_15 + person_16 > 0
		AND (
			TIMESHIFT < 2
			AND viewing_type_flag = 0
			)

	COMMIT WORK

	INSERT INTO V289_M13_individual_viewing_live_vosdal_sv (
		SUBSCRIBER_ID
		,ACCOUNT_NUMBER
		,STB_BROADCAST_START_TIME
		,STB_BROADCAST_END_TIME
		,STB_EVENT_START_TIME
		,TIMESHIFT
		,service_key
		,Platform_flag
		,Original_Service_key
		,AdSmart_flag
		,DTH_VIEWING_EVENT_ID
		,person_1
		,person_2
		,person_3
		,person_4
		,person_5
		,person_6
		,person_7
		,person_8
		,person_9
		,person_10
		,person_11
		,person_12
		,person_13
		,person_14
		,person_15
		,person_16
		)
	SELECT m13.subscriber_id
		,m13.account_number
		,barb_min_start_date_time_utc
		,barb_min_end_date_time_utc
		,event_start_date_time
		,'TIMESHIFT' = CASE 
			WHEN live_recorded = 'LIVE'
				THEN 0
			ELSE (
					(
						CASE 
							WHEN (event_start_date_BARB - barb_min_start_date) < 0
								THEN 0
							ELSE (event_start_date_BARB - barb_min_start_date)
							END
						) + 1
					)
			END
		,parent_service_key
		,HD_flag
		,service_key
		,0
		,pk_viewing_prog_instance_fact
		,person_1
		,person_2
		,person_3
		,person_4
		,person_5
		,person_6
		,person_7
		,person_8
		,person_9
		,person_10
		,person_11
		,person_12
		,person_13
		,person_14
		,person_15
		,person_16
	FROM V289_M13_individual_viewing_working_table_SV AS m13
	WHERE person_1 + person_2 + person_3 + person_4 + person_5 + person_6 + person_7 + person_8 + person_9 + person_10 + person_11 + person_12 + person_13 + person_14 + person_15 + person_16 > 0
		AND (
			TIMESHIFT > 1
			OR viewing_type_flag = 1
			)

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | M13.2 - Update subscriber_is 99 for Pull VOD events' TO client

	SELECT b.account_number
		,'subscriber_id' = convert(INTEGER, min(si_external_identifier))
		,'primary_box' = convert(BIT, max(CASE 
					WHEN si_service_instance_type = 'Primary DTV'
						THEN 1
					ELSE 0
					END))
	INTO #subscriber_details
	FROM CUST_SERVICE_INSTANCE AS b
	JOIN V289_M13_individual_viewing_live_vosdal AS base ON base.account_number = b.account_number
		AND base.subscriber_id = 99
	WHERE si_service_instance_type = 'Primary DTV'
		AND @proc_date BETWEEN effective_from_dt
			AND effective_to_dt
	GROUP BY b.account_number

	COMMIT WORK

	CREATE hg INDEX wef ON #subscriber_details (account_number)

	COMMIT WORK

	UPDATE V289_M13_individual_viewing_live_vosdal AS a
	SET a.subscriber_id = b.subscriber_id
	FROM V289_M13_individual_viewing_live_vosdal AS a
	JOIN #subscriber_details AS b ON a.account_number = b.account_number
	WHERE a.subscriber_id = 99 message convert(TIMESTAMP, now()) || ' | M13.2 - Subscriber_is 99 updated: ' || @@rowcount TO client

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | Begining M13.3 - Update final individual details table...' TO client

	COMMIT WORK

	TRUNCATE TABLE V289_M13_individual_details_sv

	COMMIT WORK

	INSERT INTO V289_M13_individual_details_sv (
		account_number
		,person_number
		,ind_scaling_weight
		,gender
		,age_band
		,head_of_hhd
		,hhsize
		)
	SELECT hh.account_number
		,hh.hh_person_number
		,w.scaling_weighting
		,'gender' = CASE 
			WHEN hh.person_gender = 'M'
				THEN 1
			WHEN hh.person_gender = 'F'
				THEN 2
			ELSE 99
			END
		,'age_band' = CASE 
			WHEN hh.person_ageband = '0-11'
				THEN 0
			WHEN hh.person_ageband = '12-19'
				THEN 1
			WHEN hh.person_ageband = '20-24'
				THEN 2
			WHEN hh.person_ageband = '25-34'
				THEN 3
			WHEN hh.person_ageband = '35-44'
				THEN 4
			WHEN hh.person_ageband = '45-64'
				THEN 5
			WHEN hh.person_age >= 65
				THEN 6
			ELSE 99
			END
		,hh.person_head
		,hh.household_size
	FROM V289_M08_SKY_HH_composition_sv AS hh
	JOIN V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING_sv AS w ON hh.account_number = w.account_number
		AND hh.HH_person_number = w.HH_person_number
	WHERE (
			hh.panel_flag = 1
			OR hh.nonviewer_household = 1
			)
			
			
		
		------ Clearing the working table, leaving the split events that spanned midnight
		DELETE FROM V289_M13_individual_viewing_working_table_SV 
		WHERE ((DATE (event_start_date_time) <= @proc_date) OR (keeper <> 1 ))
	
		
		SELECT pk_viewing_prog_instance_fact, count(*) hits 
		INTO #t1
		FROM V289_M13_individual_viewing_working_table_SV 
		GROUP BY pk_viewing_prog_instance_fact 
		HAVING hits >=3
		COMMIT
		
		CREATE HG INDEX id1 ON #t1 (pk_viewing_prog_instance_fact)
		COMMIT
		
		DELETE FROM V289_M13_individual_viewing_working_table_SV 
		WHERE pk_viewing_prog_instance_fact IN (SELECT pk_viewing_prog_instance_fact FROM #t1)			

	COMMIT WORK
END;
GO 
commit;
