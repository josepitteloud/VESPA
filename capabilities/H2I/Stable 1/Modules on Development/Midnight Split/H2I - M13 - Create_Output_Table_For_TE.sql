/*
                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES

--------------------------------------------------------------------------------------------------------------
**Project Name:                                                 Skyview H2I
**Analysts:                             Angel Donnarumma        (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jason Thompson          (Jason.Thompson@skyiq.co.uk)
                                                                                ,Hoi Yu Tang            (HoiYu.Tang@skyiq.co.uk)
                                                                                ,Jose Pitteloud         (jose.pitteloud@skyiq.co.uk)
**Stakeholder:                          SkyIQ
                                                                              ,Jose Loureda           (Jose.Loureda@skyiq.co.uk)
**Due Date:                             11/07/2014
**Project Code (Insight Collation):     v289
**Sharepoint Folder:

        http://sp-department.bskyb.com/sites/SIGEvolved/Shared%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FSIGEvolved%2FShared%20Documents%2F01%20Analysis%20Requests%2FV289%20-%20Skyview%20Futures%2F01%20Plans%20Briefs%20and%20Project%20Admin

**Business Brief:

        This Module produces the final individual level viewing table that will be sent to TechEdge

**Module:

        M13: Create output tables for TechEdge
                        M13.0 - Initialising Environment
                        M13.1 - Transpose Individuals to Columns
                        M13.2 - Final Viewing Output Table
                        M13.3 - Final Individual Table

--------------------------------------------------------------------------------------------------------------
*/

---------------------------------
-----------------------------------
-- M13.0 - Initialising Environment
-----------------------------------

create or replace procedure v289_M13_Create_Final_TechEdge_Output_Tables
        @proc_date  date    =   null
		, @fresh_start BIT = 0 
		
as begin

        MESSAGE cast(now() as timestamp)||' | Begining M13.0 - Initialising Environment' TO CLIENT

        declare @person_loop int                
        declare @sql_text varchar(10000)        
		COMMIT
		
		IF @fresh_start = 1  AND EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = UPPER ('V289_M13_individual_viewing_working_table'))  
		DROP TABLE V289_M13_individual_viewing_working_table
		
		IF NOT EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = UPPER ('V289_M13_individual_viewing_working_table'))  
		BEGIN 
			create table V289_M13_individual_viewing_working_table (
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

			create hg index hg1 on V289_M13_individual_viewing_working_table(event_id)     
			create hg index hg2 on V289_M13_individual_viewing_working_table(overlap_batch)
			create hg index hg3 on V289_M13_individual_viewing_working_table(account_number)
			create hg index hg4 on V289_M13_individual_viewing_working_table(subscriber_id) 
			create hg index hg5 on V289_M13_individual_viewing_working_table(service_key)  
			create hg index hg6 on V289_M13_individual_viewing_working_table(parent_service_key)   
			create hg index hg7 on V289_M13_individual_viewing_working_table(event_start_date_time)
			create hg index hg8 on V289_M13_individual_viewing_working_table(event_end_date_time)  
			create hg index hg9 on V289_M13_individual_viewing_working_table(barb_min_start_date_time_utc) 
			create hg index hg10 on V289_M13_individual_viewing_working_table(barb_min_end_date_time_utc)  
			create hg index hg11 on V289_M13_individual_viewing_working_table(pk_viewing_prog_instance_fact)       
			create hg index hg12 on V289_M13_individual_viewing_working_table(event_start_date_BARB)       
			create hg index hg13 on V289_M13_individual_viewing_working_table(barb_min_start_date) 
			create lf index lf1 on V289_M13_individual_viewing_working_table(HD_flag)      
			create lf index lf2 on V289_M13_individual_viewing_working_table(live_recorded)
			commit -- ; --(^_^)
		END 
        MESSAGE cast(now() as timestamp)||' | @ M13.0: Initialising Environment DONE' TO CLIENT
        commit -- ; --(^_^)
		
		
        -----------------------------------
        -- M13.1 - Transpose Individuals to Columns
        -----------------------------------
		MESSAGE cast(now() as timestamp)||' | Begining M13.1 - Create individual viewing working table...' TO CLIENT

		DELETE V289_M13_individual_viewing_working_table 
		WHERE ((DATE (event_start_date_time) <> @proc_date) OR (keeper <> 1 ))


        UPDATE 		V289_M13_individual_viewing_working_table
		SET keeper = 0 
		WHERE DATE (event_start_date_time) = @proc_date
        commit -- ; --(^_^)

        -- Populate the working viewing table with a single copy of each viewing event and overlap batches where relevant
        -- This will do linear only (i.e. not Pull vod)
        insert into     V289_M13_individual_viewing_working_table(
                        event_id
                ,       pk_viewing_prog_instance_fact
                ,       overlap_batch
                ,       account_number
                ,       subscriber_id
                ,       service_key
                ,       parent_service_key
                ,       HD_flag
                ,       event_start_date_time
                ,       event_end_date_time
                ,       barb_min_start_date_time_utc
                ,       barb_min_end_date_time_utc
                ,       event_start_date_BARB
                ,       barb_min_start_date
                ,       live_recorded
                ,       viewing_type_flag
                )
        select
                        dp_raw.dth_event_id
                ,       dp.event_id
                ,       dp.overlap_batch
                ,       dp.account_number
                ,       dp.subscriber_id
                ,       dp_raw.service_key
                ,       CASE WHEN cmap.[format] =  'HD'    then    cmap.parent_service_key ELSE dp_raw.service_key END
                ,       case    cmap.[format]
                                when    'HD'    then    1
                                when    '3D'    then    1
                                else                    0
                        end
                ,       dp.event_Start_utc
                ,       dp.event_end_utc
                ,       dp.barb_min_start_date_time_utc
                ,       dp.barb_min_end_date_time_utc
                ,       case when    datepart(hour,dp.event_Start_utc) < 2   then    dateadd(dd,-1,date(dp.event_Start_utc))
                                else    date(dp.event_Start_utc)
                        end 
                ,       case when    datepart(hour,dp.barb_min_start_date_time_utc) < 2  then    dateadd(dd,-1,date(dp.barb_min_start_date_time_utc))
                                else    date(dp.barb_min_start_date_time_utc)
                        end
                ,       dp_raw.live_recorded
                ,       dp.viewing_type_flag
        from v289_M06_dp_raw_data               	AS      dp_raw
		inner join      V289_M07_dp_data     		AS      dp              on      dp_raw.pk_viewing_prog_instance_fact    = dp.event_id
		inner join      vespa_Analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES          as      cmap    on      dp_raw.service_key = cmap.service_key
        where dp.barb_min_start_date_time_utc is not null -- We do not want to pass non minute attributed events to TE
			AND  dp.barb_min_end_date_time_utc is not null 
			and   dp.event_Start_utc between cmap.effective_from and cmap.effective_to -- otherwise get duplicated rows in TE output
			and   cast(dp_raw.event_Start_date_time_utc as date) between cmap.effective_from and cmap.effective_to
        commit -- ; --(^_^)


        -- Populate the working viewing table with a single copy of each viewing event and overlap batches where relevant
        -- This will do Pull vod
        insert into     V289_M13_individual_viewing_working_table(
                        event_id
                ,       pk_viewing_prog_instance_fact
                ,       overlap_batch
                ,       account_number
                ,       subscriber_id
                ,       service_key
                ,       parent_service_key
                ,       HD_flag
                ,       event_start_date_time
                ,       event_end_date_time
                ,       barb_min_start_date_time_utc
                ,       barb_min_end_date_time_utc
                ,       event_start_date_BARB
                ,       barb_min_start_date
                ,       live_recorded
                ,       viewing_type_flag
				, 		keeper
                )
    	select
						dp_raw.dth_event_id
				,	   dp.event_id
				,	   dp.overlap_batch
				,	   dp.account_number
				,	   dp.subscriber_id
				,	   dp_raw.service_key
				,       CASE WHEN cmap.[format] =    'HD'    then    cmap.parent_service_key ELSE dp_raw.service_key END
				,	   case	cmap.[format]
								when	'HD'	then	1
								when	'3D'	then	1
								else					0
						end
				,	   dp.event_Start_utc
				,	   dp.event_end_utc
				,	   dp.barb_min_start_date_time_utc
				,	   dp.barb_min_end_date_time_utc
				,	   case	when	datepart(hour,dp.event_Start_utc) < 2   then	dateadd(dd,-1,date(dp.event_Start_utc))
								else	date(dp.event_Start_utc)
						end
				,	   case	when	datepart(hour,dp.barb_min_start_date_time_utc) < 2  then	dateadd(dd,-1,date(dp.barb_min_start_date_time_utc))
								else	date(dp.barb_min_start_date_time_utc)
						end
				,	   'RECORDED' --dp_raw.live_recorded
				,	   dp.viewing_type_flag
				, 0 -- We don't want to keep any event overnight
		from v289_M17_vod_raw_data						as	  dp_raw
		inner join	  V289_M07_dp_data					as	  dp	  on	  dp_raw.pk_viewing_prog_instance_fact	=	   dp.event_id
		inner join	  vespa_Analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES		  as	  cmap	on	  dp_raw.service_key =  cmap.service_key
		where dp.barb_min_start_date_time_utc is not null -- We do not want to pass non minute attributed events to TE
			AND  dp.barb_min_end_date_time_utc is not null 
			and   dp.event_Start_utc between cmap.effective_from and cmap.effective_to -- otherwise get duplicated rows in TE output																																										  and cast(dp_raw.event_Start_date_time_utc as date) between cmap.effective_from and cmap.effective_to
		commit -- ; --(^_^)
		
		--Filtering events played more than 28 days ago
		DELETE FROM V289_M13_individual_viewing_working_table
		WHERE DATEDIFF(dd,DATE (barb_min_start_date), @proc_date) > 28

        -- Loop through each possible person (max 16) in a hhd and add their viewing

        MESSAGE cast(now() as timestamp)||' | Begining M13.1 - Iterate over individuals and add their viewing...' TO CLIENT
        commit -- ; --(^_^)

        -- Individuals with a weight
        select  account_number, hh_person_number
        into    #scale_accs
        from    V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING
        where   scaling_date = @proc_date
        commit

        create  hg index ind1 on #scale_accs(account_number)
        create  hg index ind2 on #scale_accs(hh_person_number)
        commit

        set @person_loop = 1
        commit -- ; --(^_^)

        while @person_loop <= 16
        begin 		-- update events with person where overlap_batch match (i.e. overlap_batch is not null)
					set     @sql_text =     'update  V289_M13_individual_viewing_working_table m13
									set             person_' || @person_loop || ' = 1
									from    V289_M10_session_individuals m10
									inner join #scale_accs m11 on m10.account_number = m11.account_number
															   and  m10.hh_person_number = m11.hh_person_number
									where
													m13.pk_viewing_prog_instance_fact = m10.event_id
											and     m13.overlap_batch = m10.overlap_batch
											and m10.hh_person_number = ' || @person_loop
					commit
					execute (@sql_text)
					commit

					-- now update when overlap_batch is null (i.e. the event is not overlapping another in same hhd which is most of them)
					set     @sql_text =     'update  V289_M13_individual_viewing_working_table m13
									set             person_' || @person_loop || ' = 1
									from    V289_M10_session_individuals m10
									inner join #scale_accs m11 on m10.account_number = m11.account_number
															   and  m10.hh_person_number = m11.hh_person_number
									where
													m13.pk_viewing_prog_instance_fact = m10.event_id
											and     m13.overlap_batch is null
											and     m10.overlap_batch is null
											and     m10.hh_person_number = ' || @person_loop

					commit

					execute (@sql_text)
					commit

					set @person_loop = @person_loop + 1
					commit
        end
        commit -- ; --(^_^)

		
		-----------------------------------
        -- M13.2 - Midnight Split
        -----------------------------------
		MESSAGE cast(now() as timestamp)||' | Begining M13.2 - Midnight Split started' TO CLIENT
		CREATE TABLE #midnight_working
			(event_id							bigint				  	null
			,pk_viewing_prog_instance_fact  	bigint				  	null
			,overlap_batch				  		int						null
			,account_number				 		varchar(20)			 	null
			,subscriber_id				  		numeric(10)			 	null
			,event_start_date_time  			timestamp			   	null
			,event_end_date_time				timestamp			   	null
			,barb_min_start_date_time_utc   	timestamp	   			null
			,barb_min_end_date_time_utc		 	timestamp	   			null
			,event_start_date_BARB  			timestamp			   	null
			,barb_min_start_date				timestamp			   	null
			,duration							INTEGER					NULL
			,split_type							tinyint					NULL
			,event_rank							tinyint					NULL
			,proc_flag							tinyint			DEFAULT 0 	NULL
			,MA_flag								BIT 			DEFAULT 0 NOT NULL 
			, live_recorded 					varchar(8)			  	not	null
			)
		COMMIT
		CREATE HG INDEX id1 ON #midnight_working(event_id)
		CREATE HG INDEX id2 ON #midnight_working(pk_viewing_prog_instance_fact)
		CREATE HG INDEX id3 ON #midnight_working(account_number)
		CREATE DTTM INDEX id4 ON #midnight_working(event_start_date_time)
		CREATE DTTM INDEX id5 ON #midnight_working(event_end_date_time)
		CREATE DTTM INDEX id6 ON #midnight_working(barb_min_start_date_time_utc)
		CREATE DTTM INDEX id7 ON #midnight_working(barb_min_end_date_time_utc)
		COMMIT
		
		MESSAGE cast(now() as timestamp)||' | Begining M13.2 - #midnight_working table created' TO CLIENT
		INSERT INTO #midnight_working
			(event_id							
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
			,event_rank			
			, split_type 
			, MA_flag
			, live_recorded 
			)
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
			, 1  			------ 1st event
			, CASE 	WHEN DATE (barb_min_start_date_time_utc) <> DATE (barb_min_end_date_time_utc) 
				AND DATE (event_start_date_time) <> DATE (event_end_date_time)	THEN 3
				WHEN  DATE (event_start_date_time) <> DATE (event_end_date_time) 
				AND live_recorded = 'LIVE' 														THEN 1
				WHEN DATE (barb_min_start_date_time_utc) = DATE (barb_min_end_date_time_utc) 
				AND DATE (event_start_date_time) <> DATE (event_end_date_time)	THEN 1
				WHEN DATE (barb_min_start_date_time_utc) <> DATE (barb_min_end_date_time_utc) 
				AND DATE (event_start_date_time) = DATE (event_end_date_time)	THEN 2
				ELSE 0 END 			AS split_type
			, CASE 	WHEN DATEPART(MINUTE, barb_min_start_date_time_utc) = DATEPART(MINUTE, event_start_date_time) 
					AND DATEPART(HOUR, barb_min_start_date_time_utc) = DATEPART(HOUR, event_start_date_time) 
					AND DATE (barb_min_start_date_time_utc) = DATE(event_start_date_time) 
					THEN 1 
					WHEN 60 - DATEPART (ss, event_start_date_time) >29 THEN 1 
					ELSE 0 END
			, live_recorded
		FROM V289_M13_individual_viewing_working_table
		WHERE 	(DATE(event_start_date_time ) 		<> DATE(event_end_date_time)
			OR 	DATE(barb_min_start_date_time_utc) 	<> DATE(barb_min_end_date_time_utc))
			AND barb_min_start_date_time_utc IS NOT NULL 
			AND barb_min_end_date_time_utc IS NOT NULL
			AND DATEDIFF(hour, event_start_date_time, event_END_date_time) <=23
			
		MESSAGE cast(now() as timestamp)||' | Begining M13.2 - #midnight_working populated, rows: '||@@rowcount TO CLIENT	
			
					
		COMMIT	
			
			
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
			
		COMMIT
		MESSAGE cast(now() as timestamp)||' | Begining M13.2 - #midnight_working first update, rows: '||@@rowcount TO CLIENT	
				
		UPDATE #midnight_working 
			SET event_start_date_time = event_end_date_time
		WHERE event_end_date_time < event_start_date_time  AND event_rank <> 1
		 AND proc_flag = 1

		UPDATE #midnight_working 
			SET event_end_date_time = event_start_date_time
		WHERE event_end_date_time < event_start_date_time  AND event_rank = 1
		AND proc_flag = 1
		COMMIT

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
		
		COMMIT
		
		UPDATE #midnight_working 
			SET barb_min_start_date_time_utc = barb_min_end_date_time_utc
		WHERE barb_min_end_date_time_utc < barb_min_start_date_time_utc  AND event_rank <> 1
		 AND proc_flag = 4
		
		COMMIT
		
		UPDATE #midnight_working 
			SET barb_min_end_date_time_utc = barb_min_start_date_time_utc
		WHERE barb_min_end_date_time_utc < barb_min_start_date_time_utc  AND event_rank = 1
		AND proc_flag = 4
		
		COMMIT
		
		UPDATE #midnight_working  
		SET barb_min_start_date_time_utc = barb_min_end_date_time_utc
		WHERE proc_flag = 4 AND split_type = 1 AND barb_min_start_date_time_utc > barb_min_end_date_time_utc AND DATEPART(MINUTE, barb_min_end_date_time_utc) = 59 AND DATEPART(HOUR, barb_min_end_date_time_utc) = 23

		COMMIT
		
		UPDATE #midnight_working  
		SET barb_min_start_date_time_utc =  DATEADD(MINUTE, 1, barb_min_start_date_time_utc)
		WHERE proc_flag = 4 AND split_type = 1 AND DATEPART(MINUTE, barb_min_start_date_time_utc) = 59 AND DATEPART(HOUR, barb_min_start_date_time_utc) = 23
			AND event_rank = 2 AND MA_flag = 0 
			AND pk_viewing_prog_instance_fact IN (SELECT pk_viewing_prog_instance_fact FROM #midnight_working 
											WHERE split_type = 1 AND DATEPART(MINUTE, barb_min_start_date_time_utc) = 0 AND DATEPART(HOUR, barb_min_start_date_time_utc) = 0
											AND event_rank = 1 AND MA_flag = 0 AND barb_min_start_date_time_utc = barb_min_end_date_time_utc) 

		COMMIT											
		
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
		COMMIT
		
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
				
		COMMIT
		
		UPDATE V289_M13_individual_viewing_working_table
		SET	a.event_end_date_time = b.event_end_date_time
			,a.barb_min_end_date_time_utc = b.barb_min_end_date_time_utc	
			,a.duration_min = b.duration
		FROM V289_M13_individual_viewing_working_table AS a 
		JOIN #midnight_working AS b ON a.event_id = b.event_id 
									AND a.pk_viewing_prog_instance_fact = b.pk_viewing_prog_instance_fact
									AND COALESCE(a.overlap_batch,0)	= COALESCE(b.overlap_batch ,0)
									AND a.account_number = b.account_number 
									
		WHERE event_rank = 1  
			AND DATE (a.event_start_date_time) = @proc_date 
		
		COMMIT
		
		MESSAGE cast(now() as timestamp)||' | Begining M13.2 - V289_M13_individual_viewing_working_table update, rows: '||@@rowcount TO CLIENT	
		
		INSERT INTO V289_M13_individual_viewing_working_table
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
		JOIN V289_M13_individual_viewing_working_table AS b ON a.event_id = b.event_id 
									AND a.pk_viewing_prog_instance_fact = b.pk_viewing_prog_instance_fact
									AND COALESCE(a.overlap_batch,0)	= COALESCE(b.overlap_batch ,0)
									AND a.account_number = b.account_number 
		WHERE event_rank <> 1
		COMMIT 	
		
		DROP TABLE #midnight_working
		COMMIT
		
  		MESSAGE cast(now() as timestamp)||' | Begining M13.2 - V289_M13_individual_viewing_working_table insert , rows: '||@@rowcount TO CLIENT							
        -----------------------------------
        -- M13.2 - Final Viewing Output Table
        -----------------------------------

        MESSAGE cast(now() as timestamp)||' | Begining M13.2 - Update final individual viewing output table...' TO CLIENT
        commit -- ; --(^_^)

        -- This will need re-working to make sure we get the right data
        -- Also needs to be MA version for start/end times. Have cheated here for now

        truncate table V289_M13_individual_viewing_live_vosdal
        commit -- ; --(^_^)

        insert into     V289_M13_individual_viewing_live_vosdal(
                                                SUBSCRIBER_ID
                                                ,ACCOUNT_NUMBER
                                                ,STB_BROADCAST_START_TIME
                                                ,STB_BROADCAST_END_TIME
                                                ,STB_EVENT_START_TIME
                                                ,TIMESHIFT
                                                ,service_key    -- field name OK, but should be parent_service_key
                                                ,Platform_flag
                                                ,Original_Service_key   -- field name OK, but should be service_key
                                                ,AdSmart_flag
                                                ,DTH_VIEWING_EVENT_ID -- will populate with pk_viewing_prog_instance_fact
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
        select          m13.subscriber_id
						,m13.account_number
						,barb_min_start_date_time_utc
						,barb_min_end_date_time_utc
						,event_start_date_time
						,CASE WHEN    live_recorded = 'LIVE'  THEN    0
								ELSE    ((case when (event_start_date_BARB - barb_min_start_date) < 0 then 0
														else (event_start_date_BARB - barb_min_start_date)
											end
										)+ 1)
								END AS TIMESHIFT
						,parent_service_key
						,HD_flag
						,service_key
						,0
						,pk_viewing_prog_instance_fact -- event_id
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
        from            V289_M13_individual_viewing_working_table m13
        where           person_1 + person_2 + person_3 + person_4 + person_5 + person_6 + person_7 + person_8 + person_9 + person_10
						+ person_11 + person_12 + person_13 + person_14 + person_15 + person_16 > 0
                        AND (TIMESHIFT < 2 and viewing_type_flag = 0)
						AND DATE (event_start_date_time) = @proc_date
        commit -- ; --(^_^)


        truncate table V289_M13_individual_viewing_timeshift_pullvod
        commit -- ; --(^_^)

        insert into     V289_M13_individual_viewing_live_vosdal( -- ALL VIEWING TO GO INTO A SINGLE TABLE FOR TE --V289_M13_individual_viewing_timeshift_pullvod(
                                                SUBSCRIBER_ID
                                                ,ACCOUNT_NUMBER
                                                ,STB_BROADCAST_START_TIME
                                                ,STB_BROADCAST_END_TIME
                                                ,STB_EVENT_START_TIME
                                                ,TIMESHIFT
                                                ,service_key    -- field name OK, but should be parent_service_key
                                                ,Platform_flag
                                                ,Original_Service_key   -- field name OK, but should be service_key
                                                ,AdSmart_flag
                                                ,DTH_VIEWING_EVENT_ID -- will populate with pk_viewing_prog_instance_fact
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
        select          m13.subscriber_id
						,m13.account_number
						,barb_min_start_date_time_utc
						,barb_min_end_date_time_utc
						,event_start_date_time
						,CASE	WHEN    live_recorded = 'LIVE'  THEN    0
								ELSE    ((case when (event_start_date_BARB - barb_min_start_date) < 0 then 0
												else (event_start_date_BARB - barb_min_start_date)
										end
										)+ 1)
								END AS TIMESHIFT
						,parent_service_key
						,HD_flag
						,service_key
						,0
						,pk_viewing_prog_instance_fact -- event_id
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
        from            V289_M13_individual_viewing_working_table m13
        where           person_1 + person_2 + person_3 + person_4 + person_5 + person_6 + person_7 + person_8 + person_9 + person_10
                        + person_11 + person_12 + person_13 + person_14 + person_15 + person_16 > 0
                        AND (TIMESHIFT > 1 or viewing_type_flag = 1)
						AND DATE (event_start_date_time) = @proc_date
        commit -- ; --(^_^)

		
        MESSAGE cast(now() as timestamp)||' | M13.2 - Update subscriber_is 99 for Pull VOD events' TO CLIENT
		
		SELECT 	b.account_number 
				,CONVERT (integer,min(si_external_identifier)) as subscriber_id
				,CONVERT (bit, max(case when si_service_instance_type = 'Primary DTV' then 1 else 0 end)) as primary_box
		INTO 	#subscriber_details
		FROM 	CUST_SERVICE_INSTANCE as b
		INNER JOIN V289_M13_individual_viewing_live_vosdal as base 	ON   base.account_number = b.account_number AND base.subscriber_id = 99
		WHERE    si_service_instance_type = 'Primary DTV'
			AND @proc_date BETWEEN effective_from_dt AND effective_to_dt
		GROUP BY b.account_number 
		
		COMMIT 
		CREATE HG INDEX wef ON #subscriber_details(account_number)
		COMMIT
		
		UPDATE V289_M13_individual_viewing_live_vosdal
		SET a.subscriber_id = b.subscriber_id
		FROM V289_M13_individual_viewing_live_vosdal AS a 
		JOIN #subscriber_details as b ON a.account_number = b.account_number
		WHERE a.subscriber_id = 99 
		
		MESSAGE cast(now() as timestamp)||' | M13.2 - Subscriber_is 99 updated: '||@@rowcount  TO CLIENT
		COMMIT


        -----------------------------------
        -- M13.3 - Final Individual Table
        -----------------------------------

        MESSAGE cast(now() as timestamp)||' | Begining M13.3 - Update final individual details table...' TO CLIENT
        commit -- ; --(^_^)

        truncate table V289_M13_individual_details
        commit -- ; --(^_^)

        insert into     V289_M13_individual_details     (
                                                                                                        account_number
                                                                                                ,       person_number
                                                                                                ,       ind_scaling_weight
                                                                                                ,       gender
                                                                                                ,       age_band
                                                                                                ,       head_of_hhd
                                                                                                ,       hhsize
                                                                                        )
        select
                        hh.account_number
                ,       hh.hh_person_number
                ,       w.scaling_weighting
                ,       case
                                when hh.person_gender = 'M' then 1
                                when hh.person_gender = 'F' then 2
                                else 99
                        end             as      gender
                ,       case
                                when hh.person_ageband = '0-11'     then 0 -- between 0 and 11 then 0
                                when hh.person_ageband = '12-19'    then 1 -- between 12 and 19 then 1
                                when hh.person_ageband = '20-24'    then 2 --  between 20 and 24 then 2
                                when hh.person_ageband = '25-34'    then 3 -- between 25 and 34 then 3
                                when hh.person_ageband = '35-44'    then 4 -- between 35 and 44 then 4
                                when hh.person_ageband = '45-64'    then 5 --  between 45 and 64 then 5
                                when hh.person_age >= 65 then 6
                                else 99
                        end             as      age_band
                ,       hh.person_head
                ,       hh.household_size
        from
                                        V289_M08_sky_hh_composition                     as      hh
                inner join      V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING  as      w       on      hh.account_number       =       w.account_number
																						and     hh.HH_person_number     =       w.HH_person_number

        WHERE	(
						hh.panel_flag				=	1
					or	hh.nonviewer_household 	=	1
				)
        commit -- ; --(^_^)
		
		------ Clearing the working table, leaving the split events that spanned midnight
		DELETE FROM V289_M13_individual_viewing_working_table 
		WHERE ((DATE (event_start_date_time) <= @proc_date) OR (keeper <> 1 ))
	
		
		SELECT pk_viewing_prog_instance_fact, count(*) hits 
		INTO #t1
		FROM V289_M13_individual_viewing_working_table 
		GROUP BY pk_viewing_prog_instance_fact 
		HAVING hits >=3
		COMMIT
		
		CREATE HG INDEX id1 ON #t1 (pk_viewing_prog_instance_fact)
		COMMIT
		
		DELETE FROM V289_M13_individual_viewing_working_table 
		WHERE pk_viewing_prog_instance_fact IN (SELECT pk_viewing_prog_instance_fact FROM #t1)
        ---------------------------------------------------------------------------------------

end; -- procedure

commit;
grant execute on v289_M13_Create_Final_TechEdge_Output_Tables to vespa_group_low_security;
commit;



