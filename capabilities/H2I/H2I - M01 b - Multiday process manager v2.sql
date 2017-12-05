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
**Project Name:							Skyview H2I
**Analysts:                             Angel Donnarumma	(angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jason Thompson		(Jason.Thompson@skyiq.co.uk)
										,Hoi Yu Tang		(HoiYu.Tang@skyiq.co.uk)
										,Jose Pitteloud		(jose.pitteloud@skyiq.co.uk)
**Stakeholder:                          SkyIQ
										,Jose Loureda		(Jose.Loureda@skyiq.co.uk)
**Due Date:                             11/07/2014
**Project Code (Insight Collation):     v289
**Sharepoint Folder:    

	http://sp-department.bskyb.com/sites/SIGEvolved/Shared%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FSIGEvolved%2FShared%20Documents%2F01%20Analysis%20Requests%2FV289%20-%20Skyview%20Futures%2F01%20Plans%20Briefs%20and%20Project%20Admin
                                                                        
**Business Brief:

	This module is in charge of managing the execution of the M01 - process manager to run over a date range. It will execute the process manager for each day requested and after each run it will insert the outputs tables in the historic view

**Module:
	
	M01B: Process Manager
			M01B.0 - Initialising Environment
			M01B.1 - 
			M01B.2 - 
			M01B.3 - 
	
	
-------			EXECUTE v289_m01_B_Multi_day_process_manager 0, '2013-09-20','2013-09-21', 10, 'Run 1 30-10 AM'	
--------------------------------------------------------------------------------------------------------------
*/


-----------------------------------
-- M01B.0 - Initialising Environment
-----------------------------------

CREATE OR REPLACE PROCEDURE v289_m01_B_Multi_day_process_manager
	 @fresh_start    	BIT 		= 0 
    ,@start_date     	DATE 		= NULL
    ,@end_date     		DATE		= NULL
	,@sample_prop   	SMALLINT    = 100
	,@comments			VARCHAR	(500)
	,@m12				BIT			= 0
	,@m13				BIT			= 0
	,@m11				BIT			= 0
	,@h2I				BIT 		= 0 
	,@M000				BIT			= 0
	
AS BEGIN

	MESSAGE cast(now() as timestamp)||' | Begining  M01B.0 - Initialising Environment' TO CLIENT
	
	-- Variables
	DECLARE @current_day 	DATE
	DECLARE @sql_       	VARCHAR(2000)
	DECLARE @sql_2       	VARCHAR(2000)
	DECLARE @run_id			INT 
	DECLARE @exe_status		INT
	DECLARE @tablename		VARCHAR(200)
	DECLARE @writing 		BIT = 0 
	
	IF EXISTS (SELECT role_name From  sysrolegrants WHERE grantee_name = user_name() AND role_name = 'vespa_shared_admin_group') 
	SET @writing = 1 
	
	
	
	SET @run_id = (SELECT COALESCE(MAX(run_id)+1, 1) FROM vespa_shared.V289_Master_run_log) 
	
	-- Inserting the run info to the log
 
	INSERT INTO vespa_shared.V289_Master_run_log (
		  		 run_id 			
				, start_date 		
				, end_date 			
				, sample_size 		
				, fresh_start		
				, Comments			
				)
	SELECT 	
			  @run_id
			, @start_date     	 
			, @end_date
			, @sample_prop
			, @fresh_start
			, @comments
			
	COMMIT

	-- Creating the dates tables
	MESSAGE cast(now() as timestamp)||' | @ M01B.1: Identifying Dates to process' TO CLIENT

	INSERT INTO  vespa_shared.v289_Process_dates 
					( calendar_date		
					, processed 			
					, process_started 		
					, process_ended 		
					, run_id 
					)
	SELECT DISTINCT 
		  calendar_date
		, CAST (0 AS BIT ) AS processed
		, CAST (NULL AS DATE) AS process_started
		, CAST (NULL AS DATE) AS process_ended
		, @run_id AS run_id
	INTO vespa_shared.v289_Process_dates
	FROM SKY_CALENDAR
	WHERE calendar_date BETWEEN @start_date AND @end_date
	
	COMMIT
		
	MESSAGE cast(now() as timestamp)||' | @ M01B.1: Identifying Dates to process DONE. Number of days: '||@@rowcount TO CLIENT
	
		
	WHILE EXISTS (SELECT top 1 calendar_date FROM vespa_shared.v289_Process_dates WHERE run_id = @run_id AND processed = 0)
	BEGIN 	
	
		SET @current_day = (SELECT top 1 calendar_date FROM vespa_shared.v289_Process_dates WHERE run_id = @run_id AND processed = 0)
		
		SET @exe_status = -1
		
		IF @M000 = 1  
		BEGIN
		EXECUTE @exe_status = v289_m000_Prevalidation @current_day
		END
		ELSE SET @exe_status = 0 
		IF @exe_status = 0 
			MESSAGE cast(now() as timestamp)||' | @ M01B.0 Pre validation module OK: '||@current_day TO CLIENT
		ELSE 
		BEGIN
			MESSAGE cast(now() as timestamp)||' | @ M01B.0 Pre validation module FAILED: '||@exe_status TO CLIENT
			goto end_
		END
		
		SET @exe_status = -1
			
		SET @sql_ = ' @exe_status = v289_m01_process_manager '|| @fresh_start||', '||''''||@current_day ||''''||', ' ||@sample_prop 
			
			
		MESSAGE cast(now() as timestamp)||' | @ M01B.1: Executing day: '||@current_day TO CLIENT
		
		UPDATE vespa_shared.v289_Process_dates
		SET process_started = GETDATE() 
		WHERE calendar_date = @current_day AND run_id = @run_id
		COMMIT 
			
		MESSAGE 'Executing: '|| @sql_ TO CLIENT
		
		SET @exe_status = -1
		
		----------------------------------------------------
		If @H2I = 1 
		EXECUTE (@sql_)			---------------------------- MAIN PROCESS. 
		----------------------------------------------------			
		
		UPDATE vespa_shared.v289_Process_dates
		SET   process_ended 	= GETDATE() 
			, execution_code 	= @exe_status
			, processed 		=  1 
		WHERE calendar_date = @current_day AND run_id = @run_id	
		
		MESSAGE cast(now() as timestamp)||' | @ M01B.1: Day processed: '||@current_day TO CLIENT
		COMMIT 
	
		-----------------------------------	
		-- EFFECTIVE SAMPLE SIZE 
		-----------------------------------	
		
		INSERT INTO vespa_shared.M12_effective_sample_size
		SELECT  sum(scaling_weighting * scaling_weighting)AS large
        ,   sum(scaling_weighting) AS small
        ,   (small * small) / large AS effective_sample_size
        ,   count(1) AS total_ind
        ,   count(DISTINCT m11.account_number) AS total_accounts
        ,   scaling_date
		,   @run_id
		FROM V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING AS m11
		JOIN V289_M08_SKY_HH_composition AS m08 on   m11.account_number      = m08.account_number
												 AND m11.hh_person_number    = m08.hh_person_number
												 AND m08.panel_flag = 1
		GROUP BY scaling_date				
		-----------------------------------	
		-- M11.0 - Pasting results into historic tables 
		-----------------------------------
		MESSAGE cast(now() as timestamp)||' | @ M01B: Saving M11 tables'
		IF @m11 = 1 
		BEGIN
			
			--SC3I_weightings 
			SET @tablename = 'SC3I_weightings_'||DATEFORMAT(@current_day, 'YYYYMMDD')||'_'||RIGHT(CAST('00'||@run_id AS VARCHAR),3)
			SET @sql_= 
				' @exe_status = CALL dba.sp_create_table (''vespa_shared'',' ||''''||@tablename||''''||','||''''||
				'scaling_day AS date,' || 
				'scaling_segment_ID AS int,' ||
				'vespa_accounts AS bigint,' ||
				'sky_base_accounts AS bigint,' ||
				'weighting AS double,' ||
				'sum_of_weights AS double,' ||
				'indices_actual AS double,' ||
				'indices_weighted AS double,' ||
				'convergence AS tinyint,' ||
				'primary key (scaling_day, scaling_segment_ID)'||''''||')'

			MESSAGE 'Creating table in vespa shared.'||@tablename TO CLIENT 
			SET @exe_status = -1
			EXECUTE (@sql_)
			IF  @exe_status = 0 	SET  @exe_status = -1
			COMMIT
			WAITFOR DELAY '00:00:05'
			IF NOT EXISTS(SELECT tname FROM syscatalog WHERE creator = 'vespa_shared' and tabletype = 'TABLE' and upper(tname) = UPPER(@tablename))   
			BEGIN
				SET @exe_status = -1
				EXECUTE (@sql_)
				IF  @exe_status = 0 	SET  @exe_status = -1
				WAITFOR DELAY '00:00:05'
			END
				
			IF EXISTS(SELECT tname FROM syscatalog WHERE creator = 'vespa_shared' and tabletype = 'TABLE' and upper(tname) = UPPER(@tablename))   
			BEGIN 
				SET @sql_2 = 'INSERT INTO vespa_shared.'||@tablename
				SET @sql_2 = @sql_2 ||' SELECT * FROM SC3I_weightings '
				SET @sql_2 = @sql_2 ||'COMMIT '
				SET @sql_2 = @sql_2 ||'create hg index hg1 on vespa_shared.'||@tablename||' (scaling_segment_ID) '
				SET @sql_2 = @sql_2 ||'commit'
					MESSAGE 'EXECUTING: '||@sql_2 TO CLIENT 
				
				
				EXECUTE  (@sql_2)
			END 
			ELSE
			BEGIN 
				MESSAGE 'Table creation in vespa shared  failed, results stored in local schema at: '||@tablename TO CLIENT 
				SET @sql_2 = 'SELECT * INTO '||@tablename
				SET @sql_2 = @sql_2 ||' FROM SC3I_weightings'
				SET @sql_2 = @sql_2 ||' create hg index hg1 on '||@tablename||' (scaling_segment_ID)'
				SET @sql_2 = @sql_2 ||' commit'
										
				MESSAGE 'EXECUTING: '||@sql_2 TO CLIENT 
			
				EXECUTE  (@sql_2)
			
			END
			COMMIT 

			MESSAGE cast(now() as timestamp)||' | @ M01B.5: Saving M11 tables DONE' TO CLIENT			
			
		END -- End of @M11 saving tables

		-----------------------------------	
		-- M12.0 - Pasting results into historic tables 
		-----------------------------------
		MESSAGE cast(now() as timestamp)||' | @ M01B: Saving M12 tables'
		IF @m12 = 1 
		BEGIN
			SET @tablename = 'V289_M12_historic_results_'||DATEFORMAT(@current_day, 'YYYYMMDD')||'_'||RIGHT(CAST('00'||@run_id AS VARCHAR),3)
			SET @sql_= '@exe_status = CALL dba.sp_create_table (''vespa_shared'',' ||''''||@tablename||''''||','||''''||'source   AS VARCHAR (5) ,scaling_date AS datetime  ,service_key AS INT, channel_name AS varchar (200) ,channel_pack AS varchar (200) ,daypart  AS varchar (11) ,event_id  AS bigint  ,session_start  AS datetime, session_end  AS datetime, overlap_batch AS int ,duration_seg AS int  ,account_number AS varchar (20) , viewing_type_flag AS tinyint, hh_person_number AS tinyint  ,age AS varchar (5) ,gender   AS VARCHAR (9) ,weight   AS REAL'||''''||')'

			MESSAGE 'Creating table in vespa shared.'||@tablename TO CLIENT 
			SET @exe_status = -1
			EXECUTE  (@sql_)
			IF  @exe_status = 0 	SET  @exe_status = -1
			COMMIT
			WAITFOR DELAY '00:00:05'
			IF NOT EXISTS(SELECT tname FROM syscatalog WHERE creator = 'vespa_shared' and tabletype = 'TABLE' and upper(tname) = UPPER(@tablename))   
			BEGIN
				SET @exe_status = -1
				EXECUTE (@sql_)
				IF  @exe_status = 0 	SET  @exe_status = -1
				WAITFOR DELAY '00:00:05'
			END
				
			IF EXISTS(SELECT tname FROM syscatalog WHERE creator = 'vespa_shared' and tabletype = 'TABLE' and upper(tname) = UPPER(@tablename))   
			BEGIN 
				SET @sql_2 = 'INSERT INTO vespa_shared.'||@tablename
				SET @sql_2 = @sql_2 ||' SELECT * FROM v289_m12_dailychecks_base '
				SET @sql_2 = @sql_2 ||'COMMIT '
				SET @sql_2 = @sql_2 ||'create hg index hg1 on vespa_shared.'||@tablename||' (account_number) '
				SET @sql_2 = @sql_2 ||'create hg index hg2 on vespa_shared.'||@tablename||' (event_id) '
				SET @sql_2 = @sql_2 ||'create hg index hg3 on vespa_shared.'||@tablename||' (channel_name) '
				SET @sql_2 = @sql_2 ||'create lf index lf1 on vespa_shared.'||@tablename||' (hh_person_number) '
				SET @sql_2 = @sql_2 ||'create lf index lf2 on vespa_shared.'||@tablename||' (channel_pack) '
				SET @sql_2 = @sql_2 ||'create lf index lf3 on vespa_shared.'||@tablename||' (daypart) '
				SET @sql_2 = @sql_2 ||'commit'
				
				MESSAGE 'EXECUTING: '||@sql_2 TO CLIENT 
				
				EXECUTE (@sql_2)

			END 
			ELSE
			BEGIN 
				MESSAGE 'Table creation in vespa shared  failed, results stored in local schema at: '||@tablename TO CLIENT 
				SET @sql_2 = 'SELECT * INTO '||@tablename
				SET @sql_2 = @sql_2 ||' FROM v289_m12_dailychecks_base'
				SET @sql_2 = @sql_2 ||' create hg index hg1 on '||@tablename||' (account_number)'
				SET @sql_2 = @sql_2 ||' create hg index hg2 on '||@tablename||' (event_id)'
				SET @sql_2 = @sql_2 ||' create hg index hg3 on '||@tablename||' (channel_name)'
				SET @sql_2 = @sql_2 ||' create lf index lf1 on '||@tablename||' (hh_person_number)'
				SET @sql_2 = @sql_2 ||' create lf index lf2 on '||@tablename||' (channel_pack)'
				SET @sql_2 = @sql_2 ||' create lf index lf3 on '||@tablename||' (daypart)'
				SET @sql_2 = @sql_2 ||' commit'
										
				MESSAGE 'EXECUTING: '||@sql_2 TO CLIENT 
			
				EXECUTE  (@sql_2)
			
			END
				
			
			--DROP TABLE v289_m12_dailychecks_base
			COMMIT 

			
			--v289_S12_weighted_duration_skyview
			SET @tablename = 'v289_S12_weighted_duration_skyview_'||DATEFORMAT(@current_day, 'YYYYMMDD')||'_'||RIGHT(CAST('00'||@run_id AS VARCHAR),3)
			SET @sql_= '@exe_status = CALL dba.sp_create_table (''vespa_shared'',' ||''''||@tablename||''''||','||''''||'source AS VARCHAR (4),scaling_date AS date,age AS varchar(10),gender AS varchar(9),daypart AS varchar(11),household AS varchar(20),person AS integer,viewing_type_flag AS smallint,ukbase AS double,viewersbase AS double,duration_mins AS double,duration_weighted_mins AS double'||''''||')'

			MESSAGE 'Creating table in vespa shared.'||@tablename TO CLIENT 
			SET @exe_status = -1
			EXECUTE  (@sql_)
			IF  @exe_status = 0 	SET  @exe_status = -1
			COMMIT
			WAITFOR DELAY '00:00:05'
			IF NOT EXISTS(SELECT tname FROM syscatalog WHERE creator = 'vespa_shared' and tabletype = 'TABLE' and upper(tname) = UPPER(@tablename))   
			BEGIN
				SET @exe_status = -1
				EXECUTE (@sql_)
				WAITFOR DELAY '00:00:05'
			END
				
			IF EXISTS(SELECT tname FROM syscatalog WHERE creator = 'vespa_shared' and tabletype = 'TABLE' and upper(tname) = UPPER(@tablename))   
			BEGIN 
				SET @sql_2 = 'INSERT INTO vespa_shared.'||@tablename
				SET @sql_2 = @sql_2 ||' SELECT * FROM v289_S12_weighted_duration_skyview '
				SET @sql_2 = @sql_2 ||'COMMIT '
				SET @sql_2 = @sql_2 ||'create hg index hg1 on vespa_shared.'||@tablename||' (household) '
				SET @sql_2 = @sql_2 ||'commit'
					MESSAGE 'EXECUTING: '||@sql_2 TO CLIENT 
							
				EXECUTE  (@sql_2)
			
			END 
			ELSE
			BEGIN 
				MESSAGE 'Table creation in vespa shared  failed, results stored in local schema at: '||@tablename TO CLIENT 
				SET @sql_2 = 'SELECT * INTO '||@tablename
				SET @sql_2 = @sql_2 ||' FROM v289_S12_weighted_duration_skyview'
				SET @sql_2 = @sql_2 ||' create hg index hg1 on '||@tablename||' (household)'
				SET @sql_2 = @sql_2 ||' commit'
										
				MESSAGE 'EXECUTING: '||@sql_2 TO CLIENT 
			
				EXECUTE (@sql_2)
			
			END
			COMMIT 

			
			--v289_m12_piv_distributions
			SET @tablename = 'v289_m12_piv_distributions_'||DATEFORMAT(@current_day, 'YYYYMMDD')||'_'||RIGHT(CAST('00'||@run_id AS VARCHAR),3)
			SET @sql_= '@exe_status = CALL dba.sp_create_table (''vespa_shared'',' ||''''||@tablename||''''||','||''''||'source AS VARCHAR (5),thedate AS datetime,hhsize AS int,viewers_size AS int,session_size AS int, hits AS int'||''''||')'

			MESSAGE 'Creating table in vespa_shared.'||@tablename TO CLIENT 
			SET @exe_status = -1
			EXECUTE (@sql_)
			IF  @exe_status = 0 	SET  @exe_status = -1
			COMMIT
			WAITFOR DELAY '00:00:05'
			IF NOT EXISTS(SELECT tname FROM syscatalog WHERE creator = 'vespa_shared' and tabletype = 'TABLE' and upper(tname) = UPPER(@tablename))   
			BEGIN
				SET @exe_status = -1
				EXECUTE (@sql_)
				IF  @exe_status = 0 	SET  @exe_status = -1
				WAITFOR DELAY '00:00:05'
			END
				
			IF EXISTS(SELECT tname FROM syscatalog WHERE creator = 'vespa_shared' and tabletype = 'TABLE' and upper(tname) = UPPER(@tablename))   
			BEGIN 
				SET @sql_2 = 'INSERT INTO vespa_shared.'||@tablename
				SET @sql_2 = @sql_2 ||' SELECT * FROM v289_m12_piv_distributions '
				SET @sql_2 = @sql_2 ||'COMMIT '
				SET @sql_2 = @sql_2 ||'commit'
				
				MESSAGE 'EXECUTING: '||@sql_2 TO CLIENT 
			
				EXECUTE (@sql_2)
					
			END 
			ELSE
			BEGIN 
				MESSAGE 'Table creation in vespa shared  failed, results stored in local schema at: '||@tablename TO CLIENT 
				SET @sql_2 = 'SELECT * INTO '||@tablename
				SET @sql_2 = @sql_2 ||' FROM v289_m12_piv_distributions'
				SET @sql_2 = @sql_2 ||' commit'
										
				MESSAGE 'EXECUTING: '||@sql_2 TO CLIENT 
			
				EXECUTE (@sql_2)
			
			END
			COMMIT 
				
			--v289_s12_overall_consumption_hhlevel
			SET @tablename = 'v289_s12_overall_consumption_hhlevel_'||DATEFORMAT(@current_day, 'YYYYMMDD')||'_'||RIGHT(CAST('00'||@run_id AS VARCHAR),3)
			SET @sql_= ' @exe_status = CALL dba.sp_create_table (''vespa_shared'',' ||''''||@tablename||''''||','||''''||'source AS VARCHAR (5),scaling_date AS DATE,sample AS int,Scaled_Sample AS float,viewers_scaled_sample AS float,tmw_tot AS float,tmws_tot AS float,thw_avg AS float,thws_avg AS float,tmw_tot_pv AS float,tmws_tot_pv AS float,thw_avg_pv AS float,thws_avg_pv AS float'||''''||')'
			
			MESSAGE 'Creating table in vespa shared.'||@tablename TO CLIENT 
			SET @exe_status = -1
			EXECUTE (@sql_)
			IF  @exe_status = 0 	SET  @exe_status = -1
			COMMIT
			WAITFOR DELAY '00:00:05'
			IF NOT EXISTS(SELECT tname FROM syscatalog WHERE creator = 'vespa_shared' and tabletype = 'TABLE' and upper(tname) = UPPER(@tablename))   
			BEGIN
				SET @exe_status = -1
				EXECUTE (@sql_)
				IF  @exe_status = 0 	SET  @exe_status = -1
				WAITFOR DELAY '00:00:05'
			END
				
			IF EXISTS(SELECT tname FROM syscatalog WHERE creator = 'vespa_shared' and tabletype = 'TABLE' and upper(tname) = UPPER(@tablename))   
			BEGIN 
				SET @sql_2 = ' INSERT INTO vespa_shared.'||@tablename
				SET @sql_2 = @sql_2 ||' SELECT * FROM v289_s12_overall_consumption_hhlevel '
				SET @sql_2 = @sql_2 ||'COMMIT '
				SET @sql_2 = @sql_2 ||'commit'
				MESSAGE 'EXECUTING: '||@sql_2 TO CLIENT 
				
				EXECUTE (@sql_2)
				
			END 
			ELSE
			BEGIN 
				MESSAGE 'Table creation in vespa shared  failed, results stored in local schema at: '||@tablename TO CLIENT 
				SET @sql_2 = ' SELECT * INTO '||@tablename
				SET @sql_2 = @sql_2 ||' FROM v289_s12_overall_consumption_hhlevel'
				SET @sql_2 = @sql_2 ||' commit'
										
				MESSAGE 'EXECUTING: '||@sql_2 TO CLIENT 
				
				EXECUTE (@sql_2)
				
			END
				
			
			COMMIT 


			--V289_s12_v_genderage_distribution
			SET @tablename = 'V289_s12_v_genderage_distribution_'||DATEFORMAT(@current_day, 'YYYYMMDD')||'_'||RIGHT(CAST('00'||@run_id AS VARCHAR),3)
			SET @sql_= ' @exe_status = CALL dba.sp_create_table (''vespa_shared'',' ||''''||@tablename||''''||','||''''||'source AS VARCHAR (5),ageband AS VARCHAR (10),genre AS VARCHAR (9),hits AS int,sow AS int'||''''||')'

			MESSAGE 'Creating table in vespa shared.'||@tablename TO CLIENT 
			SET @exe_status = -1
			EXECUTE (@sql_)
			IF  @exe_status = 0 	SET  @exe_status = -1
			COMMIT
			WAITFOR DELAY '00:00:05'
			IF NOT EXISTS(SELECT tname FROM syscatalog WHERE creator = 'vespa_shared' and tabletype = 'TABLE' and upper(tname) = UPPER(@tablename))   
			BEGIN
				SET @exe_status = -1
				EXECUTE (@sql_)
				IF  @exe_status = 0 	SET  @exe_status = -1
				WAITFOR DELAY '00:00:05'
			END
				
			IF EXISTS(SELECT tname FROM syscatalog WHERE creator = 'vespa_shared' and tabletype = 'TABLE' and upper(tname) = UPPER(@tablename))   
			BEGIN 
				SET @sql_2 = 'INSERT INTO vespa_shared.'||@tablename
				SET @sql_2 = @sql_2 ||' SELECT * FROM V289_s12_v_genderage_distribution '
				SET @sql_2 = @sql_2 ||'COMMIT '
				SET @sql_2 = @sql_2 ||'commit'
				MESSAGE 'EXECUTING: '||@sql_2 TO CLIENT 
				
				EXECUTE (@sql_2)
				
			END 
			ELSE
			BEGIN 
				MESSAGE 'Table creation in vespa shared  failed, results stored in local schema at: '||@tablename TO CLIENT 
				SET @sql_2 = 'SELECT * INTO '||@tablename
				SET @sql_2 = @sql_2 ||' FROM V289_s12_v_genderage_distribution'
				SET @sql_2 = @sql_2 ||' commit'
										
				MESSAGE 'EXECUTING: '||@sql_2 TO CLIENT 
				
				EXECUTE (@sql_2)
				
			END
			COMMIT 

			--V289_s12_v_hhsize_distribution
			SET @tablename = 'V289_s12_v_hhsize_distribution_'||DATEFORMAT(@current_day, 'YYYYMMDD')||'_'||RIGHT(CAST('00'||@run_id AS VARCHAR),3)
			SET @sql_= '@exe_status = CALL dba.sp_create_table (''vespa_shared'',' ||''''||@tablename||''''||','||''''||'source AS VARCHAR (5),hhsize AS int,hits AS int,ukbase AS float'||''''||')'
		
			MESSAGE 'Creating table in vespa shared.'||@tablename TO CLIENT 
			SET @exe_status = -1
			EXECUTE (@sql_)
			IF  @exe_status = 0 	SET  @exe_status = -1
			COMMIT
			WAITFOR DELAY '00:00:05'
			IF NOT EXISTS(SELECT tname FROM syscatalog WHERE creator = 'vespa_shared' and tabletype = 'TABLE' and upper(tname) = UPPER(@tablename))   
			BEGIN
				SET @exe_status = -1
				EXECUTE (@sql_)
				IF  @exe_status = 0 	SET  @exe_status = -1
				WAITFOR DELAY '00:00:05'
			END
				
			IF EXISTS(SELECT tname FROM syscatalog WHERE creator = 'vespa_shared' and tabletype = 'TABLE' and upper(tname) = UPPER(@tablename))   
			BEGIN 
				SET @sql_2 = 'INSERT INTO vespa_shared.'||@tablename
				SET @sql_2 = @sql_2 ||' SELECT * FROM V289_s12_v_hhsize_distribution '
				SET @sql_2 = @sql_2 ||'COMMIT '
				SET @sql_2 = @sql_2 ||'commit'
				MESSAGE 'EXECUTING: '||@sql_2 TO CLIENT 
				
				EXECUTE (@sql_2)
				
			END 
			ELSE
			BEGIN 
				MESSAGE 'Table creation in vespa shared  failed, results stored in local schema at: '||@tablename TO CLIENT 
				SET @sql_2 = 'SELECT * INTO '||@tablename
				SET @sql_2 = @sql_2 ||' FROM V289_s12_v_hhsize_distribution'
				SET @sql_2 = @sql_2 ||' commit'
										
				MESSAGE 'EXECUTING: '||@sql_2 TO CLIENT 
				
				EXECUTE (@sql_2)
				
			END
			COMMIT 

			MESSAGE cast(now() as timestamp)||' | @ M01B.5: V289_historic_results table DONE' TO CLIENT			
			
		END -- End of @M12 saving tables
		
		-----------------------------------	
		-- M13.0 - Pasting results into historic tables 
		-----------------------------------		
		
		MESSAGE cast(now() as timestamp)||' | @ M01B: Saving M13 tables'
		IF @m13 = 1 
		BEGIN
			------------------------------------------------------
			-- M13 Live individual viewing 
			------------------------------------------------------
			MESSAGE cast(now() as timestamp)||' | @ M01B: V289_M13_individual_viewing_live_vosdal'
			SET @tablename 	= 'V289_M13_individual_viewing_live_vosdal_'||DATEFORMAT(@current_day, 'YYYYMMDD')||'_'||RIGHT(CAST('00'||@run_id AS VARCHAR),3)
			SET @sql_		= '@exe_status = CALL dba.sp_create_table (''vespa_shared'',' ||''''||@tablename||''''||','||''''||'SUBSCRIBER_ID AS decimal(10,0), ACCOUNT_NUMBER AS varchar(20), STB_BROADCAST_START_TIME AS DATETIME, STB_BROADCAST_END_TIME AS DATETIME, STB_EVENT_START_TIME AS DATETIME, TIMESHIFT AS INT, service_key AS INT, Platform_flag AS INT, Original_Service_key AS INT, AdSmart_flag AS INT, DTH_VIEWING_EVENT_ID AS bigint, person_1 AS smallint, person_2 AS smallint, person_3 AS smallint, person_4 AS smallint, person_5 AS smallint, person_6 AS smallint, person_7 AS smallint, person_8 AS smallint, person_9 AS smallint, person_10 AS smallint, person_11 AS smallint, person_12 AS smallint, person_13 AS smallint, person_14 AS smallint, person_15 AS smallint, person_16 AS smallint'||''''||')'


			MESSAGE 'Creating table in vespa shared.'||@tablename TO CLIENT 
			SET @exe_status = -1
			EXECUTE (@sql_)
			IF  @exe_status = 0 	SET  @exe_status = -1
			COMMIT
			WAITFOR DELAY '00:00:05'
			IF NOT EXISTS(SELECT tname FROM syscatalog WHERE creator = 'vespa_shared' and tabletype = 'TABLE' and upper(tname) = UPPER(@tablename))  
			BEGIN
				SET @exe_status = -1
				EXECUTE (@sql_)
				IF  @exe_status = 0 	SET  @exe_status = -1
				WAITFOR DELAY '00:00:05'
			END			
			
	
			IF EXISTS(SELECT tname FROM syscatalog WHERE creator = 'vespa_shared' and tabletype = 'TABLE' and upper(tname) = UPPER(@tablename)) 
			BEGIN 
				SET @sql_2 = 'INSERT INTO vespa_shared.'||@tablename
				SET @sql_2 = @sql_2 ||' SELECT * FROM V289_M13_individual_viewing_live_vosdal'
				SET @sql_2 = @sql_2 ||' COMMIT '
				SET @sql_2 = @sql_2 ||' create hg index hg1 on vespa_shared.'||@tablename||' (account_number)'
				SET @sql_2 = @sql_2 ||' create hg index hg3 on vespa_shared.'||@tablename||' (service_key)'
				SET @sql_2 = @sql_2 ||' commit'
				
				MESSAGE 'EXECUTING: '||@sql_2 TO CLIENT 
				
				EXECUTE (@sql_2)
						
			END 
			ELSE
			BEGIN 
				MESSAGE 'Table creation in vespa shared  failed, results stored in local schema at: '||@tablename TO CLIENT 
				SET @sql_2 = 'SELECT * INTO '||@tablename
				SET @sql_2 = @sql_2 ||' FROM V289_M13_individual_viewing_live_vosdal'
				SET @sql_2 = @sql_2 ||' create hg index hg1 on '||@tablename||' (account_number)'
				SET @sql_2 = @sql_2 ||' create hg index hg3 on '||@tablename||' (service_key)'
				SET @sql_2 = @sql_2 ||' commit'
										
				MESSAGE 'EXECUTING: '||@sql_2 TO CLIENT 
				
				EXECUTE (@sql_2)
				
			END
			MESSAGE cast(now() as timestamp)||' | @ M01B.5: V289_M13_individual_viewing_timeshift_pullvod table DONE' TO CLIENT
			------------------------------------------------------	
			-- M13 	V289_M13_individual_viewing_timeshift_pullvod
			------------------------------------------------------
			MESSAGE cast(now() as timestamp)||' | @ M01B: V289_M13_individual_viewing_timeshift_pullvod'	
			SET @tablename 	= 'V289_M13_individual_viewing_timeshift_pullvod_'||DATEFORMAT(@current_day, 'YYYYMMDD')||'_'||RIGHT(CAST('00'||@run_id AS VARCHAR),3)
			SET @sql_		= '@exe_status = CALL dba.sp_create_table (''vespa_shared'',' ||''''||@tablename||''''||','||''''||'SUBSCRIBER_ID AS decimal(10,0), ACCOUNT_NUMBER AS varchar(20), STB_BROADCAST_START_TIME AS DATETIME, STB_BROADCAST_END_TIME AS DATETIME, STB_EVENT_START_TIME AS DATETIME, TIMESHIFT AS INT, service_key AS INT, Platform_flag AS INT, Original_Service_key AS INT, AdSmart_flag AS INT, DTH_VIEWING_EVENT_ID AS bigint, person_1 AS smallint, person_2 AS smallint, person_3 AS smallint, person_4 AS smallint, person_5 AS smallint, person_6 AS smallint, person_7 AS smallint, person_8 AS smallint, person_9 AS smallint, person_10 AS smallint, person_11 AS smallint, person_12 AS smallint, person_13 AS smallint, person_14 AS smallint, person_15 AS smallint, person_16 AS smallint'||''''||')'

			MESSAGE 'Creating table in vespa shared.'||@tablename TO CLIENT 
			SET @exe_status = -1
			EXECUTE (@sql_)
			IF  @exe_status = 0 	SET  @exe_status = -1
			COMMIT
			WAITFOR DELAY '00:00:05'
			IF object_id('vespa_shared.'||@tablename) IS NULL 
			BEGIN
				SET @exe_status = -1
				EXECUTE (@sql_)
				IF  @exe_status = 0 	SET  @exe_status = -1
				WAITFOR DELAY '00:00:05'
			END			
			
	
			IF object_id('vespa_shared.'||@tablename) IS NOT NULL 
			BEGIN 
				SET @sql_2 = 'INSERT INTO vespa_shared.'||@tablename
				SET @sql_2 = @sql_2 ||' SELECT * FROM V289_M13_individual_viewing_timeshift_pullvod'
				SET @sql_2 = @sql_2 ||' COMMIT '
				SET @sql_2 = @sql_2 ||' create hg index hg1 on vespa_shared.'||@tablename||' (account_number)'
				SET @sql_2 = @sql_2 ||' create hg index hg3 on vespa_shared.'||@tablename||' (service_key)'
				SET @sql_2 = @sql_2 ||' commit'

				MESSAGE 'EXECUTING: '||@sql_2 TO CLIENT
				
				EXECUTE (@sql_2)
				
		
			END 
			ELSE
			BEGIN 
				MESSAGE 'Table creation in vespa shared  failed, results stored in local schema at: '||@tablename TO CLIENT 
				SET @sql_2 = 'SELECT * INTO '||@tablename
				SET @sql_2 = @sql_2 ||' FROM V289_M13_individual_viewing_timeshift_pullvod'
				SET @sql_2 = @sql_2 ||' create hg index hg1 on '||@tablename||' (account_number)'
				SET @sql_2 = @sql_2 ||' create hg index hg3 on '||@tablename||' (service_key)'
				SET @sql_2 = @sql_2 ||' commit'
						
				MESSAGE 'EXECUTING: '||@sql_2 TO CLIENT 
				
				EXECUTE (@sql_2)
				
			END
				MESSAGE cast(now() as timestamp)||' | @ M01B.5: V289_M13_individual_viewing_timeshift_pullvod table DONE' TO CLIENT
			
			------------------------------------------------------	
			-- M13 	V289_M13_individual_details
			------------------------------------------------------
			MESSAGE cast(now() as timestamp)||' | @ M01B: V289_M13_individual_details'	
			SET @tablename 	= 'V289_M13_individual_details_'||DATEFORMAT(@current_day, 'YYYYMMDD')||'_'||RIGHT(CAST('00'||@run_id AS VARCHAR),3)
			SET @sql_		= '@exe_status = CALL dba.sp_create_table (''vespa_shared'',' ||''''||@tablename||''''||','||''''||'account_number AS varchar(20), person_number AS integer, ind_scaling_weight AS double, gender AS integer, age_band AS integer, head_of_hhd AS integer, hhsize AS integer'||''''||')'

			MESSAGE 'Creating table in vespa shared.'||@tablename TO CLIENT 
			SET @exe_status = -1
			EXECUTE (@sql_)
			IF  @exe_status = 0 	SET  @exe_status = -1
			COMMIT
			WAITFOR DELAY '00:00:05'
			IF object_id('vespa_shared.'||@tablename) IS NULL 
			BEGIN
				SET @exe_status = -1
				EXECUTE (@sql_)
				IF  @exe_status = 0 	SET  @exe_status = -1
				WAITFOR DELAY '00:00:05'
			END			
			
	
			IF object_id('vespa_shared.'||@tablename) IS NOT NULL 
			BEGIN 
				SET @sql_2 = 'INSERT INTO vespa_shared.'||@tablename
				SET @sql_2 = @sql_2 ||' SELECT * FROM V289_M13_individual_details'
				SET @sql_2 = @sql_2 ||' COMMIT '
				SET @sql_2 = @sql_2 ||' create hg index hg1 on vespa_shared.'||@tablename||' (account_number)'
				SET @sql_2 = @sql_2 ||' create lf index lf1 on vespa_shared.'||@tablename||' (person_number)'
				SET @sql_2 = @sql_2 ||' commit'
				MESSAGE 'EXECUTING: '||@sql_2 TO CLIENT 
				
				EXECUTE (@sql_2)
						
			END 
			ELSE
			BEGIN 
				MESSAGE 'Table creation in vespa shared  failed, results stored in local schema at: '||@tablename TO CLIENT 
				SET @sql_2 = 'SELECT * INTO '||@tablename
				SET @sql_2 = @sql_2 ||' FROM V289_M13_individual_details'
				SET @sql_2 = @sql_2 ||' COMMIT '
				SET @sql_2 = @sql_2 ||' create hg index hg1 on '||@tablename||' (account_number)'
				SET @sql_2 = @sql_2 ||' create lf index lf1 on '||@tablename||' (person_number)'
				SET @sql_2 = @sql_2 ||' commit'
										
				MESSAGE 'EXECUTING: '||@sql_2 TO CLIENT 
				
				EXECUTE (@sql_2)
				
			END
				MESSAGE cast(now() as timestamp)||' | @ M01B.5: V289_M13_individual_details table DONE' TO CLIENT
		
		END -- End of @M13 saving tables		
		
		
	END -- End of date loop
		MESSAGE cast(now() as timestamp)||' | @ M01B.1: Proccessing days DONE' TO CLIENT
	
	UPDATE vespa_shared.V289_Master_run_log
	SET completed_date = GETDATE()
	WHERE run_id = @run_id
	
	MESSAGE cast(now() as timestamp)||' | @ M01B.1: Master Log Updated' TO CLIENT
	end_:
END; 	-- End of procedure

COMMIT;
GRANT EXECUTE ON v289_m01_B_Multi_day_process_manager TO vespa_group_low_security;
COMMIT;



