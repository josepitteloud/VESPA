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

        M14: Create output tables for Olive
                        M14.0 - Create procedure
                        M14.1 - Create monthly output tables for Olive
                        M14.2 - Create view so that points to the last 2 month's of data


--------------------------------------------------------------------------------------------------------------
*/





---------------------------------


---------------------------
-- M14.0 - Create procedure
---------------------------

CREATE OR REPLACE PROCEDURE v289_M14_Create_Final_Olive_Output_Tables
AS BEGIN


	-----------------------------------------------
	-- M14.1 Create monthly output tables for Olive
	-----------------------------------------------
	MESSAGE CAST(NOW() AS TIMESTAMP)||' | M14.1 - Determining required monthly output tables' TO CLIENT

	--  Define required monthly output table names from H2I output table
	-- DROP TABLE #TARGET_TABLES;
	SELECT
			DT
		,   DATEPART(YEAR,DT)                                           AS  YYYY
		,   DATEPART(MONTH,DT)                                          AS  MM
		,   'V289_M14_event_individuals_' || YYYY || RIGHT(0 || MM,2)	AS  NAM
		,   'V289_M14_individuals_details_' || YYYY || RIGHT(0 || MM,2)	AS  NAM2
		,   RANK()  OVER    (ORDER BY DT)   AS  RNK
	INTO    #TARGET_TABLES
	FROM    (	-- Get unique date(s) available from the individually-assigned output data
				SELECT  DISTINCT(EVENT_DATE)    AS  DT
				FROM    V289_M10_session_individuals
			)   AS  A
	ORDER BY RNK
	COMMIT -- (^_^)
	
	-- SELECT TOP 20 * FROM #TARGET_TABLES;



	-----------------------------------------------
	-- M14.2 Create monthly output tables for Olive
	-----------------------------------------------
	MESSAGE CAST(NOW() AS TIMESTAMP)||' | M14.2 - Append H2I outputs to monthly tables' TO CLIENT

/* -- For testing in script form:
	CREATE OR REPLACE VARIABLE @I INT = 0	;
	CREATE OR REPLACE VARIABLE @SQL_ VARCHAR(10000)	;
	CREATE OR REPLACE VARIABLE @I_TAB VARCHAR(255)	;
	CREATE OR REPLACE VARIABLE @YYYY INT	;
	CREATE OR REPLACE VARIABLE @MM INT	;
	CREATE OR REPLACE VARIABLE @I_MAX INT	;
*/
--/* -- To be used in procedure form
	DECLARE @I INT = 0 COMMIT
	DECLARE @SQL_ VARCHAR(10000) COMMIT
	DECLARE @I_TAB VARCHAR(255) COMMIT
	DECLARE @I_TAB2 VARCHAR(255) COMMIT
	DECLARE @YYYY INT COMMIT
	DECLARE @MM INT COMMIT
	DECLARE @I_MAX INT COMMIT
	DECLARE @SCALING_DATE	DATE COMMIT
--*/
	-- Set the iteration limit
	SELECT @I_MAX = MAX(RNK) FROM #TARGET_TABLES	COMMIT -- (^_^)

	-- Now perform the loop over the number of monthly tables (usually this will be just the one, but this allows for processing many)
	WHILE @I < @I_MAX --(SELECT MAX(RNK) FROM #TARGET_TABLES)
		BEGIN

			-- Progress loop counter
			SET @I = @I + 1
			
			-- Update some variables for the i-th loop
			SELECT
					@I_TAB  =   NAM		-- target monthly table name
				,	@I_TAB2	=   NAM2		-- target monthly table name
				,   @YYYY   =   YYYY
				,   @MM     =   MM
			FROM    #TARGET_TABLES WHERE RNK = @I
			COMMIT
			
			
			------------------------------------------------------------------------------------------------------------------
			-- Check for existence of i-th monthly events table (if it does not exist, then create the table)
			------------------------------------------------------------------------------------------------------------------
			IF NOT EXISTS   (
								SELECT  1
								FROM	SYSOBJECTS
								WHERE
											[name]	=	@I_TAB
									AND     [uid]   =   USER_ID()	-- THIS WILL HAVE TO UPDATED IN PROD ENVIRONMENT, BUT RETAIN FOR DEV TESTING
							)
				BEGIN
						
						MESSAGE CAST(NOW() AS TIMESTAMP)||' | M14.2 - Creating new monthly table : ' || @I_TAB TO CLIENT
						
						SET @SQL_ = '
									CREATE TABLE ' || @I_TAB || '(
											scaling_date		date		not null
										,	event_id			bigint		not null
										,	account_number		varchar(20)	not null
										,	person_number		tinyint		not null
										,	scaling_weighting	float		not null
										,	event_start_datetime	timestamp	null	default null
										,	event_end_datetime	timestamp		null	default null
										)
									COMMIT
									
									CREATE HG INDEX HG_IDX_1 ON ' || @I_TAB || '(event_id)
									CREATE HG INDEX HG_IDX_2 ON ' || @I_TAB || '(account_number)
									CREATE HG INDEX HG_IDX_3 ON ' || @I_TAB || '(scaling_date)
									CREATE HG INDEX HG_IDX_4 ON ' || @I_TAB || '(event_start_datetime)
									CREATE HG INDEX HG_IDX_5 ON ' || @I_TAB || '(event_end_datetime)
									CREATE DATE INDEX DATE_IDX_1 ON ' || @I_TAB || '(scaling_date)
									CREATE DTTM INDEX DTTM_IDX_1 ON ' || @I_TAB || '(event_start_datetime)
									CREATE DTTM INDEX DTTM_IDX_2 ON ' || @I_TAB || '(event_end_datetime)
									COMMIT
									
									GRANT SELECT ON ' || @I_TAB || ' TO vespa_group_low_security
									COMMIT
									'
					--SELECT  @SQL_
					EXECUTE (@SQL_)
					COMMIT
				END
			COMMIT -- END IF
			
			
			-- Now insert the data into monthly table
			MESSAGE CAST(NOW() AS TIMESTAMP)||' | M14.2 - Inserting results into monthly table : ' || @I_TAB || ' ...' TO CLIENT
			SET @SQL_ = '	INSERT INTO ' || @I_TAB ||'
							SELECT
									M11.scaling_date
								,	M10.event_id -- needs to be DTH_viewing_event_ID
								,   M10.account_number
								,   M10.hh_person_number
								,	M11.scaling_weighting
								,	CASE
										WHEN	M10.overlap_batch IS NULL		THEN	NULL
										ELSE											M07.chunk_start
									END
								,	CASE
										WHEN	M10.overlap_batch IS NULL		THEN	NULL
										ELSE											M07.chunk_end
									END
							FROM    
											V289_M10_session_individuals			AS	M10
								INNER JOIN	V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING	AS	M11		ON	M10.account_number		=	M11.account_number
																								AND	M10.hh_person_number	=	M11.hh_person_number
																								AND	M10.event_date			=	M11.scaling_date
								INNER JOIN	V289_M07_dp_data						AS	M07		ON	M10.account_number		=	M07.account_number
																								AND	M10.event_id			=	M07.event_id
																								AND	(CASE	WHEN	M10.overlap_batch	IS NULL	THEN -1	ELSE	M10.overlap_batch	END)	=	(CASE	WHEN	M07.overlap_batch IS NULL	THEN -1	ELSE	M07.overlap_batch	END)

							WHERE
									DATEPART(YEAR,M10.event_date)   =   ' || @YYYY || '
								AND DATEPART(MONTH,M10.event_date)  =   ' || @MM || '
							COMMIT
							'
			--SELECT @SQL_
			EXECUTE (@SQL_)
			
			MESSAGE CAST(NOW() AS TIMESTAMP)||' | M14.2 - Inserting results into monthly table : ' || @I_TAB || ' ...DONE. ' || @@ROWCOUNT || ' rows affected.' TO CLIENT
			
			COMMIT
			
			



			------------------------------------------------------------------------------------------------------------------
			-- Check for existence of i-th monthly individuals details table (if it does not exist, then create the table)
			------------------------------------------------------------------------------------------------------------------
			IF NOT EXISTS   (
								SELECT  1
								FROM	SYSOBJECTS
								WHERE
											[name]	=	@I_TAB2
									AND     [uid]   =   USER_ID()	-- THIS WILL HAVE TO UPDATED IN PROD ENVIRONMENT, BUT RETAIN FOR DEV TESTING
							)
				BEGIN
						
						MESSAGE CAST(NOW() AS TIMESTAMP)||' | M14.2 - Creating new monthly table : ' || @I_TAB2 TO CLIENT
						
						SET @SQL_ = '
									CREATE TABLE ' || @I_TAB2 || '(
											scaling_date					date			not null
										,	account_number					varchar(20)		not null
                                        ,	person_number                  	int             not null
                                        ,	ind_scaling_weight				double          not null
                                        ,	gender							int             not null
                                        ,	age_band						int             not null
                                        ,	head_of_hhd						int             not null
                                        ,	hhsize							int             not null
										)
									COMMIT
									
									CREATE DATE INDEX DATE_IDX_1 ON ' || @I_TAB2 || '(scaling_date)
									CREATE HG INDEX HG_IDX_1 ON ' || @I_TAB2 || '(account_number)
									CREATE HG INDEX LF_IDX_1 ON ' || @I_TAB2 || '(person_number)
									CREATE HG INDEX LF_IDX_2 ON ' || @I_TAB2 || '(gender)
									CREATE HG INDEX LF_IDX_3 ON ' || @I_TAB2 || '(age_band)
									CREATE HG INDEX LF_IDX_4 ON ' || @I_TAB2 || '(head_of_hhd)
									CREATE HG INDEX LF_IDX_5 ON ' || @I_TAB2 || '(hhsize)
									COMMIT
									
									GRANT SELECT ON ' || @I_TAB2 || ' TO vespa_group_low_security
									COMMIT
									'
					--SELECT  @SQL_
					EXECUTE (@SQL_)
					COMMIT
				END
			COMMIT -- END IF
			
			
			-- Now insert the data into monthly table
			SET	@SCALING_DATE =	(
									SELECT	MAX(SCALING_DATE)
									FROM	V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING
								)
			COMMIT
			
			MESSAGE CAST(NOW() AS TIMESTAMP)||' | M14.2 - scaling date : ' || @SCALING_DATE || ' ...' TO CLIENT

			MESSAGE CAST(NOW() AS TIMESTAMP)||' | M14.2 - Inserting results into monthly table : ' || @I_TAB2 || ' ...' TO CLIENT
			SET @SQL_ = '	INSERT INTO ' || @I_TAB2 ||'
							SELECT
									''' || @SCALING_DATE || '''
								,	account_number
								,	person_number
								,	ind_scaling_weight
								,	gender
								,	age_band
								,	head_of_hhd
								,	hhsize
							FROM	V289_M13_individual_details
							COMMIT
							'
			EXECUTE (@SQL_)
			
			MESSAGE CAST(NOW() AS TIMESTAMP)||' | M14.2 - Inserting results into monthly table : ' || @I_TAB2 || ' ...DONE. ' || @@ROWCOUNT || ' rows affected.' TO CLIENT
			
			COMMIT
			
			
		END -- WHILE
	COMMIT -- (^_^)


	
	
	
	-------------------------------------------------------------------------------------------------
	-- M14.3 Create view so that points to the last 2 month's of data for event-level tables
	-------------------------------------------------------------------------------------------------
	MESSAGE CAST(NOW() AS TIMESTAMP)||' | M14.3 - Create view to point to the two latest monthly tables...' TO CLIENT

	-------------------------------------------------
	-- V289_M14_event_individuals_YYYYMM
	-------------------------------------------------
	
	-- First identify the 2 most recent Olive monthly output tables
	SELECT  *
	INTO	#TARGET_CURRENT_TABLES
	FROM
		(
			SELECT
					[NAME]
				,   RANK() OVER (ORDER BY [NAME] DESC)  AS  RNK
			FROM    SYSOBJECTS
			WHERE
			        [NAME] LIKE 'V289_M14_event_individuals_%'
					-- [NAME] LIKE 'V289%' -- JUST FOR TESTING WHILE WE DON'T HAVE ANY HISTORICAL M14 TABLES...
				AND [NAME] <> 'V289_M14_event_individuals_CURRENT'
				AND [TYPE] = 'U'
				AND [uid]   =   USER_ID()   -- THIS WILL HAVE TO UPDATED IN PROD ENVIRONMENT, BUT RETAIN FOR DEV TESTING
		)   AS  A
	WHERE   RNK < 3
	COMMIT -- (^_^)
	
	
	-- Generate view creation query if there is only a single monthly table available (when we run this for the first time)
	IF (SELECT MAX(RNK) FROM #TARGET_CURRENT_TABLES) = 1
		SET @SQL_ = '
			CREATE OR REPLACE VIEW V289_M14_event_individuals_CURRENT
				AS
					SELECT	*
					FROM	' || (SELECT [NAME] FROM #TARGET_CURRENT_TABLES WHERE RNK = 1) || '
			'
	COMMIT -- (^_^)
	
	
	-- Generate view creation query when there is more than one monthly table available (usual scenario going forward)
	IF (SELECT MAX(RNK) FROM #TARGET_CURRENT_TABLES) > 1
		SET @SQL_ = '
			CREATE OR REPLACE VIEW V289_M14_event_individuals_CURRENT
				AS
					SELECT	*
					FROM	' || (SELECT [NAME] FROM #TARGET_CURRENT_TABLES WHERE RNK = 1) || '
					UNION ALL
					SELECT	*
					FROM	' || (SELECT [NAME] FROM #TARGET_CURRENT_TABLES WHERE RNK = 2) || '				
			'
	COMMIT -- (^_^)
	
	--SELECT @SQL_
	EXECUTE (@SQL_)
	COMMIT -- (^_^)
	


	

	-------------------------------------------------
	-- V289_M14_individuals_details_YYYYMM
	-------------------------------------------------
	
	-- First identify the 2 most recent Olive monthly output tables
	DROP TABLE #TARGET_CURRENT_TABLES
	COMMIT
	
	SELECT  *
	INTO	#TARGET_CURRENT_TABLES
	FROM
		(
			SELECT
					[NAME]
				,   RANK() OVER (ORDER BY [NAME] DESC)  AS  RNK
			FROM    SYSOBJECTS
			WHERE
			        [NAME] LIKE 'V289_M14_individuals_details_%'
					-- [NAME] LIKE 'V289%' -- JUST FOR TESTING WHILE WE DON'T HAVE ANY HISTORICAL M14 TABLES...
				AND [NAME] <> 'V289_M14_individuals_details_CURRENT'
				AND [TYPE] = 'U'
				AND [uid]   =   USER_ID()   -- THIS WILL HAVE TO UPDATED IN PROD ENVIRONMENT, BUT RETAIN FOR DEV TESTING
		)   AS  A
	WHERE   RNK < 3
	COMMIT -- (^_^)
	
	
	-- Generate view creation query if there is only a single monthly table available (when we run this for the first time)
	IF (SELECT MAX(RNK) FROM #TARGET_CURRENT_TABLES) = 1
		SET @SQL_ = '
			CREATE OR REPLACE VIEW V289_M14_individuals_details_CURRENT
				AS
					SELECT	*
					FROM	' || (SELECT [NAME] FROM #TARGET_CURRENT_TABLES WHERE RNK = 1) || '
			'
	COMMIT -- (^_^)
	
	
	-- Generate view creation query when there is more than one monthly table available (usual scenario going forward)
	IF (SELECT MAX(RNK) FROM #TARGET_CURRENT_TABLES) > 1
		SET @SQL_ = '
			CREATE OR REPLACE VIEW V289_M14_individuals_details_CURRENT
				AS
					SELECT	*
					FROM	' || (SELECT [NAME] FROM #TARGET_CURRENT_TABLES WHERE RNK = 1) || '
					UNION ALL
					SELECT	*
					FROM	' || (SELECT [NAME] FROM #TARGET_CURRENT_TABLES WHERE RNK = 2) || '				
			'
	COMMIT -- (^_^)
	
	--SELECT @SQL_
	EXECUTE (@SQL_)
	COMMIT -- (^_^)

	


	MESSAGE CAST(NOW() AS TIMESTAMP)||' | M14.3 - Create view to point to the two latest monthly tables...DONE' TO CLIENT
	
	
end; -- procedure
commit;


-- Finish and grant permissions on procedure
grant execute on v289_M14_Create_Final_Olive_Output_Tables to vespa_group_low_security;
commit;
