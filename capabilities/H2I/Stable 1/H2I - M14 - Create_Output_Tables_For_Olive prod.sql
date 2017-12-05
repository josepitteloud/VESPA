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

** Outputs: 

		-- H2I_event_individuals_YYYYMM 	(Monthly Table holding events information)
		-- H2I_event_individuals_CURRENT 	(Table holding events information from the last 2 months)
		-- H2I_individuals_details_YYYYMM 	(Monthly Table holding individual information)
		-- H2I_individuals_details_CURRENT 	(Table holding individual information from the last 2 months)
--------------------------------------------------------------------------------------------------------------
*/
---------------------------
-- M14.0 - Create procedure
---------------------------
CREATE OR REPLACE PROCEDURE ${SQLFILE_ARG001}.v289_M14_Populating_Final_Olive_Output_Tables 
AS
BEGIN
	-----------------------------------------------
	-- M14.1 Create monthly output tables for Olive
	-----------------------------------------------
	MESSAGE CAST(NOW() AS TIMESTAMP) || ' | M14.1 - Determining required monthly output tables' TO CLIENT

	SELECT DT
		,DATEPART(YEAR, DT) AS YYYY
		,DATEPART(MONTH, DT) AS MM
		,'H2I_event_individuals_' || YYYY || RIGHT(0 || MM, 2) AS NAM
		,'H2I_individuals_details_' || YYYY || RIGHT(0 || MM, 2) AS NAM2
		,RANK() OVER (ORDER BY DT) AS RNK
	INTO #TARGET_TABLES
	FROM (SELECT DISTINCT (EVENT_DATE) AS DT FROM V289_M10_session_individuals ) AS A
	ORDER BY RNK

	COMMIT
		-----------------------------------------------
		-- M14.2 Create monthly output tables for Olive
		-----------------------------------------------
		MESSAGE CAST(NOW() AS TIMESTAMP) || ' | M14.2 - Append H2I outputs to monthly tables' TO CLIENT

	DECLARE @I INT = 0
	DECLARE @SQL_ VARCHAR(10000)
	DECLARE @I_TAB VARCHAR(255)
	DECLARE @I_TAB2 VARCHAR(255)
	DECLARE @YYYY INT
	DECLARE @MM INT
	DECLARE @I_MAX INT
	DECLARE @SCALING_DATE DATE
	COMMIT

	SELECT @I_MAX = MAX(RNK)
	FROM #TARGET_TABLES

	COMMIT 
--
	
	WHILE @I < @I_MAX
	
	BEGIN
		-- Progress loop counter
		SET @I = @I + 1

		-- Update some variables for the i-th loop
		SELECT @I_TAB = NAM
			,@I_TAB2 = NAM2
			,@YYYY = YYYY
			,@MM = MM
		FROM #TARGET_TABLES
		WHERE RNK = @I

		COMMIT

		------------------------------------------------------------------------------------------------------------------
		-- Check for existence of i-th monthly events table (if it does not exist, then create the table)
		------------------------------------------------------------------------------------------------------------------
		MESSAGE CAST(NOW() AS TIMESTAMP) || ' | M14.2 - Inserting results into monthly table : ' || @I_TAB TO CLIENT
		SET @SQL_ = 'INSERT INTO ${CBAF_DB_DATA_SCHEMA}.'|| @I_TAB 
					||' SELECT '
					||' M11.scaling_date'
					||' , M10.event_id '
					||' , M10.account_number'
					||' , M10.hh_person_number'
					||' , M11.scaling_weighting'
					||' , CASE WHEN	M10.overlap_batch IS NULL		THEN	NULL'
					||'    ELSE	M07.chunk_start END'
					||' , CASE WHEN	M10.overlap_batch IS NULL		THEN	NULL'
					||'    ELSE	M07.chunk_end END'
					||' FROM    V289_M10_session_individuals			AS	M10'
					||' JOIN	V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING	AS	M11		ON	M10.account_number		=	M11.account_number'
					||'   AND	M10.hh_person_number	=	M11.hh_person_number'
					||'   AND	M10.event_date			=	M11.scaling_date'
					||' JOIN	V289_M07_dp_data						AS	M07		ON	M10.account_number		=	M07.account_number'
					||'   AND	M10.event_id			=	M07.event_id'
					||'   AND	(CASE	WHEN	M10.overlap_batch	IS NULL	THEN -1	ELSE	M10.overlap_batch	END)	=	(CASE	WHEN	M07.overlap_batch IS NULL	THEN -1	ELSE	M07.overlap_batch	END)'
					||' WHERE DATEPART(YEAR,M10.event_date)   =   '|| @YYYY  
					||'   AND DATEPART(MONTH,M10.event_date)  =   ' || @MM 
					||' COMMIT' 

		EXECUTE (@SQL_) 
		EXECUTE dba.cbaf_create_live_view '${CBAF_DB_LIVE_SCHEMA}' ,@I_TAB ,'select * from ${CBAF_DB_DATA_SCHEMA}.'||@I_TAB
		EXECUTE dba.create_restricted_views_all @I_TAB ,'${CBAF_DB_LIVE_SCHEMA}'
		MESSAGE CAST(NOW() AS TIMESTAMP) || ' | M14.2 - Inserting results into monthly table : ' || @I_TAB || ' ...DONE. ' || @@ROWCOUNT || ' rows affected.' TO CLIENT
		COMMIT

		------------------------------------------------------------------------------------------------------------------
		-- Check for existence of i-th monthly individuals details table (if it does not exist, then create the table)
		------------------------------------------------------------------------------------------------------------------
		IF NOT EXISTS ( SELECT tname FROM syscatalog WHERE creator = '${CBAF_DB_DATA_SCHEMA}' AND UPPER(tname) = UPPER(@I_TAB2) AND UPPER(tabletype) = 'TABLE' )
		BEGIN
			MESSAGE CAST(NOW() AS TIMESTAMP) || ' | M14.2 - Creating new monthly table : ' || @I_TAB2 TO CLIENT

			SET @SQL_ = 'CREATE TABLE ${CBAF_DB_DATA_SCHEMA}.'|| @I_TAB2 
						|| '(scaling_date			date			not null'
						||', account_number			varchar(20)		not null'
						||', person_number          int             not null'
						||', ind_scaling_weight		double          not null'
						||', gender					int             not null'
						||', age_band				int             not null'
						||', head_of_hhd			int             not null'
						||', hhsize					int             not null   )'
						||' COMMIT'
						||' CREATE DATE INDEX DATE_IDX_1 ON ${CBAF_DB_DATA_SCHEMA}.' || @I_TAB2 || '(scaling_date)'
						||' CREATE HG INDEX HG_IDX_1 ON ${CBAF_DB_DATA_SCHEMA}.' || @I_TAB2 || '(account_number)'
						||' CREATE HG INDEX LF_IDX_1 ON ${CBAF_DB_DATA_SCHEMA}.' || @I_TAB2 || '(person_number)'
						||' CREATE HG INDEX LF_IDX_2 ON ${CBAF_DB_DATA_SCHEMA}.' || @I_TAB2 || '(gender)'
						||' CREATE HG INDEX LF_IDX_3 ON ${CBAF_DB_DATA_SCHEMA}.' || @I_TAB2 || '(age_band)'
						||' CREATE HG INDEX LF_IDX_4 ON ${CBAF_DB_DATA_SCHEMA}.' || @I_TAB2 || '(head_of_hhd)'
						||' CREATE HG INDEX LF_IDX_5 ON ${CBAF_DB_DATA_SCHEMA}.' || @I_TAB2 || '(hhsize)'
						||' COMMIT'

			EXECUTE (@SQL_)
			COMMIT
		END

		-- Now insert the data into monthly table
		SET @SCALING_DATE = (SELECT MAX(SCALING_DATE) FROM V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING )

		COMMIT 
		MESSAGE CAST(NOW() AS TIMESTAMP) || ' | M14.2 - scaling date : ' || @SCALING_DATE || ' ...' TO CLIENT 
		MESSAGE CAST(NOW() AS TIMESTAMP) || ' | M14.2 - Inserting results into monthly table : ' || @I_TAB2 || ' ...' TO CLIENT

		SET @SQL_ = '	INSERT INTO ${CBAF_DB_DATA_SCHEMA}.' || @I_TAB2 || '
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
							COMMIT'

		EXECUTE (@SQL_) 
		
		EXECUTE dba.cbaf_create_live_view '${CBAF_DB_LIVE_SCHEMA}'
										,@I_TAB2
										,'select * from ${CBAF_DB_DATA_SCHEMA}.'||@I_TAB2

		EXECUTE dba.create_restricted_views_all @I_TAB2 ,'${CBAF_DB_LIVE_SCHEMA}' 
		
		MESSAGE CAST(NOW() AS TIMESTAMP) || ' | M14.2 - Inserting results into monthly table : ' || @I_TAB2 || ' ...DONE. ' || @@ROWCOUNT || ' rows affected.' TO CLIENT

		COMMIT
	END -- WHILE

	COMMIT -- (^_^)
		-------------------------------------------------------------------------------------------------
		-- M14.3 Create view so that points to the last 2 month's of data for event-level tables
		-------------------------------------------------------------------------------------------------
		MESSAGE CAST(NOW() AS TIMESTAMP) || ' | M14.3 - Create view to point to the two latest monthly tables...' TO CLIENT

	IF NOT EXISTS (SELECT tname
					FROM syscatalog
					WHERE creator = '${CBAF_DB_DATA_SCHEMA}'
						AND UPPER(tname) = UPPER('H2I_event_individuals_CURRENT')
						AND UPPER(tabletype) = 'TABLE')
	BEGIN
		CREATE TABLE ${CBAF_DB_DATA_SCHEMA}.H2I_event_individuals_CURRENT (
			scaling_date DATE NOT NULL
			,event_id BIGINT NOT NULL
			,account_number VARCHAR(20) NOT NULL
			,person_number TINYINT NOT NULL
			,scaling_weighting FLOAT NOT NULL
			,event_start_datetime TIMESTAMP NULL DEFAULT NULL
			,event_end_datetime TIMESTAMP NULL DEFAULT NULL
			)

		COMMIT

		CREATE HG INDEX HG_IDX_1 ON ${CBAF_DB_DATA_SCHEMA}.H2I_event_individuals_CURRENT (event_id)
		CREATE HG INDEX HG_IDX_2 ON ${CBAF_DB_DATA_SCHEMA}.H2I_event_individuals_CURRENT (account_number)
		CREATE HG INDEX HG_IDX_3 ON ${CBAF_DB_DATA_SCHEMA}.H2I_event_individuals_CURRENT (scaling_date)
		CREATE HG INDEX HG_IDX_4 ON ${CBAF_DB_DATA_SCHEMA}.H2I_event_individuals_CURRENT (event_start_datetime)
		CREATE HG INDEX HG_IDX_5 ON ${CBAF_DB_DATA_SCHEMA}.H2I_event_individuals_CURRENT (event_end_datetime)
		CREATE DATE INDEX DATE_IDX_1 ON ${CBAF_DB_DATA_SCHEMA}.H2I_event_individuals_CURRENT (scaling_date)
		CREATE DTTM INDEX DTTM_IDX_1 ON ${CBAF_DB_DATA_SCHEMA}.H2I_event_individuals_CURRENT (event_start_datetime)
		CREATE DTTM INDEX DTTM_IDX_2 ON ${CBAF_DB_DATA_SCHEMA}.H2I_event_individuals_CURRENT (event_end_datetime)
		COMMIT
	END

	TRUNCATE TABLE ${CBAF_DB_DATA_SCHEMA}.H2I_event_individuals_CURRENT
	COMMIT

	IF NOT EXISTS (SELECT tname FROM syscatalog WHERE creator = '${CBAF_DB_DATA_SCHEMA}' AND UPPER(tname) = UPPER('H2I_individuals_details_CURRENT') AND UPPER(tabletype) = 'TABLE' )
	BEGIN
		CREATE TABLE ${CBAF_DB_DATA_SCHEMA}.H2I_individuals_details_CURRENT (
			scaling_date DATE NOT NULL
			,account_number VARCHAR(20) NOT NULL
			,person_number INT NOT NULL
			,ind_scaling_weight DOUBLE NOT NULL
			,gender INT NOT NULL
			,age_band INT NOT NULL
			,head_of_hhd INT NOT NULL
			,hhsize INT NOT NULL
			)

		COMMIT

		CREATE DATE INDEX DATE_IDX_1 ON ${CBAF_DB_DATA_SCHEMA}.H2I_individuals_details_CURRENT (scaling_date)
		CREATE HG INDEX HG_IDX_1 ON ${CBAF_DB_DATA_SCHEMA}.H2I_individuals_details_CURRENT (account_number)
		CREATE HG INDEX LF_IDX_1 ON ${CBAF_DB_DATA_SCHEMA}.H2I_individuals_details_CURRENT (person_number)
		CREATE HG INDEX LF_IDX_2 ON ${CBAF_DB_DATA_SCHEMA}.H2I_individuals_details_CURRENT (gender)
		CREATE HG INDEX LF_IDX_3 ON ${CBAF_DB_DATA_SCHEMA}.H2I_individuals_details_CURRENT (age_band)
		CREATE HG INDEX LF_IDX_4 ON ${CBAF_DB_DATA_SCHEMA}.H2I_individuals_details_CURRENT (head_of_hhd)
		CREATE HG INDEX LF_IDX_5 ON ${CBAF_DB_DATA_SCHEMA}.H2I_individuals_details_CURRENT (hhsize)
		COMMIT
	END
	TRUNCATE TABLE ${CBAF_DB_DATA_SCHEMA}.H2I_individuals_details_CURRENT
	-------------------------------------------------
	-- V289_M14_event_individuals_YYYYMM
	-------------------------------------------------
	-- First identify the 2 most recent Olive monthly output tables
	SELECT *
	INTO #TARGET_CURRENT_TABLES
	FROM (SELECT tname 
				,RANK() OVER (ORDER BY tname DESC ) AS RNK
		FROM syscatalog
		WHERE tname LIKE 'H2I_event_individuals_%'
			AND tname <> 'H2I_event_individuals_CURRENT'
			AND UPPER(tabletype) = 'TABLE'
			AND creator = '${CBAF_DB_DATA_SCHEMA}' ) AS A
	WHERE RNK < 3

	COMMIT

	-- Generate view creation query if there is only a single monthly table available (when we run this for the first time)
	IF (SELECT MAX(RNK) FROM #TARGET_CURRENT_TABLES ) = 1
		SET @SQL_ = 'INSERT INTO ${CBAF_DB_DATA_SCHEMA}.H2I_event_individuals_CURRENT '
					||' SELECT	* FROM	' || (SELECT tname FROM #TARGET_CURRENT_TABLES WHERE RNK = 1 )

	COMMIT 
		-- Generate view creation query when there is more than one monthly table available (usual scenario going forward)

	IF (SELECT MAX(RNK) FROM #TARGET_CURRENT_TABLES ) > 1
		SET @SQL_ = 'INSERT INTO ${CBAF_DB_DATA_SCHEMA}.H2I_event_individuals_CURRENT '
					||' SELECT	* FROM	' || (SELECT tname FROM #TARGET_CURRENT_TABLES WHERE RNK = 1 )
					||' UNION ALL'
					||' SELECT	* FROM	' || (SELECT tname FROM #TARGET_CURRENT_TABLES WHERE RNK = 2)

	COMMIT -- (^_^)

	--SELECT @SQL_
	EXECUTE (@SQL_)

	COMMIT -- (^_^)

	EXECUTE dba.cbaf_create_live_view '${CBAF_DB_LIVE_SCHEMA}'
										,'H2I_event_individuals_CURRENT'
										,'select * from ${CBAF_DB_DATA_SCHEMA}.H2I_event_individuals_CURRENT'

	EXECUTE dba.create_restricted_views_all 'H2I_event_individuals_CURRENT'
											,'${CBAF_DB_LIVE_SCHEMA}'

	COMMIT

	-------------------------------------------------
	-- H2I_individuals_details_YYYYMM
	-------------------------------------------------
	-- First identify the 2 most recent Olive monthly output tables
	DROP TABLE #TARGET_CURRENT_TABLES

	COMMIT

	SELECT *
	INTO #TARGET_CURRENT_TABLES
	FROM (SELECT tname
			,RANK() OVER (ORDER BY tname DESC) AS RNK
		FROM syscatalog
		WHERE tname LIKE 'H2I_individuals_details_%'
			AND tname <> 'H2I_individuals_details_CURRENT' 
			AND UPPER(tabletype) = 'TABLE'
			AND creator = '${CBAF_DB_DATA_SCHEMA}'
		) AS A
	WHERE RNK < 3

	COMMIT -- (^_^)

	-- Generate view creation query if there is only a single monthly table available (when we run this for the first time)
	IF (SELECT MAX(RNK) FROM #TARGET_CURRENT_TABLES ) = 1
		SET @SQL_ = 'INSERT INTO ${CBAF_DB_DATA_SCHEMA}.H2I_individuals_details_CURRENT
					SELECT	* FROM	' || (SELECT tname FROM #TARGET_CURRENT_TABLES WHERE RNK = 1 ) 
					
	COMMIT -- (^_^)

	-- Generate view creation query when there is more than one monthly table available (usual scenario going forward)
	IF (SELECT MAX(RNK) FROM #TARGET_CURRENT_TABLES ) > 1 
		SET @SQL_ = 'INSERT INTO ${CBAF_DB_DATA_SCHEMA}.H2I_individuals_details_CURRENT'
					||' SELECT	* FROM	' || (SELECT tname FROM #TARGET_CURRENT_TABLES WHERE RNK = 1) 
					||' UNION ALL '
					||' SELECT	* FROM	' || (SELECT tname FROM #TARGET_CURRENT_TABLES WHERE RNK = 2)

	COMMIT -- (^_^)

	--SELECT @SQL_
	EXECUTE (@SQL_)

	COMMIT -- (^_^)

	EXECUTE dba.cbaf_create_live_view '${CBAF_DB_LIVE_SCHEMA}'
										,'H2I_individuals_details_CURRENT'
										,'select * from ${CBAF_DB_DATA_SCHEMA}.H2I_individuals_details_CURRENT'

	EXECUTE dba.create_restricted_views_all 'H2I_individuals_details_CURRENT'
											,'${CBAF_DB_LIVE_SCHEMA}' 
											
	MESSAGE CAST(NOW() AS TIMESTAMP) || ' | M14.3 - Create view to point to the two latest monthly tables...DONE' TO CLIENT
END;
GO ;
COMMIT;

