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

		MESSAGE CAST(NOW() AS TIMESTAMP) || ' | M14.2 - Inserting results into monthly table : ' || @I_TAB || ' ...DONE. ' || @@ROWCOUNT || ' rows affected.' TO CLIENT
		COMMIT

		------------------------------------------------------------------------------------------------------------------
		-- Check for existence of i-th monthly individuals details table (if it does not exist, then create the table)
		------------------------------------------------------------------------------------------------------------------
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

		MESSAGE CAST(NOW() AS TIMESTAMP) || ' | M14.2 - Inserting results into monthly table : ' || @I_TAB2 || ' ...DONE. ' || @@ROWCOUNT || ' rows affected.' TO CLIENT

		COMMIT
	END -- WHILE
	COMMIT -- (^_^)
END;
GO ;
COMMIT

