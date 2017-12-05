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
**PROJECT NAME:                         SKYVIEW H2I
**ANALYSTS:                             ANGEL DONNARUMMA    (ANGEL.DONNARUMMA_MIRABEL@SKYIQ.CO.UK)
**LEAD(S):                              JASON THOMPSON      (JASON.THOMPSON@SKYIQ.CO.UK)
                                        ,HOI YU TANG        (HOIYU.TANG@SKYIQ.CO.UK)
                                        ,JOSE PITTELOUD     (JOSE.PITTELOUD@SKYIQ.CO.UK)
**STAKEHOLDER:                          SKYIQ
                                        ,JOSE LOUREDA       (JOSE.LOUREDA@SKYIQ.CO.UK)
**DUE DATE:                             11/07/2014
**PROJECT CODE (INSIGHT COLLATION):     V289
**SHAREPOINT FOLDER:

    HTTP://SP-DEPARTMENT.BSKYB.COM/SITES/SIGEVOLVED/SHARED%20DOCUMENTS/FORMS/ALLITEMS.ASPX?ROOTFOLDER=%2FSITES%2FSIGEVOLVED%2FSHARED%20DOCUMENTS%2F01%20ANALYSIS%20REQUESTS%2FV289%20-%20SKYVIEW%20FUTURES%2F01%20PLANS%20BRIEFS%20AND%20PROJECT%20ADMIN

**BUSINESS BRIEF:

    THIS SIMPLE PREPARATION MODULE DROPS ALL OBJECTS CREATED BY THE H2I ALGORITHM. A LOOKUP TABLE REFERENCING ALL
	KNOWN OBJECTS WILL BE REQUIRED, AND IS CREATED IN THE BLOCK COMMENT AT THE TOP THIS CODE.


--------------------------------------------------------------------------------------------------------------
*/



/* INITIALISATION - RUN THE BELOW SCRIPT IF DEPLOYING PROCEDURE FOR THE FIRST TIME TO CREATE THE REQUIRED LOOKUP TABLE



*/


-- START OF PROCEDURE HERE
CREATE OR REPLACE PROCEDURE V289_M000C_DROP_H2I_OBJECTS
		@ignore_procs_flag		bit	= 0
AS BEGIN
	
    ---------------------------------------------
	-- M000C.1 - INITIALISE LIST OF KNOWN OBJECTS
    ---------------------------------------------

	MESSAGE cast(now() as timestamp)||' | @ M000C: Creating H2I object list...' TO CLIENT

	-- CREATE THE LOCAL LOOKUP TABLE
	CREATE TABLE #V289_H2I_OBJECTS
		(
				ID          INT          DEFAULT    AUTOINCREMENT
			,   OBJ_NAME 	VARCHAR(255) DEFAULT    NULL
		)
	COMMIT
	

	-- Insert object names
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'V289_H2I_objects' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'SC3I_Todays_panel_members' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'SC3I_weighting_working_table' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'SC3I_category_working_table' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'SC3_Weightings' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'SC3_Intervals' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'VESPA_HOUSEHOLD_WEIGHTING' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'SC3_Sky_base_segment_snapshots' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'SC3_Todays_panel_members' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'SC3_Todays_segment_weights' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'SC3_scaling_weekly_sample' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'SC3_weighting_working_table' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'SC3_category_working_table' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'SC3_category_subtotals' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'SC3_metrics' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'SC3_non_convergences' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'SC3I_Variables_lookup_v1_1' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_adulthhonly' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_adulthhonly_agedistribution' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_adulthhwithchildren_agedistribution' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_barb_agegender_distribution' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_barb_ageweighted_distribution' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_barbhhsizedistribution' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_genderage_lookup' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_genderage_matrix_hoi' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_heathmap_hourswatch_barbskyweighted' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_heathmap_hourswatch_dpweighted' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_heatmap_sessionsize_barbskyweighted' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_m01_t_process_manager' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_m01_t_process_manager_v2' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_m05_def_genderage_matrix' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_M05_def_sessionsize_matrix' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_M06_dp_raw_data' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'V289_M07_dp_data' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_m07_overlap_batches' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'V289_M08_SKY_HH_composition' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'V289_M08_SKY_HH_view' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'V289_M12_Skyview_weighted_duration' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'V289_PIV_Grouped_Segments_desc' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_s12_avgminwatched_x_dimensions' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_s12_avgminwatched_x_genderage' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_s12_overall_consumption' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_s12_sovminwatched_x_dimensions' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'V289_s12_v_genderage_distribution' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'V289_s12_v_hhsize_distribution' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_S12_v_weighted_duration_skyview' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_S12_weighted_duration_skyview' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'V289_Tables_check' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_vespa_adultsonly_HH_distribution' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_vespa_adultsonly_hhsize_distribution' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_vespa_adultswithchildren_hhsize_distribution' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_vespa_agedistribution' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_vespa_agegender_distribution' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_vespa_hhsize_distribution' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_view_barbagedistribution' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'SC3I_Segments_lookup_v1_1' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'SC3I_Sky_base_segment_snapshots' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'SC3I_category_subtotals' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'SC3I_metrics' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'SC3I_non_convergences' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'SC3I_Weightings' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'SC3I_Intervals' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'barb_weights' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'barb_rawview' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'Barb_skytvs' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'skybarb_fullview' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'skybarb' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_validation_table' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_m000_Prevalidation' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_m00_initialisation' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_m01_process_manager' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_m02_housekeeping' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_m03_barb_data_extraction' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_m04_barb_data_preparation' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_m05_barb_Matrices_generation' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_m06_DP_data_extraction' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_m07_dp_data_preparation' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_m08_Experian_data_preparation' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_m09_Session_size_definition' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_M17_vod_raw_data' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_m17_PullVOD_data_extraction' COMMIT	
	
	
	-- BARB source tables/views
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'PI_BARB_import' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'BARB_Individual_Panel_Member_Details' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'BARB_Panel_Member_Responses_Weights_and_Viewing_Categories' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'BARB_PVF_Viewing_Record_Panel_Members' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'BARB_PVF06_Viewing_Record_Panel_Members' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'BARB_Panel_Demographic_Data_TV_Sets_Characteristics' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'BARB_PVF04_Individual_Member_Details' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'BARB_Channel_Map' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'BARB_INDV_PANELMEM_DET' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'BARB_PANEL_DEMOGR_TV_CHAR' COMMIT
	
	-- M10
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_M10_individuals_selection' COMMIT 	-- procedure
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'V289_M10_combined_event_data' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'V289_M10_log' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'V289_M10_PIV_by_date' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'V289_M10_PIV_default' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'V289_M10_session_individuals' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'V289_H2I_check_M10_validation_tables' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'V289_M10_UNIQUE_VIEWERS_EVENT_ASSIGNMENT' COMMIT
	
	-- M11
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'V289_M11_01_SC3_v1_1__do_weekly_segmentation' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'V289_M11_02_SC3_v1_1__prepare_panel_members' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'V289_M11_03_SC3I_v1_1__add_individual_data' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'V289_M11_04_SC3I_v1_1__make_weights' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'V289_M11_04_SC3I_v1_1__make_weights_BARB' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'V289_M11_04_Barb_weighted_population' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING' COMMIT
	
	-- M12
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_m12_validation' COMMIT
	
	-- M13
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_M13_Create_Final_TechEdge_Output_Tables' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'V289_M13_individual_viewing' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'V289_M13_individual_details' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'V289_M13_individual_viewing_live_vosdal' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'V289_M13_individual_viewing_timeshift_pullvod' COMMIT
	
	-- Additional M05 objects
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_vsizealloc_matrix_small' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_vsizealloc_matrix_big' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_nonviewers_matrix' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_genderage_matrix' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_sessionsize_matrix' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_sessionsize_matrix_ID' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'v289_sessionsize_matrix_default' COMMIT
	
	-- M14 objects
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'V289_M14_event_individuals_' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'V289_M14_event_individuals_CURRENT' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'V289_M14_individuals_details_' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'V289_M14_individuals_details_CURRENT' COMMIT
	
	-- M15 objects
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'temp_house' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'temp_inds' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'age_sex_allocs' COMMIT
	INSERT INTO #V289_H2I_objects(OBJ_NAME) SELECT 'age_sex_allocs2' COMMIT



	CREATE UNIQUE HG INDEX UHG_IDX_1 ON #V289_H2I_objects(ID)	COMMIT
	CREATE UNIQUE HG INDEX uhg_idx_2 ON #V289_H2I_objects(OBJ_NAME)	COMMIT

	-- select top 20 * from #V289_H2I_objects;



	------------------------------------------------------------------------------------------------
	-- M000c.2 - Compile list of objects found and the required drop statement required for each one
	------------------------------------------------------------------------------------------------

	MESSAGE cast(now() as timestamp)||' | @ M000C: Detecting local H2I objects...' TO CLIENT
	
    CREATE TABLE #H2I_DROP_SQL
        (
            NUM     INT             IDENTITY
        ,   SQL_    VARCHAR(1000)   NULL    DEFAULT NULL
        )
    CREATE UNIQUE HG INDEX UHG_IDX_1 ON #H2I_DROP_SQL(NUM)
    COMMIT

	-- Exact object name matches
    INSERT INTO #H2I_DROP_SQL(SQL_)
    SELECT
            (
                CASE    SYS.[TYPE]
                    WHEN    'U'     THEN    'DROP TABLE'
                    WHEN    'V'     THEN    'DROP VIEW'
                    WHEN    'P'     THEN    'DROP PROCEDURE'
                END
            )   ||  ' ' || USER || '.' || H2I.OBJ_NAME
    FROM
                    #V289_H2I_objects	AS  H2I
        INNER JOIN  SYSOBJECTS          AS  SYS     ON  SYS.UID         		=   USER_ID()
                                                    AND UPPER(H2I.OBJ_NAME)		=   UPPER(SYS.NAME)
                                                    AND SYS.[TYPE]      		IN  ('U','V','P')


	-- Partial object name matches (e.g. Monthly tables)
    INSERT INTO #H2I_DROP_SQL(SQL_)
    SELECT
            (
                CASE    SYS.[TYPE]
                    WHEN    'U'     THEN    'DROP TABLE'
                    WHEN    'V'     THEN    'DROP VIEW'
                    WHEN    'P'     THEN    'DROP PROCEDURE'
                END
            )   ||  ' ' || USER || '.' || SYS.NAME
    FROM
                    #V289_H2I_objects	AS  H2I
        INNER JOIN  SYSOBJECTS          AS  SYS     ON  SYS.UID         	=   USER_ID()
                                                    AND UPPER(H2I.OBJ_NAME)	=   UPPER(LEFT(SYS.NAME,27))
                                                    AND SYS.[TYPE]      	IN  ('U','V','P')
													AND	LENGTH(SYS.NAME)	=	33	-- Additional table name length check for robustness
	WHERE	H2I.OBJ_NAME	=	'V289_M14_event_individuals_'

    
	
	-- Remove drop procedure commands so that they're preserved in the schema
	IF	@ignore_procs_flag	=	1
		DELETE FROM #H2I_DROP_SQL
		WHERE	SQL_	LIKE	'DROP PROCEDURE%'
	COMMIT
		

	---------------------------------
	-- M000c.3 - Drop the H2I objects
	---------------------------------

	MESSAGE cast(now() as timestamp)||' | @ M000C: Dropping local H2I objects...' TO CLIENT
	
	-- Define iteration limits
	DECLARE @I_MIN INT
    SELECT  @I_MIN = MIN(NUM)
    FROM    #H2I_DROP_SQL
    COMMIT

    DECLARE @I_MAX INT
    SELECT  @I_MAX = MAX(NUM)
    FROM    #H2I_DROP_SQL
    COMMIT

    DECLARE @i int
    SET @i = @I_MIN
    COMMIT
	
	
	-- Initialise SQL string
    DECLARE @sql_ VARCHAR(1000) DEFAULT NULL
    COMMIT


    -- Begin loop here
    WHILE @i <= @I_MAX
        BEGIN

            IF	EXISTS
						(
							SELECT	1
							FROM	#H2I_DROP_SQL
							WHERE   NUM = @i
						)
				BEGIN

					SELECT  @sql_ =  SQL_
					FROM    #H2I_DROP_SQL
					WHERE   NUM = @i

					EXECUTE (@sql_)
					COMMIT

					MESSAGE cast(now() as timestamp)||' | @ M000C: '||@sql_  TO CLIENT
				
				END

            -- Iterate loop counter
            SET @i = @i + 1
            COMMIT

        END


	MESSAGE cast(now() as timestamp)||' | @ M000C: FINISHED!' TO CLIENT
	
	
END; -- procedure
COMMIT;

GRANT EXECUTE ON V289_M000c_drop_H2I_objects TO vespa_group_low_security;
COMMIT;




