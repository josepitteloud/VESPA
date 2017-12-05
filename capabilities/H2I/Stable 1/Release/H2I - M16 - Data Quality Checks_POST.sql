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
**Project Name:                         Skyview H2I
**Analysts:                             Angel Donnarumma        (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jason Thompson          (Jason.Thompson@skyiq.co.uk)
                                        ,Hoi Yu Tang            (HoiYu.Tang@skyiq.co.uk)
                                        ,Jose Pitteloud         (jose.pitteloud@skyiq.co.uk)
										,Christopher Spencer	(Christopher.Spencer2@bskyb.com)
**Stakeholder:                          SkyIQ
                                        ,Jose Loureda           (Jose.Loureda@skyiq.co.uk)
**Due Date:                             11/07/2014
**Project Code (Insight Collation):     v289
**Sharepoint Folder:

        http://sp-department.bskyb.com/sites/SIGEvolved/Shared%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FSIGEvolved%2FShared%20Documents%2F01%20Analysis%20Requests%2FV289%20-%20Skyview%20Futures%2F01%20Plans%20Briefs%20and%20Project%20Admin

**Business Brief:

		Even before the project begins to run we need revise the data resource and guarantee the integrity of the data
		we will processing, this module is for that!
		
**Section:

        M16: Data Quality Checks
		
            M16.0 - Initialising Environment
			M16.1 - Checking KPIs through Metrics of Central Tendency
            M16.2 - Measuring Barb Integrity
            M16.3 - Measuring Vespa Integrity
			M16.4 - Are we good to go?
            M16.5 - Returning Results

--------------------------------------------------------------------------------------------------------------
*/

create or replace procedure  ${SQLFILE_ARG001} .v289_m16_data_quality_checks_post
	@proc_date 		date = null
	,@sample_proc	INT

as begin
	DECLARE @sample DECIMAL(6,5)
	SET @SAMPLE = @SAMPLE_proc / 100
----------------------------------
--M16.0 - Initialising Environment
----------------------------------
---- PARAMETERS
	DECLARE @exp_acct		INT			= 	8000000	------ Expected MIN SKY Accounts at M08 - First extraction: 8.000.000
	DECLARE @panel_accounts INT 		= 	200000	------ Min Panel accounts at M08 - 200.000
	DECLARE @exp_B_events	INT 		= 	60000	------ Expected MIN Barb Events at M04 - 60.000
	DECLARE @modules		INT 		= 	16 		------ Currently number of modules in H2I - 16
	DECLARE @segments		INT 		=  	139		------ Number of segments - 139
	DECLARE @exp_V_events	INT 		=	18000000 ----- Expected MIN EVENTS in VEspa DP Prog - 18.000.000
	DECLARE @dr_m06_acct	DECIMAL(3,2)= 	0.9		------ Expected Drop in M06 accounts count from M08 - 90%
	DECLARE @dr_m07			DECIMAL(3,2)= 	0.9		------ Expected Drop in M07 accounts/events  count from M06 - 90%
	DECLARE @dr_m10			DECIMAL(3,2)= 	0.95	------ Expected Drop in M10 accounts/events  count from M07 - 95%
	DECLARE @dr_m11			DECIMAL(3,2)= 	0.95	------ Expected Drop in M11 accounts/events  count from M10 - 95%
	DECLARE @dr_m13			DECIMAL(3,2)= 	0.95	------ Expected Drop in M13/M14 accounts/events  count from M10 - 95%
	
	
	IF NOT EXISTS(SELECT tname FROM syscatalog WHERE creator = ${SQLFILE_ARG001} and tabletype = 'TABLE' and upper(tname) = 'V289_M16_POST_METRICS')		
	CREATE TABLE v289_M16_POST_METRICS
		(Metric VARCHAR (100), expected_value VARCHAR(20))
	COMMIT
	DELETE FROM v289_M16_POST_METRICS
	INSERT INTO v289_M16_POST_METRICS
		SELECT 'Expected MIN SKY Accounts at M08 ' a ,   CAST (@exp_acct AS VARCHAR) b UNION
		SELECT 'MIN Panel Accounts at M8',   			CAST (@panel_accounts AS VARCHAR) UNION
		SELECT 'Expected MIN Barb Events at M4',   	CAST (@exp_B_events AS VARCHAR) UNION
		SELECT 'Currently number of modules in H2I',  CAST (@modules AS VARCHAR) 	UNION
		SELECT 'number of segments',   				CAST (@segments AS VARCHAR)		UNION
		SELECT 'Expected MIN EVENTS in VEspa DP Prog',   CAST(@exp_V_events AS VARCHAR)	UNION
		SELECT 'Expected Drop in M6 Accounts count from M8',   CAST(@dr_m06_acct* 100 AS VARCHAR)||'%' 		UNION
		SELECT 'Expected Drop in M7 Accounts/Events  count from M6',   CAST(@dr_m07* 100 AS VARCHAR)||'%' 	UNION
		SELECT 'Expected Drop in M10 Accounts/Events  count from M7',  CAST(@dr_m10* 100 AS VARCHAR)||'%'	UNION
		SELECT 'Expected Drop in M11 Accounts/Events  count from M10', CAST(@dr_m11* 100 AS VARCHAR)||'%'	UNION
		SELECT 'Expected Drop in M13/M14 Accounts/Events  count from M10', CAST(@dr_m13* 100 AS VARCHAR)||'%' 
	COMMIT
----- WORKING Variables	
	DECLARE @query varchar(5000)
	DECLARE @evt1	INT
	DECLARE @evt2	INT
	DECLARE @acct1	INT
	DECLARE @acct2  INT
	DECLARE @acct3  INT
	
	SET @evt1 = (SELECT  COUNT(*) FROM v289_M06_dp_raw_data WHERE account_number is NOT NULL AND dth_event_id IS NOT NULL )
	SET @evt2 = (SELECT  COUNT(*) FROM V289_M07_dp_data WHERE account_number is NOT NULL) 
	SET @acct1 = (SELECT  COUNT(DISTINCT account_number) FROM V289_M08_SKY_HH_view WHERE account_number is NOT NULL AND panel_flag = 1 )
	SET @acct2 = (SELECT  COUNT(DISTINCT account_number) FROM v289_M06_dp_raw_data WHERE account_number is NOT NULL AND dth_event_id IS NOT NULL) 
	SET @acct3 = (SELECT  COUNT(DISTINCT account_number) FROM V289_M07_dp_data WHERE account_number is NOT NULL) 
	
------------------------------------------------------------------------------------------------------------------------
	MESSAGE cast(now() as timestamp)||' | Begining M16.3 - Measuring H2I Integrity' TO CLIENT
	insert  into v289_m16_dq_fact_checks_post    (
													source
													,module_
													,test_context
													,processing_date
													,actual_value
													,test_result
												)
												
	SELECT 'H2I','M01' , 'All modules in process manager', @proc_date, COUNT (*)  AS value_  , CASE WHEN value_ = @modules     then 'Passed' else 'Failed' end   as result  FROM v289_m01_t_process_manager   UNION
	SELECT 'H2I','M01' , 'All modules excecuted'		 , @proc_date, SUM (status) AS value_, CASE WHEN value_ BETWEEN @modules -1  AND @modules then 'Passed' else 'Failed' end   as result  FROM v289_m01_t_process_manager   UNION

	SELECT 'H2I','M00' , 'Total Segments'				, @proc_date, COUNT(DISTINCT segment_id) AS value_, CASE WHEN value_ = @segments then 'Passed' else 'Failed' end   as result  FROM V289_PIV_Grouped_Segments_desc   UNION

	SELECT 'H2I','M04' , 'Events in Skybarb_fullview'	, @proc_date, COUNT (*)  AS value_, CASE WHEN value_ > @exp_B_events then 'Passed' else 'Failed' end   as result  FROM skybarb_fullview   UNION

	SELECT 'H2I','M05' , 'Records in size alloc matrix small'	, @proc_date, COUNT (*)  AS value_, CASE WHEN value_ >= 8  then 'Passed' else 'Failed' end   as result  FROM v289_vsizealloc_matrix_small   UNION
	SELECT 'H2I','M05' , 'Records in size alloc matrix big'		, @proc_date, COUNT (*)  AS value_, CASE WHEN value_ >= 30 then 'Passed' else 'Failed' end   as result  FROM v289_vsizealloc_matrix_big   UNION
	SELECT 'H2I','M05' , 'Records in non- viewers matrix'		, @proc_date, COUNT (*)  AS value_, CASE WHEN value_ >= 80 then 'Passed' else 'Failed' end   as result  FROM v289_nonviewers_matrix   UNION
	SELECT 'H2I','M05' , 'Records in genderage matrix'			, @proc_date, COUNT (*)  AS value_, CASE WHEN value_ >=400 then 'Passed' else 'Failed' end   as result  FROM v289_genderage_matrix   UNION
	SELECT 'H2I','M05' , 'Records in daily session size matrix'	, @proc_date, COUNT (*)  AS value_, CASE WHEN value_ >=100 then 'Passed' else 'Failed' end   as result  FROM v289_sessionsize_matrix   UNION
	SELECT 'H2I','M05' , 'Records in default session size matrix', @proc_date, COUNT (*)  AS value_, CASE WHEN value_ =5004 then 'Passed' else 'Failed' end   as result  FROM v289_sessionsize_matrix_default   UNION

	SELECT 'H2I','M06' , 'Records in M06 raw data table'		, @proc_date, COUNT(*) AS value_, CASE WHEN value_ >(@exp_V_events * @sample) then 'Passed' else 'Failed' end   as result  FROM v289_M06_dp_raw_data WHERE account_number is NOT NULL AND dth_event_id IS NOT NULL   UNION
	SELECT 'H2I','M06' , 'Unique accounts in M06 raw data table', @proc_date, COUNT(DISTINCT account_number) AS value_, CASE WHEN value_ >(@dr_m06_acct * @acct1) then 'Passed' else 'Failed' end   as result  FROM v289_M06_dp_raw_data WHERE account_number is NOT NULL AND dth_event_id IS NOT NULL   UNION

	SELECT 'H2I','M07' , 'Records in M07 viewing events table'  , @proc_date, COUNT(*) AS value_, CASE WHEN value_ >(@dr_m07 * @evt1) then 'Passed' else 'Failed' end   as result  FROM V289_M07_dp_data   UNION
	SELECT 'H2I','M07' , 'Unique accounts in M07 events table'  , @proc_date, COUNT(DISTINCT account_number) AS value_, CASE WHEN value_ >(@dr_m07 * @acct2) then 'Passed' else 'Failed' end   as result  FROM V289_M07_dp_data   UNION
	SELECT 'H2I','M07' , 'Session sizes within the range in M07', @proc_date, SUM(CASE WHEN session_size BETWEEN 1 AND 8 THEN 0 ELSE 1 END) AS value_, CASE WHEN value_ = 0 then 'Passed' else 'Failed' end   as result  FROM V289_M07_dp_data   UNION

	SELECT 'H2I','M08' , 'Total records in M08 Sky HH view'  	, @proc_date, COUNT(*) AS value_, CASE WHEN value_ >(@exp_acct) then 'Passed' else 'Failed' end   as result  						FROM V289_M08_SKY_HH_view   UNION
	SELECT 'H2I','M08' , 'Unique accounts in M08 Sky HH view'	, @proc_date, COUNT(DISTINCT account_number) AS value_, CASE WHEN value_ >(@exp_acct) then 'Passed' else 'Failed' end   as result  	FROM V289_M08_SKY_HH_view   UNION
	SELECT 'H2I','M08' , 'Panel count in M08 Sky HH view'		, @proc_date, SUM(panel_flag) AS value_, CASE WHEN value_ >(@panel_accounts * @sample) then 'Passed' else 'Failed' end   as result  FROM V289_M08_SKY_HH_view   UNION
	SELECT 'H2I','M08' , 'Total records in M08 Sky HH Comp'		, @proc_date, COUNT(*) AS value_, CASE WHEN value_ >(@exp_acct) then 'Passed' else 'Failed' end   as result  						FROM V289_M08_SKY_HH_composition   UNION
	SELECT 'H2I','M08' , 'Unique accounts in M08 Sky HH Comp'	, @proc_date, COUNT(DISTINCT account_number) AS value_, CASE WHEN value_ >(@exp_acct) then 'Passed' else 'Failed' end   as result  	FROM V289_M08_SKY_HH_composition   UNION
	SELECT 'H2I','M08' , 'Not null sex/agebands '				, @proc_date, COUNT(*) AS value_, CASE WHEN value_ =0 then 'Passed' else 'Failed' end   as result  									FROM V289_M08_SKY_HH_composition WHERE (person_ageband IS NULL) OR (person_gender IS NULL)   UNION

	SELECT 'H2I','M10' , 'Unique accounts in M10 session individuals'	, @proc_date, COUNT(DISTINCT account_number) AS value_, CASE WHEN value_ >(@dr_m10 * @acct3) then 'Passed' else 'Failed' end   as result  	FROM V289_M10_session_individuals   UNION
	SELECT 'H2I','M10' , 'Unique event count in M10 session individual'	, @proc_date, COUNT(DISTINCT event_id) AS value_, CASE WHEN value_ >(@dr_m10 	* @evt1) then 'Passed' else 'Failed' end   as result  		FROM V289_M10_session_individuals   UNION
	SELECT 'H2I','M10' , 'Person number out-of-range in M10'			, @proc_date, COUNT(*) AS value_, CASE WHEN value_ =0 then 'Passed' else 'Failed' end   as result  											FROM V289_M10_session_individuals WHERE hh_person_number NOT BETWEEN 1 AND 15   UNION

	SELECT 'H2I','M11' , 'Unique accounts in M11 ind. Weighting', @proc_date, COUNT(DISTINCT account_number) AS value_, CASE WHEN value_ >(@dr_m11 * @acct3) then 'Passed' else 'Failed' end   as result  			FROM V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING   UNION
	SELECT 'H2I','M11' , 'Count of invalid weights in M11'		, @proc_date, COUNT(*) AS value_, CASE WHEN value_ =0 then 'Passed' else 'Failed' end   as result  FROM V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING WHERE (scaling_weighting <=0) OR (scaling_weighting is NULL)   UNION
	SELECT 'H2I','M11' , 'Person number out-of-range in M11'	, @proc_date, COUNT(*) AS value_, CASE WHEN value_ =0 then 'Passed' else 'Failed' end   as result  FROM V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING WHERE hh_person_number NOT BETWEEN 1 AND 15   UNION

	SELECT 'H2I','M13' , 'Unique accounts in M13 Techedge details'	, @proc_date, COUNT(DISTINCT account_number) AS value_, CASE WHEN value_ >(@dr_m13 * @acct3) then 'Passed' else 'Failed' end   as result  FROM V289_M13_individual_details   UNION
	SELECT 'H2I','M13' , 'Person number count out-of-range in M13'	, @proc_date, COUNT(*) AS value_, CASE WHEN value_ = 0 then 'Passed' else 'Failed' end   as result  FROM V289_M13_individual_details WHERE person_number NOT BETWEEN 1 AND 15   UNION
	SELECT 'H2I','M13' , 'Count of invalid weights in M13'			, @proc_date, COUNT(*) AS value_, CASE WHEN value_ = 0 then 'Passed' else 'Failed' end   as result  FROM V289_M13_individual_details WHERE (ind_scaling_weight <=0) OR (ind_scaling_weight is NULL)   UNION
	SELECT 'H2I','M13' , 'Not null sex/agebands '					, @proc_date, COUNT(*) AS value_, CASE WHEN value_ = 0 then 'Passed' else 'Failed' end   as result  FROM V289_M13_individual_details WHERE (age_band IS NULL) OR (gender IS NULL)   
												

MESSAGE cast(now() as timestamp)||' | M16_Post Finished' TO CLIENT   

end;
GO
commit;
grant execute on v289_m16_data_quality_checks_post to vespa_group_low_security;
commit;