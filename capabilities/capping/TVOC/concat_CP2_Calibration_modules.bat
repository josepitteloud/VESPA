REM This is a simple cmd batch script that concatenates all of the Capping calibration modules into a single for simple deployment of all all procedures.
REM Simply execute the combined CP2_Calibration_all_modules.sql file to deploy-redeploy all procedures.

copy	/Y	/B  								"V306_CP2_M00_Initialise.sql" 								CP2_Calibration_all_modules.sql
copy	/Y	/B	CP2_Calibration_all_modules.sql+"V306_CP_2_M00_2_output_to_logger.sql"						CP2_Calibration_all_modules.sql
copy	/Y	/B	CP2_Calibration_all_modules.sql+"V306_CP2_M01_Process_Manager.sql"							CP2_Calibration_all_modules.sql
copy	/Y	/B	CP2_Calibration_all_modules.sql+"V306_CP2_M02_Capping_Stage1.sql"							CP2_Calibration_all_modules.sql
copy	/Y	/B  CP2_Calibration_all_modules.sql+"V306_CP2_M03_Capping_Stage2_phase1.sql" 					CP2_Calibration_all_modules.sql
copy	/Y	/B  CP2_Calibration_all_modules.sql+"V306_CP2_M03_Capping_Stage2_phase2.sql" 					CP2_Calibration_all_modules.sql
copy	/Y	/B  CP2_Calibration_all_modules.sql+"V306_CP2_M04_Profiling.sql" 								CP2_Calibration_all_modules.sql
copy	/Y	/B	CP2_Calibration_all_modules.sql+"V306_CP2_M05_Build_Day_Caps.sql"							CP2_Calibration_all_modules.sql
copy	/Y	/B  CP2_Calibration_all_modules.sql+"V306_CP2_M05_2_Time_Tables.sql"							CP2_Calibration_all_modules.sql
copy	/Y	/B  CP2_Calibration_all_modules.sql+"V306_CP2_M06_BARB_Minutes.sql"								CP2_Calibration_all_modules.sql
copy	/Y	/B  CP2_Calibration_all_modules.sql+"V306_CP2_M07_VESPA_Minutes.sql"							CP2_Calibration_all_modules.sql
copy	/Y	/B  CP2_Calibration_all_modules.sql+"V306_CP2_M07_1_BARB_vs_VESPA.sql"							CP2_Calibration_all_modules.sql
copy	/Y	/B  CP2_Calibration_all_modules.sql+"V306_CP2_M07_5_Save_metadata_params.sql"					CP2_Calibration_all_modules.sql
copy	/Y	/B  CP2_Calibration_all_modules.sql+"V306_CP2_M07_6_calculate_diff_by_time_range_LIVE.sql"		CP2_Calibration_all_modules.sql
copy	/Y	/B  CP2_Calibration_all_modules.sql+"V306_CP2_M07_6_calculate_diff_by_time_range_PLAYBACK.sql"	CP2_Calibration_all_modules.sql
copy	/Y	/B  CP2_Calibration_all_modules.sql+"V306_CP2_M07_7_tune_iteration_params.sql"					CP2_Calibration_all_modules.sql
copy	/Y	/B  CP2_Calibration_all_modules.sql+"V306_CP2_M07_8_optimise_capping_parameters.sql"			CP2_Calibration_all_modules.sql
copy	/Y	/B  CP2_Calibration_all_modules.sql+"V306_CP2_M999_DROP_LOCAL_TABLE.sql"						CP2_Calibration_all_modules.sql


start "" "C:\Program Files (x86)\Synametrics Technologies\WinSQL\Winsql.exe" -g CP2_Calibration_all_modules.sql