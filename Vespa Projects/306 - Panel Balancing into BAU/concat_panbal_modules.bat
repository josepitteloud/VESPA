REM This is a simple cmd batch script that concatenates all of the panel balancing modules into a single for simple deployment of all procedures.
REM Simply execute the combined V306_MXX_Panbal_all_modules.sql file to deploy-redeploy all procedures.

copy	/Y	/B  								"V306_M00_Initialise.sql" 						V306_MXX_Panbal_all_modules.sql
copy	/Y	/B  V306_MXX_Panbal_all_modules.sql+"V306_M01_Process_Manager.sql" 					V306_MXX_Panbal_all_modules.sql
copy	/Y	/B  V306_MXX_Panbal_all_modules.sql+"V306_M02_Waterfall.sql" 						V306_MXX_Panbal_all_modules.sql
copy	/Y	/B  V306_MXX_Panbal_all_modules.sql+"V306_M03_Panbal_Segments.sql" 					V306_MXX_Panbal_all_modules.sql
copy	/Y	/B  V306_MXX_Panbal_all_modules.sql+"V306_M04_PanBal_SAV.sql" 						V306_MXX_Panbal_all_modules.sql
copy	/Y	/B  V306_MXX_Panbal_all_modules.sql+"V306_M05_Virtual_panels_update.sql" 			V306_MXX_Panbal_all_modules.sql
copy	/Y	/B  V306_MXX_Panbal_all_modules.sql+"V306_M06_Panbal_Main.sql" 						V306_MXX_Panbal_all_modules.sql
copy	/Y	/B  V306_MXX_Panbal_all_modules.sql+"V306_M07_VolCheck.sql" 						V306_MXX_Panbal_all_modules.sql
copy	/Y	/B  V306_MXX_Panbal_all_modules.sql+"V306_M08_PanBal_Metrics.sql" 					V306_MXX_Panbal_all_modules.sql
copy	/Y	/B  V306_MXX_Panbal_all_modules.sql+"V306_Update_Panel_Movements_Log.sql" 			V306_MXX_Panbal_all_modules.sql

start "" "C:\Program Files (x86)\Synametrics Technologies\WinSQL\Winsql.exe" -g V306_MXX_Panbal_all_modules.sql