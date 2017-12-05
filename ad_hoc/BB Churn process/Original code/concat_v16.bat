REM This is a simple cmd batch script that concatenates all of the Capping Forecasting modules into a single for simple deployment of all all procedures.
REM Simply execute the combined CP2_Forecasting_all_modules.sql file to deploy-redeploy all procedures.

copy	/Y	/B  	"FORECAST_Future_Platform v16.sql"		CP2_Forecasting_all_modules.sql
copy	/Y	/B  CP2_Forecasting_all_modules.sql+"Proc_FORECAST_2-1_Platform_Cuscan_Rates.sql"						CP2_Forecasting_all_modules.sql
copy	/Y	/B  CP2_Forecasting_all_modules.sql+"Proc_FORECAST_2-2_Platform_Syscan_Rates.sql"						CP2_Forecasting_all_modules.sql
copy	/Y	/B  CP2_Forecasting_all_modules.sql+"Proc_FORECAST_2-3_TA_Call_Volume_Distribution.sql"						CP2_Forecasting_all_modules.sql
copy	/Y	/B  CP2_Forecasting_all_modules.sql+"Proc_FORECAST_WebChat_Volume_Distribution.sql" 					CP2_Forecasting_all_modules.sql
copy	/Y	/B  CP2_Forecasting_all_modules.sql+"Proc_FORECAST_TA_PC_Conversion.sql"							CP2_Forecasting_all_modules.sql
copy	/Y	/B	CP2_Forecasting_all_modules.sql+"Proc_Platform_Trend_Regression.sql"							CP2_Forecasting_all_modules.sql
copy	/Y	/B  CP2_Forecasting_all_modules.sql+"Proc_FORECAST_PC_AB_Durations_And_Status_Changes.sql"							CP2_Forecasting_all_modules.sql
copy	/Y	/B	CP2_Forecasting_all_modules.sql+"Proc_Offer_Applied_Durations.sql"							CP2_Forecasting_all_modules.sql
copy	/Y	/B  CP2_Forecasting_all_modules.sql+"Proc_FORECAST_New_Cust_Sample.sql"					CP2_Forecasting_all_modules.sql
copy	/Y	/B  CP2_Forecasting_all_modules.sql+"Proc_FORECAST_Activation_Vols.sql"						CP2_Forecasting_all_modules.sql
copy	/Y	/B  CP2_Forecasting_all_modules.sql+"Proc_FORECAST_Sim_Opening_Base.sql"								CP2_Forecasting_all_modules.sql
copy	/Y	/B  CP2_Forecasting_all_modules.sql+"Proc_FORECAST_Create_Fcast_Loop2_Table.sql"						CP2_Forecasting_all_modules.sql
copy	/Y	/B  CP2_Forecasting_all_modules.sql+"Proc_FORECAST_Loop_Table_2_Weekly_Actions.sql"		CP2_Forecasting_all_modules.sql
copy	/Y	/B  CP2_Forecasting_all_modules.sql+"Proc_FORECAST_Insert_New_Custs_Into_Loop_Table.sql"					CP2_Forecasting_all_modules.sql
copy	/Y	/B  CP2_Forecasting_all_modules.sql+"Proc_FORECAST_Loop_Table_2_Update_for_Next_Wk.sql"	CP2_Forecasting_all_modules.sql

copy	/Y	/B  CP2_Forecasting_all_modules.sql+"Proc_FORECAST_Create_New_Forecast_Loop_Table.sql"			CP2_Forecasting_all_modules.sql
copy	/Y	/B  CP2_Forecasting_all_modules.sql+"Proc_Forecast_View.sql" 					CP2_Forecasting_all_modules.sql
copy	/Y	/B	CP2_Forecasting_all_modules.sql+"Proc_Subs_Calendar.sql"						CP2_Forecasting_all_modules.sql
