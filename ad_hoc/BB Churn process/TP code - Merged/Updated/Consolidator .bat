COPY /y /B 											"01 - Forecast_TP_Rates 2.SQL"								BB_churn_ALL_MODULES2.SQL
COPY /y /B 		BB_churn_ALL_MODULES2.SQL+"02 - TP_Regression_Coefficient 2.SQL"								BB_churn_ALL_MODULES2.SQL
COPY /y /B 		BB_churn_ALL_MODULES2.SQL+"03 - TP_Intraweek_PCs_Dist 2.SQL"									BB_churn_ALL_MODULES2.SQL
COPY /y /B 		BB_churn_ALL_MODULES2.SQL+"04 - TP_Intraweek_ABs_Dist 2.SQL"									BB_churn_ALL_MODULES2.SQL
COPY /y /B 		BB_churn_ALL_MODULES2.SQL+"05 - TP_Intraweek_BCRQ_Dist 2.SQL"									BB_churn_ALL_MODULES2.SQL
COPY /y /B 		BB_churn_ALL_MODULES2.SQL+"06 - TP_PC_Duration_Dist 2.SQL"										BB_churn_ALL_MODULES2.SQL
COPY /y /B 		BB_churn_ALL_MODULES2.SQL+"07 - TP_PC_Status_Movement_Probabilities 2.SQL"						BB_churn_ALL_MODULES2.SQL
COPY /y /B 		BB_churn_ALL_MODULES2.SQL+"08 - TP_AB_Status_Movement_Probabilities 2.SQL"						BB_churn_ALL_MODULES2.SQL
COPY /y /B 		BB_churn_ALL_MODULES2.SQL+"09 - TP_Offer_Applied_Duration_Dist 2.SQL"							BB_churn_ALL_MODULES2.SQL
COPY /y /B 		BB_churn_ALL_MODULES2.SQL+"10 - TP_Build_ForeCAST_New_Cust_Sample 2.SQL"						BB_churn_ALL_MODULES2.SQL
COPY /y /B 		BB_churn_ALL_MODULES2.SQL+"11 - TP_Forecast_Activation_Vols 2.SQL"								BB_churn_ALL_MODULES2.SQL
COPY /y /B 		BB_churn_ALL_MODULES2.SQL+"12 - TP_Forecast_Create_Opening_Base 2.SQL"							BB_churn_ALL_MODULES2.SQL
COPY /y /B 		BB_churn_ALL_MODULES2.SQL+"13 - TP_my_sky_login_prob.SQL"										BB_churn_ALL_MODULES2.SQL
COPY /y /B 		BB_churn_ALL_MODULES2.SQL+"14 - TP_BB_Calls_prob.SQL"											BB_churn_ALL_MODULES2.SQL
COPY /y /B 		BB_churn_ALL_MODULES2.SQL+"15 - TP_DTV_TA_calls_1m_prob.SQL"									BB_churn_ALL_MODULES2.SQL
COPY /y /B 		BB_churn_ALL_MODULES2.SQL+"16 - TP_ForeCAST_Create_ForeCAST_Loop_Table_2.SQL"					BB_churn_ALL_MODULES2.SQL
COPY /y /B 		BB_churn_ALL_MODULES2.SQL+"17 - Forecast_TP_Loop_Table_2_Actions.SQL"							BB_churn_ALL_MODULES2.SQL
COPY /y /B 		BB_churn_ALL_MODULES2.SQL+"18 - TP_Forecast_Insert_New_Custs_Into_Loop_Table_2.SQL"			BB_churn_ALL_MODULES2.SQL
COPY /y /B 		BB_churn_ALL_MODULES2.SQL+"19 - TP_Forecast_Create_New_Forecast_Loop_Table.SQL"				BB_churn_ALL_MODULES2.SQL
COPY /y /B 		BB_churn_ALL_MODULES2.SQL+"20 - TP_Forecast_Loop_Table_2_Update_For_Nxt_Wk.SQL"				BB_churn_ALL_MODULES2.SQL

