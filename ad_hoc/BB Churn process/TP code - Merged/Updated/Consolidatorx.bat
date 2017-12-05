COPY /y /B 											"01 - Forecast_TP_Rates 2.SQL"								BB_churn_ALL_MODULES 2.SQL
COPY /y /B 		BB_churn_ALL_MODULES 2.SQL+"02 - TP_Regression_Coefficient 2.SQL"								BB_churn_ALL_MODULES 2.SQL
COPY /y /B 		BB_churn_ALL_MODULES 2.SQL+"03 - TP_Intraweek_PCs_Dist 2.SQL"									BB_churn_ALL_MODULES 2.SQL
COPY /y /B 		BB_churn_ALL_MODULES 2.SQL+"04 - TP_Intraweek_ABs_Dist 2.SQL"									BB_churn_ALL_MODULES 2.SQL
