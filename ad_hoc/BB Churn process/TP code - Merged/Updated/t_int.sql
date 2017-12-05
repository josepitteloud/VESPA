SELECT COALESCE(a.account_number, y.account_number) AS account_number
				, COALESCE(a.end_date, y.end_date) AS end_date
				, a.BB_all_calls_1m_raw
				, a.DTV_TA_calls_1m_raw
				, y.mysky
INTO t_int
FROM citeam.cust_fcast_weekly_base AS a 
Full Outer JOIN my_sky AS y ON a.account_number = y.account_number AND y.end_date = a.end_date;
COMMIT; 
CREATE HG INDEX id1 ON t_int(account_number); 
CREATE DATE  INDEX id2 ON t_int(end_date);

			
		SET TP_counter = 1;
		SET TP_true_sample_rate = (SELECT CAST(sum(CASE WHEN sample = 'A' THEN 1 ELSE 0 END) AS FLOAT) / count(*) FROM TP_FORECAST_Base_Sample );
		DELETE FROM TP_FORECAST_Looped_Sim_Output_Platform;

		-- Start Loop
		WHILE TP_counter <= TP_n_weeks_to_foreCAST loop
				MESSAGE TP_counter ||' | TP_counter' TO CLIENT;
				MESSAGE TP_n_weeks_to_foreCAST ||' | TP_n_weeks_to_foreCAST' TO CLIENT;
				MESSAGE TP_true_sample_rate ||' | True_Sample_Rat' TO CLIENT;
					-- Create ForeCAST Loop Table 2
						CALL TP_ForeCAST_Create_ForeCAST_Loop_Table_2(TP_ForeCAST_Start_Wk, TP_ForeCAST_End_Wk, TP_true_sample_rate);
						CALL ForeCAST_TP_Loop_Table_2_Actions(TP_counter, 1.02) ;
						CALL TP_ForeCAST_Insert_New_Custs_Into_Loop_Table_2(TP_ForeCAST_Start_Wk, TP_ForeCAST_End_Wk, TP_true_sample_rate);
				MESSAGE CAST(now() as timestamp)||' | Before insert' TO CLIENT;	
						INSERT INTO TP_FORECAST_Looped_Sim_Output_Platform
						SELECT * FROM TP_ForeCAST_Loop_Table_2;
				MESSAGE CAST(now() as timestamp)||' | After insert' TO CLIENT;	
						DELETE FROM TP_ForeCAST_Loop_Table_2 	WHERE CusCan = 1;
						DELETE TP_ForeCAST_Loop_Table_2 		WHERE SysCan = 1;
						DELETE TP_ForeCAST_Loop_Table_2 		WHERE HM = 1;
						DELETE TP_ForeCAST_Loop_Table_2 		WHERE _3rd_Party = 1;
				MESSAGE CAST(now() as timestamp)||' | Delete done' TO CLIENT;
					
						SET TP_counter = TP_counter + 1;
					-- Update TP_ForeCAST_Loop_Table_2 fields for next week
						CALL TP_ForeCAST_Loop_Table_2_Update_For_Nxt_Wk();
					-- Create new foreCAST loop table for start of next week's loop
						CALL TP_ForeCAST_Create_New_ForeCAST_Loop_Table;
							
		END loop;
		--Select * into FORECAST_Looped_Sim_Output_Platform_201601_V16 from TP_FORECAST_Looped_Sim_Output_Platform;
		MESSAGE CAST(now() as timestamp)||' | Run simulation loop DONE' TO CLIENT;

GO