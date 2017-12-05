-- First you need to impersonate CITeam
SETUSER CITeam;

-- Drop procedure if exists CITeam.Forecast_Loop_Table_2_Update_For_Nxt_Wk;
CREATE PROCEDURE CITeam.Forecast_Loop_Table_2_Update_For_Nxt_Wk () SQL Security INVOKER

BEGIN
	--------------------------------------------------------------------------
	-- Update table for start of next loop -----------------------------------
	--------------------------------------------------------------------------
	UPDATE Forecast_Loop_Table_2 base
	SET DTV_PC_Future_Sub_Effective_Dt = Cast(base.end_date + dur.Days_To_churn AS DATE)
	FROM Forecast_Loop_Table_2 base
	INNER JOIN DTV_PC_Duration_Dist dur ON rand_DTV_PC_Duration BETWEEN dur.PC_Days_Lower_Prcntl AND dur.PC_Days_Upper_Prcntl
	WHERE (TA_DTV_PC > 0 OR WC_DTV_PC > 0 OR TA_Sky_Plus_Save > 0 OR WC_Sky_Plus_Save > 0 OR Other_DTV_PC > 0 OR base.DTV_Status_Code = 'AB') 
				AND base.DTV_Status_Code_EoW = 'PC';

	UPDATE Forecast_Loop_Table_2 base
	SET DTV_AB_Future_Sub_Effective_Dt = Cast(base.end_date + 50 AS DATE)
	FROM Forecast_Loop_Table_2 base
	WHERE DTV_AB > 0 AND base.DTV_Status_Code_EoW = 'AB';

	UPDATE Forecast_Loop_Table_2 base
	SET curr_offer_start_date_DTV = end_date + 3
		, Curr_Offer_end_Date_Intended_DTV = dateadd(month, Total_Offer_Duration_Mth, end_date + 3) -- Default 10m offer
	FROM Forecast_Loop_Table_2 base
	INNER JOIN Offer_Applied_Dur_Dist offer ON base.rand_New_Off_Dur BETWEEN offer.Dur_Pctl_Lower_Bound AND offer.Dur_Pctl_Upper_Bound 
			AND Offer_Segment = 'TA'
	WHERE DTV_Offer_Applied = 1 AND TA_Call_Cust > 0;

	UPDATE Forecast_Loop_Table_2 base
	SET curr_offer_start_date_DTV = end_date + 3
		, Curr_Offer_end_Date_Intended_DTV = dateadd(month, Total_Offer_Duration_Mth, end_date + 3) -- Default 10m offer
	FROM Forecast_Loop_Table_2 base
	INNER JOIN Offer_Applied_Dur_Dist offer ON base.rand_New_Off_Dur BETWEEN offer.Dur_Pctl_Lower_Bound AND offer.Dur_Pctl_Upper_Bound 
	AND Offer_Segment = 'Other'
	WHERE DTV_Offer_Applied = 1 AND TA_Call_Cust = 0;

	UPDATE Forecast_Loop_Table_2 base
	SET curr_offer_start_date_DTV = end_date + 3
		, Curr_Offer_end_Date_Intended_DTV = dateadd(month, Total_Offer_Duration_Mth, end_date + 3) -- Default 10m offer
	FROM Forecast_Loop_Table_2 base
	INNER JOIN Offer_Applied_Dur_Dist offer ON base.rand_New_Off_Dur BETWEEN offer.Dur_Pctl_Lower_Bound AND offer.Dur_Pctl_Upper_Bound 
											AND Offer_Segment = 'Reactivations'
	WHERE DTV_Offer_Applied = 1 AND ((DTV_Status_Code = 'PC' AND DTV_Status_Code_EoW = 'AC') OR ((TA_DTV_PC > 0 OR WC_DTV_PC > 0 OR TA_Sky_Plus_Save > 0 OR WC_Sky_Plus_Save > 0 OR Other_DTV_PC > 0) 
			AND DTV_Status_Code_EoW = 'AC'));

	UPDATE Forecast_Loop_Table_2
	SET DTV_Status_Code = Coalesce(DTV_Status_Code_EoW, DTV_Status_Code);

	UPDATE Forecast_Loop_Table_2 base
	SET DTV_PC_Future_Sub_Effective_Dt = NULL
	WHERE base.DTV_Status_Code != 'PC';

	UPDATE Forecast_Loop_Table_2 base
	SET DTV_AB_Future_Sub_Effective_Dt = NULL
	WHERE base.DTV_Status_Code != 'AB';

	UPDATE Forecast_Loop_Table_2
	SET end_date = end_date + 7;

	UPDATE Forecast_Loop_Table_2
	SET Prev_offer_end_date_DTV = Curr_Offer_end_Date_Intended_DTV
	WHERE Curr_Offer_end_Date_Intended_DTV <= end_date;

	UPDATE Forecast_Loop_Table_2
	SET Curr_Offer_end_Date_Intended_DTV = NULL
	WHERE Curr_Offer_end_Date_Intended_DTV <= end_date;

	UPDATE Forecast_Loop_Table_2
	SET Prev_offer_end_date_DTV = NULL
	WHERE Prev_offer_end_date_DTV < (end_date) - 53 * 7;

	UPDATE Forecast_Loop_Table_2
	SET DTV_BB_LR_Offer_End_Dt = CASE WHEN Coalesce(Curr_Offer_end_Date_intended_DTV, Prev_offer_end_date_DTV) IS NOT NULL 
									AND ABS(Coalesce(Curr_Offer_end_Date_intended_DTV, Prev_offer_end_date_DTV) - End_Date) 
											<= ABS(Coalesce(Curr_Offer_end_Date_intended_BB, Prev_offer_end_date_BB, Cast('9999-09-09' AS DATE)) - End_Date) 
									AND ABS(Coalesce(Curr_Offer_end_Date_intended_DTV, Prev_offer_end_date_DTV) - End_Date) <= ABS(Coalesce(Curr_Offer_end_Date_intended_LR, Prev_offer_end_date_LR, Cast('9999-09-09' AS DATE)) - End_Date) THEN Coalesce(Curr_Offer_end_Date_intended_DTV, Prev_offer_end_date_DTV) -- DTV Offer End Dt
			WHEN Coalesce(Curr_Offer_end_Date_intended_BB, Prev_offer_end_date_BB) IS NOT NULL AND ABS(Coalesce(Curr_Offer_end_Date_intended_BB, Prev_offer_end_date_BB) - End_Date) <= ABS(Coalesce(Curr_Offer_end_Date_intended_DTV, Prev_offer_end_date_DTV, Cast('9999-09-09' AS DATE)) - End_Date) AND ABS(Coalesce(Curr_Offer_end_Date_intended_BB, Prev_offer_end_date_BB) - End_Date) <= ABS(Coalesce(Curr_Offer_end_Date_intended_LR, Prev_offer_end_date_LR, Cast('9999-09-09' AS DATE)) - End_Date) THEN Coalesce(Curr_Offer_end_Date_intended_BB, Prev_offer_end_date_BB) -- BB Offer End Dt
			WHEN Coalesce(Curr_Offer_end_Date_intended_LR, Prev_offer_end_date_LR) IS NOT NULL AND ABS(Coalesce(Curr_Offer_end_Date_intended_LR, Prev_offer_end_date_LR) - End_Date) <= ABS(Coalesce(Curr_Offer_end_Date_intended_DTV, Prev_offer_end_date_DTV, Cast('9999-09-09' AS DATE)) - End_Date) AND ABS(Coalesce(Curr_Offer_end_Date_intended_LR, Prev_offer_end_date_LR) - End_Date) <= ABS(Coalesce(Curr_Offer_end_Date_intended_BB, Prev_offer_end_date_BB, Cast('9999-09-09' AS DATE)) - End_Date) THEN Coalesce(Curr_Offer_end_Date_intended_LR, Prev_offer_end_date_LR) -- LR Offer End Dt
			END;

	UPDATE Forecast_Loop_Table_2
	SET Last_TA_Call_dt = CASE WHEN TA_Call_Cust > 0 THEN end_date - 3 ELSE Last_TA_Call_dt END
		, Last_AB_Dt = CASE WHEN DTV_AB > 0 THEN end_date - 3 ELSE Last_AB_Dt END;

	UPDATE Forecast_Loop_Table_2
	SET
		weekid = weekid + 1
		, offer_length_DTV = CASE 	WHEN 1 + (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 <= 3 THEN 'Offer Length 3M' 
									WHEN (1 + (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 > 3) AND (1 + (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 <= 6) THEN 'Offer Length 6M' 
									WHEN (1 + (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 > 6) AND (1 + (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 <= 9) THEN 'Offer Length 9M' 
									WHEN (1 + (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 > 9) AND (1 + (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 <= 12) THEN 'Offer Length 12M' 
									WHEN 1 + (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 > 12 THEN 'Offer Length 12M +' 
									WHEN Curr_Offer_end_Date_Intended_DTV IS NULL THEN 
					'No Offer' END
		, Time_To_Offer_End_DTV = CASE 	WHEN Curr_Offer_end_Date_Intended_DTV BETWEEN (end_date + 1) AND (end_date + 7) THEN 'Offer Ending in Next 1 Wks' 
										WHEN Curr_Offer_end_Date_Intended_DTV BETWEEN (end_date + 8) AND (end_date + 14) THEN 'Offer Ending in Next 2-3 Wks' 
										WHEN Curr_Offer_end_Date_Intended_DTV BETWEEN (end_date + 15) AND (end_date + 21) THEN 'Offer Ending in Next 2-3 Wks' 
										WHEN Curr_Offer_end_Date_Intended_DTV BETWEEN (end_date + 22) AND (end_date + 28) THEN 'Offer Ending in Next 4-6 Wks' 
										WHEN Curr_Offer_end_Date_Intended_DTV BETWEEN (end_date + 29) AND (end_date + 35) THEN 'Offer Ending in Next 4-6 Wks' 
										WHEN Curr_Offer_end_Date_Intended_DTV BETWEEN (end_date + 36) AND (end_date + 42) THEN 'Offer Ending in Next 4-6 Wks' 
										WHEN Curr_Offer_end_Date_Intended_DTV > (end_date + 42) THEN 'Offer Ending in 7+ Wks' 
										WHEN Prev_offer_end_date_DTV BETWEEN (end_date - 7) AND end_date THEN 'Offer Ended in last 1 Wks' 
										WHEN Prev_offer_end_date_DTV BETWEEN (end_date - 14) AND (end_date - 8) THEN 'Offer Ended in last 2-3 Wks' 
										WHEN Prev_offer_end_date_DTV BETWEEN (end_date - 21) AND (end_date - 15) THEN 'Offer Ended in last 2-3 Wks' 
										WHEN Prev_offer_end_date_DTV BETWEEN (end_date - 28) AND (end_date - 22) THEN 'Offer Ended in last 4-6 Wks' 
										WHEN Prev_offer_end_date_DTV BETWEEN (end_date - 35) AND (end_date - 29) THEN 'Offer Ended in last 4-6 Wks' 
										WHEN Prev_offer_end_date_DTV BETWEEN (end_date - 42) AND (end_date - 36) THEN 'Offer Ended in last 4-6 Wks' 
										WHEN Prev_offer_end_date_DTV < (end_date - 42) THEN 'Offer Ended 7+ Wks' ELSE 'No Offer End DTV' END
		, Time_To_Offer_End_BB = CASE WHEN Curr_Offer_end_Date_intended_BB BETWEEN (end_date + 1) AND (end_date + 7) THEN 'Offer Ending in Next 1 Wks' 
									WHEN Curr_Offer_end_Date_intended_BB BETWEEN (end_date + 8) AND (end_date + 14) THEN 'Offer Ending in Next 2-3 Wks' 
									WHEN Curr_Offer_end_Date_intended_BB BETWEEN (end_date + 15) AND (end_date + 21) THEN 'Offer Ending in Next 2-3 Wks' 
									WHEN Curr_Offer_end_Date_intended_BB BETWEEN (end_date + 22) AND (end_date + 28) THEN 'Offer Ending in Next 4-6 Wks' 
									WHEN Curr_Offer_end_Date_intended_BB BETWEEN (end_date + 29) AND (end_date + 35) THEN 'Offer Ending in Next 4-6 Wks' 
									WHEN Curr_Offer_end_Date_intended_BB BETWEEN (end_date + 36) AND (end_date + 42) THEN 'Offer Ending in Next 4-6 Wks' 
									WHEN Curr_Offer_end_Date_intended_BB BETWEEN (end_date + 43) AND (end_date + 49) THEN 'Offer Ending in 7+ Wks' 
									WHEN Curr_Offer_end_Date_intended_BB BETWEEN (end_date + 50) AND (end_date + 56) THEN 'Offer Ending in 7+ Wks' 
									WHEN Curr_Offer_end_Date_intended_BB BETWEEN (end_date + 57) AND (end_date + 63) THEN 'Offer Ending in 7+ Wks' 
									WHEN Curr_Offer_end_Date_intended_BB BETWEEN (end_date + 64) AND (end_date + 70) THEN 'Offer Ending in 7+ Wks' 
									WHEN Curr_Offer_end_Date_intended_BB BETWEEN (end_date + 71) AND (end_date + 77) THEN 'Offer Ending in 7+ Wks' 
									WHEN Curr_Offer_end_Date_intended_BB BETWEEN (end_date + 78) AND (end_date + 84) THEN 'Offer Ending in 7+ Wks' 
									WHEN Curr_Offer_end_Date_intended_BB BETWEEN (end_date + 85) AND (end_date + 91) THEN 'Offer Ending in 7+ Wks' 
									WHEN Curr_Offer_end_Date_intended_BB >= (end_date + 92) THEN 'Offer Ending in 7+ Wks' 
									WHEN Prev_offer_end_Date_BB BETWEEN (end_date - 7) AND end_date THEN 'Offer Ended in last 1 Wks' 
									WHEN Prev_offer_end_Date_BB BETWEEN (end_date - 14) AND (end_date - 8) THEN 'Offer Ended in last 2-3 Wks' 
									WHEN Prev_offer_end_Date_BB BETWEEN (end_date - 21) AND (end_date - 15) THEN 'Offer Ended in last 2-3 Wks' 
									WHEN Prev_offer_end_Date_BB BETWEEN (end_date - 28) AND (end_date - 22) THEN 'Offer Ended in last 4-6 Wks' 
									WHEN Prev_offer_end_Date_BB BETWEEN (end_date - 35) AND (end_date - 29) THEN 'Offer Ended in last 4-6 Wks' 
									WHEN Prev_offer_end_Date_BB BETWEEN (end_date - 42) AND (end_date - 36) THEN 'Offer Ended in last 4-6 Wks' 
									WHEN Prev_offer_end_Date_BB BETWEEN (end_date - 49) AND (end_date - 43) THEN 'Offer Ended 7+ Wks' 
									WHEN Prev_offer_end_Date_BB BETWEEN (end_date - 56) AND (end_date - 50) THEN 'Offer Ended 7+ Wks' 
									WHEN Prev_offer_end_Date_BB BETWEEN (end_date - 63) AND (end_date - 57) THEN 'Offer Ended 7+ Wks' 
									WHEN Prev_offer_end_Date_BB BETWEEN (end_date - 70) AND (end_date - 64) THEN 'Offer Ended 7+ Wks' 
									WHEN Prev_offer_end_Date_BB BETWEEN (end_date - 77) AND (end_date - 71) THEN 'Offer Ended 7+ Wks' 
									WHEN Prev_offer_end_Date_BB BETWEEN (end_date - 84) AND (end_date - 78) THEN 'Offer Ended 7+ Wks' 
									WHEN Prev_offer_end_Date_BB BETWEEN (end_date - 91) AND (end_date - 85) THEN 'Offer Ended 7+ Wks' 
									WHEN Prev_offer_end_Date_BB <= (end_date - 92) THEN 'Offer Ended 7+ Wks' 
									WHEN Prev_offer_end_Date_BB IS NULL THEN 'Null' 
									WHEN Curr_Offer_end_Date_intended_BB IS NULL THEN 'Null' 
									ELSE 'No Offer End BB' END
		, Time_To_Offer_End_LR = CASE WHEN Curr_Offer_end_Date_Intended_LR BETWEEN (end_date + 1) AND (end_date + 7) THEN 'Offer Ending in Next 1 Wks' 
									WHEN Curr_Offer_end_Date_Intended_LR BETWEEN (end_date + 8) AND (end_date + 14) THEN 'Offer Ending in Next 2-3 Wks' 
									WHEN Curr_Offer_end_Date_Intended_LR BETWEEN (end_date + 15) AND (end_date + 21) THEN 'Offer Ending in Next 2-3 Wks' 
									WHEN Curr_Offer_end_Date_Intended_LR BETWEEN (end_date + 22) AND (end_date + 28) THEN 'Offer Ending in Next 4-6 Wks' 
									WHEN Curr_Offer_end_Date_Intended_LR BETWEEN (end_date + 29) AND (end_date + 35) THEN 'Offer Ending in Next 4-6 Wks' 
									WHEN Curr_Offer_end_Date_Intended_LR BETWEEN (end_date + 36) AND (end_date + 42) THEN 'Offer Ending in Next 4-6 Wks' 
									WHEN Curr_Offer_end_Date_Intended_LR > (end_date + 42) THEN 'Offer Ending in 7+ Wks' 
									WHEN Prev_offer_end_date_LR BETWEEN (end_date - 7) AND end_date THEN 'Offer Ended in last 1 Wks' 
									WHEN Prev_offer_end_date_LR BETWEEN (end_date - 14) AND (end_date - 8) THEN 'Offer Ended in last 2-3 Wks' 
									WHEN Prev_offer_end_date_LR BETWEEN (end_date - 21) AND (end_date - 15) THEN 'Offer Ended in last 2-3 Wks' 
									WHEN Prev_offer_end_date_LR BETWEEN (end_date - 28) AND (end_date - 22) THEN 'Offer Ended in last 4-6 Wks' 
									WHEN Prev_offer_end_date_LR BETWEEN (end_date - 35) AND (end_date - 29) THEN 'Offer Ended in last 4-6 Wks' 
									WHEN Prev_offer_end_date_LR BETWEEN (end_date - 42) AND (end_date - 36) THEN 'Offer Ended in last 4-6 Wks' 
									WHEN Prev_offer_end_date_LR < (end_date - 42) THEN 'Offer Ended 7+ Wks' ELSE 'No Offer End LR' END
		, Time_To_Offer_End = CASE WHEN DTV_BB_LR_offer_end_dt BETWEEN (end_date + 1) AND (end_date + 7) THEN 'Offer Ending in Next 1 Wks' 
									WHEN DTV_BB_LR_offer_end_dt BETWEEN (end_date + 8) AND (end_date + 14) THEN 'Offer Ending in Next 2-3 Wks' 
									WHEN DTV_BB_LR_offer_end_dt BETWEEN (end_date + 15) AND (end_date + 21) THEN 'Offer Ending in Next 2-3 Wks' 
									WHEN DTV_BB_LR_offer_end_dt BETWEEN (end_date + 22) AND (end_date + 28) THEN 'Offer Ending in Next 4-6 Wks' 
									WHEN DTV_BB_LR_offer_end_dt BETWEEN (end_date + 29) AND (end_date + 35) THEN 'Offer Ending in Next 4-6 Wks' 
									WHEN DTV_BB_LR_offer_end_dt BETWEEN (end_date + 36) AND (end_date + 42) THEN 'Offer Ending in Next 4-6 Wks' 
									WHEN DTV_BB_LR_offer_end_dt > (end_date + 42) THEN 'Offer Ending in 7+ Wks' 
									WHEN DTV_BB_LR_offer_end_dt BETWEEN (end_date - 7) AND end_date THEN 'Offer Ended in last 1 Wks' 
									WHEN DTV_BB_LR_offer_end_dt BETWEEN (end_date - 14) AND (end_date - 8) THEN 'Offer Ended in last 2-3 Wks' 
									WHEN DTV_BB_LR_offer_end_dt BETWEEN (end_date - 21) AND (end_date - 15) THEN 'Offer Ended in last 2-3 Wks' 
									WHEN DTV_BB_LR_offer_end_dt BETWEEN (end_date - 28) AND (end_date - 22) THEN 'Offer Ended in last 4-6 Wks' 
									WHEN DTV_BB_LR_offer_end_dt BETWEEN (end_date - 35) AND (end_date - 29) THEN 'Offer Ended in last 4-6 Wks' 
									WHEN DTV_BB_LR_offer_end_dt BETWEEN (end_date - 42) AND (end_date - 36) THEN 'Offer Ended in last 4-6 Wks' 
									WHEN DTV_BB_LR_offer_end_dt < (end_date - 42) THEN 'Offer Ended 7+ Wks' ELSE 'No Offer' END
		, DTV_Tenure = CASE WHEN Cast(end_date AS INT) - Cast(dtv_act_date AS INT) < round(365 / 12 * 1, 0) THEN 'M01' 
							WHEN Cast(end_date AS INT) - Cast(dtv_act_date AS INT) < round(365 / 12 * 10, 0) THEN 'M10' 
							WHEN Cast(end_date AS INT) - Cast(dtv_act_date AS INT) < round(365 / 12 * 14, 0) THEN 'M14' 
							WHEN Cast(end_date AS INT) - Cast(dtv_act_date AS INT) < round(365 / 12 * 2 * 12, 0) THEN 'M24' 
							WHEN Cast(end_date AS INT) - Cast(dtv_act_date AS INT) < round(365 / 12 * 3 * 12, 0) THEN 'Y03' 
							WHEN Cast(end_date AS INT) - Cast(dtv_act_date AS INT) < round(365 / 12 * 5 * 12, 0) THEN 'Y05' 
							WHEN Cast(end_date AS INT) - Cast(dtv_act_date AS INT) >= round(365 / 12 * 5 * 12, 0) THEN 'Y05+' END
		, Previous_Abs = Previous_Abs + CASE WHEN DTV_AB > 0 THEN 1 ELSE 0 END
		, DTV_Activation_Type = NULL;

	UPDATE Forecast_Loop_Table_2
	SET Time_Since_Last_TA_call = CASE WHEN Last_TA_Call_dt IS NULL THEN 'No Prev TA Calls' 
										WHEN (Cast(end_date AS INT) - Cast(Last_TA_Call_dt AS INT)) / 7 = 0 THEN '0 Wks since last TA Call' 
										WHEN (Cast(end_date AS INT) - Cast(Last_TA_Call_dt AS INT)) / 7 = 1 THEN '01 Wks since last TA Call' 
										WHEN (Cast(end_date AS INT) - Cast(Last_TA_Call_dt AS INT)) / 7 BETWEEN 2 AND 5 THEN '02-05 Wks since last TA Call' 
										WHEN (Cast(end_date AS INT) - Cast(Last_TA_Call_dt AS INT)) / 7 BETWEEN 6 AND 35 THEN '06-35 Wks since last TA Call' 
										WHEN (Cast(end_date AS INT) - Cast(Last_TA_Call_dt AS INT)) / 7 BETWEEN 36 AND 41 THEN '36-46 Wks since last TA Call' 
										WHEN (Cast(end_date AS INT) - Cast(Last_TA_Call_dt AS INT)) / 7 BETWEEN 42 AND 46 THEN '36-46 Wks since last TA Call' 
										WHEN (Cast(end_date AS INT) - Cast(Last_TA_Call_dt AS INT)) / 7 = 47 THEN '47 Wks since last TA Call' 
										WHEN (Cast(end_date AS INT) - Cast(Last_TA_Call_dt AS INT)) / 7 BETWEEN 48 AND 52 THEN '48-52 Wks since last TA Call' 
										WHEN (Cast(end_date AS INT) - Cast(Last_TA_Call_dt AS INT)
					) / 7 BETWEEN 53 AND 60 THEN '53-60 Wks since last TA Call' WHEN (Cast(end_date AS INT) - Cast(Last_TA_Call_dt AS INT)) / 7 > 60 THEN '61+ Wks since last TA Call'
			ELSE ''
			END
		, Time_Since_Last_AB = CASE WHEN Last_AB_Dt IS NULL THEN 'No Prev AB Calls' WHEN (Cast(end_date AS INT) - Cast(Last_AB_Dt AS INT)) / 31 = 0 THEN '0 Mnths since last AB' WHEN (Cast(end_date AS INT) - Cast(Last_AB_Dt AS INT)) / 31 = 1 THEN '1-2 Mnths since last AB' WHEN (Cast(end_date AS INT) - Cast(Last_AB_Dt AS INT)) / 31 = 2 THEN '1-2 Mnths since last AB' WHEN (Cast(end_date AS INT) - Cast(Last_AB_Dt AS INT)) / 31 = 3 THEN '3 Mnths since last AB' WHEN (Cast(end_date AS INT) - Cast(Last_AB_Dt AS INT)) / 31 = 4 THEN '4 Mnths since last AB' WHEN (Cast(end_date AS INT) - Cast(Last_AB_Dt AS INT)) / 31 = 5 THEN '5-7 Mnths since last AB' WHEN (Cast(end_date AS INT) - Cast(Last_AB_Dt AS INT)) / 31 = 6 THEN '5-7 Mnths since last AB' WHEN (Cast(end_date AS INT) - Cast(Last_AB_Dt AS INT)) / 31 = 7 THEN '5-7 Mnths since last AB' WHEN (Cast(end_date AS INT) - Cast(Last_AB_Dt AS INT)) / 31 = 8 THEN '8-12 Mnths since last AB' WHEN (Cast(end_date AS INT) - Cast(Last_AB_Dt AS INT)) / 31 = 9 
				THEN '8-12 Mnths since last AB' WHEN (Cast(end_date AS INT) - Cast(Last_AB_Dt AS INT)) / 31 = 10 THEN '8-12 Mnths since last AB' WHEN (Cast(end_date AS INT) - Cast(Last_AB_Dt AS INT)) / 31 = 11 THEN '8-12 Mnths since last AB' WHEN (Cast(end_date AS INT) - Cast(Last_AB_Dt AS INT)) / 31 = 12 THEN '8-12 Mnths since last AB' WHEN (Cast(end_date AS INT) - Cast(Last_AB_Dt AS INT)) / 31 > 12 THEN '12+ Mnths since last AB' ELSE '' END
		, Previous_AB_Count = CASE WHEN Previous_Abs = 0 THEN '0 Previous_Abs' WHEN Previous_Abs = 1 THEN '1 Previous_Abs' WHEN Previous_Abs = 2 THEN '2 Previous_Abs' WHEN Previous_Abs = 3 THEN '3 Previous_Abs' WHEN Previous_Abs = 4 THEN '4-7 Previous_Abs' WHEN Previous_Abs = 5 THEN '4-7 Previous_Abs' WHEN Previous_Abs = 6 THEN '4-7 Previous_Abs' WHEN Previous_Abs = 7 THEN '4-7 Previous_Abs' WHEN Previous_Abs = 8 THEN '8-10 Previous_Abs' WHEN Previous_Abs = 9 THEN '8-10 Previous_Abs' WHEN Previous_Abs = 10 THEN '8-10 Previous_Abs' WHEN Previous_Abs = 11 THEN '11-15 Previous_Abs' WHEN Previous_Abs = 12 THEN '11-15 Previous_Abs' WHEN Previous_Abs = 13 THEN '11-15 Previous_Abs' WHEN Previous_Abs = 14 THEN '11-15 Previous_Abs' WHEN Previous_Abs = 15 THEN '11-15 Previous_Abs' WHEN Previous_Abs >= 16 THEN '16 + Previous_Abs' ELSE '' END;
END;

-- Grant execute rights to the members of CITeam
GRANT EXECUTE
	ON CITeam.Forecast_Loop_Table_2_Update_For_Nxt_Wk
	TO CITeam;

-- Change back to your account
SETUSER;

-- Test it
Call CITeam.Forecast_Loop_Table_2_Update_For_Nxt_Wk(10);
