------------ CREATING FULL BASE 
message convert(TIMESTAMP, now()) || ' | TA DTV model - Initialization begin ' TO client

DECLARE @start_dt DATE 
DECLARE @end_dt DATE 
DECLARE @multi FLOAT 
DECLARE @rand1 CHAR(1) 

SET @multi 		= CAST((SELECT RAND(DATEPART(MS,GETDATE()))) AS FLOAT)
SET @start_dt  	= '2017-03-01'
SET @rand1 = '4'

SELECT account_number, dtv
	, RAND(cb_key_household * @multi) 		AS rand2
	, SUBSTRING(account_number, 9,1) 		AS filter_1 
	, CAST (NULL AS CHAR(1))                AS sample_group 
	
	, CAST (0 AS BIT)						AS TA_FLAG
	, CAST(NULL AS DATE) 					AS TA_dt
	, CAST(NULL AS INT) 					AS TA_count
	
INTO TA_DTV_FEB17_BASE
FROM sharmaa.View_attachments_201702
WHERE dtv = 1							-- DTV active customers 
	
	message convert(TIMESTAMP, now()) || ' | TA DTV model - TA_DTV_FEB17_BASE: '||@@rowcount TO client
	
COMMIT 
CREATE HG INDEX id1 ON TA_DTV_FEB17_BASE(account_number) 
CREATE HG INDEX id2 ON TA_DTV_FEB17_BASE(sample_group) 
COMMIT 

--- Selectinf the trainning/Validation sample 30%/70%
UPDATE TA_DTV_FEB17_BASE
SET sample_group = CASE WHEN rand2 <= 0.30 THEN 'T' ELSE 'V' END 
COMMIT 
	message convert(TIMESTAMP, now()) || ' | TA DTV model - TA_DTV_FEB17_BASE sample updated : '||@@rowcount TO client
------------------- TA DTV MODEL script - FEB 2017

SELECT a.ACCOUNT_NUMBER
		, 1 flag
		, MIN(event_dt) 		AS min_dt
		, SUM(total_calls)		AS t_calls
		
INTO #TA_DTV_FEB17_TARGET		
FROM CITEAM.View_CUST_CALLS_HIST 	AS a 
JOIN TA_DTV_FEB17_BASE 				AS b ON a.account_number = b.account_number 
WHERE event_dt between @start_dt AND DATEADD(day,30,@start_dt )
	AND typeofevent='TA' 
	AND a.DTV = 1
	AND a.account_number IS NOT NULL
GROUP BY 	a.account_number 

	message convert(TIMESTAMP, now()) || ' | TA DTV model - #TA_DTV_FEB17_TARGET: '||@@rowcount TO client
	
COMMIT
CREATE HG INDEX id1 ON #TA_DTV_FEB17_TARGET(account_number) 
COMMIT

UPDATE TA_DTV_FEB17_BASE
SET TA_flag = 1
	, a.TA_dt = min_dt
	, a.TA_count = t_calls
FROM TA_DTV_FEB17_BASE AS a 
JOIN #TA_DTV_FEB17_TARGET AS b on a.account_number = b.account_number

message convert(TIMESTAMP, now()) || ' | TA DTV model - TA_DTV_FEB17_BASE | TA updated : '||@@rowcount TO client
COMMIT
--- BUILDING THE SAMPLE ~10% based on one digit of the account_number
SELECT  
	  rand2
	, sample_group
	, TA_FLAG
	, TA_dt
	, TA_count
	, b.*
INTO TA_DTV_FEB17_SAMPLE
FROM TA_DTV_FEB17_BASE AS a 
JOIN sharmaa.View_attachments_201702 AS b ON a.account_number = b.account_number
WHERE filter_1 = @rand1 

message convert(TIMESTAMP, now()) || ' | TA DTV model - TA_DTV_FEB17_SAMPLE: '||@@rowcount TO client

COMMIT 
CREATE HG INDEX id1 ON TA_DTV_FEB17_SAMPLE (account_number)
COMMIT 

ALTER TABLE TA_DTV_FEB17_SAMPLE 
ADD( DTV_TENURE_latest VARCHAR(12) DEFAULT NULL
	, DTV_TENURE VARCHAR(12) DEFAULT NULL
	, BB_TENURE_latest  VARCHAR(12) DEFAULT NULL)
	, BB_TENURE  VARCHAR(12) DEFAULT NULL
	, offer_length_DTV			VARCHAR(40) DEFAULT NULL
	, Time_To_Offer_End_DTV		VARCHAR(40) DEFAULT NULL
	, Time_To_Offer_End_BB		VARCHAR(40) DEFAULT NULL
	, Time_To_Offer_End_LR		VARCHAR(40) DEFAULT NULL
	, Time_To_Offer_End			VARCHAR(40) DEFAULT NULL
	, Time_Since_Last_TA_call  	VARCHAR(40) DEFAULT NULL
	, Time_Since_Last_AB		VARCHAR(40) DEFAULT NULL
	, Previous_AB_Count			VARCHAR(40) DEFAULT NULL

UPDATE TA_DTV_FEB17_SAMPLE
SET BB_tenure_latest = case
				 when (Observation_dt - BB_latest_act_date) <=  365 then 'A.<1 Yr'
				 when (Observation_dt - BB_latest_act_date) <= 1095 then 'B.1-3 Yrs'
				 when (Observation_dt - BB_latest_act_date) <= 1825 then 'C.3-5 Yrs'
				 when (Observation_dt - BB_latest_act_date) <= 3650 then 'D.5-10 Yrs'
				 when (Observation_dt - BB_latest_act_date)  > 3650 then 'E.>10 Yrs'
				 end
	, DTV_tenure_latest = case
				 when (Observation_dt - dtv_latest_act_date) <=  365 then 'A.<1 Yr'
				 when (Observation_dt - dtv_latest_act_date) <= 1095 then 'B.1-3 Yrs'
				 when (Observation_dt - dtv_latest_act_date) <= 1825 then 'C.3-5 Yrs'
				 when (Observation_dt - dtv_latest_act_date) <= 3650 then 'D.5-10 Yrs'
				 when (Observation_dt - dtv_latest_act_date)  > 3650 then 'E.>10 Yrs'
				 end
	, BB_tenure = case
				 when (Observation_dt - BB_first_act_date) <=  365 then 'A.<1 Yr'
				 when (Observation_dt - BB_first_act_date) <= 1095 then 'B.1-3 Yrs'
				 when (Observation_dt - BB_first_act_date) <= 1825 then 'C.3-5 Yrs'
				 when (Observation_dt - BB_first_act_date) <= 3650 then 'D.5-10 Yrs'
				 when (Observation_dt - BB_first_act_date)  > 3650 then 'E.>10 Yrs'
				 end
	, DTV_tenure = case
				 when (Observation_dt - dtv_first_act_date) <=  365 then 'A.<1 Yr'
				 when (Observation_dt - dtv_first_act_date) <= 1095 then 'B.1-3 Yrs'
				 when (Observation_dt - dtv_first_act_date) <= 1825 then 'C.3-5 Yrs'
				 when (Observation_dt - dtv_first_act_date) <= 3650 then 'D.5-10 Yrs'
				 when (Observation_dt - dtv_first_act_date)  > 3650 then 'E.>10 Yrs'
				 end					 
				 
 ----------------------------------
UPDATE TA_DTV_FEB17_SAMPLE
SET 	 
	 offer_length_DTV = CASE 	WHEN 1 + (Curr_Offer_end_date_Intended_DTV - curr_offer_start_date_DTV) / 31 <= 3 THEN 'Offer Length 3M' 
									WHEN (1 + (Curr_Offer_end_date_Intended_DTV - curr_offer_start_date_DTV) / 31 > 3) AND (1 + (Curr_Offer_end_date_Intended_DTV - curr_offer_start_date_DTV) / 31 <= 6) THEN 'Offer Length 6M' 
									WHEN (1 + (Curr_Offer_end_date_Intended_DTV - curr_offer_start_date_DTV) / 31 > 6) AND (1 + (Curr_Offer_end_date_Intended_DTV - curr_offer_start_date_DTV) / 31 <= 9) THEN 'Offer Length 9M' 
									WHEN (1 + (Curr_Offer_end_date_Intended_DTV - curr_offer_start_date_DTV) / 31 > 9) AND (1 + (Curr_Offer_end_date_Intended_DTV - curr_offer_start_date_DTV) / 31 <= 12) THEN 'Offer Length 12M' 
									WHEN 1 + (Curr_Offer_end_date_Intended_DTV - curr_offer_start_date_DTV) / 31 > 12 THEN 'Offer Length 12M +' 
									WHEN Curr_Offer_end_date_Intended_DTV IS NULL THEN 
					'No Offer' END
		, Time_To_Offer_End_DTV = CASE 	WHEN Curr_Offer_end_date_Intended_DTV BETWEEN (Observation_dt + 1) AND (Observation_dt + 7) THEN 'Offer Ending in Next 1 Wks' 
										WHEN Curr_Offer_end_date_Intended_DTV BETWEEN (Observation_dt + 8) AND (Observation_dt + 14) THEN 'Offer Ending in Next 2-3 Wks' 
										WHEN Curr_Offer_end_date_Intended_DTV BETWEEN (Observation_dt + 15) AND (Observation_dt + 21) THEN 'Offer Ending in Next 2-3 Wks' 
										WHEN Curr_Offer_end_date_Intended_DTV BETWEEN (Observation_dt + 22) AND (Observation_dt + 28) THEN 'Offer Ending in Next 4-6 Wks' 
										WHEN Curr_Offer_end_date_Intended_DTV BETWEEN (Observation_dt + 29) AND (Observation_dt + 35) THEN 'Offer Ending in Next 4-6 Wks' 
										WHEN Curr_Offer_end_date_Intended_DTV BETWEEN (Observation_dt + 36) AND (Observation_dt + 42) THEN 'Offer Ending in Next 4-6 Wks' 
										WHEN Curr_Offer_end_date_Intended_DTV > (Observation_dt + 42) THEN 'Offer Ending in 7+ Wks' 
										WHEN Prev_offer_end_date_DTV BETWEEN (Observation_dt - 7) AND Observation_dt THEN 'Offer Ended in last 1 Wks' 
										WHEN Prev_offer_end_date_DTV BETWEEN (Observation_dt - 14) AND (Observation_dt - 8) THEN 'Offer Ended in last 2-3 Wks' 
										WHEN Prev_offer_end_date_DTV BETWEEN (Observation_dt - 21) AND (Observation_dt - 15) THEN 'Offer Ended in last 2-3 Wks' 
										WHEN Prev_offer_end_date_DTV BETWEEN (Observation_dt - 28) AND (Observation_dt - 22) THEN 'Offer Ended in last 4-6 Wks' 
										WHEN Prev_offer_end_date_DTV BETWEEN (Observation_dt - 35) AND (Observation_dt - 29) THEN 'Offer Ended in last 4-6 Wks' 
										WHEN Prev_offer_end_date_DTV BETWEEN (Observation_dt - 42) AND (Observation_dt - 36) THEN 'Offer Ended in last 4-6 Wks' 
										WHEN Prev_offer_end_date_DTV < (Observation_dt - 42) THEN 'Offer Ended 7+ Wks' ELSE 'No Offer End DTV' END
		, Time_To_Offer_End_BB = CASE WHEN Curr_Offer_end_date_intended_BB BETWEEN (Observation_dt + 1) AND (Observation_dt + 7) THEN 'Offer Ending in Next 1 Wks' 
									WHEN Curr_Offer_end_date_intended_BB BETWEEN (Observation_dt + 8) AND (Observation_dt + 14) THEN 'Offer Ending in Next 2-3 Wks' 
									WHEN Curr_Offer_end_date_intended_BB BETWEEN (Observation_dt + 15) AND (Observation_dt + 21) THEN 'Offer Ending in Next 2-3 Wks' 
									WHEN Curr_Offer_end_date_intended_BB BETWEEN (Observation_dt + 22) AND (Observation_dt + 28) THEN 'Offer Ending in Next 4-6 Wks' 
									WHEN Curr_Offer_end_date_intended_BB BETWEEN (Observation_dt + 29) AND (Observation_dt + 35) THEN 'Offer Ending in Next 4-6 Wks' 
									WHEN Curr_Offer_end_date_intended_BB BETWEEN (Observation_dt + 36) AND (Observation_dt + 42) THEN 'Offer Ending in Next 4-6 Wks' 
									WHEN Curr_Offer_end_date_intended_BB BETWEEN (Observation_dt + 43) AND (Observation_dt + 49) THEN 'Offer Ending in 7+ Wks' 
									WHEN Curr_Offer_end_date_intended_BB BETWEEN (Observation_dt + 50) AND (Observation_dt + 56) THEN 'Offer Ending in 7+ Wks' 
									WHEN Curr_Offer_end_date_intended_BB BETWEEN (Observation_dt + 57) AND (Observation_dt + 63) THEN 'Offer Ending in 7+ Wks' 
									WHEN Curr_Offer_end_date_intended_BB BETWEEN (Observation_dt + 64) AND (Observation_dt + 70) THEN 'Offer Ending in 7+ Wks' 
									WHEN Curr_Offer_end_date_intended_BB BETWEEN (Observation_dt + 71) AND (Observation_dt + 77) THEN 'Offer Ending in 7+ Wks' 
									WHEN Curr_Offer_end_date_intended_BB BETWEEN (Observation_dt + 78) AND (Observation_dt + 84) THEN 'Offer Ending in 7+ Wks' 
									WHEN Curr_Offer_end_date_intended_BB BETWEEN (Observation_dt + 85) AND (Observation_dt + 91) THEN 'Offer Ending in 7+ Wks' 
									WHEN Curr_Offer_end_date_intended_BB >= (Observation_dt + 92) THEN 'Offer Ending in 7+ Wks' 
									WHEN Prev_offer_end_date_BB BETWEEN (Observation_dt - 7) AND Observation_dt THEN 'Offer Ended in last 1 Wks' 
									WHEN Prev_offer_end_date_BB BETWEEN (Observation_dt - 14) AND (Observation_dt - 8) THEN 'Offer Ended in last 2-3 Wks' 
									WHEN Prev_offer_end_date_BB BETWEEN (Observation_dt - 21) AND (Observation_dt - 15) THEN 'Offer Ended in last 2-3 Wks' 
									WHEN Prev_offer_end_date_BB BETWEEN (Observation_dt - 28) AND (Observation_dt - 22) THEN 'Offer Ended in last 4-6 Wks' 
									WHEN Prev_offer_end_date_BB BETWEEN (Observation_dt - 35) AND (Observation_dt - 29) THEN 'Offer Ended in last 4-6 Wks' 
									WHEN Prev_offer_end_date_BB BETWEEN (Observation_dt - 42) AND (Observation_dt - 36) THEN 'Offer Ended in last 4-6 Wks' 
									WHEN Prev_offer_end_date_BB BETWEEN (Observation_dt - 49) AND (Observation_dt - 43) THEN 'Offer Ended 7+ Wks' 
									WHEN Prev_offer_end_date_BB BETWEEN (Observation_dt - 56) AND (Observation_dt - 50) THEN 'Offer Ended 7+ Wks' 
									WHEN Prev_offer_end_date_BB BETWEEN (Observation_dt - 63) AND (Observation_dt - 57) THEN 'Offer Ended 7+ Wks' 
									WHEN Prev_offer_end_date_BB BETWEEN (Observation_dt - 70) AND (Observation_dt - 64) THEN 'Offer Ended 7+ Wks' 
									WHEN Prev_offer_end_date_BB BETWEEN (Observation_dt - 77) AND (Observation_dt - 71) THEN 'Offer Ended 7+ Wks' 
									WHEN Prev_offer_end_date_BB BETWEEN (Observation_dt - 84) AND (Observation_dt - 78) THEN 'Offer Ended 7+ Wks' 
									WHEN Prev_offer_end_date_BB BETWEEN (Observation_dt - 91) AND (Observation_dt - 85) THEN 'Offer Ended 7+ Wks' 
									WHEN Prev_offer_end_date_BB <= (Observation_dt - 92) THEN 'Offer Ended 7+ Wks' 
									WHEN Prev_offer_end_date_BB IS NULL THEN 'Null' 
									WHEN Curr_Offer_end_date_intended_BB IS NULL THEN 'Null' 
									ELSE 'No Offer End BB' END
		, Time_To_Offer_End_LR = CASE WHEN Curr_Offer_end_date_Intended_LR BETWEEN (Observation_dt + 1) AND (Observation_dt + 7) THEN 'Offer Ending in Next 1 Wks' 
									WHEN Curr_Offer_end_date_Intended_LR BETWEEN (Observation_dt + 8) AND (Observation_dt + 14) THEN 'Offer Ending in Next 2-3 Wks' 
									WHEN Curr_Offer_end_date_Intended_LR BETWEEN (Observation_dt + 15) AND (Observation_dt + 21) THEN 'Offer Ending in Next 2-3 Wks' 
									WHEN Curr_Offer_end_date_Intended_LR BETWEEN (Observation_dt + 22) AND (Observation_dt + 28) THEN 'Offer Ending in Next 4-6 Wks' 
									WHEN Curr_Offer_end_date_Intended_LR BETWEEN (Observation_dt + 29) AND (Observation_dt + 35) THEN 'Offer Ending in Next 4-6 Wks' 
									WHEN Curr_Offer_end_date_Intended_LR BETWEEN (Observation_dt + 36) AND (Observation_dt + 42) THEN 'Offer Ending in Next 4-6 Wks' 
									WHEN Curr_Offer_end_date_Intended_LR > (Observation_dt + 42) THEN 'Offer Ending in 7+ Wks' 
									WHEN Prev_offer_end_date_LR BETWEEN (Observation_dt - 7) AND Observation_dt THEN 'Offer Ended in last 1 Wks' 
									WHEN Prev_offer_end_date_LR BETWEEN (Observation_dt - 14) AND (Observation_dt - 8) THEN 'Offer Ended in last 2-3 Wks' 
									WHEN Prev_offer_end_date_LR BETWEEN (Observation_dt - 21) AND (Observation_dt - 15) THEN 'Offer Ended in last 2-3 Wks' 
									WHEN Prev_offer_end_date_LR BETWEEN (Observation_dt - 28) AND (Observation_dt - 22) THEN 'Offer Ended in last 4-6 Wks' 
									WHEN Prev_offer_end_date_LR BETWEEN (Observation_dt - 35) AND (Observation_dt - 29) THEN 'Offer Ended in last 4-6 Wks' 
									WHEN Prev_offer_end_date_LR BETWEEN (Observation_dt - 42) AND (Observation_dt - 36) THEN 'Offer Ended in last 4-6 Wks' 
									WHEN Prev_offer_end_date_LR < (Observation_dt - 42) THEN 'Offer Ended 7+ Wks' ELSE 'No Offer End LR' END
		, Time_To_Offer_End = CASE WHEN DTV_BB_LR_offer_end_dt BETWEEN (Observation_dt + 1) AND (Observation_dt + 7) THEN 'Offer Ending in Next 1 Wks' 
									WHEN DTV_BB_LR_offer_end_dt BETWEEN (Observation_dt + 8) AND (Observation_dt + 14) THEN 'Offer Ending in Next 2-3 Wks' 
									WHEN DTV_BB_LR_offer_end_dt BETWEEN (Observation_dt + 15) AND (Observation_dt + 21) THEN 'Offer Ending in Next 2-3 Wks' 
									WHEN DTV_BB_LR_offer_end_dt BETWEEN (Observation_dt + 22) AND (Observation_dt + 28) THEN 'Offer Ending in Next 4-6 Wks' 
									WHEN DTV_BB_LR_offer_end_dt BETWEEN (Observation_dt + 29) AND (Observation_dt + 35) THEN 'Offer Ending in Next 4-6 Wks' 
									WHEN DTV_BB_LR_offer_end_dt BETWEEN (Observation_dt + 36) AND (Observation_dt + 42) THEN 'Offer Ending in Next 4-6 Wks' 
									WHEN DTV_BB_LR_offer_end_dt > (Observation_dt + 42) THEN 'Offer Ending in 7+ Wks' 
									WHEN DTV_BB_LR_offer_end_dt BETWEEN (Observation_dt - 7) AND Observation_dt THEN 'Offer Ended in last 1 Wks' 
									WHEN DTV_BB_LR_offer_end_dt BETWEEN (Observation_dt - 14) AND (Observation_dt - 8) THEN 'Offer Ended in last 2-3 Wks' 
									WHEN DTV_BB_LR_offer_end_dt BETWEEN (Observation_dt - 21) AND (Observation_dt - 15) THEN 'Offer Ended in last 2-3 Wks' 
									WHEN DTV_BB_LR_offer_end_dt BETWEEN (Observation_dt - 28) AND (Observation_dt - 22) THEN 'Offer Ended in last 4-6 Wks' 
									WHEN DTV_BB_LR_offer_end_dt BETWEEN (Observation_dt - 35) AND (Observation_dt - 29) THEN 'Offer Ended in last 4-6 Wks' 
									WHEN DTV_BB_LR_offer_end_dt BETWEEN (Observation_dt - 42) AND (Observation_dt - 36) THEN 'Offer Ended in last 4-6 Wks' 
									WHEN DTV_BB_LR_offer_end_dt < (Observation_dt - 42) THEN 'Offer Ended 7+ Wks' ELSE 'No Offer' END
		--, Previous_Abs = Previous_Abs + CASE WHEN DTV_AB > 0 THEN 1 ELSE 0 END
FROM TA_DTV_FEB17_SAMPLE AS a 
JOIN citeam.CUST_FCAST_WEEKLY_BASE AS b ON a.account_number = b.account_number AND b.end_date = '2017-02-02'

UPDATE TA_DTV_FEB17_SAMPLE
SET Time_Since_Last_TA_call = CASE WHEN Last_TA_Call_dt IS NULL THEN 'No Prev TA Calls' 
										WHEN (Cast(Observation_dt AS INT) - Cast(Last_TA_Call_dt AS INT)) / 7 = 0 THEN '0 Wks since last TA Call' 
										WHEN (Cast(Observation_dt AS INT) - Cast(Last_TA_Call_dt AS INT)) / 7 = 1 THEN '01 Wks since last TA Call' 
										WHEN (Cast(Observation_dt AS INT) - Cast(Last_TA_Call_dt AS INT)) / 7 BETWEEN 2 AND 5 THEN '02-05 Wks since last TA Call' 
										WHEN (Cast(Observation_dt AS INT) - Cast(Last_TA_Call_dt AS INT)) / 7 BETWEEN 6 AND 35 THEN '06-35 Wks since last TA Call' 
										WHEN (Cast(Observation_dt AS INT) - Cast(Last_TA_Call_dt AS INT)) / 7 BETWEEN 36 AND 41 THEN '36-46 Wks since last TA Call' 
										WHEN (Cast(Observation_dt AS INT) - Cast(Last_TA_Call_dt AS INT)) / 7 BETWEEN 42 AND 46 THEN '36-46 Wks since last TA Call' 
										WHEN (Cast(Observation_dt AS INT) - Cast(Last_TA_Call_dt AS INT)) / 7 = 47 THEN '47 Wks since last TA Call' 
										WHEN (Cast(Observation_dt AS INT) - Cast(Last_TA_Call_dt AS INT)) / 7 BETWEEN 48 AND 52 THEN '48-52 Wks since last TA Call' 
										WHEN (Cast(Observation_dt AS INT) - Cast(Last_TA_Call_dt AS INT)) / 7 BETWEEN 53 AND 60 THEN '53-60 Wks since last TA Call' 
										WHEN (Cast(Observation_dt AS INT) - Cast(Last_TA_Call_dt AS INT)) / 7 > 60 THEN '61+ Wks since last TA Call'
			ELSE ''
			END
	, Time_Since_Last_AB = CASE WHEN Last_AB_Dt IS NULL THEN 'No Prev AB Calls' 
								WHEN (Cast(Observation_dt AS INT) - Cast(Last_AB_Dt AS INT)) / 31 = 0 THEN '0 Mnths since last AB' 
								WHEN (Cast(Observation_dt AS INT) - Cast(Last_AB_Dt AS INT)) / 31 = 1 THEN '1-2 Mnths since last AB' 
								WHEN (Cast(Observation_dt AS INT) - Cast(Last_AB_Dt AS INT)) / 31 = 2 THEN '1-2 Mnths since last AB' 
								WHEN (Cast(Observation_dt AS INT) - Cast(Last_AB_Dt AS INT)) / 31 = 3 THEN '3 Mnths since last AB' 
								WHEN (Cast(Observation_dt AS INT) - Cast(Last_AB_Dt AS INT)) / 31 = 4 THEN '4 Mnths since last AB' 
								WHEN (Cast(Observation_dt AS INT) - Cast(Last_AB_Dt AS INT)) / 31 = 5 THEN '5-7 Mnths since last AB' 
								WHEN (Cast(Observation_dt AS INT) - Cast(Last_AB_Dt AS INT)) / 31 = 6 THEN '5-7 Mnths since last AB' 
								WHEN (Cast(Observation_dt AS INT) - Cast(Last_AB_Dt AS INT)) / 31 = 7 THEN '5-7 Mnths since last AB' 
								WHEN (Cast(Observation_dt AS INT) - Cast(Last_AB_Dt AS INT)) / 31 = 8 THEN '8-12 Mnths since last AB' 
								WHEN (Cast(Observation_dt AS INT) - Cast(Last_AB_Dt AS INT)) / 31 = 9 THEN '8-12 Mnths since last AB' 
								WHEN (Cast(Observation_dt AS INT) - Cast(Last_AB_Dt AS INT)) / 31 = 10 THEN '8-12 Mnths since last AB' 
								WHEN (Cast(Observation_dt AS INT) - Cast(Last_AB_Dt AS INT)) / 31 = 11 THEN '8-12 Mnths since last AB' 
								WHEN (Cast(Observation_dt AS INT) - Cast(Last_AB_Dt AS INT)) / 31 = 12 THEN '8-12 Mnths since last AB' 
								WHEN (Cast(Observation_dt AS INT) - Cast(Last_AB_Dt AS INT)) / 31 > 12 THEN '12+ Mnths since last AB' 
								ELSE '' END

FROM TA_DTV_FEB17_SAMPLE AS a 
JOIN citeam.CUST_FCAST_WEEKLY_BASE AS b ON a.account_number = b.account_number AND b.end_date = '2017-02-02'
								
END
COMMIT 




SELECT a.account_number 
	, Simple_Segment
	, Offer_Applied
	, Offer_1m
	, Offer_3m
	, Offer_6m
	, Offer_Other
	, Sports_Downgrade
	, Movies_Downgrade
	, TA_Call_Count
	, TA_Call_Flag
	, TA_Save_Flag
	, PAT_Call_Count
	, PAT_Call
	, PAT_Save
	, Offer_Length
	, Time_To_Offer_End
	, Sports_Upgrade
	, Movies_Upgrade
	, Prev_offer_start_date_Total
	, Days_prev_offer_end_Total
	, Curr_Offer_end_Date_Total
	, Curr_Offer_start_Date_Total
	, Days_Curr_Offer_End_Total
	, offer_length_Total
	, Q_active
	, Prev_offer_end_date_DTV
	, Prev_offer_start_date_DTV
	, Days_prev_offer_end_DTV
	, Curr_Offer_end_Date_DTV
	, Curr_Offer_start_Date_DTV
	, Days_Curr_Offer_End_DTV
	, Time_to_Offer_End_DTV
	, Offer_Length_DTV
	, Prev_offer_end_date_BB
	, Prev_offer_start_date_BB
	, Days_prev_offer_end_BB
	, Curr_Offer_end_Date_BB
	, Curr_Offer_start_Date_BB
	, Days_Curr_Offer_End_BB
	, Time_to_Offer_End_BB
	, Offer_Length_BB
	, Last_TA_Call_dt
	, TA_Calls_last_90d
INTO TA_DTV_FEB17_SAMPLE_B	
FROM CITeam.CUST_FCAST_WEEKLY_BASE AS a
JOIN TA_DTV_FEB17_SAMPLE AS b ON a.account_number = b.account_number 
WHERE end_date = '2017-02-02'

---------------

ALTER TABLE TA_DTV_FEB17_SAMPLE
ADD(TA_DTV_3M INT NULL DEFAULT NULL
	, TA_DTV_6M INT NULL DEFAULT NULL
	, TA_DTV_9M INT NULL DEFAULT NULL
	, TA_DTV_12M INT NULL DEFAULT NULL
	, TA_ALL_3M INT NULL DEFAULT NULL
	, TA_ALL_6M INT NULL DEFAULT NULL
	, TA_ALL_9M INT NULL DEFAULT NULL
	, TA_ALL_12M INT NULL DEFAULT NULL) 

DECLARE @end_dt DATE 
SET @start_dt  	= '2017-03-01'
	
SELECT a.account_number 
	, COUNT(CASE WHEN DTV = 1 AND event_dt BETWEEN DATEADD(MONTH, -3,@start_dt) AND @start_dt THEN DISTINCT event_date ELSE NULL END) 		AS TA_DTV_3M
	, COUNT(CASE WHEN DTV = 1 AND event_dt BETWEEN DATEADD(MONTH, -6,@start_dt) AND @start_dt THEN DISTINCT event_date ELSE NULL END) 		AS TA_DTV_6M
	, COUNT(CASE WHEN DTV = 1 AND event_dt BETWEEN DATEADD(MONTH, -9,@start_dt) AND @start_dt THEN DISTINCT event_date ELSE NULL END) 		AS TA_DTV_9M
	, COUNT(CASE WHEN DTV = 1 AND event_dt BETWEEN DATEADD(MONTH, -12,@start_dt) AND @start_dt THEN DISTINCT event_date ELSE NULL END) 		AS TA_DTV_12M
	, COUNT(CASE WHEN event_dt BETWEEN DATEADD(MONTH, -3,@start_dt) AND @start_dt THEN DISTINCT event_date ELSE NULL END) 		AS TA_ALL_3M
	, COUNT(CASE WHEN event_dt BETWEEN DATEADD(MONTH, -6,@start_dt) AND @start_dt THEN DISTINCT event_date ELSE NULL END) 		AS TA_ALL_6M
	, COUNT(CASE WHEN event_dt BETWEEN DATEADD(MONTH, -9,@start_dt) AND @start_dt THEN DISTINCT event_date ELSE NULL END) 		AS TA_ALL_9M
	, COUNT(CASE WHEN event_dt BETWEEN DATEADD(MONTH, -12,@start_dt) AND @start_dt THEN DISTINCT event_date ELSE NULL END) 		AS TA_ALL_12M
INTO #t1 
FROM TA_DTV_FEB17_SAMPLE AS a 
JOIN CITEAM.View_CUST_CALLS_HIST 	AS b ON a.account_number = b.account_number 
WHERE event_dt between DATEADD(MONTH,-13,@start_dt) AND @start_dt
	AND typeofevent='TA' 
	AND a.account_number IS NOT NULL
GROUP BY 	a.account_number 
COMMIT 
CREATE HG INDEX id1 ON #t1 (account_number) 
COMMIT 
UPDATE TA_DTV_FEB17_SAMPLE
SET a.TA_DTV_3M = b.TA_DTV_3M
	, a.TA_DTV_6M =b.TA_DTV_6M
	, a.TA_DTV_9M =b.TA_DTV_9M
	, a.TA_DTV_12M =b.TA_DTV_12M
	, a.TA_ALL_3M =b.TA_ALL_3M
	, a.TA_ALL_6M =b.TA_ALL_6M
	, a.TA_ALL_9M =b.TA_ALL_9M
	, a.TA_ALL_12M =b.TA_ALL_12M
FROM 	TA_DTV_FEB17_SAMPLE AS a 
JOIN  #t1 AS B AS b ON a.account_number = b.account_number 
	



SELECT a.account_number
 ,  MAX(CASE WHEN a.status_code ='PC' AND effective_from_dt BETWEEN DATEADD(DAY, -30,@start_dt) AND @start_dt THEN 1 ELSE 0 END) 		AS Pending_cancel_30days
 ,  MAX(CASE WHEN a.status_code ='PC' AND effective_from_dt BETWEEN DATEADD(DAY, -60,@start_dt) AND @start_dt THEN 1 ELSE 0 END) 		AS Pending_cancel_60days
 ,  MAX(CASE WHEN a.status_code ='PC' AND effective_from_dt BETWEEN DATEADD(DAY, -90,@start_dt) AND @start_dt THEN 1 ELSE 0 END) 		AS Pending_cancel_90days
 ,  MAX(CASE WHEN a.status_code ='AB' AND effective_from_dt BETWEEN DATEADD(DAY, -30,@start_dt) AND @start_dt THEN 1 ELSE 0 END) 		AS Active_Block_30days
 ,  MAX(CASE WHEN a.status_code ='AB' AND effective_from_dt BETWEEN DATEADD(DAY, -60,@start_dt) AND @start_dt THEN 1 ELSE 0 END) 		AS Active_Block_60days
 ,  MAX(CASE WHEN a.status_code ='AB' AND effective_from_dt BETWEEN DATEADD(DAY, -90,@start_dt) AND @start_dt THEN 1 ELSE 0 END) 		AS Active_Block_90days
 ,  MAX(CASE WHEN a.status_code ='AB' AND effective_from_dt BETWEEN DATEADD(MONTH, -12, @start_dt) AND @start_dt THEN 1 ELSE 0 END) 		AS Active_Block_360days
 INTO #t1
FROM  	TA_DTV_FEB17_SAMPLE AS a
JOIN CUST_SUBS_HIST AS b ON a.account_number = b.account_number AND  effective_from_dt >= DATEADD(MONTH, -10,@start_dt) 
WHERE  subscription_sub_type = 'DTV Primary Viewing'
  AND a.account_number IS NOT NULL
  AND b.effective_from_dt != b.effective_to_dt
  AND status_code_changed ='Y'
GROUP BY  a.account_number 


COMMIT 
CREATE HG INDEX id1 ON #t1 (account_number) 
COMMIT 
UPDATE TA_DTV_FEB17_SAMPLE
SET  a.Pending_cancel_30days = b.Pending_cancel_30days
	, a.Pending_cancel_60days =b.Pending_cancel_60days
	, a.Pending_cancel_90days =b.Pending_cancel_90days
	, a.Active_Block_30days =b.Active_Block_30days
	, a.Active_Block_60days =b.Active_Block_60days
	, a.Active_Block_90days =b.Active_Block_90days 
	,a.Active_Block_360days =b.Active_Block_360days 
FROM 	TA_DTV_FEB17_SAMPLE AS a 
JOIN  #t1 AS B AS b ON a.account_number = b.account_number 


ALTER TABLE TA_DTV_FEB17_SAMPLE
ADD (my_sky_login_30D INT DEFAULT NULL 
	, my_sky_login_60D INT DEFAULT NULL 
	, my_sky_login_90D INT DEFAULT NULL 
	, my_sky_login_180D INT DEFAULT NULL 
	, my_sky_login_360D INT DEFAULT NULL )


SELECT 		COUNT(DISTINCT  CASE WHEN visit_date BETWEEN DATEADD(DAY,-30,observation_dt) and observation_dt THEN  visit_date ELSE NULL END) AS login_30M
		, 	COUNT(DISTINCT  CASE WHEN visit_date BETWEEN DATEADD(DAY,-60,observation_dt) and observation_dt THEN  visit_date ELSE NULL END) AS login_60M
		, 	COUNT(DISTINCT  CASE WHEN visit_date BETWEEN DATEADD(DAY,-90,observation_dt) and observation_dt THEN  visit_date ELSE NULL END) AS login_90M
		, 	COUNT(DISTINCT  CASE WHEN visit_date BETWEEN DATEADD(DAY,-180,observation_dt) and observation_dt THEN visit_date ELSE NULL END) AS login_180M
		, 	COUNT(DISTINCT  CASE WHEN visit_date BETWEEN DATEADD(DAY,-360,observation_dt) and observation_dt THEN visit_date ELSE NULL END) AS login_360M
      , mr.account_number
INTO #days_visited_3m
FROM vespa_shared.mysky_daily_usage 	AS mr 
JOIN TA_DTV_FEB17_SAMPLE 	AS base ON BASE.account_number = mr.account_number
WHERE visit_date BETWEEN DATEADD(mm,-361,observation_dt) AND observation_dt
GROUP BY mr.account_number

		
UPDATE TA_DTV_FEB17_SAMPLE
SET my_sky_login_30D = CASE WHEN login_30M >=5 THEN 5 ELSE login_30M  END 
, my_sky_login_60D = CASE WHEN login_60M >=5 THEN 5 ELSE login_60M  END 
, my_sky_login_90D = CASE WHEN login_90M >=5 THEN 5 ELSE login_90M  END 
, my_sky_login_180D = CASE WHEN login_180M >=5 THEN 5 ELSE login_180M  END 
, my_sky_login_360D = CASE WHEN login_360M >=5 THEN 5 ELSE login_360M  END 
FROM TA_DTV_FEB17_SAMPLE AS a
JOIN #days_visited_3m AS b ON a.account_number = b.account_number



ALTER TABLE TA_DTV_FEB17_SAMPLE
ADD (	OD_count_30D_raw INT NULL DEFAULT 0 
		, OD_count_60D_raw INT NULL DEFAULT 0 
		, OD_count_90D_raw INT NULL DEFAULT 0 
		, OD_count_180D_raw INT NULL DEFAULT 0 
		, OD_count_360D_raw INT NULL DEFAULT 0 )

SELECT a.account_number
		, 	COUNT(CASE WHEN a.last_modified_dt BETWEEN DATEADD(DAY,-30,observation_dt) and observation_dt THEN  1 ELSE NULL END) AS login_30M
		, 	COUNT(CASE WHEN a.last_modified_dt BETWEEN DATEADD(DAY,-60,observation_dt) and observation_dt THEN  1 ELSE NULL END) AS login_60M
		, 	COUNT(CASE WHEN a.last_modified_dt BETWEEN DATEADD(DAY,-90,observation_dt) and observation_dt THEN  1 ELSE NULL END) AS login_90M
		, 	COUNT(CASE WHEN a.last_modified_dt BETWEEN DATEADD(DAY,-180,observation_dt) and observation_dt THEN 1 ELSE NULL END) AS login_180M
		, 	COUNT(CASE WHEN a.last_modified_dt BETWEEN DATEADD(DAY,-360,observation_dt) and observation_dt THEN 1 ELSE NULL END) AS login_360M
INTO #OD_raw
FROM  TA_DTV_FEB17_SAMPLE AS b
JOIN cust_anytime_plus_downloads AS a ON a.account_number = b.account_number
WHERE cast(a.last_modified_dt AS DATE) >= DATEADD (DAY,-361,observation_dt)
	AND a.x_actual_downloaded_size_mb > 1 
	AND a.download_start_percent = 0
	AND b.account_number IS NOT NULL 
GROUP BY a.account_number
	
	
UPDATE TA_DTV_FEB17_SAMPLE
SET   OD_count_30D_raw = login_30M  
	, OD_count_60D_raw = login_60M  
	, OD_count_90D_raw = login_90M  
	, OD_count_180D_raw = login_180M  
	, OD_count_360D_raw = login_360M  
FROM TA_DTV_FEB17_SAMPLE AS a
JOIN #OD_raw AS b ON a.account_number = b.account_number
	
-------------------------------------------------------------------------
-----------------		ALL_offer_rem_and_end 			-----------------
ALTER TABLE TA_DTV_FEB17_SAMPLE
ADD ALL_offer_rem_and_end INT DEFAULT NULL 
-------------------------------------------

SELECT base.account_number
		, '2017-03-01' AS end_date 
      ,MAX(offer_duration) AS offer_length
      ,MAX(DATEDIFF(DD, end_date, intended_offer_end_dt)) AS length_rem
	  , BB_current_offer_duration_rem = CASE WHEN length_rem > 2854 THEN 2854
                                          WHEN length_rem < 0    THEN 0
                                          ELSE length_rem 
										END 
INTO #current_bb_offer_length      
FROM TA_DTV_FEB17_SAMPLE AS base
INNER JOIN offer_usage_all AS oua ON oua.account_number = base.account_number
WHERE   subs_type IN ('Broadband DSL Line','DTV Primary Viewing','DTV Extra Subscription','SKY TALK LINE RENTAL','SKY TALK SELECT')
	AND end_date >= offer_start_dt_actual
	AND end_date <  offer_end_dt_actual
	AND intended_total_offer_value_yearly IS NOT NULL
GROUP BY base.account_number;


SELECT base.account_number
      ,offer_end_dt_actual
	  , '2017-03-01' AS end_date
      ,rank() over(PARTITION BY base.account_number ORDER BY offer_start_dt_actual DESC) AS latest_offer
	  , BB_time_since_last_offer_end = DATEDIFF(DD, offer_end_dt_actual, end_date)
INTO #prev_bb_offer_dt      
FROM TA_DTV_FEB17_SAMPLE 			AS base
INNER JOIN offer_usage_all 				AS oua 			ON oua.account_number = base.account_number
WHERE subs_type IN ('Broadband DSL Line','DTV Primary Viewing','DTV Extra Subscription')
		AND end_date >  offer_start_dt_actual
		AND end_date >= offer_end_dt_actual
		AND intended_total_offer_value_yearly IS NOT NULL

COMMIT 
DELETE FROM #prev_bb_offer_dt      WHERE latest_offer <>1
CREATE HG INDEX id1 ON #prev_bb_offer_dt (account_number)
CREATE DATE INDEX id1 ON #prev_bb_offer_dt (offer_end_dt_actual)
COMMIT

UPDATE TA_DTV_FEB17_SAMPLE
SET ALL_offer_rem_and_end =  CASE WHEN BB_current_offer_duration_rem > 0 THEN BB_current_offer_duration_rem 
								WHEN (BB_current_offer_duration_rem = 0 OR BB_current_offer_duration_rem  IS NULL) AND BB_time_since_last_offer_end <> - 9999 THEN (0 - BB_time_since_last_offer_end) 
								ELSE - 9999 END
FROM TA_DTV_FEB17_SAMPLE		AS a	 
LEFT JOIN #current_bb_offer_length  	AS b ON a.account_number = b.account_number  
LEFT JOIN #prev_bb_offer_dt      		AS c ON a.account_number = c.account_number  
	
COMMIT 
MESSAGE 'BB_offer_rem_and_end updated: '||@@rowcount type status to client

GO										
--------------------------------------------------------------




SELECT base.account_number
	  , '2017-03-01' AS end_date 
      ,oua.offer_value
	  ,rank() over(PARTITION BY base.account_number ORDER BY offer_start_dt_actual DESC) AS latest_offer
INTO #current_bb_offer_length      
FROM TA_DTV_FEB17_SAMPLE AS base
INNER JOIN citeam.offer_usage_all AS oua ON oua.account_number = base.account_number
WHERE   subs_type IN ('Broadband DSL Line','DTV Primary Viewing','DTV Extra Subscription','SKY TALK LINE RENTAL','SKY TALK SELECT')
	AND end_date >= offer_start_dt_actual
	AND end_date <  offer_end_dt_actual
	AND intended_total_offer_value_yearly IS NOT NULL
DELETE FROM #current_bb_offer_length      WHERE latest_offer <>1



SELECT base.account_number
      ,offer_end_dt_actual
	  , '2017-03-01' AS end_date
	  , oua.offer_value
      ,rank() over(PARTITION BY base.account_number ORDER BY offer_start_dt_actual DESC) AS latest_offer
INTO #prev_bb_offer_dt      
FROM TA_DTV_FEB17_SAMPLE 			AS base
INNER JOIN citeam.offer_usage_all 				AS oua 			ON oua.account_number = base.account_number
WHERE subs_type IN ('Broadband DSL Line','DTV Primary Viewing','DTV Extra Subscription')
		AND end_date >  offer_start_dt_actual
		AND end_date >= offer_end_dt_actual
		AND intended_total_offer_value_yearly IS NOT NULL

COMMIT 
DELETE FROM #prev_bb_offer_dt      WHERE latest_offer <>1
CREATE HG INDEX id1 ON #prev_bb_offer_dt (account_number)
COMMIT



UPDATE TA_DTV_FEB17_SAMPLE
SET a.offer_value_raw =  CASE   WHEN b.offer_value is not null  THEN b.offer_value 
                            WHEN 	c.offer_value is not null    THEN c.offer_value 
							ELSE NULL END
FROM TA_DTV_FEB17_SAMPLE		AS a	 
LEFT JOIN #current_bb_offer_length  	AS b ON a.account_number = b.account_number
LEFT JOIN #prev_bb_offer_dt      		AS c ON a.account_number = c.account_number
	
COMMIT 
MESSAGE 'BB_offer_rem_and_end updated: '||@@rowcount type status to client


SELECT account_number, NTILE(10) OVER (ORDER BY offer_value_raw) ntilex
INTo #t1 
FROM TA_DTV_FEB17_SAMPLE

COMMIT 
CREATE HG INDEX id1 on #t1(account_number)
UPDATE TA_DTV_FEB17_SAMPLE
SET offer_value = CAST(ntilex AS VARCHAR)
FROM TA_DTV_FEB17_SAMPLE AS a
JOIN #t1 AS b On a.account_number = b.account_number 


--------------------------------------
	
ALTER TABLE TA_DTV_FEB17_SAMPLE
ADD movies_downgrade_12M BIT DEFAULT 0 
GO 
SELECT a.account_number, MAX(b.movies_downgrade) MD12M
INTO #t1 
FROM TA_DTV_FEB17_SAMPLE AS a 
JOIN citeam.cust_fcast_weekly_base AS b ON a.account_number =b.account_number AND b.end_date BETWEEN  '2016-03-01' AND  '2017-03-01'
GROUP BY a.account_number

COMMIT 
CREATE HG INDEX id1 ON #t1 (account_number) 

UPDATE TA_DTV_FEB17_SAMPLE
SET movies_downgrade_12M = MD12M 
FROM TA_DTV_FEB17_SAMPLE AS a 
JOIN #t1 AS b on a.account_number = b.account_number 

----------------------------------------
	
create OR REPLACE view pitteloudj.TA_DTV_FEB_SAMPLE_VIEW
  as select TA_flag,
    TA_ALL_12M,
    affluence,
    CQM_Score,
    h_fss_v3_group,
    age,
    h_mosaic_uk_group,
    BB_type_2 = CASE WHEN BB_type IN ('Sky Broadband Unlimited Fibre','Sky Fibre''Sky Fibre Unlimited Pro','Sky Fibre Lite','Sky Fibre Max') THEN 'Sky Fibre'
					 WHEN BB_type IN ('Sky Broadband Unlimited Pro','Sky Broadband Unlimited') THEN 'Sky Unlimited'
					 ELSE 'No BB' END ,
	SkyTalk_type= CASE 	WHEN SkyTalk_type IN ('Sky Pay As You Talk') THEN 'Sky Pay As You Talk'
						WHEN SkyTalk_type LIKE '%Anytime%' THEN 'Sky Talk Anytime'
						WHEN SkyTalk_type IN ('Sky Talk Evenings and Weekends Extra', 'Sky Talk Evenings and Weekends Extra (Freetime)', 'Sky Talk Evenings and Weekends Extra (Weekends)') THEN 'Sky Talk Evenings and Weekends' 
						WHEN SkyTalk_type LIKE 'Sky Talk International Extra%' THEN 'Sky Talk International'
						WHEN SkyTalk_type LIKE '%Unlimited%' THEN 'Sky Talk Unlimited'
						WHEN SkyTalk_type  IS NULL THEN 'No Sky Talk'
						ELSE 'Other Sky Talk' END ,
	
    SkyPlus,
    DTV_first_tenure_months,
    my_sky_login_30D,
    OD_count_360D,
    DTV_offer_rem_and_end_group,
	TV_package = CASE WHEN LOWER(Package_segment) LIKE '%basic%' THEN 'Basic' ELSE 'Non-Basic' END 
	,Active_Block_360days
	, ALL_offer_rem_and_end
	, Offer_Value
	, movies_downgrade_12M
	,Time_Since_Last_TA_call

FROM TA_DTV_FEB17_SAMPLE
	
	
	
	
	
ALL_offer_rem_and_end_grouped
Offer_Value_raw





CREATE VIEW pitteloudj.TA_DTV_FEB_SAMPLE_VIEW
AS
SELECT TA_flag
	, TA_ALL_12M
	, affluence
	, CQM_Score
	, h_fss_v3_group
	, age
	, h_mosaic_uk_group
	, CASE WHEN BB_type IN ('Sky Broadband Unlimited Fibre', 'Sky Fibre''Sky Fibre Unlimited Pro', 'Sky Fibre Lite', 'Sky Fibre Max') THEN 'Sky Fibre' 
			WHEN BB_type IN ('Sky Broadband Unlimited Pro', 'Sky Broadband Unlimited') THEN 'Sky Unlimited' 
			ELSE 'No BB' END AS BB_type_2
	, CASE 	WHEN SkyTalk_type IN ('Sky Pay As You Talk') THEN 'Sky Pay As You Talk' 
			WHEN SkyTalk_type LIKE '%Anytime%' THEN 'Sky Talk Anytime' 
			WHEN SkyTalk_type IN ('Sky Talk Evenings and Weekends Extra', 'Sky Talk Evenings and Weekends Extra (Freetime)', 'Sky Talk Evenings and Weekends Extra (Weekends)') THEN 'Sky Talk Evenings and Weekends' 
			WHEN SkyTalk_type LIKE 'Sky Talk International Extra%' THEN 'Sky Talk International' 
			WHEN SkyTalk_type LIKE '%Unlimited%' THEN 'Sky Talk Unlimited' 
			WHEN SkyTalk_type IS NULL THEN 'No Sky Talk' 
			ELSE 'Other Sky Talk' END AS SkyTalk_type_2
	, SkyPlus
	, DTV_first_tenure_months
	, my_sky_login_30D
	, OD_count_360D
	, DTV_offer_rem_and_end_group
FROM pitteloudj.TA_DTV_FEB17_SAMPLE







CREATE OR REPLACE VIEW pitteloudj.TA_DTV_FEB_SAMPLE_VIEW_2
AS
SELECT TA_flag
    ,(TA_ALL_12M- avg(TA_ALL_12M) over()) / stddev(TA_ALL_12M) over() as TA_ALL_12M
	, affluence
    ,(CQM_Score - avg(CQM_Score) over()) / stddev(CQM_Score) over() as CQM_Score
	, h_fss_v3_group
    ,COALESCE (a.age, 51) AS n_age
    ,(n_age - avg(n_age) over()) / stddev(n_age) over() as age
	, h_mosaic_uk_group
	, CASE 	WHEN BB_type IN ('Sky Broadband Unlimited Fibre', 'Sky Fibre''Sky Fibre Unlimited Pro', 'Sky Fibre Lite', 'Sky Fibre Max') THEN 'Sky Fibre' 
			WHEN BB_type IN ('Sky Broadband Unlimited Pro', 'Sky Broadband Unlimited') THEN 'Sky Unlimited' 
			ELSE 'No BB' END AS BB_type_2
	, CASE 	WHEN SkyTalk_type IN ('Sky Pay As You Talk') THEN 'Sky Pay As You Talk' 
			WHEN SkyTalk_type LIKE '%Anytime%' THEN 'Sky Talk Anytime' 
			WHEN SkyTalk_type IN ('Sky Talk Evenings and Weekends Extra', 'Sky Talk Evenings and Weekends Extra (Freetime)', 'Sky Talk Evenings and Weekends Extra (Weekends)') THEN 'Sky Talk Evenings and Weekends' 
			WHEN SkyTalk_type IS NULL THEN 'No Sky Talk' 
			ELSE 'Other Sky Talk' END AS SkyTalk_type
	, SkyPlus
    ,COALESCE (a.DTV_first_tenure_months, 103) AS DTV_tenure
    ,(DTV_tenure - avg(DTV_tenure) over()) / stddev(DTV_tenure) over() as DTV_first_tenure_months
    ,COALESCE (a.my_sky_login_30D, 0) AS n_login
    ,(n_login- avg(n_login) over()) / stddev(n_login) over() as my_sky_login_30D
	,(OD_count_360D- avg(OD_count_360D) over()) / stddev(OD_count_360D) over() as OD_count_360D
	, DTV_offer_rem_and_end_group
	, CASE WHEN LOWER(Package_segment) LIKE '%basic%' THEN 'Basic' ELSE 'Non-Basic' END AS TV_package
	, Active_Block_360days
    ,(ALL_offer_rem_and_end - avg(ALL_offer_rem_and_end) over()) / stddev(ALL_offer_rem_and_end) over() as ALL_offer_rem_and_end
	, Offer_Value
	, movies_downgrade_12M
    ,COALESCE (a.Time_Since_Last_TA_call , 'No Prev TA Calls') AS Time_Since_Last_TA_call 
	
FROM pitteloudj.TA_DTV_FEB17_SAMPLE as a 







