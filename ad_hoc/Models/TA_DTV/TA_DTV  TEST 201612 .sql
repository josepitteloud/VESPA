--------	Source tables that have to be updated depending on the run date 
--	View_attachments_201603
--	TA_DTV_SAMPLE_VIEW_MAR16
DROP TABLE TA_DTV_MAR16_SAMPLE
DROP TABLE TA_DTV_MAR16_BASE
GO

------------ CREATING FULL BASE 
message convert(TIMESTAMP, now()) || ' | TA DTV model - Initialization begin ' TO client

DECLARE @start_dt DATE 
DECLARE @end_dt DATE 
DECLARE @multi FLOAT 
DECLARE @rand1 CHAR(1) 


SET @multi 		= CAST((SELECT RAND(DATEPART(MS,GETDATE()))) AS FLOAT)
SET @start_dt  	= '2016-04-01'
SET @rand1 = '3'


SELECT account_number, dtv
	, RAND(cb_key_household * @multi) 		AS rand2
	, SUBSTRING(account_number, 9,1) 		AS filter_1 
	, CAST (NULL AS CHAR(1))                AS sample_group 
	
	, CAST (0 AS BIT)						AS TA_FLAG
	, CAST(NULL AS DATE) 					AS TA_dt
	, CAST(NULL AS INT) 					AS TA_count

INTO TA_DTV_MAR16_BASE
FROM sharmaa.View_attachments_201603
WHERE dtv = 1							-- DTV active customers 
	
	message convert(TIMESTAMP, now()) || ' | TA DTV model - TA_DTV_MAR16_BASE: '||@@rowcount TO client
	
COMMIT 
CREATE HG INDEX id1 ON TA_DTV_MAR16_BASE(account_number) 
CREATE HG INDEX id2 ON TA_DTV_MAR16_BASE(sample_group) 
COMMIT 

--- Selectinf the trainning/Validation sample 30%/70%
UPDATE TA_DTV_MAR16_BASE
SET sample_group = CASE WHEN rand2 <= 0.30 THEN 'T' ELSE 'V' END 
COMMIT 

	message convert(TIMESTAMP, now()) || ' | TA DTV model - TA_DTV_MAR16_BASE sample updated : '||@@rowcount TO client
------------------- TA DTV MODEL script - FEB 2016

SELECT a.ACCOUNT_NUMBER
		, 1 flag
		, MIN(event_dt) 		AS min_dt
		, SUM(total_calls)		AS t_calls
INTO #TA_DTV_DEC16_TARGET		
FROM CITEAM.View_CUST_CALLS_HIST 	AS a 
JOIN TA_DTV_MAR16_BASE 				AS b ON a.account_number = b.account_number 
WHERE event_dt between @start_dt AND DATEADD(day,30,@start_dt )
	AND typeofevent='TA' 
	AND a.DTV = 1
	AND a.account_number IS NOT NULL
GROUP BY 	a.account_number 

	message convert(TIMESTAMP, now()) || ' | TA DTV model - #TA_DTV_DEC16_TARGET: '||@@rowcount TO client
	
COMMIT
CREATE HG INDEX id1 ON #TA_DTV_DEC16_TARGET(account_number) 
COMMIT

UPDATE TA_DTV_MAR16_BASE
SET TA_flag = 1
	, a.TA_dt = min_dt
	, a.TA_count = t_calls
FROM TA_DTV_MAR16_BASE AS a 
JOIN #TA_DTV_DEC16_TARGET AS b on a.account_number = b.account_number

message convert(TIMESTAMP, now()) || ' | TA DTV model - TA_DTV_MAR16_BASE | TA updated : '||@@rowcount TO client
COMMIT
--- BUILDING THE SAMPLE ~10% based on one digit of the account_number
SELECT  
	  rand2
	, sample_group
	, TA_FLAG
	, TA_dt
	, TA_count
	, b.birth_dt
	, SkyTalk_type			-- Attachment
	, SkyPlus
    , Observation_dt
	, DATEDIFF (year, b.birth_dt, @start_dt) 	AS age
    , a.account_number
INTO TA_DTV_MAR16_SAMPLE
FROM TA_DTV_MAR16_BASE AS a 
JOIN sharmaa.View_attachments_201603 AS b ON a.account_number = b.account_number
WHERE filter_1 = @rand1 

message convert(TIMESTAMP, now()) || ' | TA DTV model - TA_DTV_MAR16_SAMPLE: '||@@rowcount TO client

COMMIT 
CREATE HG INDEX id1 ON TA_DTV_MAR16_SAMPLE (account_number)
COMMIT 

--GO
ALTER TABLE TA_DTV_MAR16_SAMPLE 
ADD( 
	    Time_Since_Last_TA_call  	VARCHAR(40) DEFAULT NULL
	,  	ALL_offer_rem_and_end INT DEFAULT NULL 
	, 	offer_value_raw FLOAT DEFAULT NULL
	, 	offer_value 	TINYINT DEFAULT NULL 
	)
	 
message convert(TIMESTAMP, now()) || ' | TA DTV model - Alter table done  ' TO client
UPDATE TA_DTV_MAR16_SAMPLE
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
			ELSE '' 	END
	
FROM TA_DTV_MAR16_SAMPLE AS a 
JOIN citeam.CUST_FCAST_WEEKLY_BASE AS b ON a.account_number = b.account_number AND b.end_date = '2016-03-31'	----######### UPDATE using the last reading of the month
	
message convert(TIMESTAMP, now()) || ' | TA DTV model - Time_Since_Last_TA_call: '||@@rowcount TO client	
------------------------------------- Offer Value -------------------------------------------------	
SELECT base.account_number
	  , @start_dt AS end_date 
      ,oua.offer_value
	  ,rank() over(PARTITION BY base.account_number ORDER BY offer_start_dt_actual DESC) AS latest_offer
INTO #current_bb_offer_length      
FROM TA_DTV_MAR16_SAMPLE AS base
INNER JOIN citeam.offer_usage_all AS oua ON oua.account_number = base.account_number
WHERE   subs_type IN ('Broadband DSL Line','DTV Primary Viewing','DTV Extra Subscription','SKY TALK LINE RENTAL','SKY TALK SELECT')
	AND end_date >= offer_start_dt_actual
	AND end_date <  offer_end_dt_actual
	AND intended_total_offer_value_yearly IS NOT NULL

COMMIT
DELETE FROM #current_bb_offer_length      WHERE latest_offer <>1
COMMIT

SELECT base.account_number
      ,offer_end_dt_actual
	  , @start_dt AS end_date
	  , oua.offer_value
      ,rank() over(PARTITION BY base.account_number ORDER BY offer_start_dt_actual DESC) AS latest_offer
INTO #prev_bb_offer_dt      
FROM TA_DTV_MAR16_SAMPLE 			AS base
INNER JOIN citeam.offer_usage_all 				AS oua 			ON oua.account_number = base.account_number
WHERE subs_type IN ('Broadband DSL Line','DTV Primary Viewing','DTV Extra Subscription')
		AND end_date >  offer_start_dt_actual
		AND end_date >= offer_end_dt_actual
		AND intended_total_offer_value_yearly IS NOT NULL

COMMIT 
DELETE FROM #prev_bb_offer_dt      WHERE latest_offer <>1
CREATE HG INDEX id1 ON #prev_bb_offer_dt (account_number)
COMMIT

UPDATE TA_DTV_MAR16_SAMPLE
SET a.offer_value_raw =  CASE   WHEN b.offer_value is not null  THEN b.offer_value 
                            WHEN 	c.offer_value is not null    THEN c.offer_value 
							ELSE NULL END
FROM TA_DTV_MAR16_SAMPLE		AS a	 
LEFT JOIN #current_bb_offer_length  	AS b ON a.account_number = b.account_number
LEFT JOIN #prev_bb_offer_dt      		AS c ON a.account_number = c.account_number
	
COMMIT 
MESSAGE 'offer_value Setp 1 updated: '||@@rowcount type status to client


SELECT account_number, NTILE(10) OVER (ORDER BY offer_value_raw) ntilex
INTo #t1 
FROM TA_DTV_MAR16_SAMPLE

COMMIT 
CREATE HG INDEX id1 on #t1(account_number)
UPDATE TA_DTV_MAR16_SAMPLE
SET offer_value = CAST(ntilex AS VARCHAR)
FROM TA_DTV_MAR16_SAMPLE AS a
JOIN #t1 AS b On a.account_number = b.account_number 
message convert(TIMESTAMP, now()) || ' | TA DTV model - offer_value: '||@@rowcount TO client		
DROP TABLE #prev_bb_offer_dt
DROP TABLE #current_bb_offer_length
----------------------------------		ALL_offer_rem_and_end		------------------------------------

SELECT base.account_number
		, @start_dt AS end_date 
      ,MAX(offer_duration) AS offer_length
      ,MAX(DATEDIFF(DD, end_date, intended_offer_end_dt)) AS length_rem
	  , BB_current_offer_duration_rem = CASE WHEN length_rem > 2854 THEN 2854
                                          WHEN length_rem < 0    THEN 0
                                          ELSE length_rem 
										END 
INTO #current_bb_offer_length_2   
FROM TA_DTV_MAR16_SAMPLE AS base
INNER JOIN citeam.offer_usage_all AS oua ON oua.account_number = base.account_number
WHERE   subs_type IN ('Broadband DSL Line','DTV Primary Viewing','DTV Extra Subscription','SKY TALK LINE RENTAL','SKY TALK SELECT')
	AND end_date >= offer_start_dt_actual
	AND end_date <  offer_end_dt_actual
	AND intended_total_offer_value_yearly IS NOT NULL
GROUP BY base.account_number

MESSAGE 'ALL_offer_rem_and_end step 1: '||@@rowcount type status to client

SELECT base.account_number
      ,offer_end_dt_actual
	  , @start_dt AS end_date
      ,rank() over(PARTITION BY base.account_number ORDER BY offer_start_dt_actual DESC) AS latest_offer
	  , BB_time_since_last_offer_end = DATEDIFF(DD, offer_end_dt_actual, end_date)
INTO #prev_bb_offer_dt_2      
FROM TA_DTV_MAR16_SAMPLE 			AS base
INNER JOIN citeam.offer_usage_all 				AS oua 			ON oua.account_number = base.account_number
WHERE subs_type IN ('Broadband DSL Line','DTV Primary Viewing','DTV Extra Subscription')
		AND end_date >  offer_start_dt_actual
		AND end_date >= offer_end_dt_actual
		AND intended_total_offer_value_yearly IS NOT NULL

COMMIT 
DELETE FROM #prev_bb_offer_dt_2      WHERE latest_offer <>1
CREATE HG INDEX id1 ON #prev_bb_offer_dt_2 (account_number)
CREATE DATE INDEX id2 ON #prev_bb_offer_dt_2 (offer_end_dt_actual)
COMMIT
MESSAGE 'ALL_offer_rem_and_end step 2: '||@@rowcount type status to client
UPDATE TA_DTV_MAR16_SAMPLE
SET ALL_offer_rem_and_end =  CASE WHEN BB_current_offer_duration_rem > 0 THEN BB_current_offer_duration_rem 
								WHEN (BB_current_offer_duration_rem = 0 OR BB_current_offer_duration_rem  IS NULL) AND BB_time_since_last_offer_end <> - 9999 THEN (0 - BB_time_since_last_offer_end) 
								ELSE - 9999 END
FROM TA_DTV_MAR16_SAMPLE		AS a	 
LEFT JOIN #current_bb_offer_length_2 	AS b ON a.account_number = b.account_number  
LEFT JOIN #prev_bb_offer_dt_2      		AS c ON a.account_number = c.account_number  
	
COMMIT 
MESSAGE 'ALL_offer_rem_and_end updated: '||@@rowcount type status to client

------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW pitteloudj.TA_DTV_SAMPLE_VIEW_MAR16
AS
SELECT TA_flag
    ,COALESCE (a.age, 51) AS n_age
    ,(n_age - avg(n_age) over()) / stddev(n_age) over() as age_transformed
	, CASE 	WHEN SkyTalk_type IN ('Sky Pay As You Talk') THEN 'Sky Pay As You Talk' 
			WHEN SkyTalk_type LIKE '%Anytime%' THEN 'Sky Talk Anytime' 
			WHEN SkyTalk_type IN ('Sky Talk Evenings and Weekends Extra', 'Sky Talk Evenings and Weekends Extra (Freetime)', 'Sky Talk Evenings and Weekends Extra (Weekends)') THEN 'Sky Talk Evenings and Weekends' 
			WHEN SkyTalk_type IS NULL THEN 'No Sky Talk' 
			ELSE 'Other Sky Talk' END AS SkyTalk_type
	, SkyPlus
    ,(ALL_offer_rem_and_end - avg(ALL_offer_rem_and_end) over()) / stddev(ALL_offer_rem_and_end) over() as ALL_offer_rem_and_end_transformed
	, Offer_Value
	,COALESCE (a.Time_Since_Last_TA_call , 'No Prev TA Calls') AS Time_Since_Last_TA_call_transformed 
	, account_number
FROM pitteloudj.TA_DTV_MAR16_SAMPLE as a 


------------------------------------------------------------------------------------------------------
--------------------------------	Applying the model		  ----------------------------------------
------------------------------------------------------------------------------------------------------
SELECT account_number
	, TA_FLAG
	, T0.C0 AS C0
	, T0.C1 AS W1
	, (CASE WHEN (T0.C0 = 1) THEN T0.C1 ELSE (1.0 - T0.C1) END) AS W2
			, prop = W1*W2
INTO TA_prop_mar16		
FROM (
	SELECT (CASE WHEN ((((((T0.C0 IS NULL) OR (T0.C1 IS NULL)) OR (T0.C2 IS NULL)) OR (T0.C3 IS NULL)) OR (T0.C4 IS NULL)) OR (T0.C5 IS NULL)) THEN NULL WHEN (T0.C6 > T0.C7) THEN 0 ELSE 1 END) AS C0
		, (CASE WHEN (T0.C6 > T0.C7) THEN (CASE WHEN (T0.C9 + T0.C10) = 0 THEN NULL ELSE T0.C9 / (T0.C9 + T0.C10) END) ELSE (CASE WHEN (T0.C9 + T0.C10) = 0 THEN NULL ELSE T0.C10 / (T0.C9 + T0.C10) END) END) AS C1
				, account_number, TA_FLAG
	FROM (
		SELECT T0.C0 AS C0
			, T0.C1 AS C1
			, T0.C2 AS C2
			, T0.C3 AS C3
			, T0.C4 AS C4
			, T0.C5 AS C5
			, T0.C6 AS C6
			, T0.C7 AS C7
			, T0.C8 AS C8
			, (CASE WHEN (T0.C8 > 709) THEN EXP((T0.C6 - T0.C8)) ELSE EXP(T0.C6) END) AS C9
			, (CASE WHEN (T0.C8 > 709) THEN EXP((T0.C7 - T0.C8)) ELSE EXP(T0.C7) END) AS C10
					, account_number, TA_FLAG
		FROM (
			SELECT T0.C0 AS C0
				, T0.C1 AS C1
				, T0.C2 AS C2
				, T0.C3 AS C3
				, T0.C4 AS C4
				, T0.C5 AS C5
				, T0.C6 AS C6
				, T0.C7 AS C7
				, (CASE WHEN (T0.C6 > T0.C7) THEN T0.C6 ELSE T0.C7 END) AS C8
						, account_number, TA_FLAG
			FROM (
				SELECT T0."SkyTalk_type" AS C0
					, T0."SkyPlus" AS C1
					, T0."Offer_Value" AS C2
					, T0."Time_Since_Last_TA_call_transformed" AS C3
					, T0."ALL_offer_rem_and_end_transformed" AS C4
					, T0."age_transformed" AS C5
					, (((((((((((((((((((((((0.40606097224196402 + (CASE WHEN (T0."SkyTalk_type" = 'No Sky Talk') THEN 0.407077644809063 ELSE 0.0 END)) + (CASE WHEN (T0."SkyTalk_type" = 'Other Sky Talk') THEN 0.16778606875767099 ELSE 0.0 END)) + (CASE WHEN (T0."SkyTalk_type" = 'Sky Pay As You Talk') THEN - 0.040141020667143597 ELSE 0.0 END)
						) + (CASE WHEN (T0."SkyTalk_type" = 'Sky Talk Anytime') THEN - 0.013688898648531799 ELSE 0.0 END)
						) + (CASE WHEN (T0."SkyPlus" = 0) THEN 1.0134031518498301 ELSE 0.0 END)
						) + (CASE WHEN (T0."Offer_Value" = 1.00000000000000000) THEN - 0.124908934776941 ELSE 0.0 END)
						) + (CASE WHEN (T0."Offer_Value" = 10.0000000000000000) THEN - 0.61234769276419798 ELSE 0.0 END)
						) + (CASE WHEN (T0."Offer_Value" = 2.00000000000000000) THEN - 0.354607700295965 ELSE 0.0 END)
						) + (CASE WHEN (T0."Offer_Value" = 3.00000000000000000) THEN - 0.30929426666026699 ELSE 0.0 END)
						) + (CASE WHEN (T0."Offer_Value" = 4.00000000000000000) THEN - 0.34270954445954299 ELSE 0.0 END)
						) + (CASE WHEN (T0."Offer_Value" = 5.00000000000000000) THEN - 0.21561204505902601 ELSE 0.0 END)
						) + (CASE WHEN (T0."Offer_Value" = 6.00000000000000000) THEN 0.044147400078484299 ELSE 0.0 END)
						) + (CASE WHEN (T0."Offer_Value" = 7.00000000000000000) THEN - 0.26877852505265998 ELSE 0.0 END)
						) + (CASE WHEN (T0."Offer_Value" = 8.00000000000000000) THEN 0.14824312955431199 ELSE 0.0 END)
						) + (CASE WHEN (T0."Time_Since_Last_TA_call_transformed" = '02-05 Wks since last TA Call') THEN - 0.18002717210470401 ELSE 0.0 END)
						) + (CASE WHEN (T0."Time_Since_Last_TA_call_transformed" = '06-35 Wks since last TA Call') THEN 0.024858238443695901 ELSE 0.0 END)
						) + (CASE WHEN (T0."Time_Since_Last_TA_call_transformed" = '36-46 Wks since last TA Call') THEN - 0.98718728167900704 ELSE 0.0 END)
						) + (CASE WHEN (T0."Time_Since_Last_TA_call_transformed" = '47 Wks since last TA Call') THEN - 1.4179166966730901 ELSE 0.0 END)
						) + (CASE WHEN (T0."Time_Since_Last_TA_call_transformed" = '48-52 Wks since last TA Call') THEN - 1.42480344523169 ELSE 0.0 END)
						) + (CASE WHEN (T0."Time_Since_Last_TA_call_transformed" = '53-60 Wks since last TA Call') THEN - 1.2876121466332799 ELSE 0.0 END)
						) + (CASE WHEN (T0."Time_Since_Last_TA_call_transformed" = '61+ Wks since last TA Call') THEN - 0.56870771805164599 ELSE 0.0 END)
						) + (T0."ALL_offer_rem_and_end_transformed" * 0.41610067388457)
						) + (T0."age_transformed" * 0.20415446039479099)
						) AS C6
						, 0.0 AS C7
								, account_number, TA_FLAG
FROM TA_DTV_SAMPLE_VIEW_MAR16 T0) T0) T0) T0) T0
------------------------------------------------------------------------------------------------------------------------
---------				 			Outputs																	  ----------
------------------------------------------------------------------------------------------------------------------------
SELECT ta_flag, C0,prop2, decile, count(*) hits FROM (
SELECT ta_flag, C0, round(prop , 2) prop2, NTILE(10) OVER( ORDER BY  prop DESC) decile 
FROM TA_prop_mar16) as a 
GROUP BY ta_flag, C0,prop2, decile








