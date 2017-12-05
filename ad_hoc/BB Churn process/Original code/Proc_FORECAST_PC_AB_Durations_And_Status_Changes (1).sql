------------------------------------------------------------------------------------------------------------------------------
-- Procedure to calc what percentage of PCs will churn or reactivate before the end of the week ------------------------------
------------------------------------------------------------------------------------------------------------------------------
SETUSER citeam;

CREATE variable Forecast_Start_Week INT;

SET Forecast_Start_Week = 201601;

DROP PROCEDURE

IF EXISTS CITeam.Intraweek_PCs_Dist;
	CREATE PROCEDURE CITeam.Intraweek_PCs_Dist (IN Forecast_Start_Week INT) RESULT (
		Next_Status_Code VARCHAR(2)
		, PC_ReAC_Offer_Applied TINYINT
		, PCs INT
		, IntaWk_PC_Lower_Pctl FLOAT
		, IntaWk_PC_Upper_Pctl FLOAT
		)

BEGIN
	SELECT *
	INTO #Sky_Calendar
	FROM Citeam.subs_calendar(Forecast_Start_Week / 100 - 1, Forecast_Start_Week / 100);

	DROP TABLE

	IF EXISTS #Acc_PC_Events_Same_Week;
		SELECT subs_week_and_year
			, event_dt
			, event_dt - datepart(weekday, event_dt + 2) PC_Event_End_Dt
			, PC_Effective_To_Dt
			, PC_Effective_To_Dt - datepart(weekday, PC_Effective_To_Dt + 2) PC_Effective_To_End_Dt
			, mor.account_number
			-- ,csh.status_code Next_Status_Code1
			, MoR.PC_Next_Status_Code Next_Status_Code
			, CASE WHEN MoR.PC_Reactivation_Offer_Id IS NOT NULL THEN 1 ELSE 0 END PC_ReAC_Offer_Applied
		INTO #Acc_PC_Events_Same_Week
		FROM CITeam.Master_Of_Retention AS MoR
		WHERE event_dt BETWEEN (
						SELECT max(calendar_date - 6 - 5 * 7)
						FROM #sky_calendar
						WHERE subs_week_and_year = Forecast_Start_Week
						) -- Last 6 Wk PC conversions
				AND (
						SELECT max(calendar_date)
						FROM #sky_calendar
						WHERE subs_week_and_year = Forecast_Start_Week
						) AND (Same_Day_Cancels > 0 OR PC_Pending_Cancellations > 0 OR Same_Day_PC_Reactivations > 0);

	SELECT Coalesce(CASE WHEN PC_Effective_To_End_Dt = PC_Event_End_Dt THEN MoR.Next_Status_Code ELSE NULL END, 'PC') Next_Status_Code
		, Cast(CASE Next_Status_Code WHEN 'AC' THEN 1 WHEN 'PO' THEN 2 WHEN 'AB' THEN 3 ELSE 0 END AS INT) Next_Status_Code_Rnk
		, Cast(CASE WHEN PC_Effective_To_End_Dt = PC_Event_End_Dt THEN MoR.PC_ReAC_Offer_Applied ELSE 0 END AS INT) PC_ReAC_Offer_Applied
		, Row_number() OVER (
			ORDER BY Next_Status_Code_Rnk
				, PC_ReAC_Offer_Applied
			) Row_ID
		,
		--        sum(Case when PC_Effective_To_End_Dt = PC_Event_End_Dt and Next_Status_Code = 'PO' then 1 else 0 end) as Intraweek_Churn,
		--        sum(Case when PC_Effective_To_End_Dt = PC_Event_End_Dt and Next_Status_Code = 'AC' then 1 else 0 end) as Intraweek_PC_Reactivation,
		count(*) AS PCs --,
		--        sum(PCs) over() Total_PCs,
		--        sum(PCs) over(order by Row_ID) Cum_PCs,
		--        Cast(Cum_PCs as float)/Total_PCs as IntaWk_PC_Upper_Pctl
		--        Cast(Intraweek_Churn as float)/PCs as Pct_Intraweek_Churn,
		--        Cast(Intraweek_PC_Reactivation as float)/PCs as Pct_Intraweek_Reactivation
	INTO #PC_Events_Same_Week
	FROM #Acc_PC_Events_Same_Week MoR
	GROUP BY Next_Status_Code
		, PC_ReAC_Offer_Applied;

	SELECT Row_ID
		, Next_Status_Code
		, PC_ReAC_Offer_Applied
		, PCs
		, sum(PCs) OVER (
			ORDER BY Row_ID
			) Cum_PCs
		, sum(PCs) OVER () Total_PCs
		, Cast(Cum_PCs AS FLOAT) / Total_PCs AS IntaWk_PC_Upper_Pctl
	INTO #PC_Events
	FROM #PC_Events_Same_Week pc1
	GROUP BY Row_ID
		, Next_Status_Code
		, PC_ReAC_Offer_Applied
		, PCs;

	SELECT pc1.Next_Status_Code
		, pc1.PC_ReAC_Offer_Applied
		, pc1.PCs
		, Coalesce(pc2.IntaWk_PC_Upper_Pctl, 0) IntaWk_PC_Lower_Pctl
		, pc1.IntaWk_PC_Upper_Pctl
	FROM #PC_Events pc1
	LEFT JOIN #PC_Events pc2 ON pc2.row_id = pc1.row_id - 1;
END;

GRANT EXECUTE ON CITeam.Intraweek_PCs_Dist TO CITeam;

SETUSER;

SELECT *
FROM CITeam.Intraweek_PCs_Dist(201601);

------------------------------------------------------------------------------------------------------------------------------
-- Procedure to calc duration between PC and intended churn date ----------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------
SETUSER citeam;

CREATE variable Forecast_Start_Week INT;

SET Forecast_Start_Week = 201601;

DROP PROCEDURE

IF EXISTS CITeam.PC_Duration_Dist;
	CREATE PROCEDURE CITeam.PC_Duration_Dist (IN Forecast_Start_Week INT) RESULT (
		Days_To_churn INT
		, PCs INT
		, Total_PCs INT
		, PC_Days_Lower_Prcntl FLOAT
		, PC_Days_Upper_Prcntl FLOAT
		)

BEGIN
	SELECT *
	INTO #Sky_Calendar
	FROM Citeam.subs_calendar(Forecast_Start_Week / 100 - 1, Forecast_Start_Week / 100);

	SELECT event_dt - datepart(weekday, event_dt + 2) PC_Event_End_Dt
		, PC_Effective_To_Dt - datepart(weekday, PC_Effective_To_Dt + 2) PC_Effective_To_End_Dt
		, PC_Future_Sub_Effective_Dt - datepart(weekday, PC_Future_Sub_Effective_Dt + 2) AS PC_Future_Sub_End_Dt
		, PC_Future_Sub_Effective_Dt - PC_Event_End_Dt AS Days_To_churn
	-- ,Count(*) PC_Pipeline_Cancellations
	INTO #PC_Events_Days_To_Intended_Churn
	FROM citeam.Master_of_retention -- from MoR
	WHERE event_dt BETWEEN (
					SELECT max(calendar_date - 6 * 7 + 1)
					FROM #sky_calendar
					WHERE subs_week_and_year = Forecast_Start_Week
					) -- Last 6 Wk PC conversions
			AND (
					SELECT max(calendar_date)
					FROM #sky_calendar
					WHERE subs_week_and_year = Forecast_Start_Week
					) AND (Same_Day_Cancels > 0 OR PC_Pending_Cancellations > 0 OR Same_Day_PC_Reactivations > 0) AND PC_Event_End_Dt != PC_Effective_To_End_Dt AND PC_Event_End_Dt != PC_Future_Sub_End_Dt AND PC_Future_Sub_Effective_Dt > event_dt
		-- group by PC_Event_End_Dt,PC_Effective_To_End_Dt,Days_To_churn
		-- order by Days_To_churn
		;

	SELECT Days_To_churn
		, Row_number() OVER (
			ORDER BY Days_To_churn
			) Row_ID
		, count(*) AS PCs
		, sum(PCs) OVER () Total_PCs
		, sum(PCs) OVER (
			ORDER BY Days_To_churn
			) Cum_PCs
		, Cast(PCs AS FLOAT) / Total_PCs AS Pct_PCs
		, Cast(NULL AS FLOAT) AS PC_Days_Lower_Prcntl
		, Cast(Cum_PCs AS FLOAT) / Total_PCs AS PC_Days_Upper_Prcntl
	INTO #PC_Days_Prcntl
	FROM #PC_Events_Days_To_Intended_Churn
	GROUP BY Days_To_churn
	ORDER BY Days_To_churn;

	UPDATE #PC_Days_Prcntl pc1
	SET pc1.PC_Days_Lower_Prcntl = Coalesce(pc2.PC_Days_Upper_Prcntl, 0)
	FROM #PC_Days_Prcntl pc1
	LEFT JOIN #PC_Days_Prcntl pc2 ON pc2.Row_ID = pc1.Row_ID - 1;

	SELECT Days_To_churn
		, PCs
		, Total_PCs
		, PC_Days_Lower_Prcntl
		, PC_Days_Upper_Prcntl
	FROM #PC_Days_Prcntl;
END;

GRANT EXECUTE
	ON CITeam.PC_Duration_Dist
	TO CITeam;

SETUSER;

SELECT *
FROM CITeam.PC_Duration_Dist(201601);

------------------------------------------------------------------------------------------------------------------------------
-- Procedure to calc rates for customers moving from PC to another status ----------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------
SETUSER citeam;

CREATE variable Forecast_Start_Week INT;

SET Forecast_Start_Week = 201601;

DROP PROCEDURE

IF EXISTS CITeam.PC_Status_Movement_Probabilities;
	CREATE PROCEDURE CITeam.PC_Status_Movement_Probabilities (IN Forecast_Start_Week INT) RESULT (
		Wks_To_Intended_Churn VARCHAR(20)
		, Status_Code_EoW VARCHAR(2)
		, Status_Code_EoW_Rnk INT
		, PC_ReAC_Offer_Applied TINYINT
		, PCs INT
		, Cum_Total_Cohort_PCs INT
		, Total_Cohort_PCs INT
		, PC_Percentile_Lower_Bound FLOAT
		, PC_Percentile_Upper_Bound FLOAT
		)

BEGIN
	SELECT *
	INTO #Sky_Calendar
	FROM CITeam.Subs_Calendar(Forecast_Start_Week / 100 - 1, Forecast_Start_Week / 100);

	DROP TABLE

	IF EXISTS #PC_Intended_Churn;
		SELECT account_number
			, event_dt
			,
			--        Cast(event_dt - datepart(weekday,event_dt+2) + 7 as date) event_dt_End_Dt,
			PC_Future_Sub_Effective_Dt
			, Cast(PC_Future_Sub_Effective_Dt - datepart(weekday, PC_Future_Sub_Effective_Dt + 2) + 7 AS DATE) PC_Future_Sub_Effective_Dt_End_Dt
			, PC_Effective_To_Dt
			,
			--        Cast(PC_Effective_To_Dt - datepart(weekday,PC_Effective_To_Dt+2)+7 as date) PC_Effective_To_Dt_End_Dt,
			PC_Next_status_code Next_status_code
			, CASE WHEN PC_Reactivation_Offer_Id IS NOT NULL THEN 1 ELSE 0 END PC_ReAC_Offer_Applied
		INTO #PC_Intended_Churn
		FROM CITeam.Master_of_Retention
		WHERE PC_Future_Sub_Effective_Dt BETWEEN (
						SELECT min(calendar_date - 6 * 7)
						FROM #sky_calendar
						WHERE subs_week_and_year = Forecast_Start_Week
						) -- Last 6 Wk PC conversions
				AND (
						SELECT min(calendar_date - 1)
						FROM #sky_calendar
						WHERE subs_week_and_year = Forecast_Start_Week
						) AND (PC_Pending_Cancellations > 0) -- the next 7 days
			AND PC_Future_Sub_Effective_Dt IS NOT NULL AND Next_status_code IS NOT NULL AND PC_Effective_To_Dt <= PC_Future_Sub_Effective_Dt;

	SELECT PCs.*
		, CASE WHEN (cast(PC_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 0 THEN 'Churn in next 1 wks' WHEN (cast(PC_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 1 THEN 'Churn in next 2 wks' WHEN (cast(PC_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 2 THEN 'Churn in next 3 wks' WHEN (cast(PC_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 3 THEN 'Churn in next 4 wks' WHEN (cast(PC_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 4 THEN 'Churn in next 5 wks' WHEN (cast(PC_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 >= 5 THEN 'Churn in next 6+ wks'
					--           when (cast(PC_Future_Sub_Effective_Dt as integer) - cast(End_Date as integer))/7>5 then '6+_Wks_To_Churn'
			END AS Wks_To_Intended_Churn
		, sc.Calendar_date End_date
		, CASE WHEN sc.calendar_date + 7 BETWEEN event_dt AND PC_Effective_To_Dt THEN 'PC' WHEN sc.calendar_date + 7 BETWEEN PC_Effective_To_Dt AND PC_Future_Sub_Effective_Dt_End_Dt THEN Next_Status_Code END Status_Code_EoW
		, CASE WHEN sc.calendar_date + 7 BETWEEN PC_Effective_To_Dt AND PC_Future_Sub_Effective_Dt_End_Dt AND Status_Code_EoW = 'AC' THEN PCs.PC_ReAC_Offer_Applied ELSE 0 END PC_ReAC_Offer_Applied_EoW
		, (CASE Status_Code_EoW WHEN 'AC' THEN 1 WHEN 'AB' THEN 2 WHEN 'PO' THEN 3 WHEN 'PC' THEN 4 END) - PC_ReAC_Offer_Applied_EoW AS Status_Code_EoW_Rnk
	INTO #PC_PL_Status
	FROM #PC_Intended_Churn PCs
	INNER JOIN #sky_calendar sc ON sc.calendar_date BETWEEN PCs.event_dt AND PCs.PC_Effective_To_Dt - 1 AND sc.subs_last_day_of_week = 'Y';

	-- Select top 100 * from #PC_PL_Status where Wks_To_Intended_Churn = '0_Wks_To_Churn' and Status_Code_EoW is null
	SELECT Wks_To_Intended_Churn
		, Status_Code_EoW
		, Status_Code_EoW_Rnk
		, PC_ReAC_Offer_Applied_EoW
		, count(*) PCs
		, Sum(PCs) OVER (
			PARTITION BY Wks_To_Intended_Churn ORDER BY Status_Code_EoW_Rnk
			) Cum_Total_Cohort_PCs
		, Sum(PCs) OVER (PARTITION BY Wks_To_Intended_Churn) Total_Cohort_PCs
		, Cast(NULL AS FLOAT) AS PC_Percentile_Lower_Bound
		, Cast(Cum_Total_Cohort_PCs AS FLOAT) / Total_Cohort_PCs AS PC_Percentile_Upper_Bound
	INTO #PC_Percentiles
	FROM #PC_PL_Status
	GROUP BY Wks_To_Intended_Churn
		, Status_Code_EoW_Rnk
		, Status_Code_EoW
		, PC_ReAC_Offer_Applied_EoW
	ORDER BY Wks_To_Intended_Churn
		, Status_Code_EoW_Rnk
		, Status_Code_EoW
		, PC_ReAC_Offer_Applied_EoW;

	UPDATE #PC_Percentiles pcp
	SET PC_Percentile_Lower_Bound = Cast(Coalesce(pcp2.PC_Percentile_Upper_Bound, 0) AS FLOAT)
	FROM #PC_Percentiles pcp
	LEFT JOIN #PC_Percentiles pcp2 ON pcp2.Wks_To_Intended_Churn = pcp.Wks_To_Intended_Churn AND pcp2.Status_Code_EoW_Rnk = pcp.Status_Code_EoW_Rnk - 1;

	SELECT *
	FROM #PC_Percentiles;
END;

GRANT EXECUTE
	ON CITeam.PC_Status_Movement_Probabilities
	TO CITeam;

SETUSER;

SELECT *
FROM CITeam.PC_Status_Movement_Probabilities(201601);

------------------------------------------------------------------------------------------------------------------------------
-- Procedure to calc what percentage of ABs will churn or reactivate before the end of the week ------------------------------
------------------------------------------------------------------------------------------------------------------------------
SETUSER citeam;

CREATE variable Forecast_Start_Week INT;

SET Forecast_Start_Week = 201601;

DROP PROCEDURE

IF EXISTS CITeam.Intraweek_ABs_Dist;
	CREATE PROCEDURE CITeam.Intraweek_ABs_Dist (IN Forecast_Start_Week INT) RESULT (
		Next_Status_Code VARCHAR(2)
		, AB_ReAC_Offer_Applied TINYINT
		, ABs INT
		, IntaWk_AB_Lower_Pctl FLOAT
		, IntaWk_AB_Upper_Pctl FLOAT
		)

BEGIN
	SELECT *
	INTO #Sky_Calendar
	FROM Citeam.subs_calendar(Forecast_Start_Week / 100 - 1, Forecast_Start_Week / 100);

	DROP TABLE

	IF EXISTS #Acc_AB_Events_Same_Week;
		SELECT subs_week_and_year
			, event_dt
			, event_dt - datepart(weekday, event_dt + 2) AB_Event_End_Dt
			, AB_Effective_To_Dt
			, AB_Effective_To_Dt - datepart(weekday, AB_Effective_To_Dt + 2) AB_Effective_To_End_Dt
			, mor.account_number
			-- ,csh.status_code Next_Status_Code1
			, MoR.AB_Next_Status_Code Next_Status_Code
			, CASE WHEN MoR.AB_Reactivation_Offer_Id IS NOT NULL THEN 1 ELSE 0 END AB_ReAC_Offer_Applied
		INTO #Acc_AB_Events_Same_Week
		FROM CITeam.Master_Of_Retention AS MoR
		WHERE event_dt BETWEEN (
						SELECT max(calendar_date - 6 - 5 * 7)
						FROM #sky_calendar
						WHERE subs_week_and_year = Forecast_Start_Week
						) -- Last 6 Wk PC conversions
				AND (
						SELECT max(calendar_date)
						FROM #sky_calendar
						WHERE subs_week_and_year = Forecast_Start_Week
						) AND AB_Pending_Terminations > 0;

	SELECT Coalesce(CASE WHEN AB_Effective_To_End_Dt = AB_Event_End_Dt THEN MoR.Next_Status_Code ELSE NULL END, 'AB') Next_Status_Code
		, Cast(CASE Next_Status_Code WHEN 'AC' THEN 1 WHEN 'SC' THEN 2 WHEN 'PC' THEN 3 WHEN 'PO' THEN 4 ELSE 0 END AS INT) Next_Status_Code_Rnk
		, Cast(CASE WHEN AB_Effective_To_End_Dt = AB_Event_End_Dt THEN MoR.AB_ReAC_Offer_Applied ELSE 0 END AS INT) AB_ReAC_Offer_Applied
		, Row_number() OVER (
			ORDER BY Next_Status_Code_Rnk
				, AB_ReAC_Offer_Applied
			) Row_ID
		,
		--        sum(Case when PC_Effective_To_End_Dt = PC_Event_End_Dt and Next_Status_Code = 'PO' then 1 else 0 end) as Intraweek_Churn,
		--        sum(Case when PC_Effective_To_End_Dt = PC_Event_End_Dt and Next_Status_Code = 'AC' then 1 else 0 end) as Intraweek_PC_Reactivation,
		count(*) AS ABs --,
		--        sum(PCs) over() Total_PCs,
		--        sum(PCs) over(order by Row_ID) Cum_PCs,
		--        Cast(Cum_PCs as float)/Total_PCs as IntaWk_PC_Upper_Pctl
		--        Cast(Intraweek_Churn as float)/PCs as Pct_Intraweek_Churn,
		--        Cast(Intraweek_PC_Reactivation as float)/PCs as Pct_Intraweek_Reactivation
	INTO #AB_Events_Same_Week
	FROM #Acc_AB_Events_Same_Week MoR
	GROUP BY Next_Status_Code
		, AB_ReAC_Offer_Applied;

	SELECT Row_ID
		, Next_Status_Code
		, AB_ReAC_Offer_Applied
		, ABs
		, sum(ABs) OVER (
			ORDER BY Row_ID
			) Cum_ABs
		, sum(ABs) OVER () Total_ABs
		, Cast(Cum_ABs AS FLOAT) / Total_ABs AS IntaWk_PC_Upper_Pctl
	INTO #AB_Events
	FROM #AB_Events_Same_Week pc1
	GROUP BY Row_ID
		, Next_Status_Code
		, AB_ReAC_Offer_Applied
		, ABs;

	SELECT pc1.Next_Status_Code
		, pc1.AB_ReAC_Offer_Applied
		, pc1.ABs
		, Coalesce(pc2.IntaWk_PC_Upper_Pctl, 0) IntaWk_PC_Lower_Pctl
		, pc1.IntaWk_PC_Upper_Pctl
	FROM #AB_Events pc1
	LEFT JOIN #AB_Events pc2 ON pc2.row_id = pc1.row_id - 1;
END;

GRANT EXECUTE
	ON CITeam.Intraweek_ABs_Dist
	TO CITeam;

SETUSER;

SELECT *
FROM CITeam.Intraweek_PCs_Dist(201601);

------------------------------------------------------------------------------------------------------------------------------
-- Procedure to calc rates for customers moving from AB to another status ----------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------
SETUSER citeam;

CREATE variable Forecast_Start_Week INT;

SET Forecast_Start_Week = 201601;

DROP PROCEDURE

IF EXISTS CITeam.AB_Status_Movement_Probabilities;
	CREATE PROCEDURE CITeam.AB_Status_Movement_Probabilities (IN Forecast_Start_Week INT) RESULT (
		Wks_To_Intended_Churn VARCHAR(20)
		, Status_Code_EoW VARCHAR(2)
		,
		--   Status_Code_EoW_Rnk integer,
		AB_ReAC_Offer_Applied TINYINT
		, ABs INT
		, Cum_Total_Cohort_ABs INT
		, Total_Cohort_ABs INT
		, AB_Percentile_Lower_Bound FLOAT
		, AB_Percentile_Upper_Bound FLOAT
		)

BEGIN
	SELECT *
	INTO #Sky_Calendar
	FROM CITeam.Subs_Calendar(Forecast_Start_Week / 100 - 1, Forecast_Start_Week / 100);

	DROP TABLE

	IF EXISTS #AB_Intended_Churn;
		SELECT account_number
			, event_dt
			, AB_Future_Sub_Effective_Dt
			, Cast(AB_Future_Sub_Effective_Dt - datepart(weekday, AB_Future_Sub_Effective_Dt + 2) + 7 AS DATE) AB_Future_Sub_Effective_Dt_End_Dt
			, AB_Effective_To_Dt
			, AB_Next_status_code Next_status_code
			, CASE WHEN AB_Reactivation_Offer_Id IS NOT NULL THEN 1 ELSE 0 END AS AB_ReAC_Offer_Applied
		INTO #AB_Intended_Churn
		FROM CITeam.Master_of_Retention
		WHERE AB_Future_Sub_Effective_Dt BETWEEN (
						SELECT min(calendar_date - 6 * 7)
						FROM #sky_calendar
						WHERE subs_week_and_year = Forecast_Start_Week
						) -- Last 6 Wk PC conversions
				AND (
						SELECT min(calendar_date - 1)
						FROM #sky_calendar
						WHERE subs_week_and_year = Forecast_Start_Week
						) AND (AB_Pending_Terminations > 0) -- the next 7 days
			AND AB_Future_Sub_Effective_Dt IS NOT NULL AND AB_Next_status_code IS NOT NULL AND AB_Effective_To_Dt <= AB_Future_Sub_Effective_Dt;

	SELECT ABs.*
		, CASE WHEN (cast(AB_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 0 THEN 'Churn in next 1 wks' WHEN (cast(AB_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 1 THEN 'Churn in next 2 wks' WHEN (cast(AB_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 2 THEN 'Churn in next 3 wks' WHEN (cast(AB_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 3 THEN 'Churn in next 4 wks' WHEN (cast(AB_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 4 THEN 'Churn in next 5 wks' WHEN (cast(AB_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 5 THEN 'Churn in next 6 wks' WHEN (cast(AB_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 6 THEN 'Churn in next 7 wks' WHEN (cast(AB_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 7 THEN 'Churn in next 8 wks' WHEN (cast(AB_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 8 THEN 'Churn in next 9 wks' WHEN (cast(AB_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)
					) / 7 >= 9 THEN 'Churn in next 10+ wks' END AS Wks_To_Intended_Churn
		, sc.Calendar_date End_date
		, CASE WHEN sc.calendar_date + 7 BETWEEN event_dt AND AB_Effective_To_Dt THEN 'AB' WHEN sc.calendar_date + 7 BETWEEN AB_Effective_To_Dt AND AB_Future_Sub_Effective_Dt_End_Dt THEN Next_Status_Code END Status_Code_EoW
		, CASE WHEN sc.calendar_date + 7 = AB_Effective_To_Dt - datepart(weekday, AB_Effective_To_Dt + 2) + 7 AND Status_Code_EoW = 'AC' THEN ABs.AB_ReAC_Offer_Applied ELSE 0 END AB_ReAC_Offer_Applied_EoW
		, (CASE Status_Code_EoW WHEN 'AC' THEN 1 WHEN 'AB' THEN 2 WHEN 'SC' THEN 3 WHEN 'PC' THEN 4 WHEN 'PO' THEN 5 END) - AB_ReAC_Offer_Applied_EoW AS Status_Code_EoW_Rnk
	INTO #AB_PL_Status
	FROM #AB_Intended_Churn ABs
	INNER JOIN #sky_calendar sc ON sc.calendar_date BETWEEN ABs.event_dt AND ABs.AB_Effective_To_Dt - 1 AND sc.subs_last_day_of_week = 'Y';

	-- Select top 100 * from #PC_PL_Status where Wks_To_Intended_Churn = '0_Wks_To_Churn' and Status_Code_EoW is null
	SELECT Wks_To_Intended_Churn
		, Status_Code_EoW
		, Status_Code_EoW_Rnk
		, AB_ReAC_Offer_Applied_EoW
		, count(*) ABs
		, Sum(ABs) OVER (
			PARTITION BY Wks_To_Intended_Churn ORDER BY Status_Code_EoW_Rnk
			) Cum_Total_Cohort_ABs
		, Sum(ABs) OVER (PARTITION BY Wks_To_Intended_Churn) Total_Cohort_ABs
		, Cast(NULL AS FLOAT) AS AB_Percentile_Lower_Bound
		, Cast(Cum_Total_Cohort_ABs AS FLOAT) / Total_Cohort_ABs AS AB_Percentile_Upper_Bound
		, Row_Number() OVER (
			PARTITION BY Wks_To_Intended_Churn ORDER BY Status_Code_EoW_Rnk
			) Row_ID
	INTO #AB_Percentiles
	FROM #AB_PL_Status
	GROUP BY Wks_To_Intended_Churn
		, Status_Code_EoW_Rnk
		, Status_Code_EoW
		, AB_ReAC_Offer_Applied_EoW
	ORDER BY Wks_To_Intended_Churn
		, Status_Code_EoW_Rnk
		, Status_Code_EoW
		, AB_ReAC_Offer_Applied_EoW;

	UPDATE #AB_Percentiles pcp
	SET AB_Percentile_Lower_Bound = Cast(Coalesce(pcp2.AB_Percentile_Upper_Bound, 0) AS FLOAT)
	FROM #AB_Percentiles pcp
	LEFT JOIN #AB_Percentiles pcp2 ON pcp2.Wks_To_Intended_Churn = pcp.Wks_To_Intended_Churn AND pcp2.Row_ID = pcp.Row_ID - 1;

	SELECT Wks_To_Intended_Churn
		, Status_Code_EoW
		, AB_ReAC_Offer_Applied_EoW
		, ABs
		, Cum_Total_Cohort_ABs
		, Total_Cohort_ABs
		, AB_Percentile_Lower_Bound
		, AB_Percentile_Upper_Bound
	FROM #AB_Percentiles;
END;

GRANT EXECUTE
	ON CITeam.AB_Status_Movement_Probabilities
	TO CITeam;

SETUSER;

SELECT *
FROM CITeam.AB_Status_Movement_Probabilities(201601);
