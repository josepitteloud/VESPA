---------------------------------------------------------------------------------------------------------------------------------
--------METRIC 14 Collect the number of Vespa Daily Panel Households Dialling Back (use Scaling definition to define a Household)
--------METRIC 15 Collect the Average of Vespa Daily Panel Households Dialling Back (use Scaling definition to define a Household)
--------METRIC 18 Collect the number of Vespa Aternate Panel Households Dialling Back (use Scaling definition to define a Household)
--------METRIC 19 Collect the Average of Vespa Aternate Panel Households Dialling Back (use Scaling definition to define a Household)
---------------------------------------------------------------------------------------------------------------------------------
DECLARE @report_date datetime , @TOTAL_P12 int,  @TOTAL_P6 int,  @TOTAL_P7 int
SET @report_date = DATE(DATEADD (dd, -2, getdate()))


IF object_id('VIQ_METRICS_14_15_18_19_FULL') IS NOT NULL
DROP TABLE VIQ_METRICS_14_15_18_19_FULL;
COMMIT;

----------------------------------------------------------------
--DUMP TABLE FOR METRIC CALCULATIONS 
---------------------------------------------------------------- 		

CREATE TABLE VIQ_METRICS_14_15_18_19_FULL
	(Account_number varchar(20),
		Log_Date datetime,
    Panel varchar(2));

COMMIT;
----------------------------------------------------------------
--INSERTING BOTH VIEWING EVENTS AND NON VIEWING EVENTS FOR DAILY PANEL 
---------------------------------------------------------------- 		
INSERT INTO VIQ_METRICS_14_15_18_19_FULL 
	(Account_number, Log_Date, Panel)
	
SELECT Account_number 												--HOUSEHOLD ID
		,CASE
			WHEN CONVERT(INTEGER,dateformat(MIN(LOG_RECEIVED_START_DATE_TIME_UTC),'hh')) <23
				THEN CAST(MIN(LOG_RECEIVED_START_DATE_TIME_UTC) AS DATE)-1 
				ELSE
					CAST(min(LOG_RECEIVED_START_DATE_TIME_UTC) AS DATE)
				END AS 				Log_Date 	-- BASED ON OPS REPORTS DEFINITION "all logs received from 23:00 on day A until 22:59 on next day (A+1) will belong to A"
     , 'DP' --Daily Panel
FROM sk_prod.VESPA_DP_PROG_VIEWED_CURRENT
WHERE  	panel_id IN (12) 											-- ONLY PANEL 12 REFERENCE
	AND 	LOG_RECEIVED_START_DATE_TIME_UTC >= @report_date
  AND 	LOG_RECEIVED_START_DATE_TIME_UTC IS NOT NULL
	AND 	LOG_START_DATE_TIME_UTC IS NOT NULL
	AND		subscriber_id IS NOT NULL
	AND		Account_number IS NOT NULL
GROUP BY Account_number
HAVING Log_Date IS NOT NULL

UNION ALL
----------------------------------------------------------------
--INSERTING BOTH VIEWING EVENTS AND NON VIEWING EVENTS FOR ALTERNATE PANEL 
---------------------------------------------------------------- 		
SELECT Account_number 												--HOUSEHOLD ID
		,CASE
			WHEN CONVERT(INTEGER,dateformat(MIN(LOG_RECEIVED_START_DATE_TIME_UTC),'hh')) <23
				THEN CAST(MIN(LOG_RECEIVED_START_DATE_TIME_UTC) AS DATE)-1 
				ELSE
					CAST(min(LOG_RECEIVED_START_DATE_TIME_UTC) AS DATE)
				END AS 				Log_Date 	-- BASED ON OPS REPORTS DEFINITION "all logs received from 23:00 on day A until 22:59 on next day (A+1) will belong to A"
     , 'AP' 														--ALTERNATE Panel
FROM sk_prod.VESPA_AP_PROG_VIEWED_CURRENT
WHERE  	panel_id IN (6,7) 
	AND 	LOG_RECEIVED_START_DATE_TIME_UTC >= @report_date
  AND 	LOG_RECEIVED_START_DATE_TIME_UTC IS NOT NULL
	AND 	LOG_START_DATE_TIME_UTC IS NOT NULL
	AND		subscriber_id IS NOT NULL
	AND		Account_number IS NOT NULL
GROUP BY Account_number
HAVING Log_Date IS NOT NULL;

COMMIT;

----------------------------------------------------------------
--CALCULATING TOTAL BOXES IN VESPA PANEL
---------------------------------------------------------------- 		
SELECT
					@TOTAL_P12= COUNT( DISTINCT sb.account_number) 		
				FROM vespa_analysts.vespa_single_box_view AS sb
				WHERE 
				 sb.status_vespa = 'Enabled'
        AND sb.in_vespa_panel = 1
				AND sb.panel = 'VESPA'
				AND sb.selection_date is not null;

				-- ALTERNATE PANEL ACCOUNTS
				
SELECT
					@TOTAL_P6 = COUNT( DISTINCT sb.account_number) 	 
				FROM vespa_analysts.vespa_single_box_view AS sb
				WHERE 
					sb.status_vespa = 'Enabled'
          AND sb.alternate_panel_6 = 1;
SELECT
      @TOTAL_P7 = COUNT( DISTINCT sb.account_number) 	 
				FROM vespa_analysts.vespa_single_box_view AS sb
				WHERE 
					sb.status_vespa = 'Enabled'
          AND sb.alternate_panel_7 = 1;
COMMIT;


----------------------------------------------------------------
--INSERTING RESULTS INTO METRIC TABLE 
---------------------------------------------------------------- 		
IF object_id('pitteloudj.Total_VIQ_METRICS') IS NOT NULL
CREATE TABLE Total_VIQ_METRICS
	(Metric_id int, 
	Metric_Desc varchar(100),
	Metric_Result float, 
	Load_timestamp datetime,
	Viewing_Date datetime);

COmmit;

INSERT INTO Total_VIQ_METRICS (Metric_id, Metric_Desc, Metric_Result, Load_timestamp, Viewing_Date)
SELECT 14, 'TOTAL DP HH Dialing Back', COUNT(DISTINCT Account_number), getdate(), Log_Date
FROM VIQ_METRICS_14_15_18_19_FULL
WHERE Panel ='DP'
GROUP BY Log_Date;

INSERT INTO Total_VIQ_METRICS (Metric_id, Metric_Desc, Metric_Result, Load_timestamp, Viewing_Date)
SELECT 15, 'AVG DP HH Dialing Back', CONVERT (decimal(8,4), COUNT(DISTINCT Account_number) / @TOTAL_P12), getdate(), Log_Date
FROM VIQ_METRICS_14_15_18_19_FULL
WHERE Panel ='DP'
GROUP BY Log_Date;

INSERT INTO Total_VIQ_METRICS (Metric_id, Metric_Desc, Metric_Result, Load_timestamp, Viewing_Date)
SELECT 151, 'TOTAL Panel 12', @TOTAL_P12 , getdate(), getdate();									-- TOTAL DAILY PANEL REGISTERED

INSERT INTO Total_VIQ_METRICS (Metric_id, Metric_Desc, Metric_Result, Load_timestamp, Viewing_Date)
SELECT 18, 'TOTAL AP HH Dialing Back', COUNT(DISTINCT Account_number), getdate(), Log_Date
FROM VIQ_METRICS_14_15_18_19_FULL
WHERE Panel ='AP'
GROUP BY Log_Date;

INSERT INTO Total_VIQ_METRICS (Metric_id, Metric_Desc, Metric_Result, Load_timestamp, Viewing_Date)
SELECT 19, 'AVG AP HH Dialing Back', CONVERT (decimal(8,4), COUNT(DISTINCT Account_number)/ @TOTAL_P6+@TOTAL_P7), getdate(), Log_Date
FROM VIQ_METRICS_14_15_18_19_FULL
WHERE Panel ='AP'
GROUP BY Log_Date;

INSERT INTO Total_VIQ_METRICS (Metric_id, Metric_Desc, Metric_Result, Load_timestamp, Viewing_Date)
SELECT 191, 'TOTAL Panel 6&7', @TOTAL_P6 + @TOTAL_P7 , getdate(), getdate();						-- TOTAL ALTERNATE PANEL REGISTERED

COMMIT


