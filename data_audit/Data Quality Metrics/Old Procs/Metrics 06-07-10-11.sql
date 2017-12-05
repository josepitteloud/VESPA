DECLARE @report_date datetime 
SET @report_date = DATEADD (dd, -2, getdate())

Commit;
------------------------------------------------------------------------------------------------------------------
--------METRIC 06 Average Impacts per Household per Impact Day(Post-Capping/Scaling/Minute Attribution) 
--------METRIC 07 Total Impacts per Impact Day
------------------------------------------------------------------------------------------------------------------

IF OBJECT_ID('pitteloudj.Temp1') IS NOT NULL
	DROP TABLE pitteloudj.Temp1;
IF OBJECT_ID('pitteloudj.Temp2') IS NOT NULL
	DROP TABLE pitteloudj.Temp2;
IF OBJECT_ID('pitteloudj.Temp3') IS NOT NULL
	DROP TABLE pitteloudj.Temp3;
COMMIT; 

SELECT 
		CONVERT(datetime,SUBSTRING (CAST(sd.Viewed_start_date_key AS VARCHAR),7,2) + '/' +  
			SUBSTRING (CAST(sd.Viewed_start_date_key AS VARCHAR),5,2) + '/' + 
				LEFT(CAST(sd.Viewed_start_date_key AS VARCHAR),4)+' 00:00:00',103)								Impact_Day, 
		COUNT(sd.impacts) 																	TOTAL_Impacts, 	-- METRIC 06
		COUNT(DISTINCT vh.account_number)													TOTAL_HH, 		-- PART OF METRIC 07
		sd.Record_Date
INTO Temp1
FROM Sk_prod.slot_data AS sd
	INNER JOIN sk_prod.viq_household AS vh ON vh.Household_key = sd.Household_key
WHERE  sd.Record_Date >= @report_date
GROUP BY sd.Record_Date, 
			CONVERT(datetime,SUBSTRING (CAST(sd.Viewed_start_date_key AS VARCHAR),7,2) + '/' +  
			SUBSTRING (CAST(sd.Viewed_start_date_key AS VARCHAR),5,2) + '/' + 
				LEFT(CAST(sd.Viewed_start_date_key AS VARCHAR),4)+' 00:00:00',103);

COMMIT;
------------------------------------------------------------------------------------------------------------------
--------METRIC 10 Avg of Daily Panel Viewing events per Household for each viewing day 
--------METRIC 11 Avg of Daily Panel Viewing instances per Household for each viewing day
------------------------------------------------------------------------------------------------------------------

SELECT 	DATE(event_start_date_time_utc)   						Event_Date,
		COUNT(dk_event_start_datehour_dim)											TOTAL_Viewing_Events,
--		SUM (DATEDIFF(ss, event_start_date_time_utc, event_end_date_time_utc))    	TOTAL_UNcapped_Duration,
--		SUM (DATEDIFF(ss, event_start_date_time_utc, capping_end_date_time_utc))  	TOTAL_Capped_Duration,
		COUNT (DISTINCT Account_number)   											TOTAL_HH,
		cb_change_date																	
INTO Temp2
FROM Sk_prod.Vespa_dp_prog_viewed_current
WHERE cb_change_date >= @report_date
	AND panel_id in (12) 
GROUP BY DATE(event_start_date_time_utc)  , cb_change_date;

commit; 

SELECT 	DATE(instance_start_date_time_utc)   						Event_Date,
		  COUNT(DISTINCT cast(dk_programme_instance_dim AS VARCHAR)+'p'+Account_number)			TOTAL_Viewing_Instances,
		COUNT (DISTINCT Account_number)   											TOTAL_HH,
--		COUNT (DISTINCT subscriber_id)   											TOTAL_STB,
		cb_change_date																	
INTO Temp3
FROM Sk_prod.Vespa_dp_prog_viewed_current
WHERE cb_change_date >= @report_date 
	AND panel_id in (12) 
GROUP BY DATE(instance_start_date_time_utc)  , cb_change_date;


COMMIT;

IF OBJECT_ID('pitteloudj.Total_VIQ_METRICS') IS NULL
CREATE TABLE Total_VIQ_METRICS
	(Metric_id int, 
	Metric_Desc varchar(100),
	Metric_Result float, 
	Load_timestamp datetime,
	Viewing_Date datetime);

Commit; 

INSERT INTO Total_VIQ_METRICS (Metric_ID, Metric_Desc, Metric_Value, Load_timestamp, Viewing_Date)
SELECT 6, 'Avg Impacts per Day', sum(TOTAL_Impacts)/Sum(TOTAL_HH), Record_Date, Impact_Day
FROM Temp1
GROUP BY Record_Date, Impact_Day;
INSERT INTO Total_VIQ_METRICS (Metric_ID, Metric_Desc, Metric_Value, Load_timestamp, Viewing_Date)
SELECT 7, 'TOTAL Impacts', sum(TOTAL_Impacts), Record_Date , Impact_Day
FROM Temp1
GROUP BY Record_Date, Impact_Day;
INSERT INTO Total_VIQ_METRICS (Metric_ID, Metric_Desc, Metric_Value, Load_timestamp, Viewing_Date)
SELECT 10, 'Avg DP VEv per HH', SUM(TOTAL_Viewing_Events)/sum(TOTAL_HH), cb_change_date , Event_Date
FROM Temp2
GROUP BY cb_change_date , Event_Date;
INSERT INTO Total_VIQ_METRICS (Metric_ID, Metric_Desc, Metric_Value, Load_timestamp, Viewing_Date)
SELECT 11, 'Avg DP VIn per HH', sum(TOTAL_Viewing_Instances)/sum(TOTAL_HH), cb_change_date , Event_Date
FROM Temp3
GROUP BY cb_change_date , Event_Date;
INSERT INTO Total_VIQ_METRICS (Metric_ID, Metric_Desc, Metric_Value, Load_timestamp, Viewing_Date)
SELECT 111, 'TOTAL DP VIn', sum(TOTAL_Viewing_Instances), cb_change_date , Event_Date
FROM Temp3
GROUP BY cb_change_date , Event_Date;
INSERT INTO Total_VIQ_METRICS (Metric_ID, Metric_Desc, Metric_Value, Load_timestamp, Viewing_Date)
SELECT 101, 'TOTAL DP VEv', sum(TOTAL_Viewing_Events), cb_change_date , Event_Date
FROM Temp2
GROUP BY cb_change_date , Event_Date;
INSERT INTO Total_VIQ_METRICS (Metric_ID, Metric_Desc, Metric_Value, Load_timestamp, Viewing_Date)
SELECT 102, 'TOTAL HH VEv', sum(TOTAL_HH), cb_change_date , Event_Date
FROM Temp2
GROUP BY cb_change_date , Event_Date;
INSERT INTO Total_VIQ_METRICS (Metric_ID, Metric_Desc, Metric_Value, Load_timestamp, Viewing_Date)
SELECT 112, 'TOTAL HH VIn', sum(TOTAL_HH), cb_change_date , Event_Date
FROM Temp3
GROUP BY cb_change_date , Event_Date;

COMMIT;

SELECT * FROM Total_VIQ_METRICS


/*

SELECT * FROM Temp1
SELECT * FROM Temp2
SELECT * FROM Temp3*/