------------------------------------------------------------------------
--08) - To collect the No of Daily Panel Viewing Events loaded for each viewing day
------------------------------------------------------------------------
DECLARE @report_date datetime --, @TOTAL_P12 int,  @TOTAL_P6 int,  @TOTAL_P7 int
SET @report_date = DATE(DATEADD (dd, -2, getdate()))
------------------------------------------------------------------------
--To get the sum of records from sub query Event_Count
------------------------------------------------------------------------
SELECT sum(Event_Count.Count) as 'No_of_Events', CAST(event_start_date_time_utc as date) as 'Event_Date'
FROM(
------------------------------------------------------------------------
--To get a count of records produced from subscriber_id and event_start_date_time_utc
------------------------------------------------------------------------
  SELECT count(1) over (partition by subscriber_id, event_start_date_time_utc) as Count
    ,subscriber_id, event_start_date_time_utc
  FROM sk_prod.VESPA_AP_PROG_VIEWED_CURRENT
  WHERE   panel_id IN (6, 7) -- ONLY PANEL 6&7 REFERENCE
  AND CAST(event_start_date_time_utc as date) = @report_date
  --AND CAST(event_start_date_time_utc as date) = '2013-04-21'
  --and subscriber_id = 116890
  GROUP BY subscriber_id, event_start_date_time_utc
) as Event_Count

GROUP BY CAST(event_start_date_time_utc as date)
  