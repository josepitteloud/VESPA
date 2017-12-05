------------------------------------------------------------------------
--16) - To collect the number of Vespa Daily Panel Households not Dialling Back (use Scaling definition to define a Household)
------------------------------------------------------------------------
DECLARE @report_date datetime --, @TOTAL_P12 int,  @TOTAL_P6 int,  @TOTAL_P7 int
SET @report_date = DATE(DATEADD (dd, -2, getdate()))
------------------------------------------------------------------------
--To get the accounts that are not dialling back with all their boxes
------------------------------------------------------------------------
SELECT count(distinct(VDPVC.Account_number))
  , VDPVC.Log_Date--, SUM(VSBV.Enabled_Households)--, getdate()
FROM(
------------------------------------------------------------------------
--To get the total panel from Single Box View table with their number of boxes
------------------------------------------------------------------------
   SELECT Account_number --HOUSEHOLD ID
    , count(distinct(Account_number)) as 'Enabled_Households' --, subscriber_id
    ,count(distinct(subscriber_id)) AS 'Subscriber_Count'
  FROM VESPA_ANALYSTS.VESPA_SINGLE_BOX_VIEW
  WHERE panel_id_Vespa IN (12)
  AND Status_Vespa = 'Enabled'
    --WHERE Account_number in( '200000850582','200000850798')
    --WHERE Account_number = '200000850798'
  GROUP BY Account_number
  ) as VSBV

inner join(
------------------------------------------------------------------------
--To get the accounts and thier number of boxes dialling back 
------------------------------------------------------------------------
  SELECT Account_number--, CAST(LOG_RECEIVED_START_DATE_TIME_UTC as date) as Start_Date
    ,CASE
    WHEN CONVERT(INTEGER,dateformat(MIN(LOG_RECEIVED_START_DATE_TIME_UTC),'hh')) <23
    THEN CAST(MIN(LOG_RECEIVED_START_DATE_TIME_UTC) AS DATE)-1
    ELSE
    CAST(min(LOG_RECEIVED_START_DATE_TIME_UTC) AS DATE)
    END AS Log_Date    -- BASED ON OPS REPORTS DEFINITION "all logs received from 23:00 on day A until 22:59 on next day (A+1) will belong to A"
    , count(distinct(subscriber_id)) AS 'Subscriber_Count'
  FROM sk_prod.VESPA_DP_PROG_VIEWED_CURRENT
  WHERE   panel_id IN (12, 4) -- ONLY PANEL 12 REFERENCE
      AND CAST(LOG_RECEIVED_START_DATE_TIME_UTC as date) >= @report_date
    --AND Account_number in( '200000850582','200000850798')
  GROUP BY Account_number
  --, CAST(LOG_RECEIVED_START_DATE_TIME_UTC as date)
  HAVING Log_Date >= @report_date
  ) as VDPVC

on VSBV.Account_number = VDPVC.Account_number

WHERE VSBV.Subscriber_Count <> VDPVC.Subscriber_Count
    --AND CAST(LOG_RECEIVED_START_DATE_TIME_UTC as date) >= @report_date
    AND VDPVC.Log_Date >= @report_date

GROUP BY VDPVC.Log_Date

  

