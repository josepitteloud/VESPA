SELECT Event_Start_Date
    , cb_change_date
    , subscriber_id
    , Number_of_Boxes
    , SUM(Total_Duration)
    , SUM(Total_Viewing_Time)
    , CONVERT(decimal(5,2),SUM(Average_Duration)/3600)  as 'Average_Duration'
    , CONVERT(decimal(5,2),SUM(Avg_Viewing_Time)/3600)  as 'Avg_Viewing_Time'

FROM (

SELECT CAST(event_start_date_time_utc as date) as 'Event_Start_Date'
    , cb_change_date
    , subscriber_id
    , SUM(duration) Over (Partition By subscriber_id,event_start_date_time_utc) as 'Total_Duration' --this is kept in to compare against the DATEDIFF function
    , SUM(DATEDIFF(ss,event_start_date_time_utc,capping_end_date_time_utc)) Over (Partition By subscriber_id,event_start_date_time_utc) as 'Total_Viewing_Time'
    , duration
    , DATEDIFF(ss,event_start_date_time_utc,capping_end_date_time_utc) as 'Viewing_Time_DateDiff'
    , COUNT(distinct(subscriber_id)) as 'Number_of_Boxes'
    --, COUNT(subscriber_id) as 'Number of Boxes'
    , AVG(DATEDIFF(ss,event_start_date_time_utc,capping_end_date_time_utc)) as 'Avg_Viewing_Time'
    , AVG(duration) as 'Average_Duration' --this is kept in to compare against the DATEDIFF function above
    , event_start_date_time_utc
    , event_end_date_time_utc
    , capping_end_date_time_utc
FROM Sk_prod.Vespa_dp_prog_viewed_current
WHERE cb_change_date IN (SELECT MAX(cb_change_date) FROM Sk_prod.Vespa_dp_prog_viewed_current )
AND subscriber_id = 25340237 -- ADD THIS TO TEST QUERY AGAINST ONE BOX
--AND CAST(event_start_date_time_utc as date) = '2013-04-16' --this is to test for one day
AND capping_end_date_time_utc is not null --to filter out events that did not go through the capping process
--AND event_start_date_time_utc = '2013-04-16 18:42:42.000000'
GROUP BY CAST(event_start_date_time_utc as date)
    , cb_change_date
    , subscriber_id
    , duration
    , event_start_date_time_utc
    , event_end_date_time_utc
    , capping_end_date_time_utc

) AvgViewingSub

GROUP BY Event_Start_Date
    , subscriber_id
    , cb_change_date
    , Number_of_Boxes
   -- , Total_Duration
    --, Total_Viewing_Time
