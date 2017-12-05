SELECT CAST(event_start_date_time_utc as date) as 'Event Start Date'
    , cb_change_date
    , account_number
    , SUM(duration) as 'Total Pre Cappigng Duration' --to compare against post capping duration
    , SUM(DATEDIFF(ss,event_start_date_time_utc,capping_end_date_time_utc)) as 'Total Post Capping Duration' 
    , COUNT(account_number) as 'Number of Households'
    , CONVERT(decimal(8,2),AVG(DATEDIFF(ss,event_start_date_time_utc,capping_end_date_time_utc)/60)/60) as 'Avg Viewing Time'
    , CONVERT(decimal(5,2),(AVG(duration)/60)/60) as 'Average Duration' --this is kept in to compare against the DATEDIFF function above
FROM Sk_prod.Vespa_dp_prog_viewed_current 
WHERE cb_change_date IN (SELECT MAX(cb_change_date) FROM Sk_prod.Vespa_dp_prog_viewed_current )-- >='2013-03-27' AND cb_change_date <'2013-04-02'
AND capping_end_date_time_utc is not null --to filter out events that did not go through the capping process
AND account_number = '620032259199' --this is to restrict the data to run fasterer
GROUP BY CAST(event_start_date_time_utc as date)
    , account_number
    , cb_change_date
    ;

