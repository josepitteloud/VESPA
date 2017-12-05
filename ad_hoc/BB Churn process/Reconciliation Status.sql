--- CITEAM

select *
        , max(end_date) over (partition by account_number order by end_date rows between 1 following and 1 following) as the_end
        , DATEDIFF (WEEK, end_date, the_end) duration
INTO BB_churn_SABB_status_duration        
from    (
SELECT account_number
    , end_date 
    , bB_status_code 
    , MAX(bB_status_code) OVER (   partition by account_number order by end_date DESC
                                           rows between    1 following and 1 following) as Prec
    , CASE WHEN bb_status_code = prec THEN 0 ELSE 1 END Continous                                          
    , CASE WHEN continous = 1 then dense_rank() over (partition by account_number   order by end_date) 
                     else  NULL  
           end        as the_session
FROM citeam.cust_fcast_weekly_base
WHERE bb_active = 1 
    AND DTV_active =0 ) as ax
where   Continous = 1


SELECT duration, bB_status_code, count(*) hits
FROM  BB_churn_SABB_status_duration
GROUP BY duration, bB_status_code

--- JC

select *
        , max(end_date) over (partition by account_number order by end_date rows between 1 following and 1 following) as the_end
        , DATEDIFF (WEEK, end_date, the_end) duration
INTO BB_churn_SABB_status_duration_JC        
from    (
SELECT account_number
    , end_date 
    , bB_status_code 
    , MAX(bB_status_code) OVER (   partition by account_number order by end_date DESC
                                           rows between    1 following and 1 following) as Prec
    , CASE WHEN bb_status_code = prec THEN 0 ELSE 1 END Continous                                          
    , CASE WHEN continous = 1 then dense_rank() over (partition by account_number   order by end_date) 
                     else  NULL  
           end        as the_session
FROM jcartwright.cust_fcast_weekly_base_2
WHERE bb_active = 1 
    AND DTV_active =0 ) as ax
where   Continous = 1


SELECT duration, bB_status_code, count(*) hits
FROM  BB_churn_SABB_status_duration_JC
GROUP BY duration, bB_status_code
