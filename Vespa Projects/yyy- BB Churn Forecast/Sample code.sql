SELECT 
      account_number
    , b.subs_year
    , b.subs_week_of_year
	, b.subs_quarter_of_year
    , MIN(call_date) 	AS first_date
    , SUM(no_of_calls)  AS s_calls 
    , count(*)          AS hits
    , CAST ( NULL AS VARCHAR(30)) AS Offer
INTO BB_CHURN_calls_details_raw_3yr_final
FROM CALLS_DETAILS AS a 
JOIN SKY_calendar AS b ON DATE(a.call_date) = DATE(b.calendar_date )
WHERE call_date >= '2014-01-01'
	AND     final_sct_grouping = 'Retention - BBCoE'
	AND account_number IS NOT NULL 
GROUP BY account_number
    , b.subs_year
    , b.subs_week_of_year
    , final_sct_grouping
	, b.subs_quarter_of_year
	
	
	 
SELECT 
    product_holding
    ,bb_type
    ,h_AGE_coarse_description AS age
    ,h_fss_v3_group             AS FSS
    ,bb_tenure
    ,affluence
    ,offer
    ,  CAST (NULL AS BIGINT ) AS TA_accounts
    , CAST (NULL AS BIGINT )  AS t_calls
    , SUM(acct)               AS BASE 
INTO BB_CHURN_SAMPLE 
FROM  monthly_base 
WHERE monthyear = 201607
GROUP BY Offer
    ,product_holding
    ,bb_type
    ,age
    ,fss
    ,bb_tenure
    ,affluence

COMMIT 

CREATE LF INDEX id2 ON BB_CHURN_SAMPLE (product_holding)
CREATE LF INDEX id3 ON BB_CHURN_SAMPLE (Offer)
CREATE LF INDEX id4 ON BB_CHURN_SAMPLE (bb_type)
CREATE LF INDEX id5 ON BB_CHURN_SAMPLE (age)
CREATE LF INDEX id6 ON BB_CHURN_SAMPLE (fss)
CREATE LF INDEX id7 ON BB_CHURN_SAMPLE (bb_tenure)
CREATE LF INDEX id8 ON BB_CHURN_SAMPLE (affluence)

SELECT Offer
    , product_holding
    , bb_type
    , age
    , fss
    , bb_tenure
    , affluence
    , count(DISTINCT account_number) acct
    , SUM(s_calls) calls 
INTO #t1 
FROM BB_CHURN_calls_details_raw_3yr_final
WHERE subs_year = 2016 ANd subs_quarter_of_year = 1
GROUP BY Offer
    , product_holding
    , bb_type
    , age
    , fss
    , bb_tenure
    , affluence
    COMMIT 

CREATE LF INDEX id2 ON  #t1  (product_holding)
CREATE LF INDEX id3 ON  #t1  (Offer)
CREATE LF INDEX id4 ON  #t1  (bb_type)
CREATE LF INDEX id5 ON  #t1  (age)
CREATE LF INDEX id6 ON  #t1  (fss)
CREATE LF INDEX id7 ON  #t1  (bb_tenure)
CREATE LF INDEX id8 ON  #t1  (affluence)    
    COMMIT 
    UPDATE BB_CHURN_SAMPLE
    SET TA_accounts = acct
        , t_calls = calls
 FROM BB_CHURN_SAMPLE AS a 
 JOIN #t1 AS b ON a.Offer = b.Offer
    AND  a.product_holding = b.product_holding
    AND  a.bb_type = b.bb_type
    AND  a.age = b.age
    AND  a.fss = b.fss
    AND  a.bb_tenure = b.bb_tenure
    AND  a.affluence = b.affluence
    