DECLARE @y INT
DECLARE @i tinyint 
SET @y = 2016
SET @i = 1
WHILE @y =2016
    BEGIN 
    WHILE @i <=8
        BEGIN 
            SELECT a.account_number
                , a.end_date 
                , a.DTV_TA_calls_1m
                , a.BB_all_calls_1m
                , a.my_sky_login_3m
                , a.Talk_tenure
                , a.RTM
                , b.Simple_Segment
            INTO #tp    
            FROM citeam.DTV_FCAST_WEEKLY_BASE AS a 
            JOIN citeam.CUST_FCAST_WEEKLY_BASE AS b ON a.account_number = b.account_number AND a.end_date = b.end_date 
            WHERE       a.subS_year = @y 
                    AND a.subs_week = @i 
            COMMIT 
            MESSAGE CAST(now() as timestamp)||' | Step 1: '||@y||'-'||@i  TO CLIENT
            CREATE LF INDEX ID1 ON #tp(DTV_TA_calls_1m)
            CREATE LF INDEX ID2 ON #tp(BB_all_calls_1m)
            CREATE LF INDEX ID3 ON #tp(my_sky_login_3m)
            CREATE LF INDEX ID4 ON #tp(Talk_tenure)
            CREATE LF INDEX ID5 ON #tp(RTM)
            CREATE LF INDEX ID6 ON #tp(simple_segment)
            COMMIT 

            INSERT INTO BB_churn_TP
            SELECT node
                    , segment
                    , end_date
                    , count(*) hits

            FROM BB_TP_Product_Churn_segments_lookup AS b  
            LEFT JOIN #tp AS a  ON a.DTV_TA_calls_1m = b.DTV_TA_calls_1m
                        AND a.BB_all_calls_1m = b.BB_all_calls_1m 
                        AND a.Simple_Segment = b.Simple_Segment
                        AND a.my_sky_login_3m = b.my_sky_login_3m
                        AND a.Talk_tenure = b.Talk_tenure
                        AND a.RTM = b.RTM
            GROUP BY node
                    , segment
                    , end_date
            MESSAGE CAST(now() as timestamp)||' | Iteration: '||@y||'-'||@i  TO CLIENT
                DROP TABLE #tp
            SET @i = @i +1 
            

            END 
        SET @y = @y +1 
        SET @i = 1
        END 

COMMIT 
