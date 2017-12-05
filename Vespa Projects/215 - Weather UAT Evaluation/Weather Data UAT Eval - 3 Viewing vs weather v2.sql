/*****************************************************************************
**
**                  SSSS   KK   KK  YY    YY   IIII   QQQQQ
**                 SS  SS  KK  KK    YY  YY     II   QQ   QQ  
**                 SS      KK KK      YYYY      II   QQ   QQ    
**                  SSSS   KKKK        YY       II   QQ   QQ      
**                     SS  KKK         YY       II   QQ  QQQ    
**                 SS  SS  KK KK       YY       II   QQ   QQQ  
**                  SSSS   KK   KKK    YY      IIII   QQQQQ QQ       
**
**
**  Project Vespa: PROJECT  V215 - Weather Data Evaluation 3
**  
**	
**
**	Related Documents:
**	
**
**
**	Code Sections:
**
**	Section A - 
**		A01	- 
**		A02 - 
**
**	Written by Jose Pitteloud
******************************************************************************/

DECLARE 
      @n int
    , @Count int
    , @Colname varchar(60)
    , @Tblname varchar(60)
    , @TotalCol int
    , @max int
    , @min int
    , @difcount int
    , @sql varchar(1000)
    , @c0 INT 
    , @c1 INT 
	  , @c2 INT 
	  , @c3 INT 
	  , @c4 INT 	
    , @c9 INT 	
    , @c5 INT 	
    , @c6 INT 	
    , @c7 INT 	
    , @c8 INT 	
    , @run_id int
    
SET @c0 = 0	--	Weather aggregation by District
SET @c1 = 0 -- 	SAMPLING POSTAL AREAS
SET @c2 = 0 --  Selecting account details from SAV
SET @c3 = 0 -- Flaging account that reported 28 days
SET @c4 = 0 -- Summarizing log history
SET @c5 = 0 -- UPDATING Q_flag
SET @c6 = 0 --  Extracting viewing data from Event table
SET @c7 = 0  -- CReating Capped Viewing table
SET @c8 = 0 --  Consolidating SAV and Viewing SAMPLED
SET @c9 = 0 -- WEATHER_VIEWING_BY_POSTAL
SET @n = 0
SET @run_id = (SELECT MAX(Mvalue)+2 FROM WEATHER_DATA_EVAL_LOG WHERE Description like 'Run ID')



IF @c0 = 1 ------------------A01		-		Weather aggregation by District and time of day
BEGIN

IF object_id('sk_uat.WEATHER_DATA_TIME_OF_DAY') IS NOT NULL	      DROP TABLE  sk_uat.WEATHER_DATA_TIME_OF_DAY

  SELECT
	  CAST(date_time AS DATE) date_1
	  , DATEPART(dw, date_time) Day_of_week
	  , MONTH(date_time) month1
	  , CASE   WHEN DATEPART(hh, date_time) BETWEEN 0 AND 7 THEN '0'
			 WHEN DATEPART(hh, date_time) BETWEEN 8 AND 11 THEN '1'
			 WHEN DATEPART(hh, date_time) BETWEEN 12 AND 15 THEN '2'
			 WHEN DATEPART(hh, date_time) BETWEEN 16 AND 19 THEN '3'
			 WHEN DATEPART(hh, date_time) BETWEEN 20 AND 24 THEN '4'
			ELSE '99'
	    END                 Time_of_Day
	  , district
	  , MAX(COALESCE (maximum_temperature_daily, temperature)) Max_Temp
	  , MIN(COALESCE (minimum_temperature_daily, temperature)) Min_Temp
	  , SUM(ISNULL(rainfall,0))       rainfall
	  , MAX(cloud_cover)    clouds
	  , MAX(wind_speed)     winds  
	INTO WEATHER_DATA_TIME_OF_DAY
	FROM sk_uat.WEATHER_DATA_history
	GROUP BY 
		  month1
		  , date_1
		  , Time_of_Day
		  , district
		  , Day_of_week
		  
INSERT INTO WEATHER_DATA_EVAL_LOG (MValue, Description ,Qty, Date_proc)
  VALUES (@run_id,'WEATHER_DATA_TIME_OF_DAY Created and populated',@@rowcount, GETDATE())		  

END
      

IF @c1 = 1 ------------------ SAMPLING POSTAL AREAS
BEGIN

IF object_id('pitteloudj.WEATHER_POSTAL_AREA_SAMPLER') IS NOT NULL	      DROP TABLE  pitteloudj.WEATHER_POSTAL_AREA_SAMPLER
	---------Building postal areas table
	SELECT DISTINCT 
		  cb_address_postcode_area
		, 0 Sample
	INTO pitteloudj.WEATHER_POSTAL_AREA_SAMPLER
	FROM sk_prod.CACI_SOCIAL_CLASS	

	--------------- Flagging post areas selected
	UPDATE WEATHER_POSTAL_AREA_SAMPLER
	SET Sample = 1  
    WHERE cb_address_postcode_area in ('PE','AB','OL','DA','CT','BT','IP','SO','IV','NR','HR','EX','SL','RG','NN','NG','SP','SN','TN','TS')

	INSERT INTO WEATHER_DATA_EVAL_LOG (MValue, Description ,Qty, Date_proc)
	VALUES (@run_id,'Postal Areas Sampler populated',@@rowcount, GETDATE())		  

END 

IF @c2 = 1 -------------- Selecting account details from SAV
BEGIN

IF object_id('pitteloudj.WEATHER_SAV_DATA') IS NOT NULL	      DROP TABLE  pitteloudj.WEATHER_SAV_DATA

	SELECT  
	    sav.account_number
	  , sav.cb_address_postcode_district
	  , sav.cb_address_postcode_area
	  , sav.h_affluence
	  , sav.cb_key_household
	  , sav.child_0_to_4
	  , sav.child_12_to_17
	  , sav.child_5_to_11
	  , sav.children_in_hh
	  , sav.employment_lifestage
	  , sav.h_lifestage
	  , sav.household_composition
	  , sav.kids_age_10to15
	  , sav.kids_age_4to9
	  , sav.kids_age_le4
	  , sav.mosaic_segments
	INTO WEATHER_SAV_DATA
	FROM sk_prod.CUST_SINGLE_ACCOUNT_VIEW               AS sav
	INNER JOIN pitteloudj.WEATHER_POSTAL_AREA_SAMPLER   AS dis 
          ON dis.cb_address_postcode_area = sav.cb_address_postcode_area AND dis.Sample = 1
  
  INSERT INTO WEATHER_DATA_EVAL_LOG (MValue, Description ,Qty, Date_proc)
	VALUES (@run_id,'WEATHER_SAV_DATA table created and populated',@@rowcount, GETDATE())		  

END	

------------------------- SELECTING account that have reported enough  days
IF @c3 = 1 -------------- Flaging account that have reported enough days
BEGIN
  IF object_id('pitteloudj.WEATHER_SUBS') IS NOT NULL	      DELETE FROM pitteloudj.WEATHER_SUBS
  
  INSERT   INTO WEATHER_SUBS
  
  SELECT    sav.account_number
          , sbo.subscriber_id
          , 0 Q_flag
  FROM WEATHER_SAV_DATA   AS sav
  INNER JOIN vespa_analysts.vespa_single_box_view as sbo ON sav.account_number = sbo.account_number

  INSERT INTO WEATHER_DATA_EVAL_LOG (MValue, Description ,Qty, Date_proc)
  VALUES (@run_id,'WEATHER_SUBS table created and populated',@@rowcount, GETDATE())		


  IF object_id('pitteloudj.WEATHER_SUBS_LOG') IS NOT NULL	      DELETE FROM   pitteloudj.WEATHER_SUBS_LOG
  
  INSERT INTO WEATHER_SUBS_LOG
  
  SELECT 
        v.subscriber_id
        , dateadd(hour,1, v.LOG_START_DATE_TIME_UTC)                 stb_log_creation_date
        , case
                when convert(integer,dateformat(MIN(DATEADD(HOUR,1, v.LOG_RECEIVED_START_DATE_TIME_UTC)),'hh')) <23
                then cast(min(dateadd(hour,1, v.LOG_RECEIVED_START_DATE_TIME_UTC)) as date)-1
                else
                cast(min(dateadd(hour,1, v.LOG_RECEIVED_START_DATE_TIME_UTC)) as date)
          end                                                                   as doc_creation_date_from_9am
        , min(dateadd(hour,1, v.EVENT_START_DATE_TIME_UTC))                     as first_event_mark
        , max(dateadd(hour,1, v.EVENT_END_DATE_TIME_UTC))                       as last_event_mark
        , count(1)                                                              as log_event_count
        , datepart(hh, min(dateadd(hour,1, v.LOG_RECEIVED_START_DATE_TIME_UTC))) as  hour_received
 FROM   sk_prod.VESPA_DP_PROG_VIEWED_201305  AS v
 INNER JOIN  WEATHER_SUBS                     AS sub ON v.account_number = sub.account_number
 WHERE  
        panel_id in (4,12)
      and     LOG_RECEIVED_START_DATE_TIME_UTC is not null
      and     LOG_START_DATE_TIME_UTC is not null
      and     v.subscriber_id is not null
 GROUP BY   v.subscriber_id
            , LOG_START_DATE_TIME_UTC
 HAVING     doc_creation_date_from_9am is not null

 INSERT INTO WEATHER_DATA_EVAL_LOG (MValue, Description ,Qty, Date_proc)
 VALUES (@run_id,'WEATHER_SUBS_LOG table created and populated',@@rowcount, GETDATE())		  

END 

IF @c4 = 1 -------------- Summarizing log history
BEGIN

IF object_id('pitteloudj.WEATHER_DATA_EVAL_LOG_SUMMARY') IS NOT NULL	      delete from  pitteloudj.WEATHER_DATA_EVAL_LOG_SUMMARY

  INSERT INTO WEATHER_DATA_EVAL_LOG_SUMMARY

  SELECT 
      subscriber_id
      , convert(date, doc_creation_date_from_9am)  AS log_date
      , count(distinct doc_creation_date_from_9am) AS log_count
      , min(first_event_mark)                      AS first_event
      , max(last_event_mark)                       AS last_event
      , sum(log_event_count)                       AS event_count
      , min(hour_received)                         AS hours_rec
  FROM WEATHER_SUBS_LOG
  GROUP BY 
        subscriber_id
      , log_date

 INSERT INTO WEATHER_DATA_EVAL_LOG (MValue, Description ,Qty, Date_proc)
 VALUES (@run_id,'WEATHER_DATA_EVAL_LOG_SUMMARY table created and populated',@@rowcount, GETDATE())		  

END

IF @c5 = 1 ----------	UPDATING Q_flag
BEGIN

  UPDATE WEATHER_SUBS 
  SET Q_flag = 1
  WHERE subscriber_id in (SELECT  subscriber_id
                          FROM WEATHER_DATA_EVAL_LOG_SUMMARY 
                          GROUP BY subscriber_id
                          HAVING COUNT(*) between 22 AND 32)

END


IF @c6 = 1 ----------	Extracting viewing data from Event table
BEGIN

IF object_id('pitteloudj.WEATHER_View_May_2013') IS NOT NULL	      DELETE FROM pitteloudj.WEATHER_View_May_2013

INSERT INTO WEATHER_View_May_2013
	
  SELECT   
		s.account_number
		, CAST(instance_start_date_time_utc AS DATE) Date_of_viewing
    , CASE   WHEN DATEPART(hh, instance_start_date_time_utc) BETWEEN 0 AND 7 THEN '0'
			 WHEN DATEPART(hh, instance_start_date_time_utc) BETWEEN 8 AND 11 THEN '1'
			 WHEN DATEPART(hh, instance_start_date_time_utc) BETWEEN 12 AND 15 THEN '2'
			 WHEN DATEPART(hh, instance_start_date_time_utc) BETWEEN 16 AND 19 THEN '3'
			 WHEN DATEPART(hh, instance_start_date_time_utc) BETWEEN 20 AND 24 THEN '4'
			ELSE '99'
	    END                 Time_of_Day
		, SUM(CASE  WHEN v.capped_partial_flag = 1   THEN  datediff(second, v.instance_start_date_time_utc, v.capping_end_date_time_utc)
                ELSE                                   datediff(second, v.instance_start_date_time_utc, v.instance_end_date_time_utc)
          END
        ) duration_capped0
	FROM Sk_prod.VESPA_DP_PROG_VIEWED_201305 as v
  INNER JOIN pitteloudj.WEATHER_SUBS AS s ON s.account_number = v.account_number AND s.Q_flag = 1 AND v.subscriber_id = s.subscriber_id
	WHERE 
         v.capped_full_flag = 0
     AND v.capping_end_date_time_utc is not null
     and v.panel_id in(12,4)
     and v.instance_start_date_time_utc < v.instance_end_date_time_utc              -- Remove 0sec instances
     and (v.reported_playback_speed is null or v.reported_playback_speed = 2)
     and v.broadcast_start_date_time_utc >= dateadd(hour, -(24*28), v.event_start_date_time_utc)
     and s.account_number is not null
     and v.subscriber_id is not null
     and v.type_of_viewing_event in ('HD Viewing Event', 'Sky+ time-shifted viewing event', 'TV Channel Viewing', 'Other Service Viewing Event')
	GROUP BY   
		  s.account_number
		, Date_of_viewing
    , Time_of_Day



		
	INSERT INTO WEATHER_DATA_EVAL_LOG (MValue, Description ,Qty, Date_proc)
	VALUES (@run_id,'WEATHER_View_Feb_2013 table created and populated',@@rowcount, GETDATE())		  
	
END	

IF @c7 = 1 ----------	CReating Capped Viewing table
BEGIN
  
  IF object_id('pitteloudj.WEATHER_View_May_2013_capped') IS NOT NULL	     DELETE FROM pitteloudj.WEATHER_View_May_2013_capped
  
  INSERT INTO WEATHER_View_May_2013_capped
  
  SELECT 
      account_number
    , Date_of_viewing
    , Time_of_Day
    , CASE WHEN SUM(duration_capped) > 60 THEN 1 ELSE 0 END cap_flag
  FROM WEATHER_View_May_2013
  GROUP BY 
      account_number
    , Date_of_viewing
    , Time_of_Day

  INSERT INTO WEATHER_DATA_EVAL_LOG (MValue, Description ,Qty, Date_proc)
  VALUES (@run_id,'WEATHER_View_May_2013_capped table created and populated',@@rowcount, GETDATE())		  

END

IF @c8 = 1 ----------	Consolidating SAV and Viewing SAMPLED
BEGIN

IF object_id('pitteloudj.WEATHER_VIEWING_BY_POSTAL_v3') IS NOT NULL	      DROP TABLE  pitteloudj.WEATHER_VIEWING_BY_POSTAL_v3
--INSERT INTO WEATHER_VIEWING_BY_POSTAL
  SELECT 
      s.cb_address_postcode_district  AS district
    , s.cb_address_postcode_area      AS area
    , f.date_of_viewing
    , f.time_of_day
    --, CASE WHEN SUM(f.duration_capped)>60 THEN 1 ELSE 0 END     AS total_duration
    , SUM(f.duration_capped)
  INTO WEATHER_VIEWING_BY_POSTAL_v3
  FROM WEATHER_POSTAL_AREA_SAMPLER    AS p
  INNER JOIN WEATHER_SAV_DATA         AS s ON p.cb_address_postcode_area = s.cb_address_postcode_area 
  INNER JOIN WEATHER_View_May_2013    AS f ON f.account_number = s.account_number
  WHERE p.Sample = 1
  GROUP BY 
      district
    , area
    , f.date_of_viewing
    , f.time_of_day
    
	INSERT INTO WEATHER_DATA_EVAL_LOG (MValue, Description ,Qty, Date_proc)
	VALUES (@run_id,'WEATHER_VIEWING_BY_POSTAL_v2 table created and populated',@@rowcount, GETDATE())		  

END    

IF @c9 = 1 ----------	c
BEGIN

IF object_id('pitteloudj.WEATHER_VIEWING_BY_POSTAL_CAPPED_v2') IS NOT NULL	      DELETE FROM   pitteloudj.WEATHER_VIEWING_BY_POSTAL_CAPPED_v2
  --INSERT INTO WEATHER_VIEWING_BY_POSTAL_CAPPED
  SELECT 
      s.cb_address_postcode_district  AS district
    , s.cb_address_postcode_area      AS area
    , f.date_of_viewing
    , f.time_of_day
    , SUM(f.cap_flag) total_accounts
  INTO WEATHER_VIEWING_BY_POSTAL_CAPPED_v2
  FROM WEATHER_POSTAL_AREA_SAMPLER    AS p
  INNER JOIN WEATHER_SAV_DATA         AS s ON p.cb_address_postcode_area = s.cb_address_postcode_area 
  INNER JOIN WEATHER_View_Feb_2013_capped    AS f ON f.account_number = s.account_number
  WHERE p.Sample = 1
  GROUP BY 
      district
    , area
    , f.date_of_viewing
    , f.time_of_day
    
	INSERT INTO WEATHER_DATA_EVAL_LOG (MValue, Description ,Qty, Date_proc)
	VALUES (6,'WEATHER_VIEWING_BY_POSTAL_CAPPED table created and populated',@@rowcount, GETDATE())		  

END    



/*
SELECT 
    Time_of_Day
  , duration_capped
  , Date_of_viewing
  , account_number
  , count(DISTINCT f.account_number) acct
INTO   
FROM WEATHER_View_Feb_2013    AS p
GROUP BY 
  area,
  f.date_of_viewing

column_name
Time_of_Day
sub_genre_description
genre_description
duration_capped
Date_of_viewing
account_number
*/



/*****************************
QA

SELECT 
  s.cb_address_postcode_area      AS area,
  f.date_of_viewing,
  count(DISTINCT f.account_number) acct
FROM WEATHER_POSTAL_AREA_SAMPLER    AS p
INNER JOIN WEATHER_SAV_DATA         AS s ON p.cb_address_postcode_area = s.cb_address_postcode_area 
INNER JOIN WEATHER_View_Feb_2013    AS f ON f.account_number = s.account_number
WHERE p.Sample = 1
GROUP BY 
  area,
  f.date_of_viewing
  
  
   CAST(datediff(MINUTE,v.event_start_date_time_utc,
                      CASE WHEN v.capping_end_date_time_utc is not null 
                           THEN v.capping_end_date_time_utc
                           ELSE v.event_end_date_time_utc 
                      END) AS int)
  
**********************************/
DELETE FROM WEATHER_DATA_TIME_OF_DAY_SUBSET;

INSERT INTO WEATHER_DATA_TIME_OF_DAY_SUBSET
SELECT * 
FROM WEATHER_DATA_TIME_OF_DAY
WHERE month1 in (2,3,4,5,6)
AND YEAR(date_1) = 2013
AND (district like 'PE%'
    OR district like 'AB%'
    OR district like 'OL%'
    OR district like 'DA%'
    OR district like 'CT%'
    OR district like 'BT%'
    OR district like 'IP%'
    OR district like 'SO%'
    OR district like 'IV%'
    OR district like 'NR%'
    OR district like 'HR%'
    OR district like 'EX%'
    OR district like 'SL%'
    OR district like 'RG%'
    OR district like 'NN%'
    OR district like 'NG%'
    OR district like 'SP%'
    OR district like 'SN%'
    OR district like 'TN%'
    OR district like 'TS%') 
    commit