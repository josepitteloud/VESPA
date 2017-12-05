--------------weather_DATA_history
CREATE VARIABLE min_hr int;
CREATE VARIABLE max_hr int;

SET min_hr = (SELECT 24-DATEPART(hh, min(date_time)) FROM sk_prod.weather_DATA_history)
SET max_hr = (SELECT DATEPART(hh, max(date_time))+1 FROM sk_prod.weather_DATA_history)

--Checking Table
SELECT top 1 * FROM sk_prod.weather_DATA_history;

--- CHECKING Source files
SELECT cb_source_file, count(1) hits
FROM sk_prod.weather_DATA_history
GROUP BY cb_source_file;

SELECT min(date_time),  max(date_time) FROM sk_prod.weather_DATA_history;

-- Checking dates
SELECT DATE(date_time), count(DISTINCT date_time) hours
FROM sk_prod.weather_DATA_history
GROUP BY DATE(date_time)
ORDER by DATE(date_time);

SELECT date_time, 
  COUNT(1) hits
FROM sk_prod.weather_DATA_history
GROUP BY date_time;

--- CHECKING rows by day by district 
SELECT DATE(date_time)
      , district 
      , count(DISTINCT date_time) hours
FROM sk_prod.weather_DATA_history
GROUP BY DATE(date_time), district 
HAVING hours not in (24, min_hr, max_hr); -- 24hr or partial 1st/ last days

--------------weather_DATA_AGGREGATED_history
CREATE VARIABLE max_hr_agg datetime;
CREATE VARIABLE max_p_agg int;
SET max_hr_agg = (SELECT MAX(weather_date) FROM sk_prod.weather_DATA_AGGREGATED_history)
SET max_p_agg = (SELECT COUNT(DISTINCT period )  FROM sk_prod.weather_DATA_AGGREGATED_history)
--Checking Table
SELECT top 1 * FROM sk_prod.weather_DATA_AGGREGATED_history;

-- Min Max dates
SELECT min(weather_date),  max(weather_date) FROM sk_prod.weather_DATA_AGGREGATED_history;

-- Checking dates
SELECT DATE(weather_date)
      , count(DISTINCT weather_date) hours
      , COUNT(DISTINCT period) periods
FROM sk_prod.weather_DATA_AGGREGATED_history
GROUP BY DATE(weather_date)
ORDER by DATE(weather_date);

SELECT weather_date, 
  COUNT(1) hits
FROM sk_prod.weather_DATA_AGGREGATED_history
GROUP BY weather_date;

--- CHECKING rows by day by district 
SELECT DATE(weather_date)
      , district 
      , count(DISTINCT Period) hours
FROM sk_prod.weather_DATA_AGGREGATED_history
GROUP BY DATE(weather_date), district 
HAVING hours not in (4, 2); 

SELECT top 100 *  FROM sk_prod.weather_DATA_AGGREGATED_history
WHERE weather_date = max_hr_agg;

select top 100 hour(CAST(period_from as datetime))  FROM sk_prod.weather_DATA_AGGREGATED_history
WHERE District ='AB10'
ORDER BY district, weather_date, period


SELECT top 10 * from sk_prod.weather_DATA_history
WHERE HOUR(date_time) between 



SELECT top 10 
  HOUR(CAST(period_from AS DATETIME)) 
, CASE WHEN period = 'P4' THEN 23 ELSE HOUR(DATEADD(minute, -1, CAST(period_to AS DATETIME))) eND

FROM sk_prod.weather_DATA_AGGREGATED_history

SELECT top 10000
	ag.district	
	, ag.max_period_temp	
	, ag.min_period_temp	
	, ag.period	
	, ag.period_from	
	, ag.period_to	
	, ag.rainfall	
	, ag.snowfall	
	, ag.weather_date
  , min(date_time) min_datetime
  , max(date_time) max_datetime
	, avg(da.temperature) 		AS avg_period_temp
	, max(cloud_cover) 			AS max_cloud_cover
	, max(relative_humidity) 	AS max_humidity
	, max(sunshine_duration)  	AS max_sunshine
	, max(wind_speed) 			AS max_windspeed
INTO WEATHER_JOIN_TEST
FROM sk_prod.weather_DATA_AGGREGATED_history AS  ag 
JOIN sk_prod.weather_DATA_HISTORY AS  da ON ag.district = da.district 
										AND CAST (weather_Date AS DATE) = CAST(da.date_time AS DATE) 
										AND HOUR(da.date_time) BETWEEN HOUR(CAST(period_from AS DATETIME)) and CASE WHEN period = 'P4' THEN 23 ELSE HOUR(DATEADD(minute, -1, CAST(period_to AS DATETIME))) eND
WHERE ag.district = 'AB10'
GROUP BY 
		ag.district	
	, ag.max_period_temp	
	, ag.min_period_temp	
	, ag.period	
	, ag.period_from	
	, ag.period_to	
	, ag.rainfall	
	, ag.snowfall	
	, ag.weather_date

  
  ALTER TABLE WEATHER_JOIN_TEST ADD (ID int IDENTITY);
  commit;
  ALTER TABLE WEATHER_JOIN_TEST ADD (W_index_1 int,W_index_2 int,W_index_3 int );
  commit;
  
  select top 100 * from WEATHER_JOIN_TEST
  ORDER BY weather_date, period
  
  
SELECT 
    weather_date
  , period
  , max_humidity 
  , avg(max_humidity) OVER (PARTITION BY district, period
                            ORDER BY district, period, weather_date
                            ROWS 3 PRECEDING) AS avg_humid
  , MAX(max_humidity) OVER (PARTITION BY district, period
                            ORDER BY district, period, weather_date
                            ROWS 3 PRECEDING) AS max_humid
from WEATHER_JOIN_TEST
ORDER BY weather_date


UPDATE   WEATHER_JOIN_TEST
SET W_index_1 = CASE WHEN (max_windspeed > 25 OR rainfall>3 OR (snowfall =1 AND rainfall>0.2)) THEN 1  
						WHEN (rainfall>1.5 OR snowfall =1) THEN 2
						ELSE 3 END
            COmmit
            
            
ALTER TABLE WEATHER_JOIN_TEST 
	ADD (
		  mov_avg_temp			DECIMAL(7,4) DEFAULT null
		, mov_cloud_cover		DECIMAL(6,4) DEFAULT null
		, mov_humidity			DECIMAL(6,4) DEFAULT null
		, mov_sunshine			DECIMAL(6,4) DEFAULT null
		, mov_rainfall			DECIMAL(6,4) DEFAULT null)            ;
            commit


SELECT 	
			district
		  , weather_date
		  , period
		  , avg(max_humidity) OVER (PARTITION BY district, period
									ORDER BY district, period, weather_date
									ROWS 3 PRECEDING) 			AS avg_humid
		  , avg(avg_period_temp) OVER (PARTITION BY district, period
									ORDER BY district, period, weather_date
									ROWS 3 PRECEDING) 			AS avg_temp
		  , avg(max_cloud_cover) OVER (PARTITION BY district, period
									ORDER BY district, period, weather_date
									ROWS 3 PRECEDING) 			AS avg_cloud
		  , avg(max_sunshine) OVER (PARTITION BY district, period
									ORDER BY district, period, weather_date
									ROWS 3 PRECEDING) 			AS avg_sunshine
		  , avg(rainfall) OVER (PARTITION BY district, period
									ORDER BY district, period, weather_date
									ROWS 3 PRECEDING) 			AS avg_rainfall
INTO #t1
    FROM WEATHER_JOIN_TEST


UPDATE WEATHER_JOIN_TEST
SET mov_avg_temp	= v.avg_temp,
	mov_cloud_cover	= v.avg_cloud,
	mov_humidity	= v.avg_humid,
	mov_sunshine	= v.avg_sunshine,
	mov_rainfall	= v.avg_rainfall
FROM 	WEATHER_JOIN_TEST as a
JOIN #t1 as v		
      ON a. district = v.district AND a.weather_date = v.weather_date AND a.period = v.period;
    commit