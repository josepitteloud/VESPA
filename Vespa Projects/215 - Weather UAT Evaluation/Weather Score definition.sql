/*
 
 
                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES

-----------------------------------------------------------------------------------

**Project Name:							WEATHER INDEX Development
**Analysts:                             Jose Pitteloud ,jose.pitteloud@skyiq.co.uk>
**Lead(s):                              Jose Pitteloud ,jose.pitteloud@skyiq.co.uk>
**Stakeholder:                          Data Strategy & Development
**Due Date:                             16/12/2013
**Project Code (Insight Collation):     V215 - Weather UA Evaluation
**SharePoint Folder:                    http://sp-department.bskyb.com/sites/SIGEvolved/Shared Documents/01 Analysis Requests/V215 - Weather Data Evaluation/

                                                                        
**Business Brief:						Currently we have in place a historic weather data feed, providing us with post-code level 6-hourly weather readings across the UK
										There is a need to use this data in building and refining existing econometric models, and furthermore to investigate the extent to which the weather is correlated to viewing data.  
										Issue:
											-The data is very granular and across many dimensions: wind, rain, snow, temperature, ….
											-Data must be aggregated in order to be used by models and analysis, an index capturing whether the weather is ‘good’ or ‘bad’
											-There is not a universal  standardized way to aggregate the weather data. 
											-Previous research shows weather perception depends on 2 factors; recent weather conditions and severe weather conditions.
										Solution: We proposed an index based on 3 components. The index will take scores from 1 – Very bad weather to 5 – Very Good weather
										The script will generate a table "WEATHER_JOIN_TEST" where the index is calculated, however, the intention is to include the index in the aggregated production weather tables. 


**Sections:
		- Section A - Data Preparation
			- A00 - Creating table with data at hourly level and at 6-hour level
			- A01 - Filling blanks. Sunshine is reported once a day but needs to be fully populated for the model
			- A02 - ADDING Calculation variables
		- Section B - Calculations
			- B00 - CALCULATING ABSOLUTE WEATHER INDEX 1, based on bad weather
			- B01 - Calculating moving Averages
			- B02 - Calculating Z-scores 
			- B03 - Defining Coefficient 
			- B04 - Calculating Weather Score 2
			- B05 - Calculating Pre-score 1
			- B06 - Final Score calculation
*/

---------------- Section A - Data Preparation
-- A00 - Creating table with data at hourly level and at 6-hour level
SELECT 
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
										AND HOUR(da.date_time) BETWEEN HOUR(CAST(period_from AS DATETIME)) 
															and CASE WHEN period = 'P4' THEN 23 ELSE HOUR(DATEADD(minute, -1, CAST(period_to AS DATETIME))) eND
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

-- A01 - Filling blanks. Sunshine is reported once a day but needs to be fully populated for the model
UPDATE	WEATHER_JOIN_TEST
SET a.max_sunshine = v.max_sunshine
FROM WEATHER_JOIN_TEST as a 
INNER JOIN (SELECT 
				  district
				, max_sunshine
				, weather_date
			FROM WEATHER_JOIN_TEST
			WHERE max_sunshine is not null) as v ON a.district = v.district AND a.weather_date = v.weather_date 
WHERE 	a.max_sunshine is null		

COMMIT
-- A02 - ADDING Calculation variables

ALTER TABLE   WEATHER_JOIN_TEST
ADD (
	  ID int IDENTITY
	, W_index_1 int
	, W_index_2 int
	, W_index_3 int
	, mov_avg_temp			DECIMAL(7,4) DEFAULT null
	, mov_cloud_cover		DECIMAL(6,4) DEFAULT null
	, mov_humidity			DECIMAL(6,4) DEFAULT null
	, mov_sunshine			DECIMAL(6,4) DEFAULT null
	, mov_rainfall			DECIMAL(6,4) DEFAULT null
	, mov_t_chg				DECIMAL(7,4) DEFAULT null
	, mov_c_chg				DECIMAL(7,4) DEFAULT null
	, mov_h_chg				DECIMAL(7,4) DEFAULT null
	, mov_s_chg				DECIMAL(7,4) DEFAULT null
	, weather_score int default 3
	)
	
COMMIT
---------------- Section B - Calculations
-- B00 - CALCULATING ABSOLUTE WEATHER INDEX 1, based on bad weather 
UPDATE   WEATHER_JOIN_TEST
SET W_index_1 = CASE 	WHEN (max_windspeed > 25 OR rainfall>3 OR (snowfall =1 AND rainfall>0.2)) THEN 1  
						WHEN (rainfall>1.5 OR snowfall =1) THEN 2
						ELSE 3 END						
COMMIT						
						
-- B01 - Calculating moving Averages
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
--------------------------------	
UPDATE WEATHER_JOIN_TEST
SET mov_avg_temp	= v.avg_temp,
	mov_cloud_cover	= v.avg_cloud,
	mov_humidity	= v.avg_humid,
	mov_sunshine	= v.avg_sunshine,
	mov_rainfall	= v.avg_rainfall
FROM 	WEATHER_JOIN_TEST as a
JOIN #t1 as v		
      ON a. district = v.district AND a.weather_date = v.weather_date AND a.period = v.period

COMMIT

-- B02 - Calculating Z-scores  			
UPDATE WEATHER_JOIN_TEST
SET   mov_t_chg = (avg_period_temp - mov_avg_temp) 		/ 6
	, mov_c_chg = (mov_cloud_cover - max_cloud_cover) 	/ 5
	, mov_h_chg = (mov_humidity - max_humidity) 		/ 17
	, mov_s_chg = (max_sunshine - mov_sunshine) 		/ 10
	, mov_r_chg = (rainfall - mov_rainfall)				/ 3
commit;

-- B03 - Defining Coefficient 
DECLARE @s0 decimal (3,2) ,@t0 decimal (3,2) ,@c0 decimal (3,2) ,@h0 decimal (3,2), @r0 decimal (3,2) 
SELECT 	  @t0 = 1 
		, @c0 = 0.5
		, @h0 = 0.7
		, @s0 = 0.7
		, @r0 = 0.8

-- B04 - Calculating Weather Score 2
UPDATE WEATHER_JOIN_TEST
SET W_index_2 =  CEILING( ((mov_t_chg*@t0)+(mov_c_chg*@c0)+(mov_h_chg*@h0)+(mov_s_chg*@s0)-(@r0*mov_r_chg))*100/15)*15

-- B05 - Calculating Pre-score 1
UPDATE WEATHER_JOIN_TEST
SET W_index_3 = CASE WHEN 	W_index_2 <-175 THEN 1
					 WHEN   W_index_2 <-75  THEN 2
					 WHEN   W_index_2 <0 	THEN 3
   					 WHEN   W_index_2 <75 	THEN 4
							ELSE 5 END

COMMIT

-- B06 - Final Score calculation
UPDATE WEATHER_JOIN_TEST
SET weather_score = CASE WHEN W_index_1 in (1,2) THEN W_index_1 ELSE W_index_3 END 

COMMIT 

	
	
	
	
	
	
	
	
	
	
	
	
