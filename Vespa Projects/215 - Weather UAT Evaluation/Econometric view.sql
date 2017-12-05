/*****************************************************************************************
*****			Weather data for HB project	Variable name
*****			Weekly National Highest Max Temperature						--------------		Highest_Max_TemperatureC
*****			Weekly National Highest Mean Temperature					--------------		Highest_Mean_TemperatureC
*****			Weekly National Highest Min Temperature						--------------		Highest_Min_TemperatureC
*****			Weekly National Lowest Max Temperature						--------------		Lowest_Max_TemperatureC
*****			Weekly National Lowest Mean Temperature						--------------		Lowest_Mean_TemperatureC
*****			Weekly National Min Temperature								--------------		Lowest_Min_TemperatureC
*****			Weekly National Highest Rainfall							--------------		Highest_Rainfall
*****			Weekly National Mean Rainfall								--------------		Mean_Rainfall
*****			Weekly National Highest Snowfall							--------------		Highest_Snowfall
*****			Weekly National Mean Snowfall								--------------		Mean_Snowfall
*****			Weather Index that you have created							--------------		Weather_Index
PERIOD 1 		00:01 to 06:00
PERIOD 2 		06:01 to 12:00
PERIOD 3 		12:01 to 18:00
PERIOD 4 		18:01 to 24:00
*/


DROP VIEW WEATHER_homebase_raw
DROP VIEW  WEATHER_homebase_weekly
DROP TABLE  Mosaic_weight_district
DROP TABLE WEATHER_SKY_movies_data


---------- CReating Raw data view
CREATE VIEW WEATHER_homebase_raw
AS
SELECT
          DATE(ag.weather_date)                         AS  w_date
        , MIN(ag.min_period_temp)                       AS Min_Temperature
        , MAX(ag.max_period_temp)                       AS Max_Temperature
        , SUM(CASE WHEN ag.snowfall     = 0 THEN ag.rainfall ELSE 0 END)        AS total_rainfall
        , SUM(CASE WHEN ag.snowfall     = 1 THEN ag.rainfall ELSE 0 END)        AS total_snowfall
        , ag.district
        , DATEPART(week, w_date)                AS n_week
        , AVG(CASE weather_score 	WHEN 1 THEN 0
									WHEN 2 THEN 1
									WHEN 3 THEN 3
									WHEN 4 THEN 4.5
									WHEN 5 THEN 5
									ELSE NULL END)                    AS w_score
FROM sk_prod.weather_DATA_AGGREGATED_history    AS ag
WHERE ag.period in('P2','P3','P4')
GROUP BY ag.district, ag.weather_date
COMMIT

-------------- Mosaic weights 
SELECT cb_address_postcode_outcode AS district
        , h_mosaic_uk_group
        , count(DISTINCT cb_key_household) AS hh_count
        , SUM(hh_count) OVER (PARTITION BY h_mosaic_uk_group) as total_group
        , CAST(hh_count AS DECIMAL(6,2))/ total_group AS weight
INTO Mosaic_weight_district
FROM sk_prod.experian_consumerview
GROUP BY district, h_mosaic_uk_group
commit
CREATE HG INDEX if1 ON Mosaic_weight_district(district)
CREATE LF INDEX if2 ON Mosaic_weight_district(h_mosaic_uk_group)
COMMIT

---------------	Consolidated view
SELECT
    w_date
    , n_week
    , h_mosaic_uk_group
    , Min_Temp  = sum(Min_Temperature * weight)
    , Max_Temp  = sum(Max_Temperature * weight)
    , rainfall  = sum(total_rainfall * weight)
    , snowfall  = sum(total_snowfall* weight)
    , score     = sum(w_score * weight)
INTO WEATHER_SKY_movies_data
FROM WEATHER_homebase_raw as a
JOIN Mosaic_weight_district as b ON a.district = b.district
GROUP BY
     w_date
    , n_week
    , h_mosaic_uk_group




CREATE VIEW WEATHER_homebase_weekly
AS
SELECT
        n_week                  AS week_number
        , min(w_date)           AS first_day_of_the_week
        , year(w_date)          AS f_year
        , avg(w_score)          AS weather_index
        , MIN(Min_Temperature)  AS Lowest_Min_temperatureC
        , MAX(Min_Temperature)  AS Highest_Min_temperatureC
        , AVG(Min_Temperature)  AS Mean_Min_temperatureC
        , MIN(Max_Temperature)  AS Lowest_Max_temperatureC
        , MAX(Max_Temperature)  AS Highest_Max_temperatureC
        , AVG(Max_Temperature)  AS Mean_Max_temperatureC
        , MAX(total_rainfall)   AS Highest_Rainfall
        , AVG(total_rainfall)   AS Mean_Rainfall
        , MAX(total_snowfall)   AS Highest_Snowfall
        , AVG(total_snowfall)   AS Mean_Snowfall
FROM WEATHER_homebase_raw
GROUP BY n_week, f_year
COMMIT

/*
SELECT 
	, min(ag.max_period_temp) 			AS Highest_Min_TemperatureC
	, MAX(ag.min_period_temp)			AS Highest_Max_TemperatureC			
	, avg(da.temperature) 		AS avg_period_temp
	, ag.period_to	
	, ag.rainfall	
	, CASE WHEN ag.snowfall	= 1 THEN Rainfall
	, ag.weather_date
  , min(date_time) min_datetime
  , max(date_time) max_datetime
INTO WEATHER_homebase_raw
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
*/