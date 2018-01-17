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

CREATE OR REPLACE VIEW weather_agg_fix_sara AS 
SELECT 
	  a.district
	, max(max_period_temp) max_temp
	, max(min_period_temp) min_temp
	, MIN(max_period_temp) min_max_temp
	, MIn(min_period_temp) min_min_temp
	, period
	, period_from	
	, period_to
	, max(rainfall) m_rainfall
	, max(snowfall) m_snowfall
	, weather_date
	, min(weather_score) m_score 
	, max(sunshine_duration) sun
FROM weather_data_aggregated_history AS a
LEFT JOIN (SELECT sunshine_duration, DATE(date_time) dt, district FROM weather_data_history WHERE sunshine_duration IS NOT NULL) AS b ON a.district = b.district AND DATE(weather_date) = dt AND period = 'P2'
GROUP BY 
	  a.district
	, period
	, period_from	
	, period_to
	, weather_date
COMMIT

---------- CReating Raw data view

CREATE OR REPLACE VIEW Weather_region_view AS
SELECT barb_desc_itv itv_region
        ,cb_address_postcode_outcode district
        , count(cb_address_postcode) postcodes
FROM BARB_TV_REGIONS
GROUP BY  itv_region, itv_region, district



CREATE OR REPLACE  VIEW WEATHER_raw_sara
AS
SELECT
          DATE(ag.weather_date)                         AS  w_date
        , MIN(ag.min_temp)                       AS Min_Temperature
        , MAX(ag.max_temp)                       AS Max_Temperature
        , AVG(CASE WHEN ag.m_snowfall     = 0 THEN ag.m_rainfall ELSE 0 END)        AS total_rainfall
        , AVG(CASE WHEN ag.m_snowfall     = 1 THEN ag.m_rainfall ELSE 0 END)        AS total_snowfall
        , b.itv_region
        , DATEPART(week, w_date)                AS n_week
        , AVG(CASE m_score                      WHEN 1 THEN 0
                                                                        WHEN 2 THEN 1
                                                                        WHEN 3 THEN 3
                                                                        WHEN 4 THEN 4.5
                                                                        WHEN 5 THEN 5
                                                                        ELSE NULL END)                    AS w_score
FROM weather_agg_fix_sara    AS ag
JOIN Weather_region_view AS b ON ag.district = b.district
WHERE ag.period in('P2','P3','P4')
GROUP BY b.itv_region, ag.weather_date
COMMIT



CREATE OR REPLACE  VIEW WEATHER_weekly_Sara
AS
SELECT
        n_week                  AS week_number
        , itv_region
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
FROM WEATHER_raw_sara
GROUP BY n_week, f_year,itv_region
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