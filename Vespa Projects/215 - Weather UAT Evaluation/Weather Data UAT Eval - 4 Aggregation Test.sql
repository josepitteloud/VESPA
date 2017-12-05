----------------------------- 9 to 18 hrs aggregates
SELECT
		-- Time & District fields
	CAST(DATEROUND (dd, date_time) AS DATE) date_1,
	district,
		-- Weather fields
	MAX(temperature) 					Max_Temp_range,
	MIN(temperature) 					Min_Temp_range,
	AVG(temperature) 					Average_Temp_range,
	AVG(wind_speed)     				Average_wind,
	MAX(CASE 	WHEN wind_speed < 1 THEN 0
			WHEN wind_speed BETWEEN  1 AND 3 THEN 1
			WHEN wind_speed BETWEEN  4 AND 6 THEN 2
			WHEN wind_speed BETWEEN  7 AND 3 THEN 3
			WHEN wind_speed BETWEEN  11 AND 16 THEN 4
			WHEN wind_speed BETWEEN  17 AND 21 THEN 5
			WHEN wind_speed BETWEEN  22 AND 27 THEN 6
			WHEN wind_speed BETWEEN  28 AND 33 THEN 7
			WHEN wind_speed BETWEEN  34 AND 40 THEN 8
			WHEN wind_speed BETWEEN  41 AND 47 THEN 9
			WHEN wind_speed BETWEEN  48 AND 55 THEN 10
			WHEN wind_speed BETWEEN  56 AND 63 THEN 11
			WHEN wind_speed > 64 THEN 12 END) 	Wind_cat    
INTO WEATHER_AGGREGATE_9_18
FROM sk_uat.WEATHER_DATA_history
WHERE DATEPART(hh, date_time) BETWEEN 9 AND 18 
GROUP BY 
	date_1,
	district
----------------------------- Daily aggregates
SELECT
		-- Time & District fields
	CAST(DATEROUND (dd, date_time) AS DATE) 	date_1,
	district,
		-- Weather fields
	MAX(COALESCE(maximum_temperature_daily, temperature)) 			Max_Temp,
	MIN(COALESCE(minimum_temperature_daily, temperature)) 			Min_Temp,
	MAX(sunshine_duration)					Sunshine,
	SUM(ISNULL(rainfall,0))       			total_rainfall
INTO WEATHER_AGGREGATE_Daily
FROM sk_uat.WEATHER_DATA_history
GROUP BY 
	date_1,
	district	 
----------------------------- Rainfall 1
SELECT
		-- Time & District fields
  CAST(DATEROUND (dd, date_time) AS DATE) 	date_1,
  district,
		-- Weather fields
	SUM(ISNULL(rainfall,0))       			rainfall_1,
	SUM(ISNULL(rainfall,0))/18     			avg_rainfall_1
INTO WEATHER_AGGREGATE_rainfall_1
FROM sk_uat.WEATHER_DATA_history
WHERE DATEPART(hh, date_time) BETWEEN 6 AND 18 
GROUP BY 
	  date_1,
	  district	 	  
----------------------------- Rainfall 2
SELECT
		-- Time & District fields
  CAST(DATEROUND (dd, date_time) AS DATE) 	date_1,
  district,
		-- Weather fields
	SUM(ISNULL(rainfall,0))       			rainfall_2,
	SUM(ISNULL(rainfall,0))/12     			avg_rainfall_2
INTO WEATHER_AGGREGATE_rainfall_2
FROM sk_uat.WEATHER_DATA_history
WHERE DATEPART(hh, date_time) BETWEEN 12 AND 18 
GROUP BY 
	  date_1,
	  district	 	  
	  	  
----------------------------- SNOW dump table
SELECT
		-- Time & District fields
	CAST(DATEROUND (dd, date_time) AS DATE) date_1,
	district,
	CASE   	WHEN DATEPART(hh, date_time) BETWEEN 1 AND 6 THEN '1'
			WHEN DATEPART(hh, date_time) BETWEEN 7 AND 12 THEN '2'
			WHEN DATEPART(hh, date_time) BETWEEN 13 AND 18 THEN '3'
			WHEN DATEPART(hh, date_time) BETWEEN 19 AND 24 THEN '4'
			END								date_part,
  		-- Snow
	MIN(temperature) 						min_temp,
	SUM(ISNULL(rainfall,0)) 				rainfall,
	SUM(CASE WHEN weather_type in (20,22,23,26,36,37,38,39,68,69,70,71,72,73,74,75,76,77,78,79,83,84,85,86,87,88,90,93,94,95,97) 
				THEN 1 ELSE 0 END) 			Snow_flag
INTO WEATHER_AGGREGATE_Snow_temp
FROM sk_uat.WEATHER_DATA_history	 
GROUP BY 
	date_1,
	district,
    date_part
----------------------------- Snow 1	
SELECT 	
	date_1,
	district,
	SUM(CASE WHEN 	(date_part BETWEEN '2' AND '3') AND 
					(min_temp <= -1) 				AND 	
					(Snow_flag > 0) 				AND 
					(rainfall > 0.1)  
				THEN rainfall ELSE 0 END) Snow_1,
	SUM(CASE WHEN 	(min_temp <= -1) 			AND 	
					(Snow_flag > 0) 			AND 
					(rainfall > 0.1)  
				THEN rainfall ELSE 0 END) Snow_2,
	SUM(CASE WHEN 	(date_part = '1')			AND 
					(min_temp <= -1) 			AND 	
					(Snow_flag > 0) 			AND 
					(rainfall > 0.1)  
				THEN rainfall ELSE 0 END) Snow_3
INTO WEATHER_AGGREGATE_Snow
FROM WEATHER_AGGREGATE_Snow_temp
GROUP BY 
	date_1,
	district		
	  
------------------------- CONSOLIDATED TABLE
SELECT 	
	d.date_1,
	d.district,
	d.Max_Temp,
	d.Min_Temp,
	d.Sunshine,
	d.total_rainfall,
	b.Max_Temp_range,
	b.Min_Temp_range,
	b.Average_Temp_range,
	b.Average_wind,
	b.Wind_cat,
	r.rainfall_1,
	r.avg_rainfall_1,		
	t.rainfall_2,
	t.avg_rainfall_2,
	s.Snow_1,
	s.Snow_2
INTO WEATHER_AGGREGATE
FROM 		WEATHER_AGGREGATE_Daily			AS d
LEFT JOIN	WEATHER_AGGREGATE_9_18 			AS b ON d.date_1 = b.date_1 AND d.district = b. district
LEFT JOIN	WEATHER_AGGREGATE_rainfall_1	AS r ON d.date_1 = r.date_1 AND d.district = r. district
LEFT JOIN	WEATHER_AGGREGATE_rainfall_2 	AS t ON d.date_1 = t.date_1 AND d.district = t. district
LEFT JOIN	WEATHER_AGGREGATE_Snow 			AS s ON d.date_1 = s.date_1 AND d.district = s. district



	  /*		  
min 6:00
max 18:00
rainfall 6:00; 12:00; 18:00; 24:00
sunshine 00:00
Max Temp
Min Temp
Max Temp range
Min Temp range
Average Temp
Sunshine
Total rainfall
Average wind speed
Rainfall 1
Average hourly rainfall 1
Max wind speed
Rainfall 2
Average hourly rainfall 2
Snow 1
Snow 2
*/