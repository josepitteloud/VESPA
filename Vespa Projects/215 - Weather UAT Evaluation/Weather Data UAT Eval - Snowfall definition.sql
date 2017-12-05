*###############################################################################
# Created on:   24/09/2012
# Created by:   Jose Pitteloud	(JPD)
# Description:  Flag that indicates when snow fall in the past 6 hours
#
#
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 23/09/2012  JPD   v01 - initial version
#
###############################################################################*/






---------------------------------------------------------------------------------
--  Snowfall indicator - Snow fallen in the past 6 hours
---------------------------------------------------------------------------------

------------------------------
-- Aggregating time slots (6 hr)
------------------------------

SELECT 
		-- Time & District fields
	  CAST(date_time as DATE) date_time1					-- Rounding the date
	, district
	, CASE   	WHEN DATEPART(hh, date_time)  BETWEEN 1 AND 6         THEN 1
			      WHEN DATEPART(hh, date_time)  BETWEEN 7 AND 12        THEN 2
			      WHEN DATEPART(hh, date_time)  BETWEEN 13 AND 18       THEN 3
			      WHEN DATEPART(hh, date_time)  in  (19,20,21,22,23,0)  THEN 4
			END								date_part			-- Defining time slot to match rainfall hours
	, MIN(temperature) 						min_temp			-- Defining min temperature of the timeslot
	, SUM(ISNULL(rainfall,0)) 				rainfall			-- Aggregating rainfall
	, SUM(CASE WHEN weather_type in (20,22,23,26,36,37,38,39,68,69,70,71,72,73,74,75,76,77,78,79,83,84,85,86,87,88,90,93,94,95,97)  
				THEN 1 ELSE 0 END) 			Snow_flag 			-- Weather codes related to snowfall
INTO #Snow_temp
FROM sk_uat.WEATHER_DATA_HISTORY
GROUP BY 
	  district
	, date_part
	, date_time1
----------------------------
-- Flagging the time slots
----------------------------	
		
SELECT 	
		district
	, 	DATEADD(hh, date_part * 6 , CAST(date_time1 AS DATETIME)) Datetime		-- Rebuilding Datetime 
	, 	1	'Snowfall_flag' 
FROM #Snow_temp
WHERE	(min_temp <= -1) 			
	AND (Snow_flag > 0)
	AND	(rainfall > 0.1)  
	

--------------------------
-- The results can be joined to the raw data table using district and datetime fields
--------------------------	