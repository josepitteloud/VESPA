/* *****************************


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

--------------------------------------------------------------------------------------------------------------
**Project Name:                                                 Channel Mapping Automation - matching algorithm
		
		Description:
			the aim is to create a process to identify changes in the BARB file and automatically update the channel mapping tables with the changes
				
		Lead: 		
		Coded by: Paolo Menna
	Sections: 
			M01 - LIST OF CHANGES IN BARB - catch the changes in the BARB file comparing previous vs current version of the file
			M02 - CREATE THE HISTORICAL BASKET OF NAMES/STATION_CODE/SERVICE_KEY
			M03 - JOIN THE LIST OF CHANGES IN BARB WITH THE REFERENCE TABLE TO RETRIEVE THE SERVICE_KEY
		
*********************************/

-------------------------------------------------
---------- M01 - LIST OF CHANGES IN BARB
-------------------------------------------------

CREATE OR replace variable @last_barb_date DATE;
SET @last_barb_date = '2015-02-15';

SELECT filename
    , log_station_code
    , log_station_name
    , max(reporting_start_date) start_date
    , max(case when reporting_end_date is null then CAST('2999-12-31' as date) else reporting_end_date END) as end_date
INTO BARB_CHANGES_LIST
FROM BARB_MASTER_FILE_LOG_STATIONS_REPORTING_RECORD
GROUP BY filename, log_station_code, log_station_name
HAVING (end_date >= @last_barb_date AND  end_date < '2999-12-31') OR (start_date >= @last_barb_date AND  start_date < '2999-12-31')
ORDER by 1,2
;
-- ###UPDATE @last_barb_date WITH THE CURRENT BARB DATE FOR THE NEXT RUN
SET @last_barb_date = (SELECT CAST(RIGHT(LEFT(filename,9),8) as DATE) as last_barb_date FROM BARB_MASTER_FILE_LOG_STATIONS_REPORTING_RECORD GROUP BY last_barb_date)


------------------------------------------------------------------------------------
---------- M02 - CREATE THE REFERENCE_TABLE BASKET OF NAMES/STATION_CODE/SERVICE_KEY
------------------------------------------------------------------------------------

SELECT full_name
	, service_key
	, log_station_code
	, log_station_name
	, reporting_start_date
	, effective_from
	, effective_to
INTO REFERENCE_TABLE
FROM (
	SELECT full_name
		, a.service_key
		, a.log_station_code
		, log_station_name
		, reporting_start_date
		, c.effective_from
		, c.effective_to
	FROM vespa_analysts.channel_map_prod_service_key_barb a													-- this table contains the mapping between service_key and log_station_code
	JOIN BARB_MASTER_FILE_LOG_STATIONS_REPORTING_RECORD b ON a.log_station_code = b.log_station_code		-- retrieve all the barb names by log station_code
	JOIN channel_map_dev_service_key_attributes c ON a.service_key = c.service_key							-- retrieve all the epg full_name by service_key
	WHERE reporting_start_date BETWEEN c.effective_from
			AND c.effective_to
	) f
GROUP BY full_name
	, service_key
	, log_station_code
	, log_station_name
	, reporting_start_date
	, effective_from
	, effective_to



----------------------------------------------------------------------------------------------------------------------------------------
---------- M03 - JOIN THE LIST OF CHANGES IN BARB BARB_CHANGES_LIST WITH THE REFERENCE TABLE REFERENCE_TABLE TO RETRIEVE THE SERVICE_KEY
----------------------------------------------------------------------------------------------------------------------------------------
--- M03.1 TERMINATION OR ATTRIBUTE CHANGE

SELECT filename
	, a.log_station_code
	, a.log_station_name
	, start_date
	, end_date
	, full_name
	, service_key
	, effective_from
	, effective_to
	, CASE WHEN end_date < '2199-12-31' THEN 'termination'
		ELSE 'attribute' END as action
INTO BARB_ATTRIBUTE_CHANGES
FROM BARB_CHANGES_LIST a
LEFT JOIN REFERENCE_TABLE b ON a.log_station_name = b.log_station_name
WHERE service_key IS NOT NULL

-------------------- ACTION: TERMINATION

UPDATE ###SERVICE_KEY_BARB_ATTRIBUTES###
SET 
SELECT * FROM BARB_ATTRIBUTE_CHANGES
WHERE action = 'termination'


UPDATE ###SERVICE_KEY_ATTRIBUTES### b
SET activex = 'N'
	, effective_to = end_date
	, barb_reported = 'NO'
	, amend_date = today()
from BARB_ATTRIBUTE_CHANGES a
WHERE action = 'termination'
and a.service_key = b.service_key
and activex = 'Y'
and barb_reported = 'YES'



--- M03.2 NEW BARB REPORTED CHANNEL

SELECT filename
	, log_station_code
	, log_station_name
	, start_date
	, end_date
	, full_name
	, service_key
	, effective_from
	, effective_to
INTO BARB_NEW_REPORTED_CHANNEL
FROM BARB_CHANGES_LIST a
LEFT JOIN REFERENCE_TABLE b ON a.log_station_name = b.log_station_name
WHERE service_key IS NULL


--------------- STRING ALGORITHM

DROP TABLE IF EXISTS matching
CREATE TABLE matching (log_station_name varchar(50), full_name varchar(50), matching_type varchar(20))


INSERT INTO matching
WITH a
AS (
    SELECT log_station_name
    FROM BARB_NEW_REPORTED_CHANNEL
    WHERE log_station_name NOT IN (select log_station_name FROM matching)
    GROUP BY log_station_name
    )
    , b
AS (
    SELECT full_name
    FROM channel_map_dev_service_key_attributes a
    WHERE full_name NOT IN (select full_name FROM matching)
    GROUP BY full_name
    )
SELECT *, 'lower_space' as matching_type

FROM a
    , b
WHERE lower(str_replace(b.full_name, ' ', NULL)) = lower(str_replace(a.log_station_name, ' ', NULL))

------ 306 Row(s) affected


/*
CREATE TABLE barb_names (
    log_station_name VARCHAR(30)
    , chain VARCHAR(3)
    )

CREATE TABLE full_names (
    full_name VARCHAR(30)
    , chain VARCHAR(3)
    )
*/

DELETE
FROM barb_names

DECLARE @barb_loop INTEGER = 0
DECLARE @max_barb_loop INTEGER

SET @max_barb_loop = (
        SELECT max(len(str_replace(log_station_name, ' ', NULL))) AS max_
        FROM BARB_NEW_REPORTED_CHANNEL a
        )

WHILE @barb_loop < @max_barb_loop
BEGIN
    --declare @loop_ integer = 0
    INSERT INTO barb_names (
        log_station_name
        , chain
        )
    SELECT log_station_name
        , left(right(lower(str_replace(log_station_name, ' ', NULL)), len(str_replace(log_station_name, ' ', NULL)) - @barb_loop), 3) AS chain
    FROM BARB_NEW_REPORTED_CHANNEL a
    WHERE len(str_replace(log_station_name, ' ', NULL)) >  @barb_loop + 2
        AND log_station_name NOT IN (select log_station_name FROM matching)
    GROUP BY log_station_name

    SET @barb_loop = @barb_loop + 1
END

-----------------------
------------- EPG table
-----------------------

DELETE
FROM full_names

DECLARE @epg_loop INTEGER = 0
DECLARE @max_epg_loop INTEGER

SET @max_epg_loop = (
        SELECT max(len(str_replace(full_name, ' ', NULL))) AS max_
        FROM channel_map_dev_service_key_attributes a
        )

WHILE @epg_loop < @max_epg_loop
BEGIN
    --declare @loop_ integer = 0
    INSERT INTO full_names (
        full_name
        , chain
        )
    SELECT full_name
        , left(right(lower(str_replace(full_name, ' ', NULL)), len(str_replace(full_name, ' ', NULL)) - @epg_loop), 3) AS chain
    FROM channel_map_dev_service_key_attributes a
    WHERE len(str_replace(full_name, ' ', NULL)) > @epg_loop + 2
        AND full_name NOT IN (SELECT full_name FROM matching)
    GROUP BY full_name

    SET @epg_loop = @epg_loop + 1
END

-------------------------------------------------------
----------- SCORING COMPUTATION - Rank matching
-------------------------------------------------------

DROP TABLE IF EXISTS scoring_table

SELECT *
    , COUNT() OVER (PARTITION BY log_station_name, rankk) as count_log_station
    , COUNT() OVER (PARTITION BY full_name, rankk_) as count_full_name
INTO scoring_table
FROM (
    SELECT log_station_name
        , full_name
        , chain_
        , count_
        , 1.00*count_^2*len(str_replace(full_name, ' ', NULL))
                /(abs(len(str_replace(full_name, ' ', NULL))-len(str_replace(log_station_name, ' ', NULL)))+1) as score
        , rank() OVER (PARTITION BY log_station_name ORDER BY score  desc) AS rankk
        , rank() OVER (PARTITION BY full_name ORDER BY score desc) AS rankk_
    FROM (
        SELECT *
            , a.chain AS chain_
            , count() OVER (PARTITION BY log_station_name, full_name) AS count_
        FROM barb_names a
        LEFT JOIN full_names b ON a.chain = b.chain
        WHERE full_name IS NOT NULL
            AND log_station_name IS NOT NULL
        ) d
    ) f
WHERE rankk = 1
and rankk_ = 1

----------- MATCHING ESTIMATION

SELECT a.log_station_name
        , a.full_name
        , reporting_start_date
        , reporting_end_date
        , service_key
        , log_station_code
FROM scoring_table a
join BARB_MASTER_FILE_LOG_STATIONS_REPORTING_RECORD b on a.log_station_name = b.log_station_name
join channel_map_dev_service_key_attributes c on a.full_name = c.full_name
where activex = 'Y'

-----------






























