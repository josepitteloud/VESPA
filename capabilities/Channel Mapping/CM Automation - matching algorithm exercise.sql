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
**Project Name:                                                 Channel Mapping Automation - Matching names exercise
		
		Description:
			First draft of the script to map BARB and EPG Names based on names
				
		Lead: 		
		Coded by: Paolo Menna
	Sections: 
			01 - Straight match between BARB and EPG Name - Lower key and spaces removed
			02 - Prepare BARB and EPG tables for the string matching
		
*********************************/
/*
create table BSS_names
	(epg_name	varchar(30)
	)

commit;

LOAD TABLE BSS_names (
    epg_name
 '\n' )
FROM '/ETL013/prod/sky/olive/data/share/clarityq/export/JosEP/names.csv' QUOTES OFF ESCAPES OFF NOTIFY 1000 DELIMITED BY ','-- START ROW ID 1
;
commit;
*/

----- 01 MATCHING TABLE 

CREATE TABLE matching (log_station_name varchar(50), epg_name varchar(50))

-- MATCHING TABLE WITH MANUAL MATCHING DATA


-- a and b ARE THE TWO TABLES WITH THE NAMES TO BE MATCHED!!

INSERT INTO matching
WITH a
AS (
	SELECT log_station_name
	FROM BARB_MASTER_FILE_LOG_STATIONS_REPORTING_RECORD
	WHERE log_station_name NOT IN (select log_station_name FROM matching)
	GROUP BY log_station_name
	)
	, b
AS (
	SELECT epg_name
	FROM channel_map_dev_service_key_attributes
	WHERE epg_name NOT IN (select epg_name FROM matching)
	GROUP BY epg_name
	)
SELECT *, 'lower_space' as matching_type

FROM a
	, b
WHERE lower(str_replace(b.epg_name, ' ', NULL)) = lower(str_replace(a.log_station_name, ' ', NULL))

------ 306 Row(s) affected 

----------------------------------------------------------------------
------------ 02 - Prepare BARB and EPG tables for the string matching
----------------------------------------------------------------------

/*
CREATE TABLE barb_names (
	log_station_name VARCHAR(30)
	, chain VARCHAR(3)
	)

CREATE TABLE epg_names (
	epg_name VARCHAR(30)
	, chain VARCHAR(3)
	)
*/
-------------------------
------------- BARB table
-------------------------

DELETE
FROM barb_names

DECLARE @barb_loop INTEGER = 0
DECLARE @max_barb_loop INTEGER

SET @max_barb_loop = (
        SELECT max(len(str_replace(log_station_name, ' ', NULL))) AS max_
        FROM BARB_MASTER_FILE_LOG_STATIONS_REPORTING_RECORD
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
    FROM BARB_MASTER_FILE_LOG_STATIONS_REPORTING_RECORD
    WHERE len(str_replace(log_station_name, ' ', NULL)) >  @barb_loop + 2
        AND log_station_name NOT IN (select log_station_name FROM matching)
    GROUP BY log_station_name

    SET @barb_loop = @barb_loop + 1
END

-----------------------
------------- EPG table
-----------------------

DELETE
FROM epg_names

DECLARE @epg_loop INTEGER = 0
DECLARE @max_epg_loop INTEGER

SET @max_epg_loop = (
        SELECT max(len(str_replace(epg_name, ' ', NULL))) AS max_
        FROM channel_map_dev_service_key_attributes
        )

WHILE @epg_loop < @max_epg_loop
BEGIN
    --declare @loop_ integer = 0
    INSERT INTO epg_names (
        epg_name
        , chain
        )
    SELECT epg_name
        , left(right(lower(str_replace(epg_name, ' ', NULL)), len(str_replace(epg_name, ' ', NULL)) - @epg_loop), 3) AS chain
    FROM channel_map_dev_service_key_attributes
    WHERE len(str_replace(epg_name, ' ', NULL)) > @epg_loop + 2
        AND epg_name NOT IN (SELECT epg_name FROM matching)
    GROUP BY epg_name

    SET @epg_loop = @epg_loop + 1
END

-------------------------------------------------------
----------- SCORING COMPUTATION - Rank matching
-------------------------------------------------------

DROP TABLE IF EXISTS scoring_table

SELECT *
	, COUNT() OVER (PARTITION BY log_station_name, rankk) as count_log_station
	, COUNT() OVER (PARTITION BY epg_name, rankk_) as count_epg_name
INTO scoring_table
FROM (
    SELECT log_station_name
        , epg_name
        , chain_
        , count_
        , 1.00*count_*len(str_replace(epg_name, ' ', NULL))
                /(abs(len(str_replace(epg_name, ' ', NULL))-len(str_replace(log_station_name, ' ', NULL)))+1) as score
        , rank() OVER (PARTITION BY log_station_name ORDER BY score  desc) AS rankk
        , rank() OVER (PARTITION BY epg_name ORDER BY score desc) AS rankk_
    FROM (
        SELECT *
            , a.chain AS chain_
            , count() OVER (PARTITION BY log_station_name, epg_name) AS count_
        FROM barb_names a
        LEFT JOIN epg_names b ON a.chain = b.chain
        WHERE epg_name IS NOT NULL
--            AND epg_name LIKE 'BBC NEWS%'
            AND log_station_name IS NOT NULL
        ) d
        where count_ > len(str_replace(epg_name, ' ', NULL)) -4
    ) f
WHERE rankk = 1
and rankk_ = 1






	
	
	
	
	
	
	
	
	
	
	