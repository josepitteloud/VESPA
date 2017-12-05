
---------- Creating working table structure
IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = UPPER('V289_M13_midnight_split_working_table'))   
BEGIN
DROP TABLE V289_M13_midnight_split_working_table
END 


CREATE TABLE V289_M13_midnight_split_working_table 
	(row_id				 BIGINT IDENTITY
	,SUBSCRIBER_ID 		decimal (10,0)
	,ACCOUNT_NUMBER 		varchar (20)
	,STB_BROADCAST_START_TIME timestamp 
	,STB_BROADCAST_END_TIME timestamp 
	,STB_EVENT_START_TIME 	timestamp 
	,STB_EVENT_END_TIME		timestamp
	,TIMESHIFT 				int 
	,service_key 			int 
	,Platform_flag 			int 
	,Original_Service_key 	int 
	,AdSmart_flag 			int 
	,DTH_VIEWING_EVENT_ID bigint 
	,person_1 		smallint ,person_2 		smallint 
	,person_3 		smallint ,person_4 		smallint 
	,person_5 		smallint ,person_6 		smallint 
	,person_7 		smallint ,person_8 		smallint 
	,person_9 		smallint ,person_10 		smallint 
	,person_11 		smallint ,person_12 		smallint 
	,person_13 		smallint ,person_14 		smallint 
	,person_15 		smallint ,person_16 		smallint 
	,Proc_flag 		tinyint			DEFAULT 0 -- 0 unprocessed; 1-8 partial processed; 9 full processed
	,duration		INTEGER			DEFAULT 0 -- in minutes	
	,split_type 	tinyint 		DEFAULT 0 -- 0: unassigned, 1: only event time span,  2: only broadcast time span, 3:both times span
	,Event_Rank 	tinyint			DEFAULT 0 -- 1 original event; 2 first split, 3 third split (only for split_type 3)
	, MA_flag		tinyint			DEFAULT 0
	)
	
COMMIT 
CREATE DTTM INDEX id1 ON V289_M13_midnight_split_working_table(STB_BROADCAST_START_TIME)
CREATE DTTM INDEX id2 ON V289_M13_midnight_split_working_table(STB_BROADCAST_END_TIME)
CREATE DTTM INDEX id3 ON V289_M13_midnight_split_working_table(STB_EVENT_START_TIME)
CREATE DTTM INDEX id4 ON V289_M13_midnight_split_working_table(STB_EVENT_END_TIME)
CREATE HG 	INDEX id5 ON V289_M13_midnight_split_working_table(duration)
CREATE HG 	INDEX id6 ON V289_M13_midnight_split_working_table(DTH_VIEWING_EVENT_ID)
CREATE LF  	INDEX id7 ON V289_M13_midnight_split_working_table(TIMESHIFT)
CREATE HG 	INDEX id8 ON V289_M13_midnight_split_working_table(ACCOUNT_NUMBER)
COMMIT

TRUNCATE TABLE V289_M13_midnight_split_working_table

--- INSERTING SPANNING EVENTS (Live and recorded)
INSERT INTO V289_M13_midnight_split_working_table (
	SUBSCRIBER_ID,ACCOUNT_NUMBER
	,STB_BROADCAST_START_TIME
	,STB_BROADCAST_END_TIME
	,STB_EVENT_START_TIME
	,TIMESHIFT,service_key,Platform_flag
	,Original_Service_key,AdSmart_flag,DTH_VIEWING_EVENT_ID
	,person_1,person_2,person_3,person_4,person_5,person_6
	,person_7,person_8,person_9,person_10,person_11,person_12
	,person_13,person_14,person_15,person_16, MA_flag, duration,STB_EVENT_END_TIME, split_type, Event_Rank)
SELECT  
	SUBSCRIBER_ID,ACCOUNT_NUMBER
	,STB_BROADCAST_START_TIME
	,STB_BROADCAST_END_TIME
	,STB_EVENT_START_TIME
	,TIMESHIFT,service_key,Platform_flag,Original_Service_key
	,AdSmart_flag,DTH_VIEWING_EVENT_ID,person_1,person_2,person_3,person_4,person_5,person_6
	,person_7,person_8,person_9,person_10,person_11,person_12
	,person_13,person_14,person_15,person_16
	, CASE 	
			WHEN TIMESHIFT IN ( 0, 1)
				AND DATEPART(MINUTE, STB_BROADCAST_START_TIME) = DATEPART(MINUTE, STB_EVENT_START_TIME) 
				AND DATEPART(HOUR, STB_BROADCAST_START_TIME) = DATEPART(HOUR, STB_EVENT_START_TIME) 
				THEN 1 
			WHEN 60 - DATEPART (ss, STB_EVENT_START_TIME) >29 THEN 1 
			ELSE 0 END																	--------------------- MA rules to adjust the splitting point
	, duration = DATEDIFF(MINUTE, STB_BROADCAST_START_TIME, STB_BROADCAST_END_TIME)+1	---------- Duration in minutes
	, STB_EVENT_END_TIME = CASE WHEN TIMESHIFT = 0 THEN DATEADD (SECOND, 59, STB_BROADCAST_END_TIME)
							ELSE DATEADD(MINUTE, duration, STB_EVENT_START_TIME)	END				---------- Calculated event end time. Duration is rounded due to minute attribution
	, CASE 	
			WHEN DATE (STB_BROADCAST_START_TIME) = DATE (STB_BROADCAST_END_TIME) 
			AND DATE (STB_EVENT_START_TIME) <> DATE (STB_EVENT_END_TIME)			THEN 1
			WHEN  DATE (STB_EVENT_START_TIME) <> DATE (STB_EVENT_END_TIME) 
			AND TIMESHIFT = 0 														THEN 1 
			WHEN DATE (STB_BROADCAST_START_TIME) <> DATE (STB_BROADCAST_END_TIME) 
			AND DATE (STB_EVENT_START_TIME) <> DATE (STB_EVENT_END_TIME)			THEN 3
			WHEN DATE (STB_BROADCAST_START_TIME) <> DATE (STB_BROADCAST_END_TIME) 
			AND DATE (STB_EVENT_START_TIME) = DATE (STB_EVENT_END_TIME)				THEN 2	
			ELSE 0 END AS split_type
	, 1 AS event_rank
FROM TE_VIEW_APRIL_1																	--------------------- Change to the monthly view 
WHERE 
		((DATE (STB_BROADCAST_START_TIME) <> DATE (STB_BROADCAST_END_TIME)    )			--------------------- BARB Attributed times spanning 
	OR
	(DATE (STB_EVENT_START_TIME) <> DATE (STB_EVENT_END_TIME)	)			)			--------------------- EVENT times spanning 
AND STB_BROADCAST_START_TIME IS NOT NULL 
AND STB_BROADCAST_END_TIME IS NOT NULL


----CREATING a BACKUP table 
SELECT * 
INTO V289_M13_midnight_split_working_table_APRIL_backup
FROM V289_M13_midnight_split_working_table
COMMIT


COMMIT
UPDATE V289_M13_midnight_split_working_table
SET STB_EVENT_END_TIME = DATEADD(SECOND, -1, STB_EVENT_END_TIME) 
WHERE 	DATEPART(HOUR, STB_EVENT_END_TIME) = 0
	AND DATEPART(MINUTE, STB_EVENT_END_TIME) = 0
	AND DATEPART(SECOND, STB_EVENT_END_TIME) = 0
	AND DATEPART(HOUR, STB_BROADCAST_END_TIME) = 23
	AND DATEPART(MINUTE, STB_BROADCAST_END_TIME) = 59
	


UPDATE V289_M13_midnight_split_working_table
SET STB_EVENT_END_TIME = DATEADD (second, 59, STB_EVENT_END_TIME)
, split_type = 1 
WHERE split_type = 2 AND TIMESHIFT = 0 AND DATE(STB_EVENT_START_TIME) = DATE(STB_EVENT_END_TIME)

DELETE FROM V289_M13_midnight_split_working_table
WHERE DATE (STB_BROADCAST_START_TIME) = DATE (STB_BROADCAST_END_TIME)  			--------------------- BARB Attributed times spanning 
	AND 
	DATE (STB_EVENT_START_TIME) = DATE (STB_EVENT_END_TIME)	

------ DUPLICATING event to be split (all the above inserted events are going to be split) - Assigning event_rank = 2 to the duplicates 
INSERT INTO V289_M13_midnight_split_working_table (SUBSCRIBER_ID,ACCOUNT_NUMBER
	,STB_BROADCAST_START_TIME
	,STB_BROADCAST_END_TIME
	,STB_EVENT_START_TIME
	,TIMESHIFT,service_key,Platform_flag
	,Original_Service_key,AdSmart_flag,DTH_VIEWING_EVENT_ID
	,person_1,person_2,person_3,person_4,person_5,person_6
	,person_7,person_8,person_9,person_10,person_11,person_12
	,person_13,person_14,person_15,person_16,duration,STB_EVENT_END_TIME, split_type, event_rank, MA_flag)
SELECT 
	SUBSCRIBER_ID,ACCOUNT_NUMBER
	,STB_BROADCAST_START_TIME
	,STB_BROADCAST_END_TIME
	,STB_EVENT_START_TIME
	,TIMESHIFT,service_key,Platform_flag
	,Original_Service_key,AdSmart_flag,DTH_VIEWING_EVENT_ID
	,person_1,person_2,person_3,person_4,person_5,person_6
	,person_7,person_8,person_9,person_10,person_11,person_12
	,person_13,person_14,person_15,person_16,duration,STB_EVENT_END_TIME, split_type	
	, 2 As event_rank
	, MA_flag
FROM V289_M13_midnight_split_working_table
COMMIT
------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------
-- Splitting events where viewing spanning happen (types 1 and 3)
---- Splitting events times first 
UPDATE V289_M13_midnight_split_working_table
SET STB_EVENT_START_TIME = CASE WHEN event_rank = 1 THEN STB_EVENT_START_TIME 										-------- Original start time
								ELSE  DATEADD(dd, DATEDIFF(dd,'2000-01-01', STB_EVENT_START_TIME)+1, '2000-01-01') 	-------- 00:00:00 next day 
								END 
	,STB_EVENT_END_TIME = CASE  WHEN event_rank = 1 THEN DATEADD(SECOND, -1, DATEADD(dd, DATEDIFF(dd,'2000-01-01', STB_EVENT_START_TIME)+1, '2000-01-01')) -------- 23:59:59 first day 
								ELSE STB_EVENT_END_TIME 															-------- Original end time
								END 
	, duration = DATEDIFF (minute, CASE WHEN event_rank = 1 THEN STB_EVENT_START_TIME 
								ELSE  DATEADD(dd, DATEDIFF(dd,'2000-01-01', STB_EVENT_START_TIME)+1, '2000-01-01')		
								END, 
								CASE  WHEN event_rank = 1 THEN DATEADD(minute, -1, DATEADD(dd, DATEDIFF(dd,'2000-01-01', STB_EVENT_START_TIME)+1, '2000-01-01'))
								ELSE STB_EVENT_END_TIME 
								END)
    , proc_flag = 1 									-------------- Setting proc_flag = 1 to identify the events that have been updated
WHERE split_type  in (1,3)
COMMIT

UPDATE V289_M13_midnight_split_working_table
	SET STB_EVENT_START_TIME = STB_EVENT_END_TIME
WHERE STB_EVENT_END_TIME < STB_EVENT_START_TIME  AND event_rank <> 1
 AND proc_flag = 1

UPDATE V289_M13_midnight_split_working_table
	SET STB_EVENT_END_TIME = STB_EVENT_START_TIME
WHERE STB_EVENT_END_TIME < STB_EVENT_START_TIME  AND event_rank = 1
AND proc_flag = 1

------------------ Updating Broadcast times (BARB Minuted attributed times) 
---------- RULES:
------	1st event (event_rank = 1): START time same as original / END TIME 	when is LIVE and STB_BROADCAST_START_TIME = STB_EVENT_START_TIME then (STB_BROADCAST_START_TIME + duration in minutes)
------																		when is LIVE and the 1st attributed minute happens the next day then STB_BROADCAST_START_TIME (rare case)
------																		when is LIVE ELSE 23:59:00 of the same day
------																		when is NOT LIVE (RECORDED) and the the MA flag = 1 AND the duration [minutes] = 0 (events started just after 23:59:00) then STB_BROADCAST_START_TIME + duration === STB_BROADCAST_START_TIME
------																		when is NOT LIVE (RECORDED) and the the MA flag = 1 AND the duration [minutes] <> 0 then STB_BROADCAST_START_TIME + duration + 1 
------																		when is NOT LIVE (RECORDED) and the the MA flag = 0 AND the duration [minutes] = 0  then STB_BROADCAST_START_TIME + duration 
------  NOT 1st event (event_rank <> 1): End time same as original / START TIME When is LIVE then  00:00:00 next DAY
------																			when not LIVE AND MA Flag = 1 	STB_BROADCAST_END_TIME - duration 
------																			when not LIVE AND MA Flag = 0 	STB_BROADCAST_END_TIME - duration-1
										

UPDATE V289_M13_midnight_split_working_table	
SET 	STB_BROADCAST_START_TIME = CASE WHEN event_rank = 1 THEN STB_BROADCAST_START_TIME 					---------------- No change - Already rounded - STB_BROADCAST_START_TIME is rounded 
								WHEN TIMESHIFT  = 0 AND event_rank <> 1 THEN DATEADD(MINUTE, DATEDIFF(MINUTE, '2000-01-01',STB_EVENT_START_TIME), '2000-01-01')		---------------- Rounded 00:00:00 next day 
								WHEN MA_flag = 1 AND  event_rank <> 1 AND TIMESHIFT <> 0 AND split_type = 1 THEN DATEADD (minute, -duration +1, STB_BROADCAST_END_TIME) 	---------------- Already rounded - STB_BROADCAST_END_TIME is rounded 
								WHEN MA_flag = 1 THEN DATEADD (minute, -duration, STB_BROADCAST_END_TIME) 	---------------- Already rounded - STB_BROADCAST_END_TIME is rounded 
								ELSE DATEADD (minute, -duration-1 , STB_BROADCAST_END_TIME)					---------------- Already rounded - STB_BROADCAST_END_TIME is rounded 
								END
	,STB_BROADCAST_END_TIME =  CASE WHEN event_rank <> 1 THEN STB_BROADCAST_END_TIME 						---------------- No change - Already rounded - STB_BROADCAST_END_TIME is rounded 
								WHEN TIMESHIFT  = 0  AND STB_BROADCAST_START_TIME = STB_EVENT_START_TIME THEN  DATEADD (minute, duration, STB_BROADCAST_START_TIME) --  rounded STB_BROADCAST_START_TIME
								WHEN TIMESHIFT  = 0  THEN CASE WHEN STB_BROADCAST_START_TIME > DATEADD(minute, -1, DATEADD(dd, DATEDIFF(dd,'2000-01-01', STB_EVENT_START_TIME)+1, '2000-01-01')) THEN STB_BROADCAST_START_TIME -- Already rounded 
                                                        ELSE DATEADD(minute, -1, DATEADD(dd, DATEDIFF(dd,'2000-01-01', STB_EVENT_START_TIME)+1, '2000-01-01')) END ------ Rounded: 23:59:00
								WHEN MA_flag = 1 AND duration = 0 THEN DATEADD (minute, duration, STB_BROADCAST_START_TIME) 	---------------- Rounded 
								WHEN MA_flag = 1 THEN DATEADD (minute, duration+1, STB_BROADCAST_START_TIME)					---------------- Rounded 
								ELSE DATEADD (minute, duration, STB_BROADCAST_START_TIME)
								END
	, proc_flag = 4 
WHERE split_type in (1,3) 
		AND proc_flag = 1 

UPDATE V289_M13_midnight_split_working_table
	SET STB_BROADCAST_START_TIME = STB_BROADCAST_END_TIME
WHERE STB_BROADCAST_END_TIME < STB_BROADCAST_START_TIME  AND event_rank <> 1
 AND proc_flag = 4

UPDATE V289_M13_midnight_split_working_table
	SET STB_BROADCAST_END_TIME = STB_BROADCAST_START_TIME
WHERE STB_BROADCAST_END_TIME < STB_BROADCAST_START_TIME  AND event_rank = 1
AND proc_flag = 4


		
COMMIT 		

UPDATE V289_M13_midnight_split_working_table 
SET STB_BROADCAST_START_TIME = STB_BROADCAST_END_TIME
WHERE proc_flag = 4 AND split_type = 1 AND STB_BROADCAST_START_TIME > STB_BROADCAST_END_TIME AND DATEPART(MINUTE, STB_BROADCAST_END_TIME) = 59 AND DATEPART(HOUR, STB_BROADCAST_END_TIME) = 23

UPDATE V289_M13_midnight_split_working_table 
SET STB_BROADCAST_START_TIME =  DATEADD(MINUTE, 1, STB_BROADCAST_START_TIME)
WHERE proc_flag = 4 AND split_type = 1 AND DATEPART(MINUTE, STB_BROADCAST_START_TIME) = 59 AND DATEPART(HOUR, STB_BROADCAST_START_TIME) = 23
	AND event_rank = 2 AND MA_flag = 0 
	AND DTH_VIEWING_EVENT_ID IN (SELECT DTH_VIEWING_EVENT_ID FROM V289_M13_midnight_split_working_table
									WHERE split_type = 1 AND DATEPART(MINUTE, STB_BROADCAST_START_TIME) = 0 AND DATEPART(HOUR, STB_BROADCAST_START_TIME) = 0
									AND event_rank = 1 AND MA_flag = 0 AND STB_BROADCAST_START_TIME = STB_BROADCAST_END_TIME) 
----------------------------------------------------------------------------------------------------
------ DUPLICATING events that needs extra split due to BROADCAST TIME SPANNING (split_type =3 that need an extra split)
INSERT INTO V289_M13_midnight_split_working_table (SUBSCRIBER_ID,ACCOUNT_NUMBER
	,STB_BROADCAST_START_TIME
	,STB_BROADCAST_END_TIME
	,STB_EVENT_START_TIME
	,TIMESHIFT,service_key,Platform_flag
	,Original_Service_key,AdSmart_flag,DTH_VIEWING_EVENT_ID
	,person_1,person_2,person_3,person_4,person_5,person_6
	,person_7,person_8,person_9,person_10,person_11,person_12
	,person_13,person_14,person_15,person_16,duration,STB_EVENT_END_TIME, split_type, event_rank)
SELECT 
	SUBSCRIBER_ID,ACCOUNT_NUMBER
	,STB_BROADCAST_START_TIME
	,STB_BROADCAST_END_TIME
	,STB_EVENT_START_TIME
	,TIMESHIFT,service_key,Platform_flag
	,Original_Service_key,AdSmart_flag,DTH_VIEWING_EVENT_ID
	,person_1,person_2,person_3,person_4,person_5,person_6
	,person_7,person_8,person_9,person_10,person_11,person_12
	,person_13,person_14,person_15,person_16,duration,STB_EVENT_END_TIME, split_type	
	, 3 As event_rank
FROM V289_M13_midnight_split_working_table
WHERE split_type = 3 
	AND	proc_flag = 4 and DATE(STB_BROADCAST_START_TIME) <> DATE(STB_BROADCAST_END_TIME)		----------- EXTRA CONDITION to prevent duplicates in case the spanning has been adjusted in the previous update 
COMMIT

-- Splitting events where broadcast spanning happen
UPDATE V289_M13_midnight_split_working_table
SET STB_BROADCAST_START_TIME = CASE WHEN (split_type = 2 AND event_rank = 1) 			-- the 1st event from type 2
										OR  (split_type = 3 AND event_rank <>3) 		-- the 1st event of the type 3 
											THEN STB_BROADCAST_START_TIME 
								ELSE  DATEADD(dd, DATEDIFF(dd,'2000-01-01', STB_BROADCAST_START_TIME)+1, '2000-01-01')		------ 00:00:00 next day 
								END 
								
	,STB_BROADCAST_END_TIME = CASE  WHEN (split_type = 2 AND event_rank = 1) 			-- the 1st chunk from type 2
										OR  (split_type = 3 AND event_rank <>3) 		-- the 1st event of the type 3 
								THEN DATEADD(minute, -1, DATEADD(dd, DATEDIFF(dd,'2000-01-01', STB_BROADCAST_START_TIME)+1, '2000-01-01'))	------- 23:59:00 first day 
								ELSE STB_BROADCAST_END_TIME 
								END 
	,duration = DATEDIFF ( MINUTE, CASE WHEN (split_type = 2 AND event_rank = 1) 		-- the 1st chunk from type 2
										OR  (split_type = 3 AND event_rank <>3) 		-- the 1st event of the type 3 
											THEN STB_BROADCAST_START_TIME 
								ELSE  DATEADD(dd, DATEDIFF(dd,'2000-01-01', STB_BROADCAST_START_TIME)+1, '2000-01-01')
								END ,
								CASE  WHEN (split_type = 2 AND event_rank = 1) 			-- the 1st chunk from type 2
										OR  (split_type = 3 AND event_rank <>3) 		-- the 1st event of the type 3 
								THEN DATEADD(minute, -1, DATEADD(dd, DATEDIFF(dd,'2000-01-01', STB_BROADCAST_START_TIME)+1, '2000-01-01'))
								ELSE STB_BROADCAST_END_TIME 
								END )
	,proc_flag = 6 								
WHERE DATE(STB_BROADCAST_START_TIME) <> DATE(STB_BROADCAST_END_TIME)
AND split_type  <> 1
COMMIT


UPDATE V289_M13_midnight_split_working_table 
SET a.STB_BROADCAST_START_TIME = b.STB_BROADCAST_END_TIME
FROM V289_M13_midnight_split_working_table AS a 
JOIN (SELECT DTH_VIEWING_EVENT_ID, event_rank, split_type , STB_BROADCAST_START_TIME, STB_BROADCAST_END_TIME
        FROM v289_M13_midnight_split_working_table AS b
        WHERE b.split_type = 3 AND b.event_rank = 1)  as b ON a.DTH_VIEWING_EVENT_ID = b.DTH_VIEWING_EVENT_ID
WHERE a.split_type = 3 AND a.event_rank = 2 
        AND b.STB_BROADCAST_END_TIME            >       a.STB_BROADCAST_START_TIME
        AND DATE(b.STB_BROADCAST_END_TIME)      = DATE (a.STB_BROADCAST_END_TIME)
        AND DATE(b.STB_BROADCAST_START_TIME)    = DATE (a.STB_BROADCAST_START_TIME)



------------------ Updating Events times
UPDATE V289_M13_midnight_split_working_table	
SET 	STB_EVENT_START_TIME = CASE WHEN event_rank = 1  OR  (split_type = 3 AND event_rank <>3)  THEN STB_EVENT_START_TIME
								ELSE DATEADD (minute, -duration, STB_EVENT_END_TIME)
								END
	,STB_EVENT_END_TIME =  CASE WHEN event_rank = 1 OR  (split_type = 3 AND event_rank <>3) THEN DATEADD (minute, CASE WHEN duration = 0 THEN 1 ELSE duration END , STB_EVENT_START_TIME)
								ELSE STB_EVENT_END_TIME 
								END
	, proc_flag = CASE WHEN split_type  = 1 THEN 7 ELSE 1 END				
WHERE proc_flag = 6
		
COMMIT 		

-- Updating proc_flag
UPDATE V289_M13_midnight_split_working_table
SET proc_flag = 9 
WHERE (DATE (STB_BROADCAST_START_TIME) = DATE (STB_BROADCAST_END_TIME)    )
	AND
	(DATE (STB_EVENT_START_TIME) = DATE (STB_EVENT_END_TIME)	)
COMMIT

SELECT COUNT (*) FROM V289_M13_midnight_split_working_table WHERE proc_flag <> 9 




---------------- FIXING Services keys 


------------------------------------------------------------------------------------------------------
/*

------------------------------------------------------------------------------------------------------
DECLARE @SQLL VARCHAR(10000)
DECLARE @dt     DATE
DECLARE @tabl VARCHAR (100)

WHILE EXISTS (SELECT top 1 proc_flag FROM v289_raw_tables_update WHERE proc_flag = 0)
BEGIN
        SELECT @dt  = MIN (dat)
        FROM v289_raw_tables_update WHERE proc_flag = 0
        SELECT @tabl = expression FROM v289_raw_tables_update WHERE @dt = dat
        ------ UPdating event_rank  = 1 events in raw TABLES
        SET @SQLL = 'UPDATE vespa_shared.'||@tabl                                                               ----------- Change the schema when testing
        ||' SET a.STB_BROADCAST_START_TIME = b.STB_BROADCAST_START_TIME'
        ||' , a.STB_BROADCAST_END_TIME = b.STB_BROADCAST_END_TIME'
        ||' , a.STB_EVENT_START_TIME = b.STB_EVENT_START_TIME'
        ||' FROM vespa_shared.'||@tabl||'  AS a '
        ||' JOIN V289_M13_midnight_split_working_table AS b ON a.DTH_VIEWING_EVENT_ID = b.DTH_VIEWING_EVENT_ID AND a.STB_EVENT_START_TIME = b.STB_EVENT_START_TIME'
        ||' WHERE  event_rank = 1  AND DATE (a.STB_EVENT_START_TIME) = '''||@dt||''''
        EXECUTE (@SQLL)
        COMMIT
        
        ------ Inserting new split events (event_rank <>1)
        SET @SQLL = 'INSERT INTO vespa_shared.'||@tabl                                          ----------- Change the schema when testing
        ||' SELECT      SUBSCRIBER_ID, ACCOUNT_NUMBER, STB_BROADCAST_START_TIME, STB_BROADCAST_END_TIME, STB_EVENT_START_TIME, TIMESHIFT'
        ||' , service_key, Platform_flag, Original_Service_key, AdSmart_flag ,DTH_VIEWING_EVENT_ID '
        ||' , person_1, person_2, person_3, person_4, person_5, person_6, person_7, person_8, person_9, person_10, person_11'
        ||' , person_12, person_13, person_14, person_15, person_16'
        ||' FROM V289_M13_midnight_split_working_table '
        ||' WHERE  event_rank <> 1  AND DATE (STB_EVENT_START_TIME) = '''||@dt||''''

        EXECUTE (@SQLL)
        COMMIT
        UPDATE v289_raw_tables_update
        SET proc_flag = 1
        WHERE dat = @dt
END
*/

