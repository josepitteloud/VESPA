/*
	Project: 	S&P UAT for Minute Attribution
	Analyst: 	Angel Donnarumma
	Date: 		16/01/2013
*/

/*

VIEWING_SLOT_INSTANCE_FACT_PREPARE_2 and FINAL_MINUTE_ATTRIBUTION 
table are the starting table to start comparing the broadcast time 
and apply the minute attribution logic which will by then outputted 
straight into the Fact table (on SMI_EXPORT DB).

Both VIEWING_SLOT_INSTANCE_FACT_PREPARE_2 and FINAL_MINUTE_ATTRIBUTION  
tables are on DIS_PREPARE DB and the output will be thrown into 
VIEWING_SLOT_INSTANCE_FACT table on SMI_EXPORT DB.

*/

-- Checking out Slot Fact
select * from SMI_ETL.VIEWING_SLOT_INSTANCE_FACT_PREPARE_2 limit 100

-- Checking out Minute Attribution table
select * from DIS_ETL.FINAL_MINUTE_ATTRIBUTION limit 100

-- Checking out merge between tables
SELECT	A.VIEWING_EVENT_ID
		,A.DK_BROADCAST_START_DATEHOUR_DIM
		,A.DK_BROADCAST_START_TIME_DIM
		,A.DK_BROADCAST_END_DATEHOUR_DIM
		,A.DK_BROADCAST_END_TIME_DIM
		,B.ATTRIBUTION_START
FROM  	VIEWING_SLOT_INSTANCE_FACT_PREPARE_2 	as A
		inner join FINAL_MINUTE_ATTRIBUTION		as B
		on A.VIEWING_EVENT_ID = B.VIEWING_EVENT_ID
		inner join VIEWING_EVENTS_47			as C
		on A.INSTANCE_START >= C.BROADCAST_START_DATETIME_UTC
			and A.INSTANCE_END < C.X_VIEWING_END_TIME
LIMIT	1000


/********************	TESTING		*********************/

/* T01-1 */

-- duplicate check on fact for view event id...
select 	count(1), count(distinct VIEWING_EVENT_ID)
from	SMI_ETL.VIEWING_SLOT_INSTANCE_FACT_PREPARE_2 -- duplicated (Due to Into Slot...)


/* T01-2 */

-- duplicate check on fact for view event id...
select 	count(1), count(distinct VIEWING_EVENT_ID)
from	DIS_ETL.FINAL_MINUTE_ATTRIBUTION -- unique