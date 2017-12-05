/*###############################################################################
# Created on:   17/10/2013
# Created by:   Mandy Ng (MNG)
# Description: CIA Aggregations - sky WiFi data exploration
#
# List of steps:
#               STEP 0.1 - upload Sky WiFi table to Olive environment
#               STEP 1.0 - Data audit
#               STEP 2.0 -
#               STEP 3.0 -
#               STEP 4.0 -
#               STEP 5.0 -
#               STEP 6.0 -
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#     - '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/wifi_sessions_20131015.csv'
#     - wifi_sessions_20131015
#     - WIFI_Entitlement_Base
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 17/10/2013  MNG   Initial version
#
###############################################################################*/






-- ##############################################################################################################
-- ##### STEP 0.1 -upload table to Olive                                                                    #####
-- ##############################################################################################################

-- Simon L extract April to June 2013 Sky WiFi session data from Composite. I uploaded this dataset to Olive via Sybase


DROP table wifi_sessions_20131015
create table wifi_sessions_20131015 (
    WIFI_SESSION_SK BIGINT,
    DW_CREATED_DT   TIMESTAMP,
    DW_LAST_MODIFIED_DT TIMESTAMP,
    LATEST_SRC  VARCHAR(8),
    SRC_SYSTEM_ID   INTEGER,
    CONTACTOR_SRC_SYSTEM_ID VARCHAR(50),
    START_TIME  TIMESTAMP,
    END_TIME    TIMESTAMP,
    MAC_ADDRESS VARCHAR(50),
    BYTES_IN    BIGINT,
    BYTES_OUT   BIGINT,
    DURATION    INTEGER,
    OWNING_CUST_ACCOUNT_ID  VARCHAR(50),
    SERVICE_INSTANCE_ID VARCHAR(50),
    Account_Number varchar(20)
);
create hg index idx1 on  wifi_sessions_20131015(WIFI_SESSION_SK);


-- ####  Load data ###############################################


-- data extracted by Simon L
delete from  wifi_sessions_20131015;
commit;
load table  wifi_sessions_20131015
(
    WIFI_SESSION_SK',',
    DW_CREATED_DT',',
    DW_LAST_MODIFIED_DT',',
    LATEST_SRC',',
    SRC_SYSTEM_ID',',
    CONTACTOR_SRC_SYSTEM_ID',',
    START_TIME',',
    END_TIME',',
    MAC_ADDRESS',',
    BYTES_IN',',
    BYTES_OUT',',
    DURATION',',
    OWNING_CUST_ACCOUNT_ID',',
    SERVICE_INSTANCE_ID'\n'
)
from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/wifi_sessions_20131015.csv'
QUOTES OFF
ESCAPES OFF
SKIP 1 -- file contain header
--LIMIT 1000
NOTIFY 200000
DELIMITED BY ','
;

select count(*) from wifi_sessions_20131015

grant select on wifi_sessions_20131015 to bednaszs


CREATE HG INDEX         acc_id ON wifi_sessions_20131015 (Account_number);
CREATE HG INDEX         cst_id ON wifi_sessions_20131015 (owning_cust_account_id);


-- 7GB file import time
--35246686 row(s) affected
--Execution time: 288.34 seconds



--## Load the sessions with zerobytes
create table wifi_sessions_zerobytes_20131015 (
    WIFI_SESSION_SK BIGINT,
    DW_CREATED_DT   TIMESTAMP,
    DW_LAST_MODIFIED_DT TIMESTAMP,
    LATEST_SRC  VARCHAR(8),
    SRC_SYSTEM_ID   INTEGER,
    CONTACTOR_SRC_SYSTEM_ID VARCHAR(50),
    START_TIME  TIMESTAMP,
    END_TIME    TIMESTAMP,
    MAC_ADDRESS VARCHAR(50),
    BYTES_IN    BIGINT,
    BYTES_OUT   BIGINT,
    DURATION    INTEGER,
    OWNING_CUST_ACCOUNT_ID  VARCHAR(50),
    SERVICE_INSTANCE_ID VARCHAR(50),
    Account_Number varchar(20)
);
create hg index idx1 on  wifi_sessions_zerobytes_20131015(WIFI_SESSION_SK);


-- ####  Load data ###############################################


-- data extracted by Simon L
delete from  wifi_sessions_zerobytes_20131015;
commit;
load table  wifi_sessions_zerobytes_20131015
(
    WIFI_SESSION_SK',',
    DW_CREATED_DT',',
    DW_LAST_MODIFIED_DT',',
    LATEST_SRC',',
    SRC_SYSTEM_ID',',
    CONTACTOR_SRC_SYSTEM_ID',',
    START_TIME',',
    END_TIME',',
    MAC_ADDRESS',',
    BYTES_IN',',
    BYTES_OUT',',
    DURATION',',
    OWNING_CUST_ACCOUNT_ID',',
    SERVICE_INSTANCE_ID'\n'
)
from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/wifi_zerobyte_sessions_20131015.csv'
QUOTES OFF
ESCAPES OFF
SKIP 1 -- file contain header
--LIMIT 1000
NOTIFY 200000
DELIMITED BY ','
;

-- count()12578574


-- combine both tables
insert into wifi_sessions_20131015
select * from wifi_sessions_zerobytes_20131015


-- total records in the wif session table
select count(*) from wifi_sessions_20131015
--47825260



-- ##############################################################################################################
-- ##### STEP 1.0 Data Audit                                                                                #####
-- ##############################################################################################################

--## match up the account number from customer table
select distinct(account_number), owning_cust_account_id
into temp_account
from sk_prod.CUST_SUBSCRIPTIONS


update wifi_sessions_20131015 wifi
set account_number = cust.account_number
from temp_account cust
where wifi.owning_cust_account_id = cust.owning_cust_account_id


--## how many wifi users cannot find the Sky account number?
select count(distinct(owning_cust_account_id)) from wifi_sessions_20131015
where account_number is null
-- 7 owning_cust_account cannot be matched

select distinct(owning_cust_account_id) from wifi_sessions_20131015
where account_number is null
--11638482927944972181834
--?
--11660361462906584301657
--11437392100761356695281
--12927608081513749980434
--12945073330756088783917
--11653156337136299550775



-- ##############################################################################################################
-- ##### STEP 2.0 Create aggregated table for all sessions and empty sessions
-- ##############################################################################################################
--- create an aggregated table for All sessions
--## aggregate to total session, total bytes in/out and total duration per account to examine the distribution
-- guide to set filters and measuring metric

select
                account_number,
                datepart(MM,start_time) as month,
                count(distinct(mac_address)) as no_active_devices,
                sum(bytes_in) as total_bytes_in,
                sum(bytes_out) as total_bytes_out,
                sum(duration) as total_duration_sec,
                count(distinct(datename(day, start_time))) as total_distinct_wifi_days,
                count(*) as total_sessions,
                max(duration) as max_duration,
                min(duration) as min_duration,
                round(avg(duration),2) as avg_duration,
                round(stddev(duration),2) as stdev_duration,
                max(bytes_in) as max_bytes_in,
                min(bytes_in) as min_bytes_in,
                round(avg(bytes_in),2) as avg_bytes_in,
                round(stddev(bytes_in),2) as stdev_bytes_in,
                max(bytes_out) as max_bytes_out,
                min(bytes_out) as min_bytes_out,
                round(avg(bytes_out),2) as avg_bytes_out,
                round(stddev(bytes_out),2) as stdev_bytes_out

into            wifi_sessions_20131015_aggregated_v2
from            wifi_sessions_20131015
group  by      datepart(MM,start_time),  account_number



-- ##############################################################################################################
-- ##### STEP 3.0 Create the universe to find out entitlement - sky customers                                 ###
-- ##############################################################################################################		
---------- Set the dates where you want to capture for the analysis
CREATE VARIABLE @Analysis_START DATE;

SET @Analysis_START = '2013-04-01';

CREATE VARIABLE @Analysis_END DATE;

SET @Analysis_END = '2013-06-31';

---------- Get the eligible account numbers in April, May and Jun
DROP TABLE WIFI_Entitlement_Base;

SELECT Account_number
	, owning_cust_account_id
	, effective_from_dt
	, effective_to_dt
	, MAX(CASE 
			WHEN effective_from_dt <= '2013-04-30'
				AND effective_to_dt > '2013-04-01'
				THEN 1
			ELSE 0
			END) AS APR_BB
	, MAX(CASE 
			WHEN effective_from_dt <= '2013-05-31'
				AND effective_to_dt > '2013-05-01'
				THEN 1
			ELSE 0
			END) AS MAY_BB
	, MAX(CASE 
			WHEN effective_from_dt <= '2013-06-30'
				AND effective_to_dt > '2013-06-01'
				THEN 1
			ELSE 0
			END) AS JUN_BB
INTO WIFI_Entitlement_Base
FROM sk_prod.cust_subs_hist
WHERE subscription_sub_type = 'CLOUDWIFI'
	AND (
		status_code IN (
			'AC'
			, 'AB'
			)
		OR (
			status_code = 'PC'
			AND prev_status_code NOT IN (
				'?'
				, 'RQ'
				, 'AP'
				, 'UB'
				, 'BE'
				, 'PA'
				)
			)
		OR (
			status_code = 'CF'
			AND prev_status_code = 'PC'
			)
		OR (
			status_code = 'AP'
			AND sale_type = 'SNS Bulk Migration'
			)
		)
	AND effective_from_dt <= @Analysis_END
	AND effective_to_dt > @Analysis_START
	AND effective_from_dt != effective_to_dt
--AND       current_product_sk IN (43373, 42128, 42131) -------- Need to be unlimited and Connect customers!! Not required now
GROUP BY Account_number
	, owning_cust_account_id;
	
	-- 4427619 Row(s) affected) unique account records
	-- 4822229 Row(s) affected -- contain accounts with multiple wifi entitlement. 


CREATE HG INDEX acc_id ON WIFI_Entitlement_Base(Account_number);
CREATE HG INDEX cst_id ON WIFI_Entitlement_Base(owning_cust_account_id);


---------- Calculate the number of days user could access wifi for by monthly
ALTER TABLE WIFI_Entitlement_Base ADD days_access_wifi_Apr BIGINT DEFAULT 0;

ALTER TABLE WIFI_Entitlement_Base ADD days_access_wifi_May BIGINT DEFAULT 0
	, ADD days_access_wifi_Jun BIGINT DEFAULT 0;

							
		
UPDATE WIFI_Entitlement_Base
SET days_access_wifi_Apr = CASE 
		WHEN (
				effective_from_dt >= '2013-04-01'
				AND effective_from_dt <= '2013-04-30'
				AND effective_to_dt > '2013-04-30'
				)
			THEN datediff(dd, effective_from_dt, '2013-04-30')
		WHEN (
				effective_from_dt >= '2013-04-01'
				AND effective_to_dt <= '2013-04-30'
				)
			THEN datediff(dd, effective_from_dt, effective_to_dt)
		WHEN (
				effective_from_dt < '2013-04-01'
				AND effective_to_dt > '2013-04-30'
				)
			THEN 30
		WHEN (
				effective_from_dt < '2013-04-01'
				AND effective_to_dt <= '2013-04-30'
				)
			THEN (datediff(dd, '2013-04-01', effective_to_dt))
		WHEN effective_from_dt > '2013-04-30'
			THEN -999  	-- accounts join in the next months
		END


-- May 		
UPDATE WIFI_Entitlement_Base
SET days_access_wifi_May = CASE 
		WHEN (
				effective_from_dt >= '2013-05-01'
				AND effective_from_dt <= '2013-05-31'
				AND effective_to_dt > '2013-05-31'
				)
			THEN datediff(dd, effective_from_dt, '2013-05-30')
		WHEN (
				effective_from_dt >= '2013-05-01'
				AND effective_to_dt <= '2013-05-31'
				)
			THEN datediff(dd, effective_from_dt, effective_to_dt)
		WHEN (
				effective_from_dt < '2013-05-01'
				AND effective_to_dt > '2013-05-31'
				)
			THEN 31
		WHEN (
				effective_from_dt < '2013-05-01'
				AND effective_to_dt <= '2013-05-31'
				)
			THEN (datediff(dd, '2013-05-01', effective_to_dt))
		WHEN effective_from_dt > '2013-05-31'
			THEN -999  	-- accounts join in the next months
		END


		
--June 
UPDATE WIFI_Entitlement_Base
SET days_access_wifi_Jun = CASE 
		WHEN (
				effective_from_dt >= '2013-06-01'
				AND effective_from_dt <= '2013-06-30'
				AND effective_to_dt > '2013-06-30'
				)
			THEN datediff(dd, effective_from_dt, '2013-06-30')
		WHEN (
				effective_from_dt >= '2013-06-01'
				AND effective_to_dt <= '2013-06-30'
				)
			THEN datediff(dd, effective_from_dt, effective_to_dt)
		WHEN (
				effective_from_dt < '2013-06-01'
				AND effective_to_dt > '2013-06-30'
				)
			THEN 30
		WHEN (
				effective_from_dt < '2013-06-01'
				AND effective_to_dt <= '2013-06-30'
				)
			THEN (datediff(dd, '2013-06-01', effective_to_dt))
		WHEN effective_from_dt > '2013-06-30'
			THEN -999  	-- accounts join in the next months
		END
		
		
		
		
				
------------ Alter the base table and include new columns for wifi aggregate usuage information (no exclusion criteria has included)
ALTER TABLE WIFI_Entitlement_Base ADD no_active_devices_Apr BIGINT DEFAULT 0
			, ADD no_active_devices_May BIGINT DEFAULT 0
			, ADD no_active_devices_Jun BIGINT DEFAULT 0
			, ADD total_bytes_in_Apr BIGINT DEFAULT 0
			, ADD total_bytes_in_May BIGINT DEFAULT 0
			, ADD total_bytes_in_Jun BIGINT DEFAULT 0
			, ADD total_bytes_out_Apr BIGINT DEFAULT 0
			, ADD total_bytes_out_May BIGINT DEFAULT 0
			, ADD total_bytes_out_Jun BIGINT DEFAULT 0
			, ADD total_duration_sec_Apr BIGINT DEFAULT 0
			, ADD total_duration_sec_May BIGINT DEFAULT 0
			, ADD total_duration_sec_Jun BIGINT DEFAULT 0
			, ADD total_sessions_Apr BIGINT DEFAULT 0
			, ADD total_sessions_May BIGINT DEFAULT 0
			, ADD total_sessions_Jun BIGINT DEFAULT 0
			, ADD total_distinct_wifi_days_Apr BIGINT DEFAULT 0
			, ADD total_distinct_wifi_days_May BIGINT DEFAULT 0
			, ADD total_distinct_wifi_days_Jun BIGINT DEFAULT 0
		)


UPDATE WIFI_Entitlement_Base base
SET no_active_devices_Apr = wifi.no_active_devices
	, total_bytes_in_Apr = wifi.total_bytes_in
	, total_bytes_out_Apr = wifi.total_bytes_out
	, total_duration_sec_Apr = wifi.total_duration_sec
	, total_sessions_Apr = wifi.total_sessions
	, total_distinct_wifi_days_Apr = wifi.total_distinct_wifi_days
FROM wifi_sessions_20131015_aggregated_v2 wifi
WHERE wifi.month = 4
	AND base.account_number = wifi.account_number

	
	
UPDATE WIFI_Entitlement_Base base
SET no_active_devices_May = wifi.no_active_devices
	, total_bytes_in_May = wifi.total_bytes_in
	, total_bytes_out_May = wifi.total_bytes_out
	, total_duration_sec_May = wifi.total_duration_sec
	, total_sessions_May = wifi.total_sessions
	, total_distinct_wifi_days_May = wifi.total_distinct_wifi_days
FROM wifi_sessions_20131015_aggregated_v2 wifi
WHERE wifi.month = 5
	AND base.account_number = wifi.account_number

	
	
UPDATE WIFI_Entitlement_Base base
SET no_active_devices_Jun = wifi.no_active_devices
	, total_bytes_in_Jun = wifi.total_bytes_in
	, total_bytes_out_Jun = wifi.total_bytes_out
	, total_duration_sec_Jun = wifi.total_duration_sec
	, total_sessions_Jun = wifi.total_sessions
	, total_distinct_wifi_days_Jun = wifi.total_distinct_wifi_days
FROM wifi_sessions_20131015_aggregated_v2 wifi
WHERE wifi.month = 6
	AND base.account_number = wifi.account_number

	



	
-- ##############################################################################################################
-- ##### STEP 4.0 Analysis
-- ##############################################################################################################	
------------------- ANALYSIS		
-- entitled users
SELECT APR_BB
	, MAY_BB
	, JUN_BB
	, count(distinct(account_number))
FROM WIFI_Entitlement_Base
GROUP BY APR_BB
	, MAY_BB
	, JUN_BB

-- wifi connect users
SELECT count(distinct(account_number))
FROM WIFI_Entitlement_Base
WHERE APR_BB = 1
	AND no_active_devices_Apr > 0

SELECT count(distinct(account_number))
FROM WIFI_Entitlement_Base
WHERE May_BB = 1
	AND no_active_devices_May > 0

SELECT count(distinct(account_number))
FROM WIFI_Entitlement_Base
WHERE Jun_BB = 1
	AND no_active_devices_Jun > 0

--- number of wifi active users (non-empty sessions
SELECT count(distinct(account_number))
FROM WIFI_Entitlement_Base
WHERE Apr_BB = 1
	AND no_active_devices_Apr > 0
	AND total_duration_sec_Apr > 0
	AND (total_bytes_in_Apr + total_bytes_out_Apr > 0)

SELECT count(distinct(account_number))
FROM WIFI_Entitlement_Base
WHERE May_BB = 1
	AND no_active_devices_May > 0
	AND total_duration_sec_May > 0
	AND (total_bytes_in_May + total_bytes_out_May > 0)

SELECT count(distinct(account_number))
FROM WIFI_Entitlement_Base
WHERE Jun_BB = 1
	AND no_active_devices_Jun > 0
	AND total_duration_sec_Jun > 0
	AND (total_bytes_in_Jun + total_bytes_out_Jun > 0)





---------------------------------For the number of active users in a specific study month, how many of them have used Wifi again in the next consecutive month(s)?

--first limit the sample group as the customers who has entitlement for the whole month
-- then calcualte the freq of using wifi across months
select total_distinct_wifi_days_Apr, total_distinct_wifi_days_May, total_distinct_wifi_days_Jun, count(*)
FROM WIFI_Entitlement_Base
where days_access_wifi_Apr = 30 and (total_distinct_wifi_days_Apr + total_distinct_wifi_days_May + total_distinct_wifi_days_Jun) < 20 and (total_distinct_wifi_days_Apr + total_distinct_wifi_days_May + total_distinct_wifi_days_Jun) > 0
group by total_distinct_wifi_days_Apr, total_distinct_wifi_days_May, total_distinct_wifi_days_Jun


select total_distinct_wifi_days_Apr, count(*)
FROM WIFI_Entitlement_Base
where days_access_wifi_Apr = 30 and days_access_wifi_May =31 and days_access_wifi_Jun=30
and (total_distinct_wifi_days_Apr + total_distinct_wifi_days_May + total_distinct_wifi_days_Jun) < 20 -- use less than 7 days on average over 3 months
and (total_distinct_wifi_days_Apr + total_distinct_wifi_days_May + total_distinct_wifi_days_Jun) > 0  -- at least used wifi once over 3 months
--and total_distinct_wifi_days_Apr = 1
and total_distinct_wifi_days_May = 0
and total_distinct_wifi_days_Jun = 0
group by total_distinct_wifi_days_Apr




days_access_wifi_Apr = 30
days_access_wifi_May = 31
days_access_wifi_Jun = 30

	

	
	
------------------------ DO WE HAVE TO SET UP ANY FILTERS? 
----- No of days account can acess wifi service
SELECT days_access_wifi_Apr
	, count(DISTINCT (account_number))
FROM WIFI_Entitlement_Base
GROUP BY days_access_wifi_Apr
HAVING days_access_wifi_Apr >= 0

SELECT days_access_wifi_May
	, count(DISTINCT (account_number))
FROM WIFI_Entitlement_Base
GROUP BY days_access_wifi_May
HAVING days_access_wifi_May > 0

SELECT days_access_wifi_Jun
	, count(DISTINCT (account_number))
FROM WIFI_Entitlement_Base
GROUP BY days_access_wifi_Jun
HAVING days_access_wifi_Jun > 0

----- No of active devices used in active sessions
SELECT no_active_devices_Apr
	, count(DISTINCT (account_number))
FROM WIFI_Entitlement_Base
WHERE total_duration_sec_Apr >= 0
	AND (total_bytes_in_Apr + total_bytes_out_Apr) > 0
GROUP BY no_active_devices_Apr



SELECT no_active_devices_May
	, count(DISTINCT (account_number))
FROM WIFI_Entitlement_Base
WHERE total_duration_sec_May > 0
	AND (total_bytes_in_May + total_bytes_out_May) > 0
GROUP BY no_active_devices_May



SELECT no_active_devices_Jun
	, count(DISTINCT (account_number))
FROM WIFI_Entitlement_Base
WHERE total_duration_sec_Jun > 0
	AND (total_bytes_in_Jun + total_bytes_out_Jun) > 0
GROUP BY no_active_devices_Jun


-------------------------------------------------------- No of unique days wifi was used for active sessions
SELECT total_distinct_wifi_days_Apr
	, count(DISTINCT (account_number))
FROM WIFI_Entitlement_Base
WHERE total_duration_sec_Apr > 0
	AND (total_bytes_in_Apr + total_bytes_out_Apr) > 0
GROUP BY total_distinct_wifi_days_Apr




SELECT total_distinct_wifi_days_May
	, count(DISTINCT (account_number))
FROM WIFI_Entitlement_Base
WHERE total_duration_sec_May > 0
	AND (total_bytes_in_May + total_bytes_out_May) > 0
GROUP BY total_distinct_wifi_days_May



SELECT total_distinct_wifi_days_Jun
	, count(DISTINCT (account_number))
FROM WIFI_Entitlement_Base
WHERE total_duration_sec_Jun > 0
	AND (total_bytes_in_Jun + total_bytes_out_Jun) > 0
GROUP BY total_distinct_wifi_days_Jun


-------------------------------------------------------- Find out the volume of users in the day of the month

select distinct(datename(day, start_time)) as Day, count(distinct(account_number)) as Apr
from wifi_sessions_20131015
where (bytes_in + bytes_out >0) and duration > 0 and datepart(MM,start_time) = 4
group by (datename(day, start_time))

select distinct(datename(day, start_time)) as Day, count(distinct(account_number)) as May
from wifi_sessions_20131015
where (bytes_in + bytes_out >0) and duration > 0 and datepart(MM,start_time) = 5
group by (datename(day, start_time))


select distinct(datename(day, start_time)) as Day, count(distinct(account_number)) as Jun
from wifi_sessions_20131015
where (bytes_in + bytes_out >0) and duration > 0 and datepart(MM,start_time) = 6
group by (datename(day, start_time))


select datepart(MM,start_time) as month,(datename(day, start_time)) as day_of_month, (datename(dw, start_time)) as day_of_week, count(distinct(account_number)) as count_user
from wifi_sessions_20131015
where (bytes_in + bytes_out >0) and duration > 0  
group by datepart(MM,start_time), (datename(day, start_time)), (datename(dw, start_time))



-- three months observation by day to find trend of customers
select (datename(day, start_time)) as day_of_month, (datename(dw, start_time)) as day_of_week, count(distinct(account_number)) as count_user
from wifi_sessions_20131015
where (bytes_in + bytes_out) >0 and duration > 0  
group by datename(day, start_time), (datename(dw, start_time))



-------------------------------------------------------- Number of empty or active session* by month
--*active session = session has >0 duration and > 0 bytes of download + upload

select datepart(MM,start_time) as month, count(*)
from wifi_sessions_20131015
where (bytes_in + bytes_out) > 0 and duration > 0
group by month


select datepart(MM,start_time) as month, count(*)
from wifi_sessions_20131015
group by month

-- three months observations to find trends of active sessions consumed
select datename(MM, start_time) as month, (datename(day, start_time)) as day_of_month, (datename(dw, start_time)) as day_of_week, count(*) as count_session
from wifi_sessions_20131015
where (bytes_in + bytes_out) >0  and duration > 0  
group by datename(MM, start_time), datename(day, start_time), (datename(dw, start_time))






--? how about time of day??



-------------------------------------------------------- Distribution of usage
-- duration vs bytes
-- gross level for all active sessions within 1 week. 
-- can I use R?

select count(*)
from wifi_sessions_20131015
where datepart(MM, start_time) = 4 and 
(bytes_in + bytes_out) > 0 and duration > 0


group by month




-- upload vs download bytes


-- wifi usage? 



-- ##############################################################################################################
-- ##### STEP 5.0 Calculate measuring metric and perform deciling                                           #####-- ##############################################################################################################
----------------------------------------------------- Calculating WiFi measuring metric

/* ######################################   Wifi selection criteria#########################
#
# Universe
# -	Take everybody where days_access_wifi_May = 31
#
# Did not watch 
# -	Flag anyone who:
# o	(Total bytes in = 0 and total bytes out = 0) or total duration sec May = 0

# Users
# -	Calculate total usage => Total bytes in + total bytes out
# -	Binning:
# o	Frequency on total distinct wifi days may
# o	Usage on (“total usage”/frequency)
# -	Populate spreadsheet with bin stats (attached)
# -	Base on the results, propose HML bin split
################################################################################*/

--------Note: the existing WIFI_Entitlement_Base has multiple recrods per account 
--- the total_distinct_access_wifi days is wrong as this belong to all sessions
--- also the days access wifi sessions is wrong because of multiple records



--- create an aggregated table for Active sessions only to have a unique record per account to work on distribution
select
                account_number,
                datepart(MM,start_time) as month,
                count(distinct(mac_address)) as no_active_devices,
                sum(bytes_in) as total_bytes_in,
                sum(bytes_out) as total_bytes_out,
                sum(duration) as total_duration_sec,
                count(distinct(datename(day, start_time))) as total_distinct_wifi_days,
                count(*) as total_sessions,
                max(duration) as max_duration,
                min(duration) as min_duration,
                round(avg(duration),2) as avg_duration,
                round(stddev(duration),2) as stdev_duration,
                max(bytes_in) as max_bytes_in,
                min(bytes_in) as min_bytes_in,
                round(avg(bytes_in),2) as avg_bytes_in,
                round(stddev(bytes_in),2) as stdev_bytes_in,
                max(bytes_out) as max_bytes_out,
                min(bytes_out) as min_bytes_out,
                round(avg(bytes_out),2) as avg_bytes_out,
                round(stddev(bytes_out),2) as stdev_bytes_out

into            wifi_sessions_20131015_aggregated
from            wifi_sessions_20131015
where 		   (bytes_in = 0 and bytes_out = 0) or duration = 0
group  by      datepart(MM,start_time),  account_number
	
	


--- create an account_attribute table
select  		account_number,
               (days_access_wifi_Apr) as days_access_wifi_Apr,
               (days_access_wifi_May) as days_access_wifi_May,
                (days_access_wifi_Jun) as days_access_wifi_Jun
into    Sky_WIFI_account_attributes
from    WIFI_Entitlement_Base
group  by account_number,days_access_wifi_May, days_access_wifi_Jun,days_access_wifi_Apr



alter table Sky_WIFI_account_attributes
			add Flag_complete_month_access_Apr bigint default null
			,add Flag_complete_month_access_May bigint default null
			,add Flag_complete_month_access_Jun bigint default null;
commit;


--- start here
--? check 
update Sky_WIFI_account_attributes	
set 	Flag_complete_month_access_Apr = CASE when(days_access_wifi_Apr < 30) Then 0 else 1 end
		,Flag_complete_month_access_May = CASE when(days_access_wifi_May < 31) Then 0 else 1 end
		,Flag_complete_month_access_Jun = CASE when(days_access_wifi_Jun < 30) Then 0 else 1 end;
commit;

-- QA -- ok, aim to get flag = 1 for 30/31 days_access_wifi
select Flag_complete_month_access_Apr, days_access_wifi_Apr
from Sky_WIFI_account_attributes
group by Flag_complete_month_access_Apr

select Flag_complete_month_access_May, days_access_wifi_May, count(*)
from Sky_WIFI_account_attributes
group by Flag_complete_month_access_May, days_access_wifi_May
order by 2

select Flag_complete_month_access_Jun, days_access_wifi_Jun, count(*)
from Sky_WIFI_account_attributes
group by Flag_complete_month_access_Jun, days_access_wifi_Jun
order by 2



--- Flat_do_not_use_Apr = 1 indicates customers do not use WiFi service in April
alter table Sky_WIFI_account_attributes
 			add Flag_do_not_use_Apr bigint default 1
			,add Flag_do_not_use_May bigint default 1
			,add Flag_do_not_use_Jun bigint default 1;
commit;



--- use the active session table to inform do not use customers. The wifi_base table has multiple records per account and cannot do group by
update                  Sky_WIFI_account_attributes     a
set                     Flag_do_not_use_Apr = 0

from Sky_WIFI_account_attributes a
inner join wifi_sessions_20131015_aggregated b on a.account_number = b.account_number where b.month = 4;



update                  Sky_WIFI_account_attributes     a
set                     Flag_do_not_use_May = 0

from Sky_WIFI_account_attributes a
inner join wifi_sessions_20131015_aggregated b on a.account_number = b.account_number where b.month = 5;


update                  Sky_WIFI_account_attributes     a
set                     Flag_do_not_use_Jun = 0

from Sky_WIFI_account_attributes a
inner join wifi_sessions_20131015_aggregated b on a.account_number = b.account_number where b.month = 6;




-- QA
select Flag_do_not_use_Apr, count(*)
from Sky_WIFI_account_attributes
group by Flag_do_not_use_Apr

--Flag_do_not_use_Apr     count()
--1       4194624 -- also contains accounts join in another month
--0       585640

select Flag_do_not_use_May, count(*)
from Sky_WIFI_account_attributes
group by Flag_do_not_use_May
--Flag_do_not_use_May     count()
--0       614303


select Flag_do_not_use_Jun, count(*)
from Sky_WIFI_account_attributes
group by Flag_do_not_use_Jun

--Flag_do_not_use_Jun     count()
--1       4158565
--0       621699


									
									
								
--- Remember the wifi_sessions_20131015_aggregated is a long table and contain active sessions only
alter table wifi_sessions_20131015_aggregated										
										 ADD Derived_usuage DECIMAL (12,4) DEFAULT 0;
commit;

update 	wifi_sessions_20131015_aggregated
set 	Derived_usuage = (total_bytes_in + total_bytes_out)/ total_distinct_wifi_days;
commit;





-- Remove accounts that do not have full month entitlement then use this table to calcaute descriptive statistics

select a.*
into temp_wifi_aggregated_clean
from wifi_sessions_20131015_aggregated a
inner join Sky_WIFI_account_attributes b on a.account_number = b.account_number
                where a.month = 4 and b.Flag_complete_month_access_Apr = 1;



insert into temp_wifi_aggregated_clean
select a.*
from wifi_sessions_20131015_aggregated a
inner join Sky_WIFI_account_attributes b on a.account_number = b.account_number
                where a.month = 5 and b.Flag_complete_month_access_May = 1;



insert into temp_wifi_aggregated_clean
select a.*
from wifi_sessions_20131015_aggregated a
inner join Sky_WIFI_account_attributes b on a.account_number = b.account_number
                where a.month = 6 and b.Flag_complete_month_access_Jun = 1;



-- QC --ok
select month, count(*)
from temp_wifi_aggregated_clean
group by month
--month   count()
--4       535555
--5       563447
--6       570816


select month, count(*) from wifi_sessions_20131015_aggregated
group by month
--month   count()
--4       559643
--5       587675
--6       597042


select month, count(*) from wifi_sessions_20131015_aggregated_v2
group by month
--month	count()
--4	574963
--5	601526
--6	611174


-- Deciling and calculate descriptive statistics within each decile by month
select
                          month,
                          count(*) as Cnt,
                          sum(Metric) / count(*) as mean_metric,
                          percentile_cont(0.5) within   group (order by Metric asc) as median_metric,
                          stddev(Metric) as stdev_metric,
                          min(Metric) as lower_boundary,
                          max(Metric) as upper_boundary_Orig

                      into temp_Decile_Boudaries
                      from (select
                                  month,
                                  Derived_usuage as Metric,                                      -- MN: have to change to SOV for
                                  ntile(10) over (order by Derived_usuage desc) as Segment
                              from temp_wifi_aggregated_clean) a
                         group by month, Segment;
                commit;

-- another summary for the freq metric
select
                          month,
                          segment,
                          count(*) as Cnt,
                          sum(Metric) / count(*) as mean_metric,
                          percentile_cont(0.5) within   group (order by Metric asc) as median_metric,
                          stddev(Metric) as stdev_metric,
                          min(Metric) as lower_boundary,
                          max(Metric) as upper_boundary_Orig


into WIFI_measurement_metric_freq_dist_summary_segment
                      from (select
                                  month,
                                  total_distinct_wifi_days as Metric,                                      -- MN: have to change to SOV for
                                  ntile(10) over (order by total_distinct_wifi_days desc) as Segment
                              from temp_wifi_aggregated_clean) a
                         group by month, Segment;
                commit;


select * from WIFI_measurement_metric_freq_dist_summary_segment


--------------------------------Refresh the statistics which presented in the excel file by using the active session [(total bytes in = 0 and total bytes out = 0) or total duration = 0] and correct wifi access days
---------------------------------- user profile

-- WIFI entitlement by month
select count(*) from Sky_WIFI_account_attributes
where days_access_wifi_Apr >=0

select count(*) from Sky_WIFI_account_attributes
where days_access_wifi_May >=0

select count(*) from Sky_WIFI_account_attributes
where days_access_wifi_Jun >=0


-- WIFI active sessions users by month
select count(*) from wifi_sessions_20131015_aggregated
group by month


----------------------------------------------------- usage profile
-- total active sessions by month
select month, sum(total_sessions)
from wifi_sessions_20131015_aggregated
group by month

--- Find out the volume of users in the day of the month
select distinct(datename(day, start_time)) as Day, count(distinct(account_number)) as Apr
from wifi_sessions_20131015
where datepart(MM,start_time) = 4 and ((bytes_in = 0 and bytes_out = 0) or duration = 0)
group by (datename(day, start_time))

select distinct(datename(day, start_time)) as Day, count(distinct(account_number)) as May
from wifi_sessions_20131015
where datepart(MM,start_time) = 5 and ((bytes_in = 0 and bytes_out = 0) or duration = 0)
group by (datename(day, start_time))


select distinct(datename(day, start_time)) as Day, count(distinct(account_number)) as Jun
from wifi_sessions_20131015
where datepart(MM,start_time) = 6 and ((bytes_in = 0 and bytes_out = 0) or duration = 0)
group by (datename(day, start_time))


select datepart(MM,start_time) as month,(datename(day, start_time)) as day_of_month, (datename(dw, start_time)) as day_of_week, count(distinct(account_number)) as count_user
from wifi_sessions_20131015 and ((bytes_in = 0 and bytes_out = 0) or duration = 0)
group by datepart(MM,start_time), (datename(day, start_time)), (datename(dw, start_time))



-- three months observation by day to find trend of customers
select (datename(day, start_time)) as day_of_month, (datename(dw, start_time)) as day_of_week, count(distinct(account_number)) as count_user
from wifi_sessions_20131015 and ((bytes_in = 0 and bytes_out = 0) or duration = 0)
group by datename(day, start_time), (datename(dw, start_time))


--------------------------------------------------------- days access wifi and used wifi
select days_access_wifi_Apr, count(*)
from  Sky_WIFI_account_attributes
where days_access_wifi_Apr >=0
group by days_access_wifi_Apr
order by 1


select days_access_wifi_May, count(*)
from  Sky_WIFI_account_attributes
where days_access_wifi_May >=0
group by days_access_wifi_May
order by 1


select days_access_wifi_Jun, count(*)
from  Sky_WIFI_account_attributes
where days_access_wifi_Jun >=0
group by days_access_wifi_Jun
order by 1


---- No of active devices for active sessions
select month, no_active_devices, count(*) from wifi_sessions_20131015_aggregated
group by month, no_active_devices
order by 1,2





