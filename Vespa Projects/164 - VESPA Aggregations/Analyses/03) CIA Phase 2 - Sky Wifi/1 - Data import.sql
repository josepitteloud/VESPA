/*###############################################################################
# Created on:   17/10/2013
# Created by:   Mandy Ng (MNG)
# Description:  CIA Aggregations - sky WiFi data import
#
# List of steps:
#               STEP 0.1 - upload Sky WiFi table to Olive environment
#               STEP 1.0 - Data audit
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#     - '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/wifi_sessions_20131015.csv'
#     - '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/wifi_zerobyte_sessions_20131015.csv'
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 17/10/2013  MNG   Initial version
# 11/11/2013  SBE   Clean up
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


--## WIFI_SESSION_SK is the unique identifier key
select count(distinct(WIFI_SESSION_SK))
from wifi_sessions_20131015
-- count(distinct(wifi_sessions_20131015.WIFI_SESSION_SK))
--35246686


--## the table cover sessions started between 2013-04-01 to 2013-06-30
select min(start_time), max(start_time)
from wifi_sessions_20131015
--min(wifi_sessions_20131015.start_time),max(wifi_sessions_20131015.end_time)
--'2013-04-01 00:00:00.000',2013-06-30 23:59:59.000000



--## distribution of duration? max, min, average, sd?
--## volume per month? by session? by distinct account?
select datepart(MM,start_time), count(*)
from wifi_sessions_20131015
group by datepart(MM,start_time)
order by 1
--datepart(mm,wifi_sessions_20131015.start_time),count()
--4,10488107
--5,12163815
--6,12594764


--including the zero session
datepart(mm,wifi_sessions_20131015.start_time)	count()
--4	14370979
--5	16461085
--6	16993196



select datepart(MM,start_time), count(distinct(owning_cust_account_id)), count(distinct(account_number))
from wifi_sessions_20131015
group by datepart(MM,start_time)
order by 1
--datepart(mm,wifi_sessions_20131015.start_time),count(distinct(wifi_sessions_20131015.owning_cust_account_id))
--4,559648
--5,587680
--6,597048


-- including the zero sessions
--datepart(mm,wifi_sessions_20131015.start_time)	count(distinct(wifi_sessions_20131015.owning_cust_account_id))	--count(distinct(wifi_sessions_20131015.account_number))
--4	574968	574962
--5	601532	601525
--6	611180	611173




--## what's the wifi usage behavious? Average session from Apri to June
select owning_cust_account_id, count(*)
into #temp
from wifi_sessions_20131015
group by (owning_cust_account_id)

--707557 row(s) affected


select  avg(expression), max(expression), min(expression), stddev(expression)
from #temp
--avg(#temp.expression),max(#temp.expression),min(#temp.expression),stddev(#temp.expression)
--49.8146241221555295,6351,1,78.69373326420724


--## aggregate to total session, total bytes in/out and total duration per account to examine the distribution
-- guide to set filters and measuring metric



-- ##############################################################################################################
-- ##############################################################################################################




