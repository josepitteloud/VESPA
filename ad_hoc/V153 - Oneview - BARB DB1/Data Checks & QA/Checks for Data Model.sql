/*

OneView - BARB DB1 Integration

Data Model Definition

*/

/*
QA of BARB tables
*/

-- Household characteristics
select top 1000 * from igonorp.BARB_PANEL_VIEWING_FILE_HOME_CHARACTERISTICS
select count(*) from igonorp.BARB_PANEL_VIEWING_FILE_HOME_CHARACTERISTICS -- 5,803
select top 1000 * from igonorp.New_BARB_PANEL_VIEWING_FILE_HOME_CHARACTERISTICS
select count(*) from igonorp.New_BARB_PANEL_VIEWING_FILE_HOME_CHARACTERISTICS -- 5,803

-- TV set characteristics
select top 1000 * from igonorp.BARB_PANEL_VIEWING_FILE_TV_SET_CHARACTERISTICS
select count(*) from igonorp.BARB_PANEL_VIEWING_FILE_TV_SET_CHARACTERISTICS -- 9,795
select top 1000 * from igonorp.New_BARB_PANEL_VIEWING_FILE_TV_SET_CHARACTERISTICS
select count(*) from igonorp.New_BARB_PANEL_VIEWING_FILE_TV_SET_CHARACTERISTICS -- 9,795

-- Panel members
select top 1000 * from igonorp.BARB_PANEL_VIEWING_FILE_INDIVIDAL_PANEL_MEMBER_DETAILS
select count(*) from igonorp.BARB_PANEL_VIEWING_FILE_INDIVIDAL_PANEL_MEMBER_DETAILS -- 13,893
select top 1000 * from igonorp.New_BARB_PANEL_VIEWING_FILE_INDIVIDAL_PANEL_MEMBER_DETAILS
select count(*) from igonorp.New_BARB_PANEL_VIEWING_FILE_INDIVIDAL_PANEL_MEMBER_DETAILS -- 13,893

-- Viewing events for panel members
select top 1000 * from igonorp.BARB_PANEL_VIEWING_FILE_VIEWING_RECORD_PANEL_MEMBERS
select count(*) from igonorp.BARB_PANEL_VIEWING_FILE_VIEWING_RECORD_PANEL_MEMBERS -- 105,673
select top 1000 * from igonorp.New_BARB_PANEL_VIEWING_FILE_VIEWING_RECORD_PANEL_MEMBERS
select count(*) from igonorp.New_BARB_PANEL_VIEWING_FILE_VIEWING_RECORD_PANEL_MEMBERS -- 105,673

-- Viewing events for guests
select top 1000 * from igonorp.BARB_PANEL_VIEWING_FILE_VIEWING_RECORD_GUESTS
select count(*) from igonorp.BARB_PANEL_VIEWING_FILE_VIEWING_RECORD_GUESTS -- 5925
select top 1000 * from igonorp.New_BARB_PANEL_VIEWING_FILE_VIEWING_RECORD_GUESTS
select count(*) from igonorp.New_BARB_PANEL_VIEWING_FILE_VIEWING_RECORD_GUESTS -- 5925

-- Weights for panel members
select top 1000 * from igonorp.BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY
select count(*) from igonorp.BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY
select top 1000 * from igonorp.New_BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY
select count(*) from igonorp.New_BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY

/*
BARB viewing events have a DB1_station_code that maps to log_Station_code that itself maps to service_key.
There are several log_station_code and/or service_key for the same DB1_station_code, so we have to
pick a winner to have a single channel_name for each DB1_station_code
*/
select SC.Station_Code as DB1_Station_Code
		,MAX(CM.service_key) as Service_Key
        ,MAX(CM.VESPA_NAME) as Channel_Name
into DB1_Station_Code_TO_Channel_Name
from igonorp.New_BARB_Log_Station_Relationship_to_DB1_Station_Record SC
left join vespa_analysts.channel_map_dev_service_key_barb LS
on SC.log_station_code = LS.log_station_code
and date(SC.Relationship_Start_Date) <= '2012-06-01' 
and (date(SC.Relationship_End_Date) >= '2012-06-07' 
    or SC.Relationship_End_Date is null 
    or trim(SC.Relationship_End_Date) = '')
and LS.effective_from <= '2012-06-01'
and LS.effective_to >= '2012-06-07'
left join VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES CM
on LS.service_key = CM.service_key
and CM.EFFECTIVE_FROM <= '2012-06-01'
and CM.EFFECTIVE_TO >= '2012-06-07'
where vespa_name is not null
group by DB1_Station_Code
order by DB1_Station_Code
-- 328

grant select on DB1_Station_Code_TO_Channel_Name to igonorp

/*
Channel mapping as per RSMB

select DB1.DB1_station_code
        ,DB1.db1_station_name 
        ,DB1_LOG.log_station_code
        ,LOG_SK.service_key
from igonorp.BARB_DB1_Stations_Reporting_Record DB1
    ,igonorp.New_BARB_Log_Station_Relationship_to_DB1_Station_Record DB1_LOG
    ,vespa_analysts.channel_map_dev_service_key_barb LOG_SK
where DB1.db1_station_code = convert(int,DB1_LOG.station_code)
and DB1_LOG.log_station_code = LOG_SK.log_station_code
and DB1.reporting_start_date <= 20120601
and (DB1.reportng_end_date >= 20120607 or DB1.reportng_end_date is null)
and date(DB1_LOG.Relationship_Start_Date) <= '2012-06-01' 
and (date(DB1_LOG.Relationship_End_Date) >= '2012-06-07' or DB1_LOG.Relationship_End_Date is null or trim(DB1_LOG.Relationship_End_Date)='') 
and LOG_SK.effective_from <= '2012-06-01'
and LOG_SK.effective_to >= '2012-06-07'
and LOG_SK.service_key is not null
and DB1.db1_station_name like '%BBC%'
group by DB1.DB1_station_code
        ,DB1.db1_station_name 
        ,DB1_LOG.log_station_code
        ,LOG_SK.service_key
order by LOG_SK.service_key
*/



select * from DB1_Station_Code_TO_Channel_Name order by channel_name

/*
If we use SK_Prod.VESPA_PROGRAMME_SCHEDULE to obtain channel/programme information
we can have different programmes for the different service_keys for DB1_station_code
*/
select *
from (
select station_code
        ,broadcast_start_date_time_utc
        ,broadcast_end_date_time_utc
        ,min(CHANNEL_NAME) as MIN_CHANNEL_NAME
        ,max(CHANNEL_NAME) as MAX_CHANNEL_NAME
        ,min(PROGRAMME_NAME) as MIN_PROGRAMME_NAME
        ,max(PROGRAMME_NAME) as MAX_PROGRAMME_NAME
from (
select SC.Station_Code
        ,LS.log_station_code
        ,PS.service_key
        ,PS.CHANNEL_NAME
        ,PS.PROGRAMME_NAME
        ,PS.broadcast_start_date_time_utc
        ,PS.broadcast_end_date_time_utc
from igonorp.New_BARB_Log_Station_Relationship_to_DB1_Station_Record SC
left join vespa_analysts.channel_map_dev_service_key_barb LS
on SC.log_station_code = LS.log_station_code
and SC.Relationship_Start_Date <= '2012-06-01' 
and (SC.Relationship_End_Date >= '2012-06-07' 
    or SC.Relationship_End_Date is null 
    or trim(SC.Relationship_End_Date) = '')
and LS.effective_from <= '2012-06-01'
and LS.effective_to >= '2012-06-07'
left join SK_Prod.VESPA_PROGRAMME_SCHEDULE PS
on LS.service_key = PS.service_key
and PS.broadcast_start_date_time_utc >= '2012-06-01 00:00:00'
and PS.broadcast_end_date_time_utc <= '2012-06-07 23:59:59'
) t1
group by Station_Code
        ,broadcast_start_date_time_utc
        ,broadcast_end_date_time_utc
) t2
where MIN_PROGRAMME_NAME <> MAX_PROGRAMME_NAME
-- 402

/*
 ****************************************
 *** Identification of Sky households ***
 ****************************************
*/

select Analogue_Terrestrial
        ,Digital_Terrestrial
        ,Analogue_Satellite
        ,Digital_Satellite
        ,Analogue_Cable
        ,Digital_Cable
        ,count(*)
from igonorp.BARB_PANEL_VIEWING_FILE_TV_SET_CHARACTERISTICS
where Sky_plus_PVR_present = 1
group by Analogue_Terrestrial
        ,Digital_Terrestrial
        ,Analogue_Satellite
        ,Digital_Satellite
        ,Analogue_Cable
        ,Digital_Cable
order by 7 desc

select count(distinct household_number) 
from igonorp.BARB_PANEL_VIEWING_FILE_TV_SET_CHARACTERISTICS
where Sky_plus_PVR_present = 1
-- 1,962

select count(distinct household_number) 
from igonorp.BARB_PANEL_VIEWING_FILE_TV_SET_CHARACTERISTICS
where digital_satellite = 1
-- 2,734

select count(distinct household_number) 
from igonorp.BARB_PANEL_VIEWING_FILE_TV_SET_CHARACTERISTICS
where reception_capability_code_1 = 2 -- '2' means Sky
-- 2,402

select count(distinct household_number) 
from igonorp.BARB_PANEL_VIEWING_FILE_TV_SET_CHARACTERISTICS
where reception_capability_code_1 = 2 
    or reception_capability_code_2 = 2
-- 2,407

------------------------------------------
-- Identification of sky households
------------------------------------------
select count(distinct household_number) 
from igonorp.New_BARB_PANEL_VIEWING_FILE_TV_SET_CHARACTERISTICS
where reception_capability_code_1_ = 'Sky' 
    or reception_capability_code_2_ = 'Sky' 
    or reception_capability_code_3_ = 'Sky' 
-- 2,407
------------------------------------------

