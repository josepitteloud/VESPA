/*
Analyst : Patrick Igonor
Date    : 20th of June 2013
Lead    : Claudio Lima
*/
select * from New_BARB_PANEL_VIEWING_FILE_VIEWING_RECORD_PANEL_MEMBERS
where Date_of_Activity_DB1 <> '2012-06-01'
--Tables of interest -
select top 10* from New_BARB_PANEL_VIEWING_FILE_VIEWING_RECORD_PANEL_MEMBERS; --- Has DB1_Station_Code
select top 10* from BARB_Log_Station_Relationship_to_DB1_Station_Record; ---Has DB1_Station_Code and Log_station_code
select top 10* from vespa_analysts.channel_map_dev_service_key_barb; ---Has service_key and log_station_code
select distinct channel_name from SK_Prod.VESPA_PROGRAMME_SCHEDULE order by 1; ---Has service_key, Programme_name, Channel_name, Genre, Sub_Genre,synopsis etc...
select top 10* from VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES; ---

--Matching Barb_Viewing data to the Master file based on Station_Code


select   VE.Household_number
        ,VE.Set_number
        ,VE.Date_of_Activity_DB1
        ,VE.Start_time_of_session
        ,VE.Event_Start_Date_Time
        ,VE.Event_End_Date_Time
        ,VE.Duration_of_session
        ,VE.DB1_Station_Code
        ,VE.Date_of_Recording_DB1
        ,VE.Start_time_of_recording
        ,VE.Person_1_viewing
        ,VE.Person_2_viewing
        ,VE.Person_3_viewing
        ,VE.Person_4_viewing
        ,VE.Person_5_viewing
        ,VE.Person_6_viewing
        ,VE.Person_7_viewing
        ,VE.Person_8_viewing
        ,VE.Person_9_viewing
        ,VE.Person_10_viewing
        ,VE.Person_11_viewing
        ,VE.Person_12_viewing
        ,VE.Person_13_viewing
        ,VE.Person_14_viewing
        ,VE.Person_15_viewing
        ,VE.Person_16_viewing
        ,VE.Session_activity_type_
        ,VE.Playback_type_
        ,VE.Viewing_platform_
        ,SC.log_Station_Code
        ,SC.Station_Code
        ,SC.Relationship_Start_Date
        ,SC.Relationship_End_Date
into     Panel_members_Station_Code
from New_BARB_PANEL_VIEWING_FILE_VIEWING_RECORD_PANEL_MEMBERS VE
left join New_BARB_Log_Station_Relationship_to_DB1_Station_Record SC
on VE.DB1_Station_Code = SC.Station_Code
and SC.Relationship_Start_Date <= '2012-06-01' and (SC.Relationship_End_Date >= '2012-06-01' or SC.Relationship_End_Date is null or trim(SC.Relationship_End_Date) = '')
---503,072 Row(s) affected

select top 10000 * from Panel_members_Station_Code

--Matching the above table (Panel_members_Station_Code) to vespa_analysts.channel_map_dev_service_key_barb based on Log_Station_Code


select   VESC.Household_number
        ,VESC.Set_number
        ,VESC.Date_of_Activity_DB1
        ,VESC.Start_time_of_session
        ,VESC.Event_Start_Date_Time
        ,VESC.Event_End_Date_Time
        ,VESC.Duration_of_session
        ,VESC.DB1_Station_Code
        ,VESC.Date_of_Recording_DB1
        ,VESC.Start_time_of_recording
        ,VESC.Person_1_viewing
        ,VESC.Person_2_viewing
        ,VESC.Person_3_viewing
        ,VESC.Person_4_viewing
        ,VESC.Person_5_viewing
        ,VESC.Person_6_viewing
        ,VESC.Person_7_viewing
        ,VESC.Person_8_viewing
        ,VESC.Person_9_viewing
        ,VESC.Person_10_viewing
        ,VESC.Person_11_viewing
        ,VESC.Person_12_viewing
        ,VESC.Person_13_viewing
        ,VESC.Person_14_viewing
        ,VESC.Person_15_viewing
        ,VESC.Person_16_viewing
        ,VESC.Session_activity_type_
        ,VESC.Playback_type_
        ,VESC.Viewing_platform_
        ,VESC.log_Station_Code
        ,VESC.Station_Code
        ,VESC.Relationship_Start_Date
        ,VESC.Relationship_End_Date
        ,LS.service_key
        ,LS.effective_from
        ,LS.effective_to
into    Panel_members_log_Station
from Panel_members_Station_Code VESC
left join vespa_analysts.channel_map_dev_service_key_barb LS
on VESC.log_Station_Code = LS.log_station_code
and LS.effective_from <= '2012-06-01'
and LS.effective_to >= '2012-06-01'
--2,136,306 Row(s) affected

--Matching the above table (Panel_members_log_Station) to VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES based on service_key in other to obtain the channel_name

select
         VESC.Household_number
        ,VESC.Set_number
        ,VESC.Date_of_Activity_DB1
        ,VESC.Start_time_of_session
        ,VESC.Event_Start_Date_Time
        ,VESC.Event_End_Date_Time
        ,VESC.Duration_of_session
        ,VESC.DB1_Station_Code
        ,VESC.Date_of_Recording_DB1
        ,VESC.Start_time_of_recording
        ,VESC.Person_1_viewing
        ,VESC.Person_2_viewing
        ,VESC.Person_3_viewing
        ,VESC.Person_4_viewing
        ,VESC.Person_5_viewing
        ,VESC.Person_6_viewing
        ,VESC.Person_7_viewing
        ,VESC.Person_8_viewing
        ,VESC.Person_9_viewing
        ,VESC.Person_10_viewing
        ,VESC.Person_11_viewing
        ,VESC.Person_12_viewing
        ,VESC.Person_13_viewing
        ,VESC.Person_14_viewing
        ,VESC.Person_15_viewing
        ,VESC.Person_16_viewing
        ,VESC.Session_activity_type_
        ,VESC.Playback_type_
        ,VESC.Viewing_platform_
        ,VESC.log_Station_Code
        ,VESC.Relationship_Start_Date
        ,VESC.Relationship_End_Date
        ,VESC.effective_from
        ,VESC.effective_to
        ,CM.CHANNEL_NAME
        ,CM.EPG_NAME
        ,CM.service_key
into   Barb_Viewing_Event_Vespa_Metadata
from Panel_members_log_Station VESC
left join VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES  CM
on VESC.service_key = CM.service_key
and CM.EFFECTIVE_FROM <= '2012-06-01'
and CM.EFFECTIVE_TO >= '2012-06-01'
--2,136,306 Row(s) affected

---Deduping the above data-----
select   row_number () over (partition by Household_number, Set_number, Event_Start_Date_Time order by len(coalesce(CHANNEL_NAME,'ZZZZZZZZZZZZZZZZ'))) as Row_Order
        ,Household_number
        ,Set_number
        ,Date_of_Activity_DB1
        ,Start_time_of_session
        ,Event_Start_Date_Time
        ,Event_End_Date_Time
        ,Duration_of_session
        ,DB1_Station_Code
        ,Date_of_Recording_DB1
        ,Start_time_of_recording
        ,Person_1_viewing
        ,Person_2_viewing
        ,Person_3_viewing
        ,Person_4_viewing
        ,Person_5_viewing
        ,Person_6_viewing
        ,Person_7_viewing
        ,Person_8_viewing
        ,Person_9_viewing
        ,Person_10_viewing
        ,Person_11_viewing
        ,Person_12_viewing
        ,Person_13_viewing
        ,Person_14_viewing
        ,Person_15_viewing
        ,Person_16_viewing
        ,Session_activity_type_
        ,Playback_type_
        ,Viewing_platform_
        ,log_Station_Code
        ,Relationship_Start_Date
        ,Relationship_End_Date
        ,service_key
        ,effective_from
        ,effective_to
        ,CHANNEL_NAME
        ,EPG_NAME
into  Barb_Viewing_Event_Vespa_Metadata_Dedups
from Barb_Viewing_Event_Vespa_Metadata
--2,136,306 Row(s) affected
--checks---
select top 1000* from Barb_Viewing_Event_Vespa_Metadata_Dedups


delete from Barb_Viewing_Event_Vespa_Metadata_Dedups
where Row_Order > 1
--2,030,633 Row(s) affected

select count(*) from  Barb_Viewing_Event_Vespa_Metadata_Dedups
--105,673

--Creating indexes to speed up the process -----

create hg index idx11 on Barb_Viewing_Event_Vespa_Metadata_Dedups(service_key,channel_name,Event_Start_Date_Time,Event_End_Date_Time);


--Matching the Barb_Viewing_Event_Vespa_Metadata_Dedups unto the Programme Schedule table in other to obtain all the programmes watched, the genre, subgenre, and synopsis based on channel_name....

select
         VESC.Household_number
        ,VESC.Set_number
        ,VESC.Date_of_Activity_DB1
        ,VESC.Start_time_of_session
        ,VESC.Event_Start_Date_Time
        ,VESC.Event_End_Date_Time
        ,VESC.Duration_of_session
        ,VESC.DB1_Station_Code
        ,VESC.Date_of_Recording_DB1
        ,VESC.Start_time_of_recording
        ,VESC.Person_1_viewing
        ,VESC.Person_2_viewing
        ,VESC.Person_3_viewing
        ,VESC.Person_4_viewing
        ,VESC.Person_5_viewing
        ,VESC.Person_6_viewing
        ,VESC.Person_7_viewing
        ,VESC.Person_8_viewing
        ,VESC.Person_9_viewing
        ,VESC.Person_10_viewing
        ,VESC.Person_11_viewing
        ,VESC.Person_12_viewing
        ,VESC.Person_13_viewing
        ,VESC.Person_14_viewing
        ,VESC.Person_15_viewing
        ,VESC.Person_16_viewing
        ,VESC.Session_activity_type_
        ,VESC.Playback_type_
        ,VESC.Viewing_platform_
        ,VESC.log_Station_Code
        ,VESC.Relationship_Start_Date
        ,VESC.Relationship_End_Date
        ,VESC.effective_from
        ,VESC.effective_to
        ,VESC.CHANNEL_NAME
        ,VESC.Service_key
        ,VESC.EPG_NAME
        ,SK.programme_name
        ,SK.genre_description
        ,SK.sub_genre_description
        ,SK.synopsis
        ,SK.broadcast_start_date_time_utc
        ,SK.broadcast_end_date_time_utc
into  Final_Barb_Viewing_Event_Vespa_Metadata
from Barb_Viewing_Event_Vespa_Metadata_Dedups VESC
left join SK_Prod.VESPA_PROGRAMME_SCHEDULE  SK
on VESC.service_key = SK.service_key
and UCASE(coalesce(VESC.channel_name,'Unknown')) = UCASE(coalesce(SK.CHANNEL_NAME,'Unknown'))
and SK.broadcast_end_date_time_utc >= VESC.Event_Start_Date_Time
and SK.broadcast_start_date_time_utc <= VESC.Event_End_Date_Time
--167,730 Row(s) affected

select count(*) from Final_Barb_Viewing_Event_Vespa_Metadata

select top 1000* from Final_Barb_Viewing_Event_Vespa_Metadata

--Final deduplication ---- drop this and use bradcast start time in the partition!!!
select   row_number () over (partition by Household_number, Set_number, Event_Start_Date_Time,programme_name order by channel_name) as Row_Order
        ,Household_number
        ,Set_number
        ,Date_of_Activity_DB1
        ,Start_time_of_session
        ,Event_Start_Date_Time
        ,Event_End_Date_Time
        ,Duration_of_session
        ,DB1_Station_Code
        ,Date_of_Recording_DB1
        ,Start_time_of_recording
        ,Person_1_viewing
        ,Person_2_viewing
        ,Person_3_viewing
        ,Person_4_viewing
        ,Person_5_viewing
        ,Person_6_viewing
        ,Person_7_viewing
        ,Person_8_viewing
        ,Person_9_viewing
        ,Person_10_viewing
        ,Person_11_viewing
        ,Person_12_viewing
        ,Person_13_viewing
        ,Person_14_viewing
        ,Person_15_viewing
        ,Person_16_viewing
        ,Session_activity_type_
        ,Playback_type_
        ,Viewing_platform_
        ,log_Station_Code
        ,Relationship_Start_Date
        ,Relationship_End_Date
        ,service_key
        ,effective_from
        ,effective_to
        ,CHANNEL_NAME
        ,EPG_NAME
        ,programme_name
        ,genre_description
        ,sub_genre_description
        ,synopsis
        ,broadcast_start_date_time_utc
        ,broadcast_end_date_time_utc
into  Final_Barb_Viewing_Event_Vespa_Metadata_dedups
from Final_Barb_Viewing_Event_Vespa_Metadata
--167,730 Row(s) affected

delete from Final_Barb_Viewing_Event_Vespa_Metadata_dedups where Row_Order > 1
--49,427 Row(s) affected

--checks
select top 1000* from Final_Barb_Viewing_Event_Vespa_Metadata_dedups

grant all on Final_Barb_Viewing_Event_Vespa_Metadata_dedups to limac;
grant all on Barb_Viewing_Event_Vespa_Metadata_Dedups to limac;
grant all on Final_Barb_Viewing_Event_Vespa_Metadata to limac;
grant all on Panel_members_log_Station to limac;
grant all on Panel_members_Station_Code to limac;
commit;

select top 100* from Final_Barb_Viewing_Event_Vespa_Metadata

