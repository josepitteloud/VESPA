/*
Analyst : Patrick Igonor
Date    : 08th of July 2013
Lead    : Claudio Lima
*/

--Tables of interest -
select top 10* from BARB_VIEWING_RECORD_PANEL_GUESTS_2012_06_01_07_Code_Descr; --- Has DB1_Station_Code
select top 10* from BARB_Log_Station_Relationship_to_DB1_Station_Record_02_2012_06_01_07_Recoded; ---Has DB1_Station_Code and Log_station_code
select top 10* from vespa_analysts.channel_map_dev_service_key_barb; ---Has service_key and log_station_code
select distinct channel_name from SK_Prod.VESPA_PROGRAMME_SCHEDULE order by 1; ---Has service_key, Programme_name, Channel_name, Genre, Sub_Genre,synopsis etc...
select top 10* from VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES; ---Extract unique channels
select count(*) from BARB_VIEWING_RECORD_PANEL_GUESTS_2012_06_01_07_Code_Descr

--Matching Barb_Viewing data to the Master file based on Station_Code

select   GVE.Household_number
        ,GVE.Date_of_Activity_DB1
        ,GVE.Set_number
        ,GVE.Start_time_of_session
        ,GVE.Event_Start_Date_Time
        ,GVE.Event_End_Date_Time
        ,GVE.Duration_of_session
        ,GVE.DB1_Station_Code
        ,GVE.Date_of_Recording_DB1
        ,GVE.Start_time_of_recording
        ,GVE.Recording_Start_Date_Time
        ,GVE.Recording_End_Date_Time
        ,GVE.Male_4_9
        ,GVE.Male_10_15
        ,GVE.Male_16_19
        ,GVE.Male_20_24
        ,GVE.Male_25_34
        ,GVE.Male_35_44
        ,GVE.Male_45_64
        ,GVE.Male_65_plus
        ,GVE.Female_4_9
        ,GVE.Female_10_15
        ,GVE.Female_16_19
        ,GVE.Female_20_24
        ,GVE.Female_25_34
        ,GVE.Female_35_44
        ,GVE.Female_45_64
        ,GVE.Female_65_plus
        ,GVE.Interactive_Bar_Code_Identifier
        ,GVE.Session_activity_type
        ,GVE.Session_activity_type_
        ,GVE.Playback_type
        ,GVE.Playback_type_
        ,GVE.Viewing_platform
        ,GVE.Viewing_platform_
        ,SC.log_Station_Code
        ,SC.Relationship_Start_Date
        ,SC.Relationship_End_Date
into     BARB_VIEWING_RECORD_PANEL_GUESTS_2012_06_01_07_Final
from BARB_VIEWING_RECORD_PANEL_GUESTS_2012_06_01_07_Code_Descr GVE
left join New_BARB_Log_Station_Relationship_to_DB1_Station_Record SC
on GVE.DB1_Station_Code = SC.Station_Code
and SC.Relationship_Start_Date <= '2012-06-01' and (SC.Relationship_End_Date >= '2012-06-07' or SC.Relationship_End_Date is null or trim(SC.Relationship_End_Date) = '')
--245,622 Row(s) affected

select top 100 * from BARB_VIEWING_RECORD_PANEL_GUESTS_2012_06_01_07_Final
select count(*) from BARB_VIEWING_RECORD_PANEL_GUESTS_2012_06_01_07_Code_Descr
--54,188 Row(s) Affected

--Matching the above table (BARB_VIEWING_RECORD_PANEL_GUESTS_2012_06_01_07_Final) to vespa_analysts.channel_map_dev_service_key_barb based on Log_Station_Code


select   GVE.Household_number
        ,GVE.Date_of_Activity_DB1
        ,GVE.Set_number
        ,GVE.Start_time_of_session
        ,GVE.Event_Start_Date_Time
        ,GVE.Event_End_Date_Time
        ,GVE.Duration_of_session
        ,GVE.DB1_Station_Code
        ,GVE.Date_of_Recording_DB1
        ,GVE.Start_time_of_recording
        ,GVE.Recording_Start_Date_Time
        ,GVE.Recording_End_Date_Time
        ,GVE.Male_4_9
        ,GVE.Male_10_15
        ,GVE.Male_16_19
        ,GVE.Male_20_24
        ,GVE.Male_25_34
        ,GVE.Male_35_44
        ,GVE.Male_45_64
        ,GVE.Male_65_plus
        ,GVE.Female_4_9
        ,GVE.Female_10_15
        ,GVE.Female_16_19
        ,GVE.Female_20_24
        ,GVE.Female_25_34
        ,GVE.Female_35_44
        ,GVE.Female_45_64
        ,GVE.Female_65_plus
        ,GVE.Interactive_Bar_Code_Identifier
        ,GVE.Session_activity_type
        ,GVE.Session_activity_type_
        ,GVE.Playback_type
        ,GVE.Playback_type_
        ,GVE.Viewing_platform
        ,GVE.Viewing_platform_
        ,GVE.log_Station_Code
        ,GVE.Relationship_Start_Date
        ,GVE.Relationship_End_Date
        ,LS.service_key
        ,LS.effective_from
        ,LS.effective_to
into   BARB_VIEWING_RECORD_PANEL_GUESTS_2012_06_01_07_Final_Log_station
from BARB_VIEWING_RECORD_PANEL_GUESTS_2012_06_01_07_Final GVE
left join vespa_analysts.channel_map_dev_service_key_barb LS
on GVE.log_Station_Code = LS.log_station_code
and LS.effective_from <= '2012-06-01'
and LS.effective_to >= '2012-06-07'
--906,053 Row(s) affected

--Matching the above table (Panel_members_log_Station) to VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES based on service_key in other to obtain the channel_name

select   GVE.Household_number
        ,GVE.Date_of_Activity_DB1
        ,GVE.Set_number
        ,GVE.Start_time_of_session
        ,GVE.Event_Start_Date_Time
        ,GVE.Event_End_Date_Time
        ,GVE.Duration_of_session
        ,GVE.DB1_Station_Code
        ,GVE.Date_of_Recording_DB1
        ,GVE.Start_time_of_recording
        ,GVE.Recording_Start_Date_Time
        ,GVE.Recording_End_Date_Time
        ,GVE.Male_4_9
        ,GVE.Male_10_15
        ,GVE.Male_16_19
        ,GVE.Male_20_24
        ,GVE.Male_25_34
        ,GVE.Male_35_44
        ,GVE.Male_45_64
        ,GVE.Male_65_plus
        ,GVE.Female_4_9
        ,GVE.Female_10_15
        ,GVE.Female_16_19
        ,GVE.Female_20_24
        ,GVE.Female_25_34
        ,GVE.Female_35_44
        ,GVE.Female_45_64
        ,GVE.Female_65_plus
        ,GVE.Interactive_Bar_Code_Identifier
        ,GVE.Session_activity_type
        ,GVE.Session_activity_type_
        ,GVE.Playback_type
        ,GVE.Playback_type_
        ,GVE.Viewing_platform
        ,GVE.Viewing_platform_
        ,GVE.log_Station_Code
        ,GVE.Relationship_Start_Date
        ,GVE.Relationship_End_Date
        ,GVE.effective_from
        ,GVE.effective_to
        ,CM.CHANNEL_NAME
        ,CM.EPG_NAME
        ,CM.service_key
into   BARB_VIEWING_RECORD_PANEL_GUESTS_VESPA_METADATA_2012_06_01_07
from BARB_VIEWING_RECORD_PANEL_GUESTS_2012_06_01_07_Final_Log_station GVE
left join VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES  CM
on GVE.service_key = CM.service_key
and CM.EFFECTIVE_FROM <= '2012-06-01'
and CM.EFFECTIVE_TO >= '2012-06-07'
--906,053 Row(s) affected

---Deduping the above table i.e BARB_VIEWING_RECORD_PANEL_GUESTS_VESPA_METADATA_2012_06_01_07-----
select   row_number () over (partition by Household_number, Set_number, Event_Start_Date_Time order by len(coalesce(CHANNEL_NAME,'ZZZZZZZZZZZZZZZZ'))) as Row_Order
        ,Household_number
        ,Date_of_Activity_DB1
        ,Set_number
        ,Start_time_of_session
        ,Event_Start_Date_Time
        ,Event_End_Date_Time
        ,Duration_of_session
        ,DB1_Station_Code
        ,Date_of_Recording_DB1
        ,Start_time_of_recording
        ,Recording_Start_Date_Time
        ,Recording_End_Date_Time
        ,Male_4_9
        ,Male_10_15
        ,Male_16_19
        ,Male_20_24
        ,Male_25_34
        ,Male_35_44
        ,Male_45_64
        ,Male_65_plus
        ,Female_4_9
        ,Female_10_15
        ,Female_16_19
        ,Female_20_24
        ,Female_25_34
        ,Female_35_44
        ,Female_45_64
        ,Female_65_plus
        ,Interactive_Bar_Code_Identifier
        ,Session_activity_type
        ,Session_activity_type_
        ,Playback_type
        ,Playback_type_
        ,Viewing_platform
        ,Viewing_platform_
        ,log_Station_Code
        ,Relationship_Start_Date
        ,Relationship_End_Date
        ,effective_from
        ,effective_to
        ,CHANNEL_NAME
        ,EPG_NAME
        ,service_key
into  BARB_VIEWING_RECORD_PANEL_GUESTS_VESPA_METADATA_2012_06_01_07_Dedups
from BARB_VIEWING_RECORD_PANEL_GUESTS_VESPA_METADATA_2012_06_01_07
--906,053 Row(s) affected
--checks---
select top 1000* from BARB_VIEWING_RECORD_PANEL_GUESTS_VESPA_METADATA_2012_06_01_07_Dedups


delete from BARB_VIEWING_RECORD_PANEL_GUESTS_VESPA_METADATA_2012_06_01_07_Dedups
where Row_Order > 1
--851865 Row(s) affected

select count(*) from  BARB_VIEWING_RECORD_PANEL_GUESTS_VESPA_METADATA_2012_06_01_07_Dedups
--54,188

--Creating indexes to speed up the process -----

create hg index idx11 on BARB_VIEWING_RECORD_PANEL_GUESTS_VESPA_METADATA_2012_06_01_07_Dedups(service_key,channel_name,Event_Start_Date_Time,Event_End_Date_Time,Recording_Start_Date_Time,Recording_End_Date_Time);


--Matching the BARB_VIEWING_RECORD_PANEL_GUESTS_VESPA_METADATA_2012_06_01_07_Dedups unto the Programme Schedule table in other to obtain all the programmes watched, the genre, subgenre, and synopsis based on channel_name....

select   GVE.Household_number
        ,GVE.Date_of_Activity_DB1
        ,GVE.Set_number
        ,GVE.Start_time_of_session
        ,GVE.Event_Start_Date_Time
        ,GVE.Event_End_Date_Time
        ,GVE.Duration_of_session
        ,GVE.DB1_Station_Code
        ,GVE.Date_of_Recording_DB1
        ,GVE.Start_time_of_recording
        ,GVE.Recording_Start_Date_Time
        ,GVE.Recording_End_Date_Time
        ,GVE.Male_4_9
        ,GVE.Male_10_15
        ,GVE.Male_16_19
        ,GVE.Male_20_24
        ,GVE.Male_25_34
        ,GVE.Male_35_44
        ,GVE.Male_45_64
        ,GVE.Male_65_plus
        ,GVE.Female_4_9
        ,GVE.Female_10_15
        ,GVE.Female_16_19
        ,GVE.Female_20_24
        ,GVE.Female_25_34
        ,GVE.Female_35_44
        ,GVE.Female_45_64
        ,GVE.Female_65_plus
        ,GVE.Interactive_Bar_Code_Identifier
        ,GVE.Session_activity_type
        ,GVE.Session_activity_type_
        ,GVE.Playback_type
        ,GVE.Playback_type_
        ,GVE.Viewing_platform
        ,GVE.Viewing_platform_
        ,GVE.log_Station_Code
        ,GVE.Relationship_Start_Date
        ,GVE.Relationship_End_Date
        ,GVE.effective_from
        ,GVE.effective_to
        ,GVE.CHANNEL_NAME
        ,GVE.EPG_NAME
        ,GVE.service_key
        ,SK.programme_name
        ,SK.genre_description
        ,SK.sub_genre_description
        ,SK.synopsis
        ,SK.broadcast_start_date_time_utc
        ,SK.broadcast_end_date_time_utc
into Final_Barb_Viewing_Event_Vespa_Metadata_2012_06_01_07_Guests
from BARB_VIEWING_RECORD_PANEL_GUESTS_VESPA_METADATA_2012_06_01_07_Dedups GVE
left join SK_Prod.VESPA_PROGRAMME_SCHEDULE  SK
on GVE.service_key = SK.service_key
and UCASE(coalesce(GVE.channel_name,'Unknown')) = UCASE(coalesce(SK.CHANNEL_NAME,'Unknown'))
and SK.broadcast_end_date_time_utc >= coalesce(GVE.Recording_Start_Date_Time,GVE.Event_Start_Date_Time)
and SK.broadcast_start_date_time_utc <= coalesce(GVE.Recording_End_Date_Time,GVE.Event_End_Date_Time)
and SK.programme_name is not null
--80,568 Row(s) affected

select top 100* from Final_Barb_Viewing_Event_Vespa_Metadata_2012_06_01_07_Guests

--Final deduplication --
select   row_number () over (partition by Household_number, Set_number, Event_Start_Date_Time,broadcast_start_date_time_utc order by channel_name) as Row_Order
        ,Household_number
        ,Date_of_Activity_DB1
        ,Set_number
        ,Start_time_of_session
        ,Event_Start_Date_Time
        ,Event_End_Date_Time
        ,Duration_of_session
        ,DB1_Station_Code
        ,Date_of_Recording_DB1
        ,Start_time_of_recording
        ,Recording_Start_Date_Time
        ,Recording_End_Date_Time
        ,Male_4_9
        ,Male_10_15
        ,Male_16_19
        ,Male_20_24
        ,Male_25_34
        ,Male_35_44
        ,Male_45_64
        ,Male_65_plus
        ,Female_4_9
        ,Female_10_15
        ,Female_16_19
        ,Female_20_24
        ,Female_25_34
        ,Female_35_44
        ,Female_45_64
        ,Female_65_plus
        ,Interactive_Bar_Code_Identifier
        ,Session_activity_type
        ,Playback_type
        ,Viewing_platform
        ,log_Station_Code
        ,Relationship_Start_Date
        ,Relationship_End_Date
        ,effective_from
        ,effective_to
        ,CHANNEL_NAME
        ,EPG_NAME
        ,service_key
        ,programme_name
        ,genre_description
        ,sub_genre_description
        ,synopsis
        ,broadcast_start_date_time_utc
        ,broadcast_end_date_time_utc
into Final_Barb_Viewing_Event_Vespa_Metadata_2012_06_01_07_Guests_dedups
from Final_Barb_Viewing_Event_Vespa_Metadata_2012_06_01_07_Guests
--80,568 Row(s) affected

delete from Final_Barb_Viewing_Event_Vespa_Metadata_2012_06_01_07_Guests_dedups where Row_Order > 1
--19,665 Row(s) affected

select count(*) from Final_Barb_Viewing_Event_Vespa_Metadata_2012_06_01_07_Guests_dedups
--60,903



select   service_key
        ,channel_name
        ,count(*) as Count
from Final_Barb_Viewing_Event_Vespa_Metadata_2012_06_01_07_Guests_dedups
where programme_name is null
group by service_key
        ,channel_name
order by Count desc

-- Add fields for Sky_Viewing
alter table Final_Barb_Viewing_Event_Vespa_Metadata_2012_06_01_07_Guests_dedups add Sky_Viewing bit default 0

-- Update Sky_Viewing
update Final_Barb_Viewing_Event_Vespa_Metadata_2012_06_01_07_Guests_dedups GD
set Sky_Viewing = 1
    from BARB_PANEL_VIEWING_FILE_TV_SET_CHARACTERISTICS_2012_06_01_07_Code_Descr ST
    where GD.Household_number =ST.Household_number
    and GD.Set_number =ST.Set_number
    and(ST.reception_capability_code_1_ = 'Sky'
    or ST.reception_capability_code_2_ = 'Sky'
    or ST.reception_capability_code_3_ = 'Sky')
--30,988 Row(s) affected

Alter table Final_Barb_Viewing_Event_Vespa_Metadata_2012_06_01_07_Guests_dedups
drop Row_Order


--checks
select top 100* from Final_Barb_Viewing_Event_Vespa_Metadata_2012_06_01_07_Guests_dedups

select count(*) from Final_Barb_Viewing_Event_Vespa_Metadata_2012_06_01_07_Guests_dedups
--62,015 Rows


----Granting Priviledges


grant all on Final_Barb_Viewing_Event_Vespa_Metadata_2012_06_01_07_Guests_dedups to limac;
grant all on Final_Barb_Viewing_Event_Vespa_Metadata_2012_06_01_07_Guests to limac;
grant all on BARB_VIEWING_RECORD_PANEL_GUESTS_VESPA_METADATA_2012_06_01_07_Dedups to limac;
grant all on BARB_VIEWING_RECORD_PANEL_MEMBERS_VESPA_METADATA_2012_06_01_07 to limac;
grant all on BARB_VIEWING_RECORD_PANEL_GUESTS_VESPA_METADATA_2012_06_01_07 to limac;
grant all on BARB_VIEWING_RECORD_PANEL_GUESTS_2012_06_01_07_Final_Log_station to limac;
grant all on BARB_VIEWING_RECORD_PANEL_GUESTS_2012_06_01_07_Final to limac;
grant all on BARB_VIEWING_RECORD_PANEL_GUESTS_2012_06_01_07_Code_Descr to limac;
commit;


