/* Mapping BARB Date/Time to Vespa Date/Time and also translating Barb codes to corresponding descriptions*/

---Mapping Barb date / time to reflect the same format as Vespa Date / time on the BARB_PANEL_VIEWING_FILE_VIEWING_RECORD_PANEL_MEMBERS table

select  file_creation_date
       ,file_creation_time
       ,file_type
       ,file_version
       ,filename
       ,Household_number
       ,Date_of_Activity_DB1
       ,Set_number
       ,Start_time_of_session
       ,cast(cast(floor(1.0*Start_time_of_session/100) as int) % 24 as int) as hour
       ,cast(Start_time_of_session as int) % 100 as min
       ,cast(hour || ':' || min as time) as Hour_Min
       ,cast(Date_of_Activity_DB1 || ' ' || Hour_Min as timestamp) as Event_Start_Date_Time
       ,Dateadd(minute,Duration_of_session,Event_Start_Date_Time) as Event_End_Date_Time
       ,Duration_of_session
       ,Session_activity_type
       ,Playback_type
       ,DB1_Station_Code
       ,Viewing_platform
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
       ,Interactive_Bar_Code_Identifier
into   New_BARB_PANEL_VIEWING_FILE_VIEWING_RECORD_PANEL_MEMBERS_
from BARB_PANEL_VIEWING_FILE_VIEWING_RECORD_PANEL_MEMBERS
--105,673 Row(s) affected

--Mapping Barb date / time to reflect the same format as Vespa Date / time on the BARB_PANEL_VIEWING_FILE_VIEWING_RECORD_GUESTS table

select  file_creation_date
       ,file_creation_time
       ,file_type
       ,file_version
       ,filename
       ,Household_number
       ,Date_of_Activity_DB1
       ,Set_number
       ,Start_time_of_session
       ,cast(cast(floor(1.0*Start_time_of_session/100) as int) % 24 as int) as hour
       ,cast(Start_time_of_session as int) % 100 as min
       ,cast(hour || ':' || min as time) as Hour_Min
       ,cast(Date_of_Activity_DB1 || ' ' || Hour_Min as timestamp) as Event_Start_Date_Time
       ,Dateadd(minute,Duration_of_session,Event_Start_Date_Time) as Event_End_Date_Time
       ,Duration_of_session
       ,Session_activity_type
       ,Playback_type
       ,DB1_Station_Code
       ,Viewing_platform
       ,Date_of_Recording_DB1
       ,Start_time_of_recording
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
into New_BARB_PANEL_VIEWING_FILE_VIEWING_RECORD_GUESTS_
from BARB_PANEL_VIEWING_FILE_VIEWING_RECORD_GUESTS
--5,925 Row(s) affected

--Checks ---
select top 10* from BARB_PANEL_VIEWING_FILE_HOME_CHARACTERISTICS
select top 10* from New_BARB_PANEL_VIEWING_FILE_VIEWING_RECORD_PANEL_MEMBERS
select top 10* from New_BARB_PANEL_VIEWING_FILE_VIEWING_RECORD_GUESTS
select Demographic_cell_1, count(*) from BARB_PANEL_VIEWING_FILE_HOME_CHARACTERISTICS
group by Demographic_cell_1
order by Demographic_cell_1


--Filling in code descriptions on the above tables ------

--Table_1 --BARB_PANEL_VIEWING_FILE_HOME_CHARACTERISTICS --

Select   Household_number
        ,cast(cast(Date_Valid_For as varchar) as date)
        ,No_of_TV_Sets
        ,No_of_VCRs
        ,No_of_PVRs
        ,No_of_DVDs
        ,No_of_People
        ,Social_Class
        ,Demographic_cell_1
        ,case when Demographic_cell_1 = 1 then 'Pre-Family - ABC1'
              when Demographic_cell_1 = 2 then 'Pre-Family - C2DE'
              when Demographic_cell_1 = 3 then 'Young-Family - ABC1'
              when Demographic_cell_1 = 4 then 'Young-Family - C2DE'
              when Demographic_cell_1 = 5 then 'Older-Family - ABC1'
              when Demographic_cell_1 = 6 then 'Older-Family - C2DE'
              when Demographic_cell_1 = 7 then 'Post Family - ABC1'
              when Demographic_cell_1 = 8 then 'Post Family - C2DE'
              when Demographic_cell_1 = 9 then 'Inactive - ABC1'
              when Demographic_cell_1 = 10 then 'Inactive - C2DE'
              end as Demographic_cell_1_
        ,BBC_ITV_Area_Segment
        ,S4C_Segment
        ,Number_of_DVD_Recorders
        ,Number_of_DVD_Players_not_recorders
        ,Number_of_Sky_plus_PVRs
        ,Number_of_other_PVRs
        ,Panel_membership_status
        ,case  when Panel_membership_status = 0 then 'Home on panel (valid reporter)'
              when Panel_membership_status = 1 then 'Home dropped off panel (non reporter)'
              end as Panel_Membership_Status_
        ,Presence_of_Children
        ,case when Presence_of_Children = 1 then 'No children'
              when Presence_of_Children = 2 then 'With children aged 0-3 years'
              when Presence_of_Children = 3 then 'With children aged 4-9 years'
              when Presence_of_Children = 4 then 'With children aged 0-3 and 4-9 years'
              when Presence_of_Children = 5 then 'With children aged 10-15 years'
              when Presence_of_Children = 6 then 'With children aged 0-3 and 10-15 years'
              when Presence_of_Children = 7 then 'With children aged 4-9 and 10-15 years'
              when Presence_of_Children = 8 then 'With children aged 0-3 and 4-9 and 10-15 years'
              when Presence_of_Children = 9 then 'Unclassified'
              end as Presence_of_Children_
         ,BBC_Region_code
         ,case when BBC_Region_Code = 503 then 'East'
               when BBC_Region_Code = 504 then 'West'
               when BBC_Region_Code = 505 then 'South West'
               when BBC_Region_Code = 506 then 'South'
               when BBC_Region_Code = 507 then 'Yorkshire & Lincolnshire'
               when BBC_Region_Code = 508 then 'North East & Cumbria'
               when BBC_Region_Code = 509 then 'North West'
               when BBC_Region_Code = 510 then 'Scotland'
               when BBC_Region_Code = 511 then 'Ulster'
               when BBC_Region_Code = 512 then 'Wales'
               when BBC_Region_Code = 513 then 'Midlands West'
               when BBC_Region_Code = 514 then 'Midland East'
               when BBC_Region_Code = 515 then 'London'
               when BBC_Region_Code = 516 then 'South East'
               end as BBC_Region_Code_
        ,Language_Spoken_at_Home
        ,case when Language_Spoken_at_Home = 1 then 'Welsh'
              when Language_Spoken_at_Home = 2 then 'English'
              when Language_Spoken_at_Home = 3 then 'Welsh & English equally'
              when Language_Spoken_at_Home = 4 then 'Welsh and other language than English (equally)'
              when Language_Spoken_at_Home = 5 then 'Other'
              when Language_Spoken_at_Home = 9 then 'Undefined'
              end as Language_Spoken_at_Home_
        ,Welsh_Speaking_Home
        ,case when Welsh_Speaking_Home = 1 then 'Wholly Welsh Speaking'
              when Welsh_Speaking_Home = 2 then 'Partly Welsh Speaking'
              when Welsh_Speaking_Home = 3 then 'Non Welsh Speaking'
              when Welsh_Speaking_Home = 9 then 'Unclassified'
              end as Welsh_Speaking_Home_
        ,Broadband
        ,case when Broadband = 1 then 'Home has Broadband Connection'
              when Broadband = 2 then 'Home has no Broadban Connection'
              end as Broadband_
        ,BBC_Sub_Reporting_Region
        ,case when BBC_Sub_Reporting_Region = 1 then 'BBC Wales (North)'
              when BBC_Sub_Reporting_Region = 2 then 'BBC Wales (West)'
              when BBC_Sub_Reporting_Region = 3 then 'BBC Wales (South)'
              when BBC_Sub_Reporting_Region = 4 then 'BBC Yorkshire and Lincolnshire (Humber)'
              when BBC_Sub_Reporting_Region = 5 then 'BBC Yorkshire and Lincolnshire (not Humber)'
              when BBC_Sub_Reporting_Region = 6 then 'BBC South (Oxford)'
              when BBC_Sub_Reporting_Region = 7 then 'BBC South (Hannington)'
              when BBC_Sub_Reporting_Region = 8 then 'BBC South (Rowridge)'
              when BBC_Sub_Reporting_Region = 9 then 'BBC Scotland (North)'
              when BBC_Sub_Reporting_Region = 10 then 'BBC Scotland (East)'
              when BBC_Sub_Reporting_Region = 11 then 'BBC Scotland (West)'
              when BBC_Sub_Reporting_Region = 12 then 'BBC Ulster (East)'
              when BBC_Sub_Reporting_Region = 13 then 'BBC Ulster (West)'
              when BBC_Sub_Reporting_Region = 14 then 'BBC East (West)'
              when BBC_Sub_Reporting_Region = 15 then 'BBC East (East)'
              when BBC_Sub_Reporting_Region = 99 then 'No BBC Sub Region, unclassified'
              end as BBC_Sub_Reporting_Region_
into New_BARB_PANEL_VIEWING_FILE_HOME_CHARACTERISTICS
from BARB_PANEL_VIEWING_FILE_HOME_CHARACTERISTICS
--5,803 Row(s) affected

--Table_2 --BARB_PANEL_VIEWING_FILE_TV_SET_CHARACTERISTICS -----
select   Household_number
        ,Date_Valid_for_DB1
        ,Set_number
        ,Blank_for_future_platforms
        ,Set_Membership_Status
        ,case when Set_Membership_Status = 0 then 'Set on Panel'
            when Set_Membership_Status = 1 then 'Set no longer on Panel'
            end as Set_Membership_Status_
       ,Teletext
       ,case when Teletext = 1 then 'With Teletext'
             when Teletext = 2 then 'Without Teletext'
             when Teletext = 9 then 'Unclassified'
             end as Teletext_
       ,Main_Location
       ,case when Main_Location = 1 then 'Main living room'
             when Main_Location = 2 then 'Kitchen'
             when Main_Location = 3 then 'Adults Room'
             when Main_Location = 4 then 'Child Bedroom'
             when Main_Location = 5 then 'Second Living/Dining Room'
             when Main_Location = 6 then 'No specific location/frequently moved'
             when Main_Location = 7 then 'Other'
             when Main_Location = 9 then 'Unclassified'
             end as Main_Location_
       ,Analogue_Terrestrial
       ,case when Analogue_Terrestrial = 1 then 'Receives Analogue Terrestrial'
             when Analogue_Terrestrial = 2 then 'Does not receive Analogue Terrestrial'
             when Analogue_Terrestrial = 9 then 'Unclassified'
             end as Analogue_Terrestrial_
       ,Digital_Terrestrial
       ,case when Digital_Terrestrial = 1 then 'Receives Digital Terrestrial'
             when Digital_Terrestrial = 2 then 'Does not receive Digital Terrestrial'
             when Digital_Terrestrial = 9 then 'Unclassified'
             end as Digital_Terrestrial_
       ,Analogue_Satellite
       ,case when Analogue_Satellite = 1 then 'Receives Analogue Satellite'
             when Analogue_Satellite = 2 then 'Does not receive Analogue Satellite'
             when Analogue_Satellite = 9 then 'Unclassified'
             end as Analogue_Satellite_
       ,Digital_Satellite
       ,case when Digital_Satellite = 1 then 'Receives Digital Satellite'
             when Digital_Satellite = 2 then 'Does not receive Digital Satellite'
             when Digital_Satellite = 9 then 'Unclassified'
             end as Digital_Satellite_
       ,Analogue_Cable
       ,case when Analogue_Cable = 1 then 'Receives Analogue Cable'
             when Analogue_Cable = 2 then 'Does not receive Analogue Cable'
             when Analogue_Cable = 9 then 'Unclassified'
             end as Analogue_Cable_
       ,Digital_Cable
       ,case when Digital_Cable = 1 then 'Receives Digital Cable'
             when Digital_Cable = 2 then 'Does not receive Digital Cable'
             when Digital_Cable = 9 then 'Unclassified'
             end as Digital_Cable_
       ,VCR_present
       ,case when VCR_present = 1 then 'Yes'
             when VCR_present = 2 then 'No'
             when VCR_present = 9 then 'Unclassified'
             end as VCR_present_
       ,Sky_plus_PVR_present
       ,case when Sky_plus_PVR_present = 1 then 'Yes'
             when Sky_plus_PVR_present = 2 then 'No'
             when Sky_plus_PVR_present = 9 then 'Unclassified'
             end as Sky_plus_PVR_present_
       ,Other_PVR_present
       ,case when Other_PVR_present = 1 then 'Yes'
             when Other_PVR_present = 2 then 'No'
             when Other_PVR_present = 9 then 'Unclassified'
             end as Other_PVR_present_
       ,DVD_Player_only_present
       ,case when DVD_Player_only_present = 1 then 'Yes'
             when DVD_Player_only_present = 2 then 'No'
             when DVD_Player_only_present = 9 then 'Unclassified'
             end as DVD_Player_only_present_
       ,DVD_Recorder_present
       ,case when DVD_Recorder_present = 1 then 'Yes'
             when DVD_Recorder_present = 2 then 'No'
             when DVD_Recorder_present = 9 then 'Unclassified'
             end as DVD_Recorder_present_
       ,HD_reception
       ,case when HD_reception = 1 then 'Yes'
             when HD_reception = 2 then 'No'
             when HD_reception = 9 then 'Unclassified'
             end as HD_reception_
       ,Reception_Capability_Code_1
       ,case when Reception_Capability_Code_1 = 1 then 'BT Vision'
             when Reception_Capability_Code_1 = 2 then 'Sky'
             when Reception_Capability_Code_1 = 3 then 'Freesat'
             when Reception_Capability_Code_1 = 4 then 'Virgin Media'
             when Reception_Capability_Code_1 = 5 then 'Tiscali'
             when Reception_Capability_Code_1 = 6 then 'PCTV'
             when Reception_Capability_Code_1 = 999 then 'Unclassified'
             end as Reception_Capability_Code_1_
       ,Reception_Capability_Code_2
       ,case when Reception_Capability_Code_2 = 1 then 'BT Vision'
             when Reception_Capability_Code_2 = 2 then 'Sky'
             when Reception_Capability_Code_2 = 3 then 'Freesat'
             when Reception_Capability_Code_2 = 4 then 'Virgin Media'
             when Reception_Capability_Code_2 = 5 then 'Tiscali'
             when Reception_Capability_Code_2 = 6 then 'PCTV'
             when Reception_Capability_Code_2 = 999 then 'Unclassified'
             end as Reception_Capability_Code_2_
       ,Reception_Capability_Code_3
       ,case when Reception_Capability_Code_3 = 1 then 'BT Vision'
             when Reception_Capability_Code_3 = 2 then 'Sky'
             when Reception_Capability_Code_3 = 3 then 'Freesat'
             when Reception_Capability_Code_3 = 4 then 'Virgin Media'
             when Reception_Capability_Code_3 = 5 then 'Tiscali'
             when Reception_Capability_Code_3 = 6 then 'PCTV'
             when Reception_Capability_Code_3 = 999 then 'Unclassified'
             end as Reception_Capability_Code_3_
       ,Reception_Capability_Code_4
       ,case when Reception_Capability_Code_4 = 1 then 'BT Vision'
             when Reception_Capability_Code_4 = 2 then 'Sky'
             when Reception_Capability_Code_4 = 3 then 'Freesat'
             when Reception_Capability_Code_4 = 4 then 'Virgin Media'
             when Reception_Capability_Code_4 = 5 then 'Tiscali'
             when Reception_Capability_Code_4 = 6 then 'PCTV'
             when Reception_Capability_Code_4 = 999 then 'Unclassified'
             end as Reception_Capability_Code_4_
       ,Reception_Capability_Code_5
       ,case when Reception_Capability_Code_5 = 1 then 'BT Vision'
             when Reception_Capability_Code_5 = 2 then 'Sky'
             when Reception_Capability_Code_5 = 3 then 'Freesat'
             when Reception_Capability_Code_5 = 4 then 'Virgin Media'
             when Reception_Capability_Code_5 = 5 then 'Tiscali'
             when Reception_Capability_Code_5 = 6 then 'PCTV'
             when Reception_Capability_Code_5 = 999 then 'Unclassified'
             end as Reception_Capability_Code_5_
       ,Reception_Capability_Code_6
       ,case when Reception_Capability_Code_6 = 1 then 'BT Vision'
             when Reception_Capability_Code_6 = 2 then 'Sky'
             when Reception_Capability_Code_6 = 3 then 'Freesat'
             when Reception_Capability_Code_6 = 4 then 'Virgin Media'
             when Reception_Capability_Code_6 = 5 then 'Tiscali'
             when Reception_Capability_Code_6 = 6 then 'PCTV'
             when Reception_Capability_Code_6 = 999 then 'Unclassified'
             end as Reception_Capability_Code_6_
       ,Reception_Capability_Code_7
       ,case when Reception_Capability_Code_7 = 1 then 'BT Vision'
             when Reception_Capability_Code_7 = 2 then 'Sky'
             when Reception_Capability_Code_7 = 3 then 'Freesat'
             when Reception_Capability_Code_7 = 4 then 'Virgin Media'
             when Reception_Capability_Code_7 = 5 then 'Tiscali'
             when Reception_Capability_Code_7 = 6 then 'PCTV'
             when Reception_Capability_Code_7 = 999 then 'Unclassified'
             end as Reception_Capability_Code_7_
       ,Reception_Capability_Code_8
       ,case when Reception_Capability_Code_8 = 1 then 'BT Vision'
             when Reception_Capability_Code_8 = 2 then 'Sky'
             when Reception_Capability_Code_8 = 3 then 'Freesat'
             when Reception_Capability_Code_8 = 4 then 'Virgin Media'
             when Reception_Capability_Code_8 = 5 then 'Tiscali'
             when Reception_Capability_Code_8 = 6 then 'PCTV'
             when Reception_Capability_Code_8 = 999 then 'Unclassified'
             end as Reception_Capability_Code_8_
       ,Reception_Capability_Code_9
       ,case when Reception_Capability_Code_9 = 1 then 'BT Vision'
             when Reception_Capability_Code_9 = 2 then 'Sky'
             when Reception_Capability_Code_9 = 3 then 'Freesat'
             when Reception_Capability_Code_9 = 4 then 'Virgin Media'
             when Reception_Capability_Code_9 = 5 then 'Tiscali'
             when Reception_Capability_Code_9 = 6 then 'PCTV'
             when Reception_Capability_Code_9 = 999 then 'Unclassified'
             end as Reception_Capability_Code_9_
       ,Reception_Capability_Code_10
       ,case when Reception_Capability_Code_10 = 1 then 'BT Vision'
             when Reception_Capability_Code_10 = 2 then 'Sky'
             when Reception_Capability_Code_10 = 3 then 'Freesat'
             when Reception_Capability_Code_10 = 4 then 'Virgin Media'
             when Reception_Capability_Code_10 = 5 then 'Tiscali'
             when Reception_Capability_Code_10 = 6 then 'PCTV'
             when Reception_Capability_Code_10 = 999 then 'Unclassified'
             end as Reception_Capability_Code_10_
into New_BARB_PANEL_VIEWING_FILE_TV_SET_CHARACTERISTICS
from BARB_PANEL_VIEWING_FILE_TV_SET_CHARACTERISTICS
--9,795 Row(s) affected

--Table_3 --BARB_PANEL_VIEWING_FILE_INDIVIDAL_PANEL_MEMBER_DETAILS -----

select   Household_number
        ,Date_valid_for_DB1
        ,Person_number
        ,Date_of_birth
       ,Person_membership_status
       ,case when Person_membership_status = 0 then 'Person on Panel'
            when Person_membership_status = 1 then 'Person no longer on Panel'
            end as Person_membership_status_
       ,Sex_code
       ,case when Sex_code = 1 then 'Male'
             when Sex_code = 2 then 'Female'
             end as Sex_code_
       ,Marital_status
       ,case when Marital_status = 1 then 'Married / living as married'
             when Marital_status = 2 then 'Single / divorced / separated'
             when Marital_status = 9 then 'Unclassified'
             end as Marital_status_
       ,Household_status
       ,case when Household_status = 1 then 'Neither housewife nor head of household'
             when Household_status = 2 then 'Housewife and NOT Head of Household'
             when Household_status = 3 then 'Head of household and NOT Housewife'
             when Household_status = 4 then 'Both housewife and head of household'
             when Household_status = 9 then 'Unclassified'
             end as Household_status_
      ,Working_status
      ,case when Working_status = 1 then 'Working 30+ hours per week'
             when Working_status = 2 then 'Working 8-29 hours per week'
             when Working_status = 3 then 'Working less than 8 hours per week'
             when Working_status = 4 then ' No paid work'
             when Working_status = 5 then 'Full time education / under school age'
             when Working_status = 9 then 'Unclassified'
             end as Working_status_
        ,Terminal_age_of_education
        ,case when Terminal_age_of_education = 1 then '15 years and under'
              when Terminal_age_of_education = 2 then '16-18 years'
              when Terminal_age_of_education = 3 then '19 + years'
              when Terminal_age_of_education = 4 then 'Still in education'
              when Terminal_age_of_education = 9 then 'Unclassified'
              end as Terminal_age_of_education_
        ,Welsh_Language_code
        ,case when Welsh_Language_code = 1 then 'Extremely well'
              when Welsh_Language_code = 2 then 'Quite well'
              when Welsh_Language_code = 3 then 'A little'
              when Welsh_Language_code = 4 then 'Can understand and speak some Welsh'
              when Welsh_Language_code = 5 then 'Can understand a little Welsh'
              when Welsh_Language_code = 9 then 'Not Welsh speaking/Not in Wales'
              when Welsh_Language_code = 0 then 'Unclassified'
              end as Welsh_Language_code_
        ,Gaelic_language_code
        ,case when Gaelic_Language_code = 1 then 'Extremely well'
              when Gaelic_Language_code = 2 then 'Quite well'
              when Gaelic_Language_code = 3 then 'A little'
              when Gaelic_Language_code = 4 then 'Can understand and speak some Gaelic'
              when Gaelic_Language_code = 5 then 'Can understand a little Gaelic'
              when Gaelic_Language_code = 9 then 'Not Gaelic speaking/Not in Scotland'
              when Gaelic_Language_code = 0 then 'Unclassified'
              end as Gaelic_Language_code_
        ,Dependency_of_Children
        ,case when Dependency_of_children = 1 then 'Parent/Guardian'
              when Dependency_of_children = 2 then 'Dependent Child'
              when Dependency_of_children = 3 then 'Neither'
              when Dependency_of_children = 9 then 'Unclassified'
              end as Dependency_of_children_
        ,Life_stage_12_classifications
        ,case when Life_stage_12_classifications = 1 then 'Single, no children, with parents, aged 16-34'
              when Life_stage_12_classifications = 2 then 'Single, no children, on own or with friends, aged 16-34'
              when Life_stage_12_classifications = 3 then 'Couple, no children, aged 16-34'
              when Life_stage_12_classifications = 4 then 'Single, no children, on own or with friends, aged 35-54'
              when Life_stage_12_classifications = 5 then 'Couple, no children, aged 35-54'
              when Life_stage_12_classifications = 6 then 'Single, no children, on own, aged 55+'
              when Life_stage_12_classifications = 7 then 'Couple, no children, aged 55+'
              when Life_stage_12_classifications = 8 then 'Either, youngest children 0-4, includes single parents'
              when Life_stage_12_classifications = 9 then 'Either, youngest children 5-9, includes single parents'
              when Life_stage_12_classifications = 10 then 'Either youngest children 10-15, includes single parents'
              when Life_stage_12_classifications = 11 then 'Either, Children 16+ none 0-15, aged 35+'
              when Life_stage_12_classifications = 12 then 'Other'
              when Life_stage_12_classifications = 99 then 'Unclassified'
              end as Life_stage_12_classifications_
        ,Ethnic_Origin
        ,case when Ethnic_Origin = 1 then 'White British'
              when Ethnic_Origin = 2 then 'Black – Caribbean'
              when Ethnic_Origin = 3 then 'Black – African'
              when Ethnic_Origin = 4 then 'Black – other'
              when Ethnic_Origin = 5 then 'Asian – Indian'
              when Ethnic_Origin = 6 then 'Asian – Pakistani'
              when Ethnic_Origin = 7 then 'Asian – Bangladeshi'
              when Ethnic_Origin = 8 then 'Chinese'
              when Ethnic_Origin = 9 then 'Any other background'
              when Ethnic_Origin = 10 then 'Other White'
              when Ethnic_Origin = 11 then 'Mixed – White/Black Caribbean'
              when Ethnic_Origin = 12 then 'Mixed – White/Black African'
              when Ethnic_Origin = 13 then 'Mixed – White/Asian'
              when Ethnic_Origin = 14 then 'Other mixed background'
              when Ethnic_Origin = 15 then 'Other Asian background'
              when Ethnic_Origin = 16 then 'Dont know'
              when Ethnic_Origin = 17 then 'Refused'
              when Ethnic_Origin = 99 then 'Unclassified'
              end as Ethnic_Origin_
into New_BARB_PANEL_VIEWING_FILE_INDIVIDAL_PANEL_MEMBER_DETAILS
from BARB_PANEL_VIEWING_FILE_INDIVIDAL_PANEL_MEMBER_DETAILS
--13,893 Row(s) affected

--Table_5 --BARB_PANEL_VIEWING_FILE_VIEWING_RECORD_PANEL_MEMBERS -----

select   Household_number
        ,Date_of_Activity_DB1
        ,Set_number
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
        ,Interactive_Bar_Code_Identifier
        ,Session_activity_type
        ,case when Session_activity_type = 1 then 'Live viewing (Excl Targeted Advertising)'
            when Session_activity_type = 4 then 'Un-coded Playback'
            when Session_activity_type = 5 then 'Time-shifted/coded playback (2-7 days) (Excl Targeted Advertising)'
            when Session_activity_type = 6 then 'Teletext'
            when Session_activity_type = 7 then 'Interactive'
            when Session_activity_type = 8 then 'EPG'
            when Session_activity_type = 9 then 'Interactive (include in Live Viewing)'
            when Session_activity_type = 11 then 'VOSDAL (Excl Targeted Advertising)'
            when Session_activity_type = 12 then 'Interactive Playback (include in VOSDAL)'
            when Session_activity_type = 13 then 'Live Viewing - Targeted Advertising'
            when Session_activity_type = 14 then 'Time-shifted/coded playback (2-7 days) - Targeted Advertising'
            when Session_activity_type = 15 then 'VOSDAL - Targeted Advertising'
            when Session_activity_type = 19 then 'Other (e.g. Play Station)'
            end as Session_activity_type_
       ,Playback_type
       ,case when Playback_type = '0' then 'N/A'
             when Playback_type = '1' then 'VCR device'
             when Playback_type = '2' then 'PVR device'
             when Playback_type = '3' then 'DVDR device'
             when Playback_type = '4' then 'DVDR - PVR Combi'
             when Playback_type = '5' then 'Other device'
             end as Playback_type_
       ,Viewing_platform
       ,case when Viewing_platform = 1 then 'Analogue terrestrial'
             when Viewing_platform = 2 then 'Digital terrestrial'
             when Viewing_platform = 3 then 'Analogue Satellite'
             when Viewing_platform = 4 then 'Digital Satellite'
             when Viewing_platform = 5 then 'Analogue Cable'
             when Viewing_platform = 6 then 'Digital Cable'
             when Viewing_platform = 7 then 'Other Platforms'
             end as Viewing_platform_
into New_BARB_PANEL_VIEWING_FILE_VIEWING_RECORD_PANEL_MEMBERS
from New_BARB_PANEL_VIEWING_FILE_VIEWING_RECORD_PANEL_MEMBERS_
--105,673 Row(s) affected

--Table_6 --BARB_PANEL_VIEWING_FILE_VIEWING_RECORD_GUESTS -----

select   Household_number
        ,Date_of_Activity_DB1
        ,Set_number
        ,Start_time_of_session
        ,Event_Start_Date_Time
        ,Event_End_Date_Time
        ,Duration_of_session
        ,DB1_Station_Code
        ,Date_of_Recording_DB1
        ,Start_time_of_recording
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
        ,case when Session_activity_type = 1 then 'Live viewing (Excl Targeted Advertising)'
            when Session_activity_type = 4 then 'Un-coded Playback'
            when Session_activity_type = 5 then 'Time-shifted/coded playback (2-7 days) (Excl Targeted Advertising)'
            when Session_activity_type = 6 then 'Teletext'
            when Session_activity_type = 7 then 'Interactive'
            when Session_activity_type = 8 then 'EPG'
            when Session_activity_type = 9 then 'Interactive (include in Live Viewing)'
            when Session_activity_type = 11 then 'VOSDAL (Excl Targeted Advertising)'
            when Session_activity_type = 12 then 'Interactive Playback (include in VOSDAL)'
            when Session_activity_type = 13 then 'Live Viewing - Targeted Advertising'
            when Session_activity_type = 14 then 'Time-shifted/coded playback (2-7 days) - Targeted Advertising'
            when Session_activity_type = 15 then 'VOSDAL - Targeted Advertising'
            when Session_activity_type = 19 then 'Other (e.g. Play Station)'
            end as Session_activity_type_
       ,Playback_type
       ,case when Playback_type = '0' then 'N/A'
             when Playback_type = '1' then 'VCR device'
             when Playback_type = '2' then 'PVR device'
             when Playback_type = '3' then 'DVDR device'
             when Playback_type = '4' then 'DVDR - PVR Combi'
             when Playback_type = '5' then 'Other device'
             end as Playback_type_
       ,Viewing_platform
       ,case when Viewing_platform = 1 then 'Analogue terrestrial'
             when Viewing_platform = 2 then 'Digital terrestrial'
             when Viewing_platform = 3 then 'Analogue Satellite'
             when Viewing_platform = 4 then 'Digital Satellite'
             when Viewing_platform = 5 then 'Analogue Cable'
             when Viewing_platform = 6 then 'Digital Cable'
             when Viewing_platform = 7 then 'Other Platforms'
             end as Viewing_platform_
into New_BARB_PANEL_VIEWING_FILE_VIEWING_RECORD_GUESTS
from New_BARB_PANEL_VIEWING_FILE_VIEWING_RECORD_GUESTS_
--5,925 Row(s) affected

---Granting Priviledges---------------------------------------------------------

grant all on PI_BARB_import to limac;

grant all on New_BARB_PANEL_VIEWING_FILE_HOME_CHARACTERISTICS to limac;

grant all on New_BARB_PANEL_VIEWING_FILE_TV_SET_CHARACTERISTICS to limac;

grant all on New_BARB_PANEL_VIEWING_FILE_INDIVIDAL_PANEL_MEMBER_DETAILS to limac;

grant all on BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY to limac;

grant all on New_BARB_PANEL_VIEWING_FILE_VIEWING_RECORD_PANEL_MEMBERS to limac;

grant all on New_BARB_PANEL_VIEWING_FILE_VIEWING_RECORD_GUESTS to limac;




