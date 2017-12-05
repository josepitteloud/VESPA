

-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
-- Project Name: Skyview Futures: Household to Individual Model
-- Author: Jason Thompson (jason.thompson@skyiq.co.uk)
-- Insight Collation: V289
-- Date: 6 June 2014


-- Business Brief:
--      To gain insights into what type of individuals watch TV together by day of week and time and genre. And how this changes by affluence and household composition.
--      These insights will feed into building a model to estimate the number of individuals watching TV from the Vespa data

-- Code Summary:
--      Transform the raw Barb DB viewing data into a format more useable for analysis. Match the Barb viewing events aganist Vespa programme schedule to understand genre etc

-- Modules:
-- A: Load raw Barb viewing data and process (see document BARB2010DSP01 Version 3.7C - Panel Viewing File.pdf for details)
--      A1: Create tables to hold raw Barb data
--      A2: Load raw Barb data into above tables
--      A3: Create tables to hold processed Barb data - more useful datatypes e.g. timestamp instead of text
--      A4: Process and load Barb data into above tables
--      A5: Combine the PVF and PV2 viewing data into the same tables

-- B: Match Barb viewing data to Vespa EPG and transform for analysis
--      B1: Get age/gender groups for panel member viewing
--      B2: Get Event start/end times
--      B3: Match viewing to Vespa programme schedule

-- C: Some QA checks

-- D: Grant permissions on tables

-- Issues/bugs
--      1. In A4, the session end times have been incorrected calculated. This is rectified in B1. Ideally this should be resolved.
--      2. In B2 I have used VOD_indicator as part of the definition an an event.
--              The data suggests this is the case, but not consistent with Barb definition.
--      3. In A4, I have not dealt with the 2 days of the year when clocks change
--      4. In A4, there are comments on potential bug when an event spans midnight and the calculated end date time is incorrect.
--              I believe this is OK.

-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------



-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
-- A1: Create tables to hold raw Barb data
-- These follow the spec in the Barb documentation (BARB2010DSP01 Version 3.7C - Panel Viewing File.pdf)
-- Note that Barb generate these tables every day. So to use these tables you need to use the data generated on the same day across them all

---- Details of each Household on the Barb panel. There are about 5000
---- Filename, Household_number are the unique fields
CREATE TABLE BARB_Panel_Demographic_Data_Home_Characteristics (
file_creation_date date, file_creation_time time, file_type varchar(12), file_version int, filename varchar(13)
,Record_Type int DEFAULT NULL
,Household_number int DEFAULT NULL
,Date_Valid_For int DEFAULT NULL
,Panel_membership_status int DEFAULT NULL
,No_of_TV_Sets int DEFAULT NULL
,No_of_VCRs int DEFAULT NULL
,No_of_PVRs int DEFAULT NULL
,No_of_DVDs int DEFAULT NULL
,No_of_People int DEFAULT NULL
,Social_Class varchar(2) DEFAULT NULL
,Presence_of_Children int DEFAULT NULL
,Demographic_cell_1 int DEFAULT NULL
,BBC_Region_code int DEFAULT NULL
,BBC_ITV_Area_Segment int DEFAULT NULL
,S4C_Segment int DEFAULT NULL
,Language_Spoken_at_Home int DEFAULT NULL
,Welsh_Speaking_Home int DEFAULT NULL
,Number_of_DVD_Recorders int DEFAULT NULL
,Number_of_DVD_Players_not_recorders int DEFAULT NULL
,Number_of_Sky_PVRs int DEFAULT NULL
,Number_of_other_PVRs int DEFAULT NULL
,Broadband int DEFAULT NULL
,BBC_Sub_Reporting_Region int DEFAULT NULL
,Mosaic_Classification int DEFAULT NULL
)


---- Details of each TV in each Household
---- Filename, , Household_number, Set_number are unique fields
CREATE TABLE BARB_Panel_Demographic_Data_TV_Sets_Characteristics (
file_creation_date date, file_creation_time time, file_type varchar(12), file_version int, filename varchar(13)
,Record_Type int DEFAULT NULL
,Household_number int DEFAULT NULL
,Date_Valid_for_DB1 int DEFAULT NULL
,Set_Membership_Status int DEFAULT NULL
,Set_number int DEFAULT NULL
,Teletext int DEFAULT NULL
,Main_Location int DEFAULT NULL
,Analogue_Terrestrial int DEFAULT NULL
,Digital_Terrestrial int DEFAULT NULL
,Analogue_Satellite int DEFAULT NULL
,Digital_Satellite int DEFAULT NULL
,Analogue_Cable int DEFAULT NULL
,Digital_Cable int DEFAULT NULL
,VCR_present int DEFAULT NULL
,Sky_PVR_present int DEFAULT NULL
,Other_PVR_present int DEFAULT NULL
,DVD_Player_only_present int DEFAULT NULL
,DVD_Recorder_present int DEFAULT NULL
,HD_reception int DEFAULT NULL
,Reception_Capability_Code1 int DEFAULT NULL
,Reception_Capability_Code2 int DEFAULT NULL
,Reception_Capability_Code3 int DEFAULT NULL
,Reception_Capability_Code4 int DEFAULT NULL
,Reception_Capability_Code5 int DEFAULT NULL
,Reception_Capability_Code6 int DEFAULT NULL
,Reception_Capability_Code7 int DEFAULT NULL
,Reception_Capability_Code8 int DEFAULT NULL
,Reception_Capability_Code9 int DEFAULT NULL
,Reception_Capability_Code10 int DEFAULT NULL
)


---- Details of each panel member
---- Filename, Household_number, Person_number are unique fields
CREATE TABLE BARB_Individual_Panel_Member_Details (
file_creation_date date, file_creation_time time, file_type varchar(12), file_version int, filename varchar(13)
,Record_type int DEFAULT NULL
,Household_number int DEFAULT NULL
,Date_valid_for_DB1 int DEFAULT NULL
,Person_membership_status int DEFAULT NULL
,Person_number int DEFAULT NULL
,Sex_code int DEFAULT NULL
,Date_of_birth int DEFAULT NULL
,Marital_status int DEFAULT NULL
,Household_status int DEFAULT NULL
,Working_status int DEFAULT NULL
,Terminal_age_of_education int DEFAULT NULL
,Welsh_Language_code int DEFAULT NULL
,Gaelic_language_code int DEFAULT NULL
,Dependency_of_Children int DEFAULT NULL
,Life_stage_12_classifications int DEFAULT NULL
,Ethnic_Origin int DEFAULT NULL
)



---- Barb apply weight to each panel member to scale up to UK base
---- Filename, Household_number, Person_number are unique fields
CREATE TABLE BARB_Panel_Member_Responses_Weights_and_Viewing_Categories (
file_creation_date date, file_creation_time time, file_type varchar(12), file_version int, filename varchar(13)
,Record_Type int DEFAULT NULL
,Household_Number int DEFAULT NULL
,Person_Number int DEFAULT NULL
,Reporting_Panel_Code int DEFAULT NULL
,Date_of_Activity_DB1 int DEFAULT NULL
,Response_Code int DEFAULT NULL
,Processing_Weight int DEFAULT NULL
,Adults_Commercial_TV_Viewing_Sextile int DEFAULT NULL
,ABC1_Adults_Commercial_TV_Viewing_Sextile int DEFAULT NULL
,Adults_Total_Viewing_Sextile int DEFAULT NULL
,ABC1_Adults_Total_Viewing_Sextile int DEFAULT NULL
,Adults_16_34_Commercial_TV_Viewing_Sextile int DEFAULT NULL
,Adults_16_34_Total_Viewing_Sextile int DEFAULT NULL
)


---- The Viewing Data. Note that this file only contains Live and VOSDAL
---- PV2 files contain other timeshift events
---- See BARB2010DSP01 Version 3.7C - Panel Viewing File.pdf for more details
---- Filename, Household_number, Set_number,  Start_time_of_session are unique fields
CREATE TABLE BARB_PVF_Viewing_Record_Panel_Members (
file_creation_date date, file_creation_time time, file_type varchar(12), file_version int, filename varchar(13)
,Record_type int DEFAULT NULL
,Household_number int DEFAULT NULL
,Date_of_Activity_DB1 int DEFAULT NULL
,Set_number int DEFAULT NULL
,Start_time_of_session int DEFAULT NULL
,Duration_of_session int DEFAULT NULL
,Session_activity_type int DEFAULT NULL
,Playback_type varchar(1) DEFAULT NULL
,DB1_Station_Code int DEFAULT NULL
,Viewing_platform int DEFAULT NULL
,Date_of_Recording_DB1 int DEFAULT NULL
,Start_time_of_recording int DEFAULT NULL
,Person_1_viewing int DEFAULT NULL
,Person_2_viewing int DEFAULT NULL
,Person_3_viewing int DEFAULT NULL
,Person_4_viewing int DEFAULT NULL
,Person_5_viewing int DEFAULT NULL
,Person_6_viewing int DEFAULT NULL
,Person_7_viewing int DEFAULT NULL
,Person_8_viewing int DEFAULT NULL
,Person_9_viewing int DEFAULT NULL
,Person_10_viewing int DEFAULT NULL
,Person_11_viewing int DEFAULT NULL
,Person_12_viewing int DEFAULT NULL
,Person_13_viewing int DEFAULT NULL
,Person_14_viewing int DEFAULT NULL
,Person_15_viewing int DEFAULT NULL
,Person_16_viewing int DEFAULT NULL
,Interactive_Bar_Code_Identifier int DEFAULT NULL
,VOD_Indicator int DEFAULT NULL
,VOD_Provider int DEFAULT NULL
,VOD_Service int DEFAULT NULL
,VOD_Type int DEFAULT NULL
,Device_in_use int DEFAULT NULL
)

create hg index ind_household_number on BARB_PVF_Viewing_Record_Panel_Members(household_number)
create hg index ind_db1 on BARB_PVF_Viewing_Record_Panel_Members(db1_station_code)



---- The Barb data also captures any viewing from guests to a Barb household
---- This is captured seperately in this file. Again this file only contains Live and VOSDAL
---- Filename, Household_number, Set_number,  Start_time_of_session are unique fields
CREATE TABLE BARB_PV2_Viewing_Record_Panel_Members (
file_creation_date date, file_creation_time time, file_type varchar(12), file_version int, filename varchar(13)
,Record_type int DEFAULT NULL
,Household_number int DEFAULT NULL
,Date_of_Activity_DB1 int DEFAULT NULL
,Set_number int DEFAULT NULL
,Start_time_of_session int DEFAULT NULL
,Duration_of_session int DEFAULT NULL
,Session_activity_type int DEFAULT NULL
,Playback_type varchar(1) DEFAULT NULL
,DB1_Station_Code varchar(5) DEFAULT NULL
,Viewing_platform int DEFAULT NULL
,Date_of_Recording_DB1 int DEFAULT NULL
,Start_time_of_recording int DEFAULT NULL
,Person_1_viewing int DEFAULT NULL
,Person_2_viewing int DEFAULT NULL
,Person_3_viewing int DEFAULT NULL
,Person_4_viewing int DEFAULT NULL
,Person_5_viewing int DEFAULT NULL
,Person_6_viewing int DEFAULT NULL
,Person_7_viewing int DEFAULT NULL
,Person_8_viewing int DEFAULT NULL
,Person_9_viewing int DEFAULT NULL
,Person_10_viewing int DEFAULT NULL
,Person_11_viewing int DEFAULT NULL
,Person_12_viewing int DEFAULT NULL
,Person_13_viewing int DEFAULT NULL
,Person_14_viewing int DEFAULT NULL
,Person_15_viewing int DEFAULT NULL
,Person_16_viewing int DEFAULT NULL
,Interactive_Bar_Code_Identifier int DEFAULT NULL
,VOD_Indicator int DEFAULT NULL
,VOD_Provider int DEFAULT NULL
,VOD_Service int DEFAULT NULL
,VOD_Type int DEFAULT NULL
,Device_in_use int DEFAULT NULL
)


CREATE TABLE BARB_PVF_Viewing_Record_Guests (
file_creation_date date, file_creation_time time, file_type varchar(12), file_version int, filename varchar(13)
,Record_type int DEFAULT NULL
,Household_number int DEFAULT NULL
,Date_of_Activity_DB1 int DEFAULT NULL
,Set_number int DEFAULT NULL
,Start_time_of_session int DEFAULT NULL
,Duration_of_session int DEFAULT NULL
,Session_activity_type int DEFAULT NULL
,Playback_type varchar(1) DEFAULT NULL
,DB1_Station_Code varchar(5) DEFAULT NULL
,Viewing_platform int DEFAULT NULL
,Date_of_Recording_DB1 int DEFAULT NULL
,Start_time_of_recording int DEFAULT NULL
,Male_4_9 int DEFAULT NULL
,Male_10_15 int DEFAULT NULL
,Male_16_19 int DEFAULT NULL
,Male_20_24 int DEFAULT NULL
,Male_25_34 int DEFAULT NULL
,Male_35_44 int DEFAULT NULL
,Male_45_64 int DEFAULT NULL
,Male_65 int DEFAULT NULL
,Female_4_9 int DEFAULT NULL
,Female_10_15 int DEFAULT NULL
,Female_16_19 int DEFAULT NULL
,Female_20_24 int DEFAULT NULL
,Female_25_34 int DEFAULT NULL
,Female_35_44 int DEFAULT NULL
,Female_45_64 int DEFAULT NULL
,Female_65 int DEFAULT NULL
,Interactive_Bar_Code_Identifier int DEFAULT NULL
,VOD_Indicator int DEFAULT NULL
,VOD_Provider int DEFAULT NULL
,VOD_Service int DEFAULT NULL
,VOD_Type int DEFAULT NULL
,Device_in_use int DEFAULT NULL
)



---- PV2 viewing files esentially capture timeshifted 1-28 days
---- These files are available the day after the PVF files which contain Live and VOSDAL
---- The fields are the same as the equivilent PVF files
CREATE TABLE BARB_PV2_Viewing_Record_Panel_Members (
file_creation_date date, file_creation_time time, file_type varchar(12), file_version int, filename varchar(13)
,Record_type int DEFAULT NULL
,Household_number int DEFAULT NULL
,Date_of_Activity_DB1 int DEFAULT NULL
,Set_number int DEFAULT NULL
,Start_time_of_session int DEFAULT NULL
,Duration_of_session int DEFAULT NULL
,Session_activity_type int DEFAULT NULL
,Playback_type varchar(1) DEFAULT NULL
,DB1_Station_Code varchar(5) DEFAULT NULL
,Viewing_platform int DEFAULT NULL
,Date_of_Recording_DB1 int DEFAULT NULL
,Start_time_of_recording int DEFAULT NULL
,Person_1_viewing int DEFAULT NULL
,Person_2_viewing int DEFAULT NULL
,Person_3_viewing int DEFAULT NULL
,Person_4_viewing int DEFAULT NULL
,Person_5_viewing int DEFAULT NULL
,Person_6_viewing int DEFAULT NULL
,Person_7_viewing int DEFAULT NULL
,Person_8_viewing int DEFAULT NULL
,Person_9_viewing int DEFAULT NULL
,Person_10_viewing int DEFAULT NULL
,Person_11_viewing int DEFAULT NULL
,Person_12_viewing int DEFAULT NULL
,Person_13_viewing int DEFAULT NULL
,Person_14_viewing int DEFAULT NULL
,Person_15_viewing int DEFAULT NULL
,Person_16_viewing int DEFAULT NULL
,Interactive_Bar_Code_Identifier int DEFAULT NULL
,VOD_Indicator int DEFAULT NULL
,VOD_Provider int DEFAULT NULL
,VOD_Service int DEFAULT NULL
,VOD_Type int DEFAULT NULL
,Device_in_use int DEFAULT NULL
)


CREATE TABLE BARB_PV2_Viewing_Record_Panel_Guests (
file_creation_date date, file_creation_time time, file_type varchar(12), file_version int, filename varchar(13)
,Record_type int DEFAULT NULL
,Household_number int DEFAULT NULL
,Date_of_Activity_DB1 int DEFAULT NULL
,Set_number int DEFAULT NULL
,Start_time_of_session int DEFAULT NULL
,Duration_of_session int DEFAULT NULL
,Session_activity_type int DEFAULT NULL
,Playback_type varchar(1) DEFAULT NULL
,DB1_Station_Code varchar(5) DEFAULT NULL
,Viewing_platform int DEFAULT NULL
,Date_of_Recording_DB1 int DEFAULT NULL
,Start_time_of_recording int DEFAULT NULL
,Male_4_9 int DEFAULT NULL
,Male_10_15 int DEFAULT NULL
,Male_16_19 int DEFAULT NULL
,Male_20_24 int DEFAULT NULL
,Male_25_34 int DEFAULT NULL
,Male_35_44 int DEFAULT NULL
,Male_45_64 int DEFAULT NULL
,Male_65 int DEFAULT NULL
,Female_4_9 int DEFAULT NULL
,Female_10_15 int DEFAULT NULL
,Female_16_19 int DEFAULT NULL
,Female_20_24 int DEFAULT NULL
,Female_25_34 int DEFAULT NULL
,Female_35_44 int DEFAULT NULL
,Female_45_64 int DEFAULT NULL
,Female_65 int DEFAULT NULL
,Interactive_Bar_Code_Identifier int DEFAULT NULL
,VOD_Indicator int DEFAULT NULL
,VOD_Provider int DEFAULT NULL
,VOD_Service int DEFAULT NULL
,VOD_Type int DEFAULT NULL
,Device_in_use int DEFAULT NULL
)



-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
-- A2: Load raw Barb data into above tables
-- WinSCP used to load the raw data into ETL
-- This code then loads from ETL into tables in A1

-- This create 2 procedures called sp_BARB_PVF and sp_BARB_PV2 which can be used to load 1 day at a time for PVF and PV2 files
-- Note that all the Barb tables listed in A1 are in a 2 files with extension PVF or PV2
-- There are 2 files for each day with naming convention e.g. B20130923.PVF or B20130923.PV2
-- The files are fixed width (the doc BARB2010DSP01 Version 3.7C - Panel Viewing File.pdf provide details)

-- Run the procedures for each day we want to load data into the above tables
-- Need to first load the files using WinSCP
-- I have created a folder in WinSCP called JasonT.
-- You can create your own folder and change in below

-- Load PVF files
EXEC sp_BARB_PVF 'JasonT/B20130923.PVF'
EXEC sp_BARB_PVF 'JasonT/B20130924.PVF'
EXEC sp_BARB_PVF 'JasonT/B20130925.PVF'

-- Load PV2 files
EXEC sp_BARB_PV2 'JasonT/B20130923.PV2'
EXEC sp_BARB_PV2 'JasonT/B20130924.PV2'
EXEC sp_BARB_PV2 'JasonT/B20130925.PV2'



---- Procedure to import the raw Barb PVF files
create PROCEDURE sp_BARB_PVF (@in_filename varchar(60)) AS
BEGIN
/*

Usage
EXEC sp_BARB_PVF 'JasonT/B20130916.PVF'
Note this is incremental
*/

DECLARE @query varchar(3000)
DECLARE @file_creation_date date
DECLARE @file_creation_time time
DECLARE @file_type  varchar(12)
DECLARE @File_Version Int
DECLARE @audit_row_count bigint
DECLARE @filename varchar(13)

-- This has 1 text column of varchar(10000) to hold a full row of data
-- Make sure this is empty
DELETE FROM PI_BARB_import

-- Load all the data into PI_BARB_import from the specified file
SET @query = 'LOAD TABLE PI_BARB_import (imported_text '
SET @query = @query || ' ''\n'' ) '
SET @query = @query || ' FROM ''/ETL013/prod/sky/olive/data/share/clarityq/export/' || @in_filename || ''' QUOTES OFF ESCAPES OFF NOTIFY 1000'
EXECUTE (@query)

SET @audit_row_count = (Select count(1) from PI_BARB_IMPORT)

-- check if file doesn't exist
IF @audit_row_count = 0
BEGIN
        SELECT now(), @file_creation_Date, @file_creation_time, @file_type, @file_version, @filename,'File not found ' || @in_filename, @audit_row_count

        RETURN
END


-- parse out the data records
-- From the Barb file spec document we can pull off some key fields from the header file
SET @file_creation_date = (SELECT CAST(substr(imported_text,7,8) AS Date)
                                FROM PI_BARB_import
                                WHERE substr(imported_text,1,2) = '01')

SET @file_creation_time = (SELECT CAST(substr(imported_text,15,2) || ':' || substr(imported_text,17,2) || ':' || substr(imported_text,19,2)  AS Time)
                                FROM PI_BARB_import
                                WHERE substr(imported_text,1,2) = '01')

SET @file_type = (SELECT substr(imported_text,21,12)
                                FROM PI_BARB_import
                                WHERE substr(imported_text,1,2) = '01')

SET @File_Version = (SELECT CAST(substr(imported_text,33,3) AS Int)
                                FROM PI_BARB_import
                                WHERE substr(imported_text,1,2) = '01')

SET @Filename = (SELECT substr(imported_text,36,13)
                                FROM PI_BARB_import
                                WHERE substr(imported_text,1,2) = '01')



-- Now go through each of the tables within the file in turn and extract the data
-- and load into the tables we have set up
-- We'll just set as either text or integer depending upon the file spec document
-- Further data transformations will be done later (e.g. dealing with date time fields)
INSERT INTO BARB_Panel_Demographic_Data_Home_Characteristics
SELECT @file_creation_date, @file_creation_time, @file_type, @file_version, @filename
,CAST(substr(imported_text,1,2) AS Int)
,CAST(substr(imported_text,3,7) AS Int)
,CAST(substr(imported_text,10,8) AS Int)
,CAST(substr(imported_text,18,1) AS Int)
,CAST(substr(imported_text,19,2) AS Int)
,CAST(substr(imported_text,21,1) AS Int)
,CAST(substr(imported_text,22,1) AS Int)
,CAST(substr(imported_text,23,1) AS Int)
,CAST(substr(imported_text,25,2) AS Int)
,substr(imported_text,27,2)
,CAST(substr(imported_text,31,1) AS Int)
,CAST(substr(imported_text,32,2) AS Int)
,CAST(substr(imported_text,40,3) AS Int)
,CAST(substr(imported_text,43,2) AS Int)
,CAST(substr(imported_text,45,2) AS Int)
,CAST(substr(imported_text,47,1) AS Int)
,CAST(substr(imported_text,48,1) AS Int)
,CAST(substr(imported_text,49,1) AS Int)
,CAST(substr(imported_text,50,1) AS Int)
,CAST(substr(imported_text,51,1) AS Int)
,CAST(substr(imported_text,52,1) AS Int)
,CAST(substr(imported_text,53,1) AS Int)
,CAST(substr(imported_text,54,2) AS Int)
,CAST(substr(imported_text,56,3) AS Int)
FROM PI_BARB_IMPORT
WHERE substr(imported_text,1,2) = '02' -- this identifies what table is being refered to see BARB2010DSP01 Version 3.7C - Panel Viewing File.pdf



INSERT INTO BARB_Panel_Demographic_Data_TV_Sets_Characteristics
SELECT @file_creation_date, @file_creation_time, @file_type, @file_version, @filename
,CAST(substr(imported_text,1,2) AS Int)
,CAST(substr(imported_text,3,7) AS Int)
,CAST(substr(imported_text,10,8) AS Int)
,CAST(substr(imported_text,18,1) AS Int)
,CAST(substr(imported_text,19,2) AS Int)
,CAST(substr(imported_text,21,1) AS Int)
,CAST(substr(imported_text,22,1) AS Int)
,CAST(substr(imported_text,23,1) AS Int)
,CAST(substr(imported_text,24,1) AS Int)
,CAST(substr(imported_text,25,1) AS Int)
,CAST(substr(imported_text,26,1) AS Int)
,CAST(substr(imported_text,27,1) AS Int)
,CAST(substr(imported_text,28,1) AS Int)
,CAST(substr(imported_text,35,1) AS Int)
,CAST(substr(imported_text,36,1) AS Int)
,CAST(substr(imported_text,37,1) AS Int)
,CAST(substr(imported_text,38,1) AS Int)
,CAST(substr(imported_text,39,1) AS Int)
,CAST(substr(imported_text,40,1) AS Int)
,CAST(substr(imported_text,41,3) AS Int)
,CAST(substr(imported_text,44,3) AS Int)
,CAST(substr(imported_text,47,3) AS Int)
,CAST(substr(imported_text,50,3) AS Int)
,CAST(substr(imported_text,53,3) AS Int)
,CAST(substr(imported_text,56,3) AS Int)
,CAST(substr(imported_text,59,3) AS Int)
,CAST(substr(imported_text,62,3) AS Int)
,CAST(substr(imported_text,65,3) AS Int)
,CAST(substr(imported_text,68,3) AS Int)
FROM PI_BARB_IMPORT
WHERE substr(imported_text,1,2) = '03' -- this identifies what table is being refered to see BARB2010DSP01 Version 3.7C - Panel Viewing File.pdf






INSERT INTO BARB_Individual_Panel_Member_Details
SELECT @file_creation_date, @file_creation_time, @file_type, @file_version, @filename
,CAST(substr(imported_text,1,2) AS Int)
,CAST(substr(imported_text,3,7) AS Int)
,CAST(substr(imported_text,10,8) AS Int)
,CAST(substr(imported_text,18,1) AS Int)
,CAST(substr(imported_text,19,2) AS Int)
,CAST(substr(imported_text,21,1) AS Int)
,CAST(substr(imported_text,22,8) AS Int)
,CAST(substr(imported_text,30,1) AS Int)
,CAST(substr(imported_text,31,1) AS Int)
,CAST(substr(imported_text,32,1) AS Int)
,CAST(substr(imported_text,33,1) AS Int)
,CAST(substr(imported_text,34,1) AS Int)
,CAST(substr(imported_text,35,1) AS Int)
,CAST(substr(imported_text,36,1) AS Int)
,CAST(substr(imported_text,37,2) AS Int)
,CAST(substr(imported_text,39,2) AS Int)
FROM PI_BARB_IMPORT
WHERE substr(imported_text,1,2) = '04' -- this identifies what table is being refered to see BARB2010DSP01 Version 3.7C - Panel Viewing File.pdf


INSERT INTO BARB_Panel_Member_Responses_Weights_and_Viewing_Categories
SELECT @file_creation_date, @file_creation_time, @file_type, @file_version, @filename
,CAST(substr(imported_text,1,2) AS Int)
,CAST(substr(imported_text,3,7) AS Int)
,CAST(substr(imported_text,10,2) AS Int)
,CAST(substr(imported_text,12,5) AS Int)
,CAST(substr(imported_text,17,8) AS Int)
,CAST(substr(imported_text,25,1) AS Int)
,CAST(substr(imported_text,26,7) AS Int)
,CAST(substr(imported_text,33,1) AS Int)
,CAST(substr(imported_text,34,1) AS Int)
,CAST(substr(imported_text,35,1) AS Int)
,CAST(substr(imported_text,36,1) AS Int)
,CAST(substr(imported_text,37,1) AS Int)
,CAST(substr(imported_text,38,1) AS Int)
FROM PI_BARB_IMPORT
WHERE substr(imported_text,1,2) = '05' -- this identifies what table is being refered to see BARB2010DSP01 Version 3.7C - Panel Viewing File.pdf



INSERT INTO BARB_PVF_Viewing_Record_Panel_Members
SELECT @file_creation_date, @file_creation_time, @file_type, @file_version, @filename
,CAST(substr(imported_text,1,2) AS Int)
,CAST(substr(imported_text,3,7) AS Int)
,CAST(substr(imported_text,10,8) AS Int)
,CAST(substr(imported_text,18,2) AS Int)
,CAST(substr(imported_text,20,4) AS Int)
,CAST(substr(imported_text,24,4) AS Int)
,CAST(substr(imported_text,28,2) AS Int)
,substr(imported_text,30,1)
,substr(imported_text,31,5)
,CAST(substr(imported_text,36,1) AS Int)
,CAST(substr(imported_text,37,8) AS Int)
,CAST(substr(imported_text,45,4) AS Int)
,CAST(substr(imported_text,49,1) AS Int)
,CAST(substr(imported_text,50,1) AS Int)
,CAST(substr(imported_text,51,1) AS Int)
,CAST(substr(imported_text,52,1) AS Int)
,CAST(substr(imported_text,53,1) AS Int)
,CAST(substr(imported_text,54,1) AS Int)
,CAST(substr(imported_text,55,1) AS Int)
,CAST(substr(imported_text,56,1) AS Int)
,CAST(substr(imported_text,57,1) AS Int)
,CAST(substr(imported_text,58,1) AS Int)
,CAST(substr(imported_text,59,1) AS Int)
,CAST(substr(imported_text,60,1) AS Int)
,CAST(substr(imported_text,61,1) AS Int)
,CAST(substr(imported_text,62,1) AS Int)
,CAST(substr(imported_text,63,1) AS Int)
,CAST(substr(imported_text,64,1) AS Int)
,CAST(substr(imported_text,65,9) AS Int)
,CAST(substr(imported_text,74,1) AS Int)
,CAST(substr(imported_text,75,5) AS Int)
,CAST(substr(imported_text,80,5) AS Int)
,CAST(substr(imported_text,85,5) AS Int)
,CAST(substr(imported_text,90,4) AS Int)
FROM PI_BARB_IMPORT
WHERE substr(imported_text,1,2) = '06' -- this identifies what table is being refered to see BARB2010DSP01 Version 3.7C - Panel Viewing File.pdf



INSERT INTO BARB_PVF_Viewing_Record_Guests
SELECT @file_creation_date, @file_creation_time, @file_type, @file_version, @filename
,CAST(substr(imported_text,1,2) AS Int)
,CAST(substr(imported_text,3,7) AS Int)
,CAST(substr(imported_text,10,8) AS Int)
,CAST(substr(imported_text,18,2) AS Int)
,CAST(substr(imported_text,20,4) AS Int)
,CAST(substr(imported_text,24,4) AS Int)
,CAST(substr(imported_text,28,2) AS Int)
,substr(imported_text,30,1)
,substr(imported_text,31,5)
,CAST(substr(imported_text,36,1) AS Int)
,CAST(substr(imported_text,37,8) AS Int)
,CAST(substr(imported_text,45,4) AS Int)
,CAST(substr(imported_text,49,2) AS Int)
,CAST(substr(imported_text,51,2) AS Int)
,CAST(substr(imported_text,53,2) AS Int)
,CAST(substr(imported_text,55,2) AS Int)
,CAST(substr(imported_text,57,2) AS Int)
,CAST(substr(imported_text,59,2) AS Int)
,CAST(substr(imported_text,61,2) AS Int)
,CAST(substr(imported_text,63,2) AS Int)
,CAST(substr(imported_text,65,2) AS Int)
,CAST(substr(imported_text,67,2) AS Int)
,CAST(substr(imported_text,69,2) AS Int)
,CAST(substr(imported_text,71,2) AS Int)
,CAST(substr(imported_text,73,2) AS Int)
,CAST(substr(imported_text,75,2) AS Int)
,CAST(substr(imported_text,77,2) AS Int)
,CAST(substr(imported_text,79,2) AS Int)
,CAST(substr(imported_text,81,9) AS Int)
,CAST(substr(imported_text,90,1) AS Int)
,CAST(substr(imported_text,91,5) AS Int)
,CAST(substr(imported_text,96,5) AS Int)
,CAST(substr(imported_text,101,5) AS Int)
,CAST(substr(imported_text,106,4) AS Int)
FROM PI_BARB_IMPORT
WHERE substr(imported_text,1,2) = '07' -- this identifies what table is being refered to see BARB2010DSP01 Version 3.7C - Panel Viewing File.pdf


SELECT 'Import complete'

END


---- Procedure to import the raw Barb PV2 files
create PROCEDURE sp_BARB_PV2 (@in_filename varchar(60)) AS

BEGIN
/*

Usage
EXEC sp_BARB_PVF 'JasonT/B20130916.PV2'
Note this is incremental
*/

DECLARE @query varchar(3000)
DECLARE @file_creation_date date
DECLARE @file_creation_time time
DECLARE @file_type  varchar(12)
DECLARE @File_Version Int
DECLARE @audit_row_count bigint
DECLARE @filename varchar(13)

-- This has 1 text column of varchar(10000) to hold a full row of data
-- Make sure this is empty
DELETE FROM PI_BARB_import

-- Load all the data into PI_BARB_import from the specified file
SET @query = 'LOAD TABLE PI_BARB_import (imported_text '
SET @query = @query || ' ''\n'' ) '
SET @query = @query || ' FROM ''/ETL013/prod/sky/olive/data/share/clarityq/export/' || @in_filename || ''' QUOTES OFF ESCAPES OFF NOTIFY 1000'
EXECUTE (@query)

SET @audit_row_count = (Select count(1) from PI_BARB_IMPORT)

-- check if file doesn't exist
IF @audit_row_count = 0
BEGIN
        SELECT now(), @file_creation_Date, @file_creation_time, @file_type, @file_version, @filename,'File not found ' || @in_filename, @audit_row_count

        RETURN
END


-- parse out the data records
-- From the Barb file spec document we can pull off some key fields from the header file
SET @file_creation_date = (SELECT CAST(substr(imported_text,7,8) AS Date)
                                FROM PI_BARB_import
                                WHERE substr(imported_text,1,2) = '01')

SET @file_creation_time = (SELECT CAST(substr(imported_text,15,2) || ':' || substr(imported_text,17,2) || ':' || substr(imported_text,19,2)  AS Time)
                                FROM PI_BARB_import
                                WHERE substr(imported_text,1,2) = '01')

SET @file_type = (SELECT substr(imported_text,21,12)
                                FROM PI_BARB_import
                                WHERE substr(imported_text,1,2) = '01')

SET @File_Version = (SELECT CAST(substr(imported_text,33,3) AS Int)
                                FROM PI_BARB_import
                                WHERE substr(imported_text,1,2) = '01')

SET @Filename = (SELECT substr(imported_text,36,13)
                                FROM PI_BARB_import
                                WHERE substr(imported_text,1,2) = '01')



-- Now go through each of the tables within the file in turn and extract the data
-- and load into the tables we have set up
-- We'll just set as either text or integer depending upon the file spec document
-- Further data transformations will be done later (e.g. dealing with date time fields)
INSERT INTO BARB_PV2_Viewing_Record_Panel_Members
SELECT @file_creation_date, @file_creation_time, @file_type, @file_version, @filename
,CAST(substr(imported_text,1,2) AS Int)
,CAST(substr(imported_text,3,7) AS Int)
,CAST(substr(imported_text,10,8) AS Int)
,CAST(substr(imported_text,18,2) AS Int)
,CAST(substr(imported_text,20,4) AS Int)
,CAST(substr(imported_text,24,4) AS Int)
,CAST(substr(imported_text,28,2) AS Int)
,substr(imported_text,30,1)
,substr(imported_text,31,5)
,CAST(substr(imported_text,36,1) AS Int)
,CAST(substr(imported_text,37,8) AS Int)
,CAST(substr(imported_text,45,4) AS Int)
,CAST(substr(imported_text,49,1) AS Int)
,CAST(substr(imported_text,50,1) AS Int)
,CAST(substr(imported_text,51,1) AS Int)
,CAST(substr(imported_text,52,1) AS Int)
,CAST(substr(imported_text,53,1) AS Int)
,CAST(substr(imported_text,54,1) AS Int)
,CAST(substr(imported_text,55,1) AS Int)
,CAST(substr(imported_text,56,1) AS Int)
,CAST(substr(imported_text,57,1) AS Int)
,CAST(substr(imported_text,58,1) AS Int)
,CAST(substr(imported_text,59,1) AS Int)
,CAST(substr(imported_text,60,1) AS Int)
,CAST(substr(imported_text,61,1) AS Int)
,CAST(substr(imported_text,62,1) AS Int)
,CAST(substr(imported_text,63,1) AS Int)
,CAST(substr(imported_text,64,1) AS Int)
,CAST(substr(imported_text,65,9) AS Int)
,CAST(substr(imported_text,74,1) AS Int)
,CAST(substr(imported_text,75,5) AS Int)
,CAST(substr(imported_text,80,5) AS Int)
,CAST(substr(imported_text,85,5) AS Int)
,CAST(substr(imported_text,90,4) AS Int)
FROM PI_BARB_IMPORT
WHERE substr(imported_text,1,2) = '16'



INSERT INTO BARB_PV2_Viewing_Record_Panel_Guests
SELECT @file_creation_date, @file_creation_time, @file_type, @file_version, @filename
,CAST(substr(imported_text,1,2) AS Int)
,CAST(substr(imported_text,3,7) AS Int)
,CAST(substr(imported_text,10,8) AS Int)
,CAST(substr(imported_text,18,2) AS Int)
,CAST(substr(imported_text,20,4) AS Int)
,CAST(substr(imported_text,24,4) AS Int)
,CAST(substr(imported_text,28,2) AS Int)
,substr(imported_text,30,1)
,substr(imported_text,31,5)
,CAST(substr(imported_text,36,1) AS Int)
,CAST(substr(imported_text,37,8) AS Int)
,CAST(substr(imported_text,45,4) AS Int)
,CAST(substr(imported_text,49,2) AS Int)
,CAST(substr(imported_text,51,2) AS Int)
,CAST(substr(imported_text,53,2) AS Int)
,CAST(substr(imported_text,55,2) AS Int)
,CAST(substr(imported_text,57,2) AS Int)
,CAST(substr(imported_text,59,2) AS Int)
,CAST(substr(imported_text,61,2) AS Int)
,CAST(substr(imported_text,63,2) AS Int)
,CAST(substr(imported_text,65,2) AS Int)
,CAST(substr(imported_text,67,2) AS Int)
,CAST(substr(imported_text,69,2) AS Int)
,CAST(substr(imported_text,71,2) AS Int)
,CAST(substr(imported_text,73,2) AS Int)
,CAST(substr(imported_text,75,2) AS Int)
,CAST(substr(imported_text,77,2) AS Int)
,CAST(substr(imported_text,79,2) AS Int)
,CAST(substr(imported_text,81,9) AS Int)
,CAST(substr(imported_text,90,1) AS Int)
,CAST(substr(imported_text,91,5) AS Int)
,CAST(substr(imported_text,96,5) AS Int)
,CAST(substr(imported_text,101,5) AS Int)
,CAST(substr(imported_text,106,4) AS Int)
FROM PI_BARB_IMPORT
WHERE substr(imported_text,1,2) = '17'


SELECT 'Import complete'

END




-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
-- A3: Create tables to hold processed Barb data - more useful datatypes e.g. timestamp instead of text
-- These tables will be easier to manuipulate.
-- Ideally this should be combined with the step above
-- This is left for future development
-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------

-- The following tables are similar to the original tables set up for Barb
-- Most of the work done here is around dates/times to make these easier to use
-- Also tables have better naming conventions consistent with Barb documentation

-- Important notes about Barb data:
-- Reporting day goes from 02:00 to 25:59 for PVF files (i.e. Barb day starts at 2am and finishes at 2am the following day)
-- All Barb times are local i.e. will be in British Summer Time when relevant
-- Need to be careful when clocks go forward/back - THIS HAS NOT BEEN DEALT WITH IN THIS CODE!!!!!


CREATE TABLE BARB_PVF06_Viewing_Record_Panel_Members (
id_row bigint primary key identity  --- so we can check if any viewing records are not matched to the schedule
,file_creation_date date, file_creation_time time, file_type varchar(12), file_version int, filename varchar(13)
,Record_type int DEFAULT NULL
,Household_number int DEFAULT NULL
,Barb_date_of_activity date -- New field
,Actual_date_of_session date  --- change datatype from Barb. If Barb start time > 24:00 then add 1 to this date
,Set_number int DEFAULT NULL
,Start_time_of_session_text varchar(6)--- change datatype from Barb to make it easier to convert to timestamp later. Working field
,Start_time_of_session timestamp  --- New field
,End_time_of_session timestamp --- new field
,Duration_of_session int DEFAULT NULL
,Session_activity_type int DEFAULT NULL
,Playback_type varchar(1) DEFAULT NULL
,DB1_Station_Code int DEFAULT NULL
,Viewing_platform int DEFAULT NULL
,Barb_date_of_recording date
,Actual_Date_of_Recording date --- change datatype from Barb
,Start_time_of_recording_text varchar(6) --- change datatype from Barb to make it easier to convert to timestamp later. Working field
,Start_time_of_recording timestamp --- new field
,Person_1_viewing int DEFAULT NULL
,Person_2_viewing int DEFAULT NULL
,Person_3_viewing int DEFAULT NULL
,Person_4_viewing int DEFAULT NULL
,Person_5_viewing int DEFAULT NULL
,Person_6_viewing int DEFAULT NULL
,Person_7_viewing int DEFAULT NULL
,Person_8_viewing int DEFAULT NULL
,Person_9_viewing int DEFAULT NULL
,Person_10_viewing int DEFAULT NULL
,Person_11_viewing int DEFAULT NULL
,Person_12_viewing int DEFAULT NULL
,Person_13_viewing int DEFAULT NULL
,Person_14_viewing int DEFAULT NULL
,Person_15_viewing int DEFAULT NULL
,Person_16_viewing int DEFAULT NULL
,Interactive_Bar_Code_Identifier int DEFAULT NULL
,VOD_Indicator int DEFAULT NULL
,VOD_Provider int DEFAULT NULL
,VOD_Service int DEFAULT NULL
,VOD_Type int DEFAULT NULL
,Device_in_use int DEFAULT NULL
)

create hg index ind_household_number on BARB_PVF06_Viewing_Record_Panel_Members(household_number)
create hg index ind_db1 on BARB_PVF06_Viewing_Record_Panel_Members(db1_station_code)
create hg index ind_start on BARB_PVF06_Viewing_Record_Panel_Members(Start_time_of_session)
create hg index ind_end on BARB_PVF06_Viewing_Record_Panel_Members(End_time_of_session)
create hg index ind_date on BARB_PVF06_Viewing_Record_Panel_Members(Barb_date_of_activity)




CREATE TABLE BARB_PVF07_Viewing_Record_Guests (
id_row bigint primary key identity  --- so we can check if any viewing records are not matched to the schedule
,file_creation_date date, file_creation_time time, file_type varchar(12), file_version int, filename varchar(13)
,Record_type int DEFAULT NULL
,Household_number int DEFAULT NULL
,Barb_date_of_activity date -- New field
,Actual_date_of_session date  --- change datatype from Barb. If Barb start time > 24:00 then add 1 to this date
,Set_number int DEFAULT NULL
,Start_time_of_session_text varchar(6)--- change datatype from Barb to make it easier to convert to timestamp later. Working field
,Start_time_of_session timestamp  --- New field
,End_time_of_session timestamp --- new field
,Duration_of_session int DEFAULT NULL
,Session_activity_type int DEFAULT NULL
,Playback_type varchar(1) DEFAULT NULL
,DB1_Station_Code int DEFAULT NULL
,Viewing_platform int DEFAULT NULL
,Barb_date_of_recording date
,Actual_Date_of_Recording date --- change datatype from Barb
,Start_time_of_recording_text varchar(6) --- change datatype from Barb to make it easier to convert to timestamp later. Working field
,Start_time_of_recording timestamp --- new field
,Male_4_9 int DEFAULT NULL
,Male_10_15 int DEFAULT NULL
,Male_16_19 int DEFAULT NULL
,Male_20_24 int DEFAULT NULL
,Male_25_34 int DEFAULT NULL
,Male_35_44 int DEFAULT NULL
,Male_45_64 int DEFAULT NULL
,Male_65 int DEFAULT NULL
,Female_4_9 int DEFAULT NULL
,Female_10_15 int DEFAULT NULL
,Female_16_19 int DEFAULT NULL
,Female_20_24 int DEFAULT NULL
,Female_25_34 int DEFAULT NULL
,Female_35_44 int DEFAULT NULL
,Female_45_64 int DEFAULT NULL
,Female_65 int DEFAULT NULL
,Interactive_Bar_Code_Identifier int DEFAULT NULL
,VOD_Indicator int DEFAULT NULL
,VOD_Provider int DEFAULT NULL
,VOD_Service int DEFAULT NULL
,VOD_Type int DEFAULT NULL
,Device_in_use int DEFAULT NULL
)


create hg index ind_hhd on BARB_PVF07_Viewing_Record_Guests(Household_number)
create hg index ind_start on BARB_PVF07_Viewing_Record_Guests(Start_time_of_session)
create hg index ind_end on BARB_PVF07_Viewing_Record_Guests(End_time_of_session)



CREATE TABLE BARB_PVF04_Individual_Member_Details (
file_creation_date date, file_creation_time time, file_type varchar(12), file_version int, filename varchar(13)
,Record_type int DEFAULT NULL
,Household_number int DEFAULT NULL
,Date_valid_for_DB1 date
,Person_membership_status int DEFAULT NULL
,Person_number int DEFAULT NULL
,Sex_code int DEFAULT NULL
,Date_of_birth date
,Marital_status int DEFAULT NULL
,Household_status int DEFAULT NULL
,Working_status int DEFAULT NULL
,Terminal_age_of_education int DEFAULT NULL
,Welsh_Language_code int DEFAULT NULL
,Gaelic_language_code int DEFAULT NULL
,Dependency_of_Children int DEFAULT NULL
,Life_stage_12_classifications int DEFAULT NULL
,Ethnic_Origin int DEFAULT NULL
)

create hg index ind_hhd on BARB_PVF04_Individual_Member_Details(Household_number)
create lf index ind_person on BARB_PVF04_Individual_Member_Details(person_number)
create lf index ind_create on BARB_PVF04_Individual_Member_Details(file_creation_date)




CREATE TABLE BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories (
file_creation_date date, file_creation_time time, file_type varchar(12), file_version int, filename varchar(13)
,Record_Type int DEFAULT NULL
,Household_Number int DEFAULT NULL
,Person_Number int DEFAULT NULL
,Reporting_Panel_Code int DEFAULT NULL
,Date_of_Activity_DB1 date
,Response_Code int DEFAULT NULL
,Processing_Weight int DEFAULT NULL
,Adults_Commercial_TV_Viewing_Sextile int DEFAULT NULL
,ABC1_Adults_Commercial_TV_Viewing_Sextile int DEFAULT NULL
,Adults_Total_Viewing_Sextile int DEFAULT NULL
,ABC1_Adults_Total_Viewing_Sextile int DEFAULT NULL
,Adults_16_34_Commercial_TV_Viewing_Sextile int DEFAULT NULL
,Adults_16_34_Total_Viewing_Sextile int DEFAULT NULL
)

create hg index ind_hhd on BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories(Household_Number)
create lf index ind_person on BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories(Person_Number)
create lf index ind_panel on BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories(Reporting_Panel_Code)
create lf index ind_date on BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories(Date_of_Activity_DB1)



CREATE TABLE BARB_PV206_Viewing_Record_Panel_Members (
id_row bigint primary key identity  --- so we can check if any viewing records are not matched to the schedule
,file_creation_date date, file_creation_time time, file_type varchar(12), file_version int, filename varchar(13)
,Record_type int DEFAULT NULL
,Household_number int DEFAULT NULL
,Barb_date_of_activity date -- New field
,Actual_date_of_session date  --- change datatype from Barb. If Barb start time > 24:00 then add 1 to this date
,Set_number int DEFAULT NULL
,Start_time_of_session_text varchar(6)--- change datatype from Barb to make it easier to convert to timestamp later. Working field
,Start_time_of_session timestamp  --- New field
,End_time_of_session timestamp --- new field
,Duration_of_session int DEFAULT NULL
,Session_activity_type int DEFAULT NULL
,Playback_type varchar(1) DEFAULT NULL
,DB1_Station_Code int DEFAULT NULL
,Viewing_platform int DEFAULT NULL
,Barb_date_of_recording date
,Actual_Date_of_Recording date --- change datatype from Barb
,Start_time_of_recording_text varchar(6) --- change datatype from Barb to make it easier to convert to timestamp later. Working field
,Start_time_of_recording timestamp --- new field
,Person_1_viewing int DEFAULT NULL
,Person_2_viewing int DEFAULT NULL
,Person_3_viewing int DEFAULT NULL
,Person_4_viewing int DEFAULT NULL
,Person_5_viewing int DEFAULT NULL
,Person_6_viewing int DEFAULT NULL
,Person_7_viewing int DEFAULT NULL
,Person_8_viewing int DEFAULT NULL
,Person_9_viewing int DEFAULT NULL
,Person_10_viewing int DEFAULT NULL
,Person_11_viewing int DEFAULT NULL
,Person_12_viewing int DEFAULT NULL
,Person_13_viewing int DEFAULT NULL
,Person_14_viewing int DEFAULT NULL
,Person_15_viewing int DEFAULT NULL
,Person_16_viewing int DEFAULT NULL
,Interactive_Bar_Code_Identifier int DEFAULT NULL
,VOD_Indicator int DEFAULT NULL
,VOD_Provider int DEFAULT NULL
,VOD_Service int DEFAULT NULL
,VOD_Type int DEFAULT NULL
,Device_in_use int DEFAULT NULL
)

create hg index ind_household_number on BARB_PV206_Viewing_Record_Panel_Members(household_number)
create hg index ind_db1 on BARB_PV206_Viewing_Record_Panel_Members(db1_station_code)
create hg index ind_start on BARB_PV206_Viewing_Record_Panel_Members(Start_time_of_session)
create hg index ind_end on BARB_PV206_Viewing_Record_Panel_Members(End_time_of_session)




CREATE TABLE BARB_PV207_Viewing_Record_Guests (
id_row bigint primary key identity  --- so we can check if any viewing records are not matched to the schedule
,file_creation_date date, file_creation_time time, file_type varchar(12), file_version int, filename varchar(13)
,Record_type int DEFAULT NULL
,Household_number int DEFAULT NULL
,Barb_date_of_activity date -- New field
,Actual_date_of_session date  --- change datatype from Barb. If Barb start time > 24:00 then add 1 to this date
,Set_number int DEFAULT NULL
,Start_time_of_session_text varchar(6)--- change datatype from Barb to make it easier to convert to timestamp later. Working field
,Start_time_of_session timestamp  --- New field
,End_time_of_session timestamp --- new field
,Duration_of_session int DEFAULT NULL
,Session_activity_type int DEFAULT NULL
,Playback_type varchar(1) DEFAULT NULL
,DB1_Station_Code int DEFAULT NULL
,Viewing_platform int DEFAULT NULL
,Barb_date_of_recording date
,Actual_Date_of_Recording date --- change datatype from Barb
,Start_time_of_recording_text varchar(6) --- change datatype from Barb to make it easier to convert to timestamp later. Working field
,Start_time_of_recording timestamp --- new field
,Male_4_9 int DEFAULT NULL
,Male_10_15 int DEFAULT NULL
,Male_16_19 int DEFAULT NULL
,Male_20_24 int DEFAULT NULL
,Male_25_34 int DEFAULT NULL
,Male_35_44 int DEFAULT NULL
,Male_45_64 int DEFAULT NULL
,Male_65 int DEFAULT NULL
,Female_4_9 int DEFAULT NULL
,Female_10_15 int DEFAULT NULL
,Female_16_19 int DEFAULT NULL
,Female_20_24 int DEFAULT NULL
,Female_25_34 int DEFAULT NULL
,Female_35_44 int DEFAULT NULL
,Female_45_64 int DEFAULT NULL
,Female_65 int DEFAULT NULL
,Interactive_Bar_Code_Identifier int DEFAULT NULL
,VOD_Indicator int DEFAULT NULL
,VOD_Provider int DEFAULT NULL
,VOD_Service int DEFAULT NULL
,VOD_Type int DEFAULT NULL
,Device_in_use int DEFAULT NULL
)

create hg index ind_hhd on BARB_PV207_Viewing_Record_Guests(Household_number)
create hg index ind_start on BARB_PV207_Viewing_Record_Guests(Start_time_of_session)
create hg index ind_end on BARB_PV207_Viewing_Record_Guests(End_time_of_session)






-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
-- A4: Process and load Barb data into above tables


-- Important notes about Barb data:
-- Reporting day goes from 02:00 to 25:59 for PVF files (i.e. Barb day starts at 2am and finishes at 2am the following day)
-- All Barb times are local i.e. will be in British Summer Time when relevant
-- Need to be careful when clocks go forward/back - THIS HAS NOT BEEN DEALT WITH IN THIS CODE!!!!!


-- BUG: In converting barb start and end time to actual start and end time there is a bug in this code. If an event spans midnight then
-- the end date and therefore the end date time will be wrong (barb date only advanced 1 day if the start time >24:00 hr not if the end time > 24:00
-- I need to check this!!!!!!



----------------------------------------
-- Insert into BARB_PVF06_Viewing_Record_Panel_Members
----------------------------------------
insert into BARB_PVF06_Viewing_Record_Panel_Members
(file_creation_date, file_creation_time, file_type, file_version, filename
,Record_type, Household_number, Barb_date_of_activity, Actual_date_of_session, Set_number
,Start_time_of_session_text, Start_time_of_session, End_time_of_session, Duration_of_session, Session_activity_type
,Playback_type, DB1_Station_Code, Viewing_platform, Barb_date_of_recording, Actual_Date_of_Recording
,Start_time_of_recording_text, Start_time_of_recording, Person_1_viewing, Person_2_viewing, Person_3_viewing
,Person_4_viewing, Person_5_viewing, Person_6_viewing, Person_7_viewing
,Person_8_viewing, Person_9_viewing, Person_10_viewing, Person_11_viewing
,Person_12_viewing, Person_13_viewing, Person_14_viewing, Person_15_viewing, Person_16_viewing
,Interactive_Bar_Code_Identifier, VOD_Indicator, VOD_Provider, VOD_Service
,VOD_Type, Device_in_use)
select
        file_creation_date, file_creation_time, file_type, file_version, filename
        ,Record_type
        ,Household_number
        -- Keep the original Date_of_Activity_DB1 as the Barb_date_of_activity
        ,date(
                cast(cast(Date_of_Activity_DB1/10000 as int) as char(4)) || '-' ||
                cast(cast(Date_of_Activity_DB1/100 as int) - cast(Date_of_Activity_DB1/10000 as int) * 100 as varchar(2)) || '-' ||
                cast(Date_of_Activity_DB1 - cast(Date_of_Activity_DB1/100 as int) * 100 as varchar(2))
                )
        -- Actual_date_of_session: A barb day can go over 24:00. In this case we need to increase the date by 1
        ,dateadd(dd, case when Start_time_of_session >= 2400 then 1 else 0 end,
                date(
                        cast(cast(Date_of_Activity_DB1/10000 as int) as char(4)) || '-' ||
                        cast(cast(Date_of_Activity_DB1/100 as int) - cast(Date_of_Activity_DB1/10000 as int) * 100 as varchar(2)) || '-' ||
                        cast(Date_of_Activity_DB1 - cast(Date_of_Activity_DB1/100 as int) * 100 as varchar(2))
                        )
                )

        ,Set_number
        ,cast(Start_time_of_session as varchar(6)) -- Start_time_of_session_text
        ,datetime('1900-01-01 00:00:00') -- Start_time_of_session. Will update this later in an update query. Easier that way
        ,datetime('1900-01-01 00:00:00') -- End_time_of_session. Will update this later in an update query. Easier that way
        ,Duration_of_session
        ,Session_activity_type
        ,Playback_type
        ,DB1_Station_Code
        ,Viewing_platform
         -- Keep the original Date_of_Recording_DB1 as the Barb_date_of_recording
        ,date(
                cast(cast(Date_of_Recording_DB1/10000 as int) as char(4)) || '-' ||
                cast(cast(Date_of_Recording_DB1/100 as int) - cast(Date_of_Recording_DB1/10000 as int) * 100 as varchar(2)) || '-' ||
                cast(Date_of_Recording_DB1 - cast(Date_of_Recording_DB1/100 as int) * 100 as varchar(2))
                )
        -- Date_of_Recording_DB1: A barb day can go over 24:00. In this case we need to increase the date by 1
        ,dateadd(dd, case when Start_time_of_recording >= 2400 then 1 else 0 end,
                date(
                        cast(cast(Date_of_Recording_DB1/10000 as int) as char(4)) || '-' ||
                        cast(cast(Date_of_Recording_DB1/100 as int) - cast(Date_of_Recording_DB1/10000 as int) * 100 as varchar(2)) || '-' ||
                        cast(Date_of_Recording_DB1 - cast(Date_of_Recording_DB1/100 as int) * 100 as varchar(2))
                        )
                )
        ,cast(Start_time_of_recording as varchar(6)) -- Start_time_of_recording_text
        ,datetime('1900-01-01 00:00:00') -- Start_time_of_recording. Will update this later in an update query. Easier that way
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
        ,VOD_Indicator
        ,VOD_Provider
        ,VOD_Service
        ,VOD_Type
        ,Device_in_use
from
        BARB_PVF_Viewing_Record_Panel_Members


--- Update the Start and end session and recording timestamps. Its easier to deal with the barb time conversion as an update rather then as in the insert statement
update BARB_PVF06_Viewing_Record_Panel_Members
        set Start_time_of_session = datetime(year(Actual_date_of_session) || '-' || month(Actual_date_of_session) || '-' || day(Actual_date_of_session) || ' '
                                                || case when cast(Start_time_of_session_text as int) >= 2400 then
                                                        -- As start time >= 24:00 then take off 24hours to convert barb time to GMT time. Actual_date_of_session has already been converted
                                                        case when len(Start_time_of_session_text) = 3 then
                                                                -- As only 3 chars then must be missing leading zero
                                                                cast(substring(Start_time_of_session_text, 1,1) as int) - 24 || ':' || substring(Start_time_of_session_text, 2,2) || ':00'
                                                        else
                                                                cast(substring(Start_time_of_session_text, 1,2) as int) - 24 || ':' || substring(Start_time_of_session_text, 3,2) || ':00'
                                                        end
                                                   else
                                                        -- Start time < 24:00 then barb time OK
                                                        case when len(Start_time_of_session_text) = 3 then
                                                                -- As only 3 chars then must be missing leading zero
                                                                substring(Start_time_of_session_text, 1,1) || ':' || substring(Start_time_of_session_text, 2,2) || ':00'
                                                        else
                                                                substring(Start_time_of_session_text, 1,2) || ':' || substring(Start_time_of_session_text, 3,2) || ':00'
                                                        end
                                                   end
                                                )

update BARB_PVF06_Viewing_Record_Panel_Members
        set End_time_of_session = dateadd(mi, Duration_of_session, Start_time_of_session) -- ERROR should be dateadd(mi, Duration_of_session -1, Start_time_of_session) data rectified later so that code will need to change if fixed here


update BARB_PVF06_Viewing_Record_Panel_Members
        set Start_time_of_recording = datetime(year(Actual_Date_of_Recording) || '-' || month(Actual_Date_of_Recording) || '-' || day(Actual_Date_of_Recording) || ' '
                                                || case when cast(Start_time_of_recording_text as int) >= 2400 then
                                                        -- As start time >= 24:00 then take off 24hours to convert barb time to GMT time. Actual_Date_of_Recording has already been converted
                                                        case when len(Start_time_of_recording_text) = 3 then
                                                                -- As only 3 chars then must be missing leading zero
                                                                cast(substring(Start_time_of_recording_text, 1,1) as int) - 24 || ':' || substring(Start_time_of_recording_text, 2,2) || ':00'
                                                        else
                                                                cast(substring(Start_time_of_recording_text, 1,2) as int) - 24 || ':' || substring(Start_time_of_recording_text, 3,2) || ':00'
                                                        end
                                                   else
                                                        -- Start time < 24:00 then barb time OK
                                                        case when len(Start_time_of_recording_text) = 3 then
                                                                -- As only 3 chars then must be missing leading zero
                                                                substring(Start_time_of_recording_text, 1,1) || ':' || substring(Start_time_of_recording_text, 2,2) || ':00'
                                                        else
                                                                substring(Start_time_of_recording_text, 1,2) || ':' || substring(Start_time_of_recording_text, 3,2) || ':00'
                                                        end
                                                   end
                                                )





----------------------------------------
-- INsert into BARB_PVF04_Individual_Member_Details
----------------------------------------
insert into BARB_PVF04_Individual_Member_Details
select
        file_creation_date, file_creation_time, file_type, file_version, filename
        ,Record_type
        ,Household_number
        ,date(
                cast(cast(Date_valid_for_DB1/10000 as int) as char(4)) || '-' ||
                cast(cast(Date_valid_for_DB1/100 as int) - cast(Date_valid_for_DB1/10000 as int) * 100 as varchar(2)) || '-' ||
                cast(Date_valid_for_DB1 - cast(Date_valid_for_DB1/100 as int) * 100 as varchar(2))
                )
        ,Person_membership_status
        ,Person_number
        ,Sex_code
        ,date(
                cast(cast(Date_of_birth/10000 as int) as char(4)) || '-' ||
                cast(cast(Date_of_birth/100 as int) - cast(Date_of_birth/10000 as int) * 100 as varchar(2)) || '-' ||
                cast(Date_of_birth - cast(Date_of_birth/100 as int) * 100 as varchar(2))
                )
        ,Marital_status
        ,Household_status
        ,Working_status
        ,Terminal_age_of_education
        ,Welsh_Language_code
        ,Gaelic_language_code
        ,Dependency_of_Children
        ,Life_stage_12_classifications
        ,Ethnic_Origin
from
        BARB_Individual_Panel_Member_Details



----------------------------------------
-- Insert into BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories
----------------------------------------
insert into BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories
select
        file_creation_date, file_creation_time, file_type, file_version, filename
        ,Record_Type
        ,Household_Number
        ,Person_Number
        ,Reporting_Panel_Code
        ,date(
                cast(cast(Date_of_Activity_DB1/10000 as int) as char(4)) || '-' ||
                cast(cast(Date_of_Activity_DB1/100 as int) - cast(Date_of_Activity_DB1/10000 as int) * 100 as varchar(2)) || '-' ||
                cast(Date_of_Activity_DB1 - cast(Date_of_Activity_DB1/100 as int) * 100 as varchar(2))
                )
        ,Response_Code
        -- The raw data is in thousands to 4 decimal places. When gets loaded in raw table this not taken into account
        -- so the last 4 digits assumed to be whole numbers not decimal. This effectively multiplies the number by 10000
        -- So if we divide by 10 then get back to thousands which is the actual wieight needed 
        ,Processing_Weight / 10
        ,Adults_Commercial_TV_Viewing_Sextile
        ,ABC1_Adults_Commercial_TV_Viewing_Sextile
        ,Adults_Total_Viewing_Sextile
        ,ABC1_Adults_Total_Viewing_Sextile
        ,Adults_16_34_Commercial_TV_Viewing_Sextile
        ,Adults_16_34_Total_Viewing_Sextile
from BARB_Panel_Member_Responses_Weights_and_Viewing_Categories





----------------------------------------
-- Insert into BARB_PVF07_Viewing_Record_Guests
----------------------------------------
insert into BARB_PVF07_Viewing_Record_Guests
(file_creation_date, file_creation_time, file_type, file_version, filename
,Record_type, Household_number, Barb_date_of_activity, Actual_date_of_session, Set_number
,Start_time_of_session_text, Start_time_of_session, End_time_of_session, Duration_of_session, Session_activity_type
,Playback_type, DB1_Station_Code, Viewing_platform, Barb_date_of_recording, Actual_Date_of_Recording
,Start_time_of_recording_text, Start_time_of_recording
,Male_4_9, Male_10_15, Male_16_19, Male_20_24, Male_25_34, Male_35_44, Male_45_64, Male_65
,Female_4_9, Female_10_15, Female_16_19, Female_20_24, Female_25_34, Female_35_44, Female_45_64, Female_65
,Interactive_Bar_Code_Identifier, VOD_Indicator, VOD_Provider, VOD_Service
,VOD_Type, Device_in_use)
select
        file_creation_date, file_creation_time, file_type, file_version, filename
        ,Record_type
        ,Household_number
        -- Keep the original Date_of_Activity_DB1 as the Barb_date_of_activity
        ,date(
                cast(cast(Date_of_Activity_DB1/10000 as int) as char(4)) || '-' ||
                cast(cast(Date_of_Activity_DB1/100 as int) - cast(Date_of_Activity_DB1/10000 as int) * 100 as varchar(2)) || '-' ||
                cast(Date_of_Activity_DB1 - cast(Date_of_Activity_DB1/100 as int) * 100 as varchar(2))
                )
        -- Actual_date_of_session: A barb day can go over 24:00. In this case we need to increase the date by 1
        ,dateadd(dd, case when Start_time_of_session >= 2400 then 1 else 0 end,
                date(
                        cast(cast(Date_of_Activity_DB1/10000 as int) as char(4)) || '-' ||
                        cast(cast(Date_of_Activity_DB1/100 as int) - cast(Date_of_Activity_DB1/10000 as int) * 100 as varchar(2)) || '-' ||
                        cast(Date_of_Activity_DB1 - cast(Date_of_Activity_DB1/100 as int) * 100 as varchar(2))
                        )
                )
        ,Set_number
        ,cast(Start_time_of_session as varchar(6)) -- Start_time_of_session_text
        ,datetime('1900-01-01 00:00:00') -- Start_time_of_session. Will update this later in an update query. Easier that way
        ,datetime('1900-01-01 00:00:00') -- End_time_of_session. Will update this later in an update query. Easier that way
        ,Duration_of_session
        ,Session_activity_type
        ,Playback_type
        ,DB1_Station_Code
        ,Viewing_platform
         -- Keep the original Date_of_Recording_DB1 as the Barb_date_of_recording
        ,date(
                cast(cast(Date_of_Recording_DB1/10000 as int) as char(4)) || '-' ||
                cast(cast(Date_of_Recording_DB1/100 as int) - cast(Date_of_Recording_DB1/10000 as int) * 100 as varchar(2)) || '-' ||
                cast(Date_of_Recording_DB1 - cast(Date_of_Recording_DB1/100 as int) * 100 as varchar(2))
                )
        -- Date_of_Recording_DB1: A barb day can go over 24:00. In this case we need to increase the date by 1
        ,dateadd(dd, case when Start_time_of_recording >= 2400 then 1 else 0 end,
                date(
                        cast(cast(Date_of_Recording_DB1/10000 as int) as char(4)) || '-' ||
                        cast(cast(Date_of_Recording_DB1/100 as int) - cast(Date_of_Recording_DB1/10000 as int) * 100 as varchar(2)) || '-' ||
                        cast(Date_of_Recording_DB1 - cast(Date_of_Recording_DB1/100 as int) * 100 as varchar(2))
                        )
                )
        ,cast(Start_time_of_recording as varchar(6)) -- Start_time_of_recording_text
        ,datetime('1900-01-01 00:00:00') -- Start_time_of_recording. Will update this later in an update query. Easier that way
        ,Male_4_9, Male_10_15, Male_16_19, Male_20_24, Male_25_34, Male_35_44, Male_45_64, Male_65
        ,Female_4_9, Female_10_15, Female_16_19, Female_20_24, Female_25_34, Female_35_44, Female_45_64, Female_65
        ,Interactive_Bar_Code_Identifier
        ,VOD_Indicator
        ,VOD_Provider
        ,VOD_Service
        ,VOD_Type
        ,Device_in_use
from
        BARB_PVF_Viewing_Record_Guests


--- Update the Start and end session and recording timestamps. Its easier to deal with the barb time conversion as an update rather then as in the insert statement
update BARB_PVF07_Viewing_Record_Guests
        set Start_time_of_session = datetime(year(Actual_date_of_session) || '-' || month(Actual_date_of_session) || '-' || day(Actual_date_of_session) || ' '
                                                || case when cast(Start_time_of_session_text as int) >= 2400 then
                                                        -- As start time >= 24:00 then take off 24hours to convert barb time to GMT time. Actual_date_of_session has already been converted
                                                        case when len(Start_time_of_session_text) = 3 then
                                                                -- As only 3 chars then must be missing leading zero
                                                                cast(substring(Start_time_of_session_text, 1,1) as int) - 24 || ':' || substring(Start_time_of_session_text, 2,2) || ':00'
                                                        else
                                                                cast(substring(Start_time_of_session_text, 1,2) as int) - 24 || ':' || substring(Start_time_of_session_text, 3,2) || ':00'
                                                        end
                                                   else
                                                        -- Start time < 24:00 then barb time OK
                                                        case when len(Start_time_of_session_text) = 3 then
                                                                -- As only 3 chars then must be missing leading zero
                                                                substring(Start_time_of_session_text, 1,1) || ':' || substring(Start_time_of_session_text, 2,2) || ':00'
                                                        else
                                                                substring(Start_time_of_session_text, 1,2) || ':' || substring(Start_time_of_session_text, 3,2) || ':00'
                                                        end
                                                   end
                                                )

update BARB_PVF07_Viewing_Record_Guests
        set End_time_of_session = dateadd(mi, Duration_of_session, Start_time_of_session) -- ERROR should be dateadd(mi, Duration_of_session -1, Start_time_of_session) data rectified later so that code will need to change if fixed here


update BARB_PVF07_Viewing_Record_Guests
        set Start_time_of_recording = datetime(year(Actual_Date_of_Recording) || '-' || month(Actual_Date_of_Recording) || '-' || day(Actual_Date_of_Recording) || ' '
                                                || case when cast(Start_time_of_recording_text as int) >= 2400 then
                                                        -- As start time >= 24:00 then take off 24hours to convert barb time to GMT time. Actual_Date_of_Recording has already been converted
                                                        case when len(Start_time_of_recording_text) = 3 then
                                                                -- As only 3 chars then must be missing leading zero
                                                                cast(substring(Start_time_of_recording_text, 1,1) as int) - 24 || ':' || substring(Start_time_of_recording_text, 2,2) || ':00'
                                                        else
                                                                cast(substring(Start_time_of_recording_text, 1,2) as int) - 24 || ':' || substring(Start_time_of_recording_text, 3,2) || ':00'
                                                        end
                                                   else
                                                        -- Start time < 24:00 then barb time OK
                                                        case when len(Start_time_of_recording_text) = 3 then
                                                                -- As only 3 chars then must be missing leading zero
                                                                substring(Start_time_of_recording_text, 1,1) || ':' || substring(Start_time_of_recording_text, 2,2) || ':00'
                                                        else
                                                                substring(Start_time_of_recording_text, 1,2) || ':' || substring(Start_time_of_recording_text, 3,2) || ':00'
                                                        end
                                                   end
                                                )





----------------------------------------
-- Insert data into BARB_PV206_Viewing_Record_Panel_Members
----------------------------------------
insert into BARB_PV206_Viewing_Record_Panel_Members
(file_creation_date, file_creation_time, file_type, file_version, filename
,Record_type, Household_number, Barb_date_of_activity, Actual_date_of_session, Set_number
,Start_time_of_session_text, Start_time_of_session, End_time_of_session, Duration_of_session, Session_activity_type
,Playback_type, DB1_Station_Code, Viewing_platform, Barb_date_of_recording, Actual_Date_of_Recording
,Start_time_of_recording_text, Start_time_of_recording, Person_1_viewing, Person_2_viewing, Person_3_viewing
,Person_4_viewing, Person_5_viewing, Person_6_viewing, Person_7_viewing
,Person_8_viewing, Person_9_viewing, Person_10_viewing, Person_11_viewing
,Person_12_viewing, Person_13_viewing, Person_14_viewing, Person_15_viewing, Person_16_viewing
,Interactive_Bar_Code_Identifier, VOD_Indicator, VOD_Provider, VOD_Service
,VOD_Type, Device_in_use)
select
        file_creation_date, file_creation_time, file_type, file_version, filename
        ,Record_type
        ,Household_number
        -- Keep the original Date_of_Activity_DB1 as the Barb_date_of_activity
        ,date(
                cast(cast(Date_of_Activity_DB1/10000 as int) as char(4)) || '-' ||
                cast(cast(Date_of_Activity_DB1/100 as int) - cast(Date_of_Activity_DB1/10000 as int) * 100 as varchar(2)) || '-' ||
                cast(Date_of_Activity_DB1 - cast(Date_of_Activity_DB1/100 as int) * 100 as varchar(2))
                )
        -- Actual_date_of_session: A barb day can go over 24:00. In this case we need to increase the date by 1
        ,dateadd(dd, case when Start_time_of_session >= 2400 then 1 else 0 end,
                date(
                        cast(cast(Date_of_Activity_DB1/10000 as int) as char(4)) || '-' ||
                        cast(cast(Date_of_Activity_DB1/100 as int) - cast(Date_of_Activity_DB1/10000 as int) * 100 as varchar(2)) || '-' ||
                        cast(Date_of_Activity_DB1 - cast(Date_of_Activity_DB1/100 as int) * 100 as varchar(2))
                        )
                )
        ,Set_number
        ,cast(Start_time_of_session as varchar(6)) -- Start_time_of_session_text
        ,datetime('1900-01-01 00:00:00') -- Start_time_of_session. Will update this later in an update query. Easier that way
        ,datetime('1900-01-01 00:00:00') -- End_time_of_session. Will update this later in an update query. Easier that way
        ,Duration_of_session
        ,Session_activity_type
        ,Playback_type
        ,DB1_Station_Code
        ,Viewing_platform
         -- Keep the original Date_of_Recording_DB1 as the Barb_date_of_recording
        ,date(
                cast(cast(Date_of_Recording_DB1/10000 as int) as char(4)) || '-' ||
                cast(cast(Date_of_Recording_DB1/100 as int) - cast(Date_of_Recording_DB1/10000 as int) * 100 as varchar(2)) || '-' ||
                cast(Date_of_Recording_DB1 - cast(Date_of_Recording_DB1/100 as int) * 100 as varchar(2))
                )
        -- Date_of_Recording_DB1: A barb day can go over 24:00. In this case we need to increase the date by 1
        ,dateadd(dd, case when Start_time_of_recording >= 2400 then 1 else 0 end,
                date(
                        cast(cast(Date_of_Recording_DB1/10000 as int) as char(4)) || '-' ||
                        cast(cast(Date_of_Recording_DB1/100 as int) - cast(Date_of_Recording_DB1/10000 as int) * 100 as varchar(2)) || '-' ||
                        cast(Date_of_Recording_DB1 - cast(Date_of_Recording_DB1/100 as int) * 100 as varchar(2))
                        )
                )
        ,cast(Start_time_of_recording as varchar(6)) -- Start_time_of_recording_text
        ,datetime('1900-01-01 00:00:00') -- Start_time_of_recording. Will update this later in an update query. Easier that way
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
        ,VOD_Indicator
        ,VOD_Provider
        ,VOD_Service
        ,VOD_Type
        ,Device_in_use
from
        BARB_PV2_Viewing_Record_Panel_Members




--- Update the Start and end session and recording timestamps. Its easier to deal with the barb time conversion as an update rather then as in the insert statement
update BARB_PV206_Viewing_Record_Panel_Members
        set Start_time_of_session = datetime(year(Actual_date_of_session) || '-' || month(Actual_date_of_session) || '-' || day(Actual_date_of_session) || ' '
                                                || case when cast(Start_time_of_session_text as int) >= 2400 then
                                                        -- As start time >= 24:00 then take off 24hours to convert barb time to GMT time. Actual_date_of_session has already been converted
                                                        case when len(Start_time_of_session_text) = 3 then
                                                                -- As only 3 chars then must be missing leading zero
                                                                cast(substring(Start_time_of_session_text, 1,1) as int) - 24 || ':' || substring(Start_time_of_session_text, 2,2) || ':00'
                                                        else
                                                                cast(substring(Start_time_of_session_text, 1,2) as int) - 24 || ':' || substring(Start_time_of_session_text, 3,2) || ':00'
                                                        end
                                                   else
                                                        -- Start time < 24:00 then barb time OK
                                                        case when len(Start_time_of_session_text) = 3 then
                                                                -- As only 3 chars then must be missing leading zero
                                                                substring(Start_time_of_session_text, 1,1) || ':' || substring(Start_time_of_session_text, 2,2) || ':00'
                                                        else
                                                                substring(Start_time_of_session_text, 1,2) || ':' || substring(Start_time_of_session_text, 3,2) || ':00'
                                                        end
                                                   end
                                                )

update BARB_PV206_Viewing_Record_Panel_Members
        set End_time_of_session = dateadd(mi, Duration_of_session, Start_time_of_session) -- Should be Duration_of_session -1 because of Barb minute attribution


update BARB_PV206_Viewing_Record_Panel_Members
        set Start_time_of_recording = datetime(year(Actual_Date_of_Recording) || '-' || month(Actual_Date_of_Recording) || '-' || day(Actual_Date_of_Recording) || ' '
                                                || case when cast(Start_time_of_recording_text as int) >= 2400 then
                                                        -- As start time >= 24:00 then take off 24hours to convert barb time to GMT time. Actual_Date_of_Recording has already been converted
                                                        case when len(Start_time_of_recording_text) = 3 then
                                                                -- As only 3 chars then must be missing leading zero
                                                                cast(substring(Start_time_of_recording_text, 1,1) as int) - 24 || ':' || substring(Start_time_of_recording_text, 2,2) || ':00'
                                                        else
                                                                cast(substring(Start_time_of_recording_text, 1,2) as int) - 24 || ':' || substring(Start_time_of_recording_text, 3,2) || ':00'
                                                        end
                                                   else
                                                        -- Start time < 24:00 then barb time OK
                                                        case when len(Start_time_of_recording_text) = 3 then
                                                                -- As only 3 chars then must be missing leading zero
                                                                substring(Start_time_of_recording_text, 1,1) || ':' || substring(Start_time_of_recording_text, 2,2) || ':00'
                                                        else
                                                                substring(Start_time_of_recording_text, 1,2) || ':' || substring(Start_time_of_recording_text, 3,2) || ':00'
                                                        end
                                                   end
                                                )




----------------------------------------
-- Insert into BARB_PV207_Viewing_Record_Guests
----------------------------------------
insert into BARB_PV207_Viewing_Record_Guests
(file_creation_date, file_creation_time, file_type, file_version, filename
,Record_type, Household_number, Barb_date_of_activity, Actual_date_of_session, Set_number
,Start_time_of_session_text, Start_time_of_session, End_time_of_session, Duration_of_session, Session_activity_type
,Playback_type, DB1_Station_Code, Viewing_platform, Barb_date_of_recording, Actual_Date_of_Recording
,Start_time_of_recording_text, Start_time_of_recording
,Male_4_9, Male_10_15, Male_16_19, Male_20_24, Male_25_34, Male_35_44, Male_45_64, Male_65
,Female_4_9, Female_10_15, Female_16_19, Female_20_24, Female_25_34, Female_35_44, Female_45_64, Female_65
,Interactive_Bar_Code_Identifier, VOD_Indicator, VOD_Provider, VOD_Service
,VOD_Type, Device_in_use)
select
        file_creation_date, file_creation_time, file_type, file_version, filename
        ,Record_type
        ,Household_number
        -- Keep the original Date_of_Activity_DB1 as the Barb_date_of_activity
        ,date(
                cast(cast(Date_of_Activity_DB1/10000 as int) as char(4)) || '-' ||
                cast(cast(Date_of_Activity_DB1/100 as int) - cast(Date_of_Activity_DB1/10000 as int) * 100 as varchar(2)) || '-' ||
                cast(Date_of_Activity_DB1 - cast(Date_of_Activity_DB1/100 as int) * 100 as varchar(2))
                )
        -- Actual_date_of_session: A barb day can go over 24:00. In this case we need to increase the date by 1
        ,dateadd(dd, case when Start_time_of_session >= 2400 then 1 else 0 end,
                date(
                        cast(cast(Date_of_Activity_DB1/10000 as int) as char(4)) || '-' ||
                        cast(cast(Date_of_Activity_DB1/100 as int) - cast(Date_of_Activity_DB1/10000 as int) * 100 as varchar(2)) || '-' ||
                        cast(Date_of_Activity_DB1 - cast(Date_of_Activity_DB1/100 as int) * 100 as varchar(2))
                        )
                )
        ,Set_number
        ,cast(Start_time_of_session as varchar(6)) -- Start_time_of_session_text
        ,datetime('1900-01-01 00:00:00') -- Start_time_of_session. Will update this later in an update query. Easier that way
        ,datetime('1900-01-01 00:00:00') -- End_time_of_session. Will update this later in an update query. Easier that way
        ,Duration_of_session
        ,Session_activity_type
        ,Playback_type
        ,DB1_Station_Code
        ,Viewing_platform
         -- Keep the original Date_of_Recording_DB1 as the Barb_date_of_recording
        ,date(
                cast(cast(Date_of_Recording_DB1/10000 as int) as char(4)) || '-' ||
                cast(cast(Date_of_Recording_DB1/100 as int) - cast(Date_of_Recording_DB1/10000 as int) * 100 as varchar(2)) || '-' ||
                cast(Date_of_Recording_DB1 - cast(Date_of_Recording_DB1/100 as int) * 100 as varchar(2))
                )
        -- Date_of_Recording_DB1: A barb day can go over 24:00. In this case we need to increase the date by 1
        ,dateadd(dd, case when Start_time_of_recording >= 2400 then 1 else 0 end,
                date(
                        cast(cast(Date_of_Recording_DB1/10000 as int) as char(4)) || '-' ||
                        cast(cast(Date_of_Recording_DB1/100 as int) - cast(Date_of_Recording_DB1/10000 as int) * 100 as varchar(2)) || '-' ||
                        cast(Date_of_Recording_DB1 - cast(Date_of_Recording_DB1/100 as int) * 100 as varchar(2))
                        )
                )
        ,cast(Start_time_of_recording as varchar(6)) -- Start_time_of_recording_text
        ,datetime('1900-01-01 00:00:00') -- Start_time_of_recording. Will update this later in an update query. Easier that way
        ,Male_4_9, Male_10_15, Male_16_19, Male_20_24, Male_25_34, Male_35_44, Male_45_64, Male_65
        ,Female_4_9, Female_10_15, Female_16_19, Female_20_24, Female_25_34, Female_35_44, Female_45_64, Female_65
        ,Interactive_Bar_Code_Identifier
        ,VOD_Indicator
        ,VOD_Provider
        ,VOD_Service
        ,VOD_Type
        ,Device_in_use
from
        BARB_PV2_Viewing_Record_Panel_Guests


--- Update the Start and end session and recording timestamps. Its easier to deal with the barb time conversion as an update rather then as in the insert statement
update BARB_PV207_Viewing_Record_Guests
        set Start_time_of_session = datetime(year(Actual_date_of_session) || '-' || month(Actual_date_of_session) || '-' || day(Actual_date_of_session) || ' '
                                                || case when cast(Start_time_of_session_text as int) >= 2400 then
                                                        -- As start time >= 24:00 then take off 24hours to convert barb time to GMT time. Actual_date_of_session has already been converted
                                                        case when len(Start_time_of_session_text) = 3 then
                                                                -- As only 3 chars then must be missing leading zero
                                                                cast(substring(Start_time_of_session_text, 1,1) as int) - 24 || ':' || substring(Start_time_of_session_text, 2,2) || ':00'
                                                        else
                                                                cast(substring(Start_time_of_session_text, 1,2) as int) - 24 || ':' || substring(Start_time_of_session_text, 3,2) || ':00'
                                                        end
                                                   else
                                                        -- Start time < 24:00 then barb time OK
                                                        case when len(Start_time_of_session_text) = 3 then
                                                                -- As only 3 chars then must be missing leading zero
                                                                substring(Start_time_of_session_text, 1,1) || ':' || substring(Start_time_of_session_text, 2,2) || ':00'
                                                        else
                                                                substring(Start_time_of_session_text, 1,2) || ':' || substring(Start_time_of_session_text, 3,2) || ':00'
                                                        end
                                                   end
                                                )

update BARB_PV207_Viewing_Record_Guests
        set End_time_of_session = dateadd(mi, Duration_of_session, Start_time_of_session) -- Should be Duration_of_session -1 because of Barb minute attribution


update BARB_PV207_Viewing_Record_Guests
        set Start_time_of_recording = datetime(year(Actual_Date_of_Recording) || '-' || month(Actual_Date_of_Recording) || '-' || day(Actual_Date_of_Recording) || ' '
                                                || case when cast(Start_time_of_recording_text as int) >= 2400 then
                                                        -- As start time >= 24:00 then take off 24hours to convert barb time to GMT time. Actual_Date_of_Recording has already been converted
                                                        case when len(Start_time_of_recording_text) = 3 then
                                                                -- As only 3 chars then must be missing leading zero
                                                                cast(substring(Start_time_of_recording_text, 1,1) as int) - 24 || ':' || substring(Start_time_of_recording_text, 2,2) || ':00'
                                                        else
                                                                cast(substring(Start_time_of_recording_text, 1,2) as int) - 24 || ':' || substring(Start_time_of_recording_text, 3,2) || ':00'
                                                        end
                                                   else
                                                        -- Start time < 24:00 then barb time OK
                                                        case when len(Start_time_of_recording_text) = 3 then
                                                                -- As only 3 chars then must be missing leading zero
                                                                substring(Start_time_of_recording_text, 1,1) || ':' || substring(Start_time_of_recording_text, 2,2) || ':00'
                                                        else
                                                                substring(Start_time_of_recording_text, 1,2) || ':' || substring(Start_time_of_recording_text, 3,2) || ':00'
                                                        end
                                                   end
                                                )

-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
--      A5: Combine the PVF and PV2 viewing data into the same tables
-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------

-- Combine Panel viewing
select * into BARB_PVF06_PV206_Viewing_Record_Panel_Members from BARB_PVF06_Viewing_Record_Panel_Members

insert into BARB_PVF06_PV206_Viewing_Record_Panel_Members
select * from BARB_PV206_Viewing_Record_Panel_Members


-- Combine Guest viewing
select * into BARB_PVF07_PV207_Viewing_Record_Guests from BARB_PVF07_Viewing_Record_Guests

insert into BARB_PVF07_PV207_Viewing_Record_Guests
select * from BARB_PV207_Viewing_Record_Guests



-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
-- B1: Get age/gender groups for panel member viewing
-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------

-- There are up to 16 people that can be on the Barb panel in a single household
-- The viewing data has a 0/1 column for each individual - so we know which individuals were watching each viewing session
-- Code goes through each person in turn to find age/gender group and append sessions where they are viewing
-- The final step is then to summarie over all these sessions to get number of people in each age/gender group for each session


-- Barb viewing session:
-- Defined as being when any of the following change (in a household on a given tv): channel, activity, platform and individuals


-- Person 1
select
        pvf.household_number
        ,pvf.Barb_date_of_activity
        ,pvf.Actual_date_of_session
        ,pvf.set_number
        ,pvf.start_time_of_session
        ,pvf.end_time_of_session
        ,Duration_of_session
        ,pvf.session_activity_type
        ,pvf.playback_type
        ,pvf.db1_station_code
        ,pvf.Viewing_platform
        ,pvf.Barb_date_of_recording
        ,pvf.Actual_Date_of_Recording
        ,pvf.Start_time_of_recording
        ,pvf.Interactive_Bar_Code_Identifier
        ,pvf.VOD_Indicator
        ,pvf.VOD_Provider
        ,pvf.VOD_Service
        ,pvf.VOD_Type
        ,pvf.Device_in_use
        ,mem.person_number
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 9) then 1 else 0 end as male_4_9
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 15) then 1 else 0 end as male_10_15
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 19) then 1 else 0 end as male_16_19
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 24) then 1 else 0 end as male_20_24
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 34) then 1 else 0 end as male_25_34
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 44) then 1 else 0 end as male_35_44
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 64) then 1 else 0 end as male_45_64
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 65) then 1 else 0 end as male_65
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 9) then 1 else 0 end as female_4_9
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 15) then 1 else 0 end as female_10_15
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 19) then 1 else 0 end as female_16_19
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 24) then 1 else 0 end as female_20_24
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 34) then 1 else 0 end as female_25_34
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 44) then 1 else 0 end as female_35_44
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 64) then 1 else 0 end as female_45_64
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 65) then 1 else 0 end as female_65
into BARB_transformed_viewing_data
from
        BARB_PVF06_PV206_Viewing_Record_Panel_Members pvf
    inner join
        BARB_PVF04_Individual_Member_Details mem
    on pvf.household_number = mem.household_number
    and pvf.file_creation_date = mem.file_creation_date
where
        person_1_viewing = 1
        and mem.person_number = 1;

-- Person 2
insert into BARB_transformed_viewing_data
select
        pvf.household_number
        ,pvf.Barb_date_of_activity
        ,pvf.Actual_date_of_session
        ,pvf.set_number
        ,pvf.start_time_of_session
        ,pvf.end_time_of_session
        ,Duration_of_session
        ,pvf.session_activity_type
        ,pvf.playback_type
        ,pvf.db1_station_code
        ,pvf.Viewing_platform
        ,pvf.Barb_date_of_recording
        ,pvf.Actual_Date_of_Recording
        ,pvf.Start_time_of_recording
        ,pvf.Interactive_Bar_Code_Identifier
        ,pvf.VOD_Indicator
        ,pvf.VOD_Provider
        ,pvf.VOD_Service
        ,pvf.VOD_Type
        ,pvf.Device_in_use
        ,mem.person_number
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 9) then 1 else 0 end as male_4_9
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 15) then 1 else 0 end as male_10_15
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 19) then 1 else 0 end as male_16_19
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 24) then 1 else 0 end as male_20_24
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 34) then 1 else 0 end as male_25_34
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 44) then 1 else 0 end as male_35_44
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 64) then 1 else 0 end as male_45_64
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 65) then 1 else 0 end as male_65
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 9) then 1 else 0 end as female_4_9
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 15) then 1 else 0 end as female_10_15
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 19) then 1 else 0 end as female_16_19
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 24) then 1 else 0 end as female_20_24
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 34) then 1 else 0 end as female_25_34
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 44) then 1 else 0 end as female_35_44
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 64) then 1 else 0 end as female_45_64
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 65) then 1 else 0 end as female_65
into BARB_transformed_viewing_data
from
        BARB_PVF06_PV206_Viewing_Record_Panel_Members pvf
    inner join
        BARB_PVF04_Individual_Member_Details mem
    on pvf.household_number = mem.household_number
    and pvf.file_creation_date = mem.file_creation_date
where
        person_2_viewing = 1
        and mem.person_number = 2;


-- Person 3
insert into BARB_transformed_viewing_data
select
        pvf.household_number
        ,pvf.Barb_date_of_activity
        ,pvf.Actual_date_of_session
        ,pvf.set_number
        ,pvf.start_time_of_session
        ,pvf.end_time_of_session
        ,Duration_of_session
        ,pvf.session_activity_type
        ,pvf.playback_type
        ,pvf.db1_station_code
        ,pvf.Viewing_platform
        ,pvf.Barb_date_of_recording
        ,pvf.Actual_Date_of_Recording
        ,pvf.Start_time_of_recording
        ,pvf.Interactive_Bar_Code_Identifier
        ,pvf.VOD_Indicator
        ,pvf.VOD_Provider
        ,pvf.VOD_Service
        ,pvf.VOD_Type
        ,pvf.Device_in_use
        ,mem.person_number
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 9) then 1 else 0 end as male_4_9
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 15) then 1 else 0 end as male_10_15
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 19) then 1 else 0 end as male_16_19
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 24) then 1 else 0 end as male_20_24
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 34) then 1 else 0 end as male_25_34
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 44) then 1 else 0 end as male_35_44
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 64) then 1 else 0 end as male_45_64
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 65) then 1 else 0 end as male_65
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 9) then 1 else 0 end as female_4_9
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 15) then 1 else 0 end as female_10_15
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 19) then 1 else 0 end as female_16_19
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 24) then 1 else 0 end as female_20_24
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 34) then 1 else 0 end as female_25_34
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 44) then 1 else 0 end as female_35_44
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 64) then 1 else 0 end as female_45_64
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 65) then 1 else 0 end as female_65
into BARB_transformed_viewing_data
from
        BARB_PVF06_PV206_Viewing_Record_Panel_Members pvf
    inner join
        BARB_PVF04_Individual_Member_Details mem
    on pvf.household_number = mem.household_number
    and pvf.file_creation_date = mem.file_creation_date
where
        person_3_viewing = 1
        and mem.person_number = 3;


-- Person 4
insert into BARB_transformed_viewing_data
select
        pvf.household_number
        ,pvf.Barb_date_of_activity
        ,pvf.Actual_date_of_session
        ,pvf.set_number
        ,pvf.start_time_of_session
        ,pvf.end_time_of_session
        ,Duration_of_session
        ,pvf.session_activity_type
        ,pvf.playback_type
        ,pvf.db1_station_code
        ,pvf.Viewing_platform
        ,pvf.Barb_date_of_recording
        ,pvf.Actual_Date_of_Recording
        ,pvf.Start_time_of_recording
        ,pvf.Interactive_Bar_Code_Identifier
        ,pvf.VOD_Indicator
        ,pvf.VOD_Provider
        ,pvf.VOD_Service
        ,pvf.VOD_Type
        ,pvf.Device_in_use
        ,mem.person_number
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 9) then 1 else 0 end as male_4_9
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 15) then 1 else 0 end as male_10_15
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 19) then 1 else 0 end as male_16_19
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 24) then 1 else 0 end as male_20_24
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 34) then 1 else 0 end as male_25_34
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 44) then 1 else 0 end as male_35_44
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 64) then 1 else 0 end as male_45_64
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 65) then 1 else 0 end as male_65
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 9) then 1 else 0 end as female_4_9
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 15) then 1 else 0 end as female_10_15
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 19) then 1 else 0 end as female_16_19
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 24) then 1 else 0 end as female_20_24
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 34) then 1 else 0 end as female_25_34
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 44) then 1 else 0 end as female_35_44
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 64) then 1 else 0 end as female_45_64
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 65) then 1 else 0 end as female_65
into BARB_transformed_viewing_data
from
        BARB_PVF06_PV206_Viewing_Record_Panel_Members pvf
    inner join
        BARB_PVF04_Individual_Member_Details mem
    on pvf.household_number = mem.household_number
    and pvf.file_creation_date = mem.file_creation_date
where
        person_4_viewing = 1
        and mem.person_number = 4;

-- Person 5
insert into BARB_transformed_viewing_data
select
        pvf.household_number
        ,pvf.Barb_date_of_activity
        ,pvf.Actual_date_of_session
        ,pvf.set_number
        ,pvf.start_time_of_session
        ,pvf.end_time_of_session
        ,Duration_of_session
        ,pvf.session_activity_type
        ,pvf.playback_type
        ,pvf.db1_station_code
        ,pvf.Viewing_platform
        ,pvf.Barb_date_of_recording
        ,pvf.Actual_Date_of_Recording
        ,pvf.Start_time_of_recording
        ,pvf.Interactive_Bar_Code_Identifier
        ,pvf.VOD_Indicator
        ,pvf.VOD_Provider
        ,pvf.VOD_Service
        ,pvf.VOD_Type
        ,pvf.Device_in_use
        ,mem.person_number
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 9) then 1 else 0 end as male_4_9
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 15) then 1 else 0 end as male_10_15
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 19) then 1 else 0 end as male_16_19
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 24) then 1 else 0 end as male_20_24
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 34) then 1 else 0 end as male_25_34
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 44) then 1 else 0 end as male_35_44
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 64) then 1 else 0 end as male_45_64
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 65) then 1 else 0 end as male_65
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 9) then 1 else 0 end as female_4_9
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 15) then 1 else 0 end as female_10_15
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 19) then 1 else 0 end as female_16_19
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 24) then 1 else 0 end as female_20_24
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 34) then 1 else 0 end as female_25_34
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 44) then 1 else 0 end as female_35_44
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 64) then 1 else 0 end as female_45_64
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 65) then 1 else 0 end as female_65
into BARB_transformed_viewing_data
from
        BARB_PVF06_PV206_Viewing_Record_Panel_Members pvf
    inner join
        BARB_PVF04_Individual_Member_Details mem
    on pvf.household_number = mem.household_number
    and pvf.file_creation_date = mem.file_creation_date
where
        person_5_viewing = 1
        and mem.person_number = 5;


-- Person 6
insert into BARB_transformed_viewing_data
select
        pvf.household_number
        ,pvf.Barb_date_of_activity
        ,pvf.Actual_date_of_session
        ,pvf.set_number
        ,pvf.start_time_of_session
        ,pvf.end_time_of_session
        ,Duration_of_session
        ,pvf.session_activity_type
        ,pvf.playback_type
        ,pvf.db1_station_code
        ,pvf.Viewing_platform
        ,pvf.Barb_date_of_recording
        ,pvf.Actual_Date_of_Recording
        ,pvf.Start_time_of_recording
        ,pvf.Interactive_Bar_Code_Identifier
        ,pvf.VOD_Indicator
        ,pvf.VOD_Provider
        ,pvf.VOD_Service
        ,pvf.VOD_Type
        ,pvf.Device_in_use
        ,mem.person_number
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 9) then 1 else 0 end as male_4_9
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 15) then 1 else 0 end as male_10_15
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 19) then 1 else 0 end as male_16_19
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 24) then 1 else 0 end as male_20_24
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 34) then 1 else 0 end as male_25_34
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 44) then 1 else 0 end as male_35_44
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 64) then 1 else 0 end as male_45_64
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 65) then 1 else 0 end as male_65
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 9) then 1 else 0 end as female_4_9
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 15) then 1 else 0 end as female_10_15
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 19) then 1 else 0 end as female_16_19
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 24) then 1 else 0 end as female_20_24
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 34) then 1 else 0 end as female_25_34
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 44) then 1 else 0 end as female_35_44
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 64) then 1 else 0 end as female_45_64
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 65) then 1 else 0 end as female_65
into BARB_transformed_viewing_data
from
        BARB_PVF06_PV206_Viewing_Record_Panel_Members pvf
    inner join
        BARB_PVF04_Individual_Member_Details mem
    on pvf.household_number = mem.household_number
    and pvf.file_creation_date = mem.file_creation_date
where
        person_6_viewing = 1
        and mem.person_number = 6;


-- Person 7
insert into BARB_transformed_viewing_data
select
        pvf.household_number
        ,pvf.Barb_date_of_activity
        ,pvf.Actual_date_of_session
        ,pvf.set_number
        ,pvf.start_time_of_session
        ,pvf.end_time_of_session
        ,Duration_of_session
        ,pvf.session_activity_type
        ,pvf.playback_type
        ,pvf.db1_station_code
        ,pvf.Viewing_platform
        ,pvf.Barb_date_of_recording
        ,pvf.Actual_Date_of_Recording
        ,pvf.Start_time_of_recording
        ,pvf.Interactive_Bar_Code_Identifier
        ,pvf.VOD_Indicator
        ,pvf.VOD_Provider
        ,pvf.VOD_Service
        ,pvf.VOD_Type
        ,pvf.Device_in_use
        ,mem.person_number
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 9) then 1 else 0 end as male_4_9
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 15) then 1 else 0 end as male_10_15
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 19) then 1 else 0 end as male_16_19
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 24) then 1 else 0 end as male_20_24
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 34) then 1 else 0 end as male_25_34
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 44) then 1 else 0 end as male_35_44
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 64) then 1 else 0 end as male_45_64
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 65) then 1 else 0 end as male_65
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 9) then 1 else 0 end as female_4_9
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 15) then 1 else 0 end as female_10_15
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 19) then 1 else 0 end as female_16_19
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 24) then 1 else 0 end as female_20_24
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 34) then 1 else 0 end as female_25_34
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 44) then 1 else 0 end as female_35_44
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 64) then 1 else 0 end as female_45_64
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 65) then 1 else 0 end as female_65
into BARB_transformed_viewing_data
from
        BARB_PVF06_PV206_Viewing_Record_Panel_Members pvf
    inner join
        BARB_PVF04_Individual_Member_Details mem
    on pvf.household_number = mem.household_number
    and pvf.file_creation_date = mem.file_creation_date
where
        person_7_viewing = 1
        and mem.person_number = 7;

-- Person 8
insert into BARB_transformed_viewing_data
select
        pvf.household_number
        ,pvf.Barb_date_of_activity
        ,pvf.Actual_date_of_session
        ,pvf.set_number
        ,pvf.start_time_of_session
        ,pvf.end_time_of_session
        ,Duration_of_session
        ,pvf.session_activity_type
        ,pvf.playback_type
        ,pvf.db1_station_code
        ,pvf.Viewing_platform
        ,pvf.Barb_date_of_recording
        ,pvf.Actual_Date_of_Recording
        ,pvf.Start_time_of_recording
        ,pvf.Interactive_Bar_Code_Identifier
        ,pvf.VOD_Indicator
        ,pvf.VOD_Provider
        ,pvf.VOD_Service
        ,pvf.VOD_Type
        ,pvf.Device_in_use
        ,mem.person_number
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 9) then 1 else 0 end as male_4_9
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 15) then 1 else 0 end as male_10_15
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 19) then 1 else 0 end as male_16_19
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 24) then 1 else 0 end as male_20_24
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 34) then 1 else 0 end as male_25_34
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 44) then 1 else 0 end as male_35_44
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 64) then 1 else 0 end as male_45_64
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 65) then 1 else 0 end as male_65
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 9) then 1 else 0 end as female_4_9
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 15) then 1 else 0 end as female_10_15
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 19) then 1 else 0 end as female_16_19
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 24) then 1 else 0 end as female_20_24
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 34) then 1 else 0 end as female_25_34
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 44) then 1 else 0 end as female_35_44
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 64) then 1 else 0 end as female_45_64
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 65) then 1 else 0 end as female_65
into BARB_transformed_viewing_data
from
        BARB_PVF06_PV206_Viewing_Record_Panel_Members pvf
    inner join
        BARB_PVF04_Individual_Member_Details mem
    on pvf.household_number = mem.household_number
    and pvf.file_creation_date = mem.file_creation_date
where
        person_8_viewing = 1
        and mem.person_number = 8;

-- Person 9
insert into BARB_transformed_viewing_data
select
        pvf.household_number
        ,pvf.Barb_date_of_activity
        ,pvf.Actual_date_of_session
        ,pvf.set_number
        ,pvf.start_time_of_session
        ,pvf.end_time_of_session
        ,Duration_of_session
        ,pvf.session_activity_type
        ,pvf.playback_type
        ,pvf.db1_station_code
        ,pvf.Viewing_platform
        ,pvf.Barb_date_of_recording
        ,pvf.Actual_Date_of_Recording
        ,pvf.Start_time_of_recording
        ,pvf.Interactive_Bar_Code_Identifier
        ,pvf.VOD_Indicator
        ,pvf.VOD_Provider
        ,pvf.VOD_Service
        ,pvf.VOD_Type
        ,pvf.Device_in_use
        ,mem.person_number
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 9) then 1 else 0 end as male_4_9
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 15) then 1 else 0 end as male_10_15
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 19) then 1 else 0 end as male_16_19
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 24) then 1 else 0 end as male_20_24
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 34) then 1 else 0 end as male_25_34
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 44) then 1 else 0 end as male_35_44
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 64) then 1 else 0 end as male_45_64
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 65) then 1 else 0 end as male_65
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 9) then 1 else 0 end as female_4_9
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 15) then 1 else 0 end as female_10_15
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 19) then 1 else 0 end as female_16_19
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 24) then 1 else 0 end as female_20_24
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 34) then 1 else 0 end as female_25_34
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 44) then 1 else 0 end as female_35_44
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 64) then 1 else 0 end as female_45_64
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 65) then 1 else 0 end as female_65
into BARB_transformed_viewing_data
from
        BARB_PVF06_PV206_Viewing_Record_Panel_Members pvf
    inner join
        BARB_PVF04_Individual_Member_Details mem
    on pvf.household_number = mem.household_number
    and pvf.file_creation_date = mem.file_creation_date
where
        person_9_viewing = 1
        and mem.person_number = 9;

-- Person 10
insert into BARB_transformed_viewing_data
select
        pvf.household_number
        ,pvf.Barb_date_of_activity
        ,pvf.Actual_date_of_session
        ,pvf.set_number
        ,pvf.start_time_of_session
        ,pvf.end_time_of_session
        ,Duration_of_session
        ,pvf.session_activity_type
        ,pvf.playback_type
        ,pvf.db1_station_code
        ,pvf.Viewing_platform
        ,pvf.Barb_date_of_recording
        ,pvf.Actual_Date_of_Recording
        ,pvf.Start_time_of_recording
        ,pvf.Interactive_Bar_Code_Identifier
        ,pvf.VOD_Indicator
        ,pvf.VOD_Provider
        ,pvf.VOD_Service
        ,pvf.VOD_Type
        ,pvf.Device_in_use
        ,mem.person_number
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 9) then 1 else 0 end as male_4_9
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 15) then 1 else 0 end as male_10_15
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 19) then 1 else 0 end as male_16_19
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 24) then 1 else 0 end as male_20_24
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 34) then 1 else 0 end as male_25_34
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 44) then 1 else 0 end as male_35_44
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 64) then 1 else 0 end as male_45_64
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 65) then 1 else 0 end as male_65
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 9) then 1 else 0 end as female_4_9
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 15) then 1 else 0 end as female_10_15
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 19) then 1 else 0 end as female_16_19
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 24) then 1 else 0 end as female_20_24
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 34) then 1 else 0 end as female_25_34
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 44) then 1 else 0 end as female_35_44
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 64) then 1 else 0 end as female_45_64
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 65) then 1 else 0 end as female_65
into BARB_transformed_viewing_data
from
        BARB_PVF06_PV206_Viewing_Record_Panel_Members pvf
    inner join
        BARB_PVF04_Individual_Member_Details mem
    on pvf.household_number = mem.household_number
    and pvf.file_creation_date = mem.file_creation_date
where
        person_10_viewing = 1
        and mem.person_number = 10;


-- Person 11
insert into BARB_transformed_viewing_data
select
        pvf.household_number
        ,pvf.Barb_date_of_activity
        ,pvf.Actual_date_of_session
        ,pvf.set_number
        ,pvf.start_time_of_session
        ,pvf.end_time_of_session
        ,Duration_of_session
        ,pvf.session_activity_type
        ,pvf.playback_type
        ,pvf.db1_station_code
        ,pvf.Viewing_platform
        ,pvf.Barb_date_of_recording
        ,pvf.Actual_Date_of_Recording
        ,pvf.Start_time_of_recording
        ,pvf.Interactive_Bar_Code_Identifier
        ,pvf.VOD_Indicator
        ,pvf.VOD_Provider
        ,pvf.VOD_Service
        ,pvf.VOD_Type
        ,pvf.Device_in_use
        ,mem.person_number
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 9) then 1 else 0 end as male_4_9
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 15) then 1 else 0 end as male_10_15
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 19) then 1 else 0 end as male_16_19
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 24) then 1 else 0 end as male_20_24
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 34) then 1 else 0 end as male_25_34
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 44) then 1 else 0 end as male_35_44
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 64) then 1 else 0 end as male_45_64
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 65) then 1 else 0 end as male_65
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 9) then 1 else 0 end as female_4_9
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 15) then 1 else 0 end as female_10_15
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 19) then 1 else 0 end as female_16_19
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 24) then 1 else 0 end as female_20_24
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 34) then 1 else 0 end as female_25_34
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 44) then 1 else 0 end as female_35_44
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 64) then 1 else 0 end as female_45_64
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 65) then 1 else 0 end as female_65
into BARB_transformed_viewing_data
from
        BARB_PVF06_PV206_Viewing_Record_Panel_Members pvf
    inner join
        BARB_PVF04_Individual_Member_Details mem
    on pvf.household_number = mem.household_number
    and pvf.file_creation_date = mem.file_creation_date
where
        person_11_viewing = 1
        and mem.person_number = 11;


-- Person 12
insert into BARB_transformed_viewing_data
select
        pvf.household_number
        ,pvf.Barb_date_of_activity
        ,pvf.Actual_date_of_session
        ,pvf.set_number
        ,pvf.start_time_of_session
        ,pvf.end_time_of_session
        ,Duration_of_session
        ,pvf.session_activity_type
        ,pvf.playback_type
        ,pvf.db1_station_code
        ,pvf.Viewing_platform
        ,pvf.Barb_date_of_recording
        ,pvf.Actual_Date_of_Recording
        ,pvf.Start_time_of_recording
        ,pvf.Interactive_Bar_Code_Identifier
        ,pvf.VOD_Indicator
        ,pvf.VOD_Provider
        ,pvf.VOD_Service
        ,pvf.VOD_Type
        ,pvf.Device_in_use
        ,mem.person_number
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 9) then 1 else 0 end as male_4_9
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 15) then 1 else 0 end as male_10_15
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 19) then 1 else 0 end as male_16_19
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 24) then 1 else 0 end as male_20_24
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 34) then 1 else 0 end as male_25_34
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 44) then 1 else 0 end as male_35_44
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 64) then 1 else 0 end as male_45_64
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 65) then 1 else 0 end as male_65
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 9) then 1 else 0 end as female_4_9
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 15) then 1 else 0 end as female_10_15
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 19) then 1 else 0 end as female_16_19
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 24) then 1 else 0 end as female_20_24
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 34) then 1 else 0 end as female_25_34
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 44) then 1 else 0 end as female_35_44
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 64) then 1 else 0 end as female_45_64
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 65) then 1 else 0 end as female_65
into BARB_transformed_viewing_data
from
        BARB_PVF06_PV206_Viewing_Record_Panel_Members pvf
    inner join
        BARB_PVF04_Individual_Member_Details mem
    on pvf.household_number = mem.household_number
    and pvf.file_creation_date = mem.file_creation_date
where
        person_12_viewing = 1
        and mem.person_number = 12;


-- Person 13
insert into BARB_transformed_viewing_data
select
        pvf.household_number
        ,pvf.Barb_date_of_activity
        ,pvf.Actual_date_of_session
        ,pvf.set_number
        ,pvf.start_time_of_session
        ,pvf.end_time_of_session
        ,Duration_of_session
        ,pvf.session_activity_type
        ,pvf.playback_type
        ,pvf.db1_station_code
        ,pvf.Viewing_platform
        ,pvf.Barb_date_of_recording
        ,pvf.Actual_Date_of_Recording
        ,pvf.Start_time_of_recording
        ,pvf.Interactive_Bar_Code_Identifier
        ,pvf.VOD_Indicator
        ,pvf.VOD_Provider
        ,pvf.VOD_Service
        ,pvf.VOD_Type
        ,pvf.Device_in_use
        ,mem.person_number
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 9) then 1 else 0 end as male_4_9
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 15) then 1 else 0 end as male_10_15
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 19) then 1 else 0 end as male_16_19
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 24) then 1 else 0 end as male_20_24
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 34) then 1 else 0 end as male_25_34
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 44) then 1 else 0 end as male_35_44
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 64) then 1 else 0 end as male_45_64
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 65) then 1 else 0 end as male_65
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 9) then 1 else 0 end as female_4_9
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 15) then 1 else 0 end as female_10_15
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 19) then 1 else 0 end as female_16_19
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 24) then 1 else 0 end as female_20_24
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 34) then 1 else 0 end as female_25_34
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 44) then 1 else 0 end as female_35_44
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 64) then 1 else 0 end as female_45_64
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 65) then 1 else 0 end as female_65
into BARB_transformed_viewing_data
from
        BARB_PVF06_PV206_Viewing_Record_Panel_Members pvf
    inner join
        BARB_PVF04_Individual_Member_Details mem
    on pvf.household_number = mem.household_number
    and pvf.file_creation_date = mem.file_creation_date
where
        person_13_viewing = 1
        and mem.person_number = 13;

-- Person 14
insert into BARB_transformed_viewing_data
select
        pvf.household_number
        ,pvf.Barb_date_of_activity
        ,pvf.Actual_date_of_session
        ,pvf.set_number
        ,pvf.start_time_of_session
        ,pvf.end_time_of_session
        ,Duration_of_session
        ,pvf.session_activity_type
        ,pvf.playback_type
        ,pvf.db1_station_code
        ,pvf.Viewing_platform
        ,pvf.Barb_date_of_recording
        ,pvf.Actual_Date_of_Recording
        ,pvf.Start_time_of_recording
        ,pvf.Interactive_Bar_Code_Identifier
        ,pvf.VOD_Indicator
        ,pvf.VOD_Provider
        ,pvf.VOD_Service
        ,pvf.VOD_Type
        ,pvf.Device_in_use
        ,mem.person_number
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 9) then 1 else 0 end as male_4_9
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 15) then 1 else 0 end as male_10_15
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 19) then 1 else 0 end as male_16_19
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 24) then 1 else 0 end as male_20_24
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 34) then 1 else 0 end as male_25_34
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 44) then 1 else 0 end as male_35_44
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 64) then 1 else 0 end as male_45_64
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 65) then 1 else 0 end as male_65
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 9) then 1 else 0 end as female_4_9
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 15) then 1 else 0 end as female_10_15
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 19) then 1 else 0 end as female_16_19
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 24) then 1 else 0 end as female_20_24
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 34) then 1 else 0 end as female_25_34
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 44) then 1 else 0 end as female_35_44
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 64) then 1 else 0 end as female_45_64
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 65) then 1 else 0 end as female_65
into BARB_transformed_viewing_data
from
        BARB_PVF06_PV206_Viewing_Record_Panel_Members pvf
    inner join
        BARB_PVF04_Individual_Member_Details mem
    on pvf.household_number = mem.household_number
    and pvf.file_creation_date = mem.file_creation_date
where
        person_14_viewing = 1
        and mem.person_number = 14;

-- Person 15
insert into BARB_transformed_viewing_data
select
        pvf.household_number
        ,pvf.Barb_date_of_activity
        ,pvf.Actual_date_of_session
        ,pvf.set_number
        ,pvf.start_time_of_session
        ,pvf.end_time_of_session
        ,Duration_of_session
        ,pvf.session_activity_type
        ,pvf.playback_type
        ,pvf.db1_station_code
        ,pvf.Viewing_platform
        ,pvf.Barb_date_of_recording
        ,pvf.Actual_Date_of_Recording
        ,pvf.Start_time_of_recording
        ,pvf.Interactive_Bar_Code_Identifier
        ,pvf.VOD_Indicator
        ,pvf.VOD_Provider
        ,pvf.VOD_Service
        ,pvf.VOD_Type
        ,pvf.Device_in_use
        ,mem.person_number
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 9) then 1 else 0 end as male_4_9
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 15) then 1 else 0 end as male_10_15
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 19) then 1 else 0 end as male_16_19
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 24) then 1 else 0 end as male_20_24
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 34) then 1 else 0 end as male_25_34
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 44) then 1 else 0 end as male_35_44
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 64) then 1 else 0 end as male_45_64
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 65) then 1 else 0 end as male_65
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 9) then 1 else 0 end as female_4_9
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 15) then 1 else 0 end as female_10_15
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 19) then 1 else 0 end as female_16_19
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 24) then 1 else 0 end as female_20_24
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 34) then 1 else 0 end as female_25_34
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 44) then 1 else 0 end as female_35_44
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 64) then 1 else 0 end as female_45_64
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 65) then 1 else 0 end as female_65
into BARB_transformed_viewing_data
from
        BARB_PVF06_PV206_Viewing_Record_Panel_Members pvf
    inner join
        BARB_PVF04_Individual_Member_Details mem
    on pvf.household_number = mem.household_number
    and pvf.file_creation_date = mem.file_creation_date
where
        person_15_viewing = 1
        and mem.person_number = 15;


-- Person 16
insert into BARB_transformed_viewing_data
select
        pvf.household_number
        ,pvf.Barb_date_of_activity
        ,pvf.Actual_date_of_session
        ,pvf.set_number
        ,pvf.start_time_of_session
        ,pvf.end_time_of_session
        ,Duration_of_session
        ,pvf.session_activity_type
        ,pvf.playback_type
        ,pvf.db1_station_code
        ,pvf.Viewing_platform
        ,pvf.Barb_date_of_recording
        ,pvf.Actual_Date_of_Recording
        ,pvf.Start_time_of_recording
        ,pvf.Interactive_Bar_Code_Identifier
        ,pvf.VOD_Indicator
        ,pvf.VOD_Provider
        ,pvf.VOD_Service
        ,pvf.VOD_Type
        ,pvf.Device_in_use
        ,mem.person_number
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 9) then 1 else 0 end as male_4_9
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 15) then 1 else 0 end as male_10_15
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 19) then 1 else 0 end as male_16_19
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 24) then 1 else 0 end as male_20_24
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 34) then 1 else 0 end as male_25_34
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 44) then 1 else 0 end as male_35_44
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 64) then 1 else 0 end as male_45_64
        ,case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 65) then 1 else 0 end as male_65
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 9) then 1 else 0 end as female_4_9
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 15) then 1 else 0 end as female_10_15
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 19) then 1 else 0 end as female_16_19
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 24) then 1 else 0 end as female_20_24
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 34) then 1 else 0 end as female_25_34
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 44) then 1 else 0 end as female_35_44
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(start_time_of_session)) <= 64) then 1 else 0 end as female_45_64
        ,case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(start_time_of_session)) >= 65) then 1 else 0 end as female_65
into BARB_transformed_viewing_data
from
        BARB_PVF06_PV206_Viewing_Record_Panel_Members pvf
    inner join
        BARB_PVF04_Individual_Member_Details mem
    on pvf.household_number = mem.household_number
    and pvf.file_creation_date = mem.file_creation_date
where
        person_16_viewing = 1
        and mem.person_number = 16;


---- Summarise Barb Panel viewing events into counts by age/gender for each session
-- Note the Error correction below. Ideally fix the code in section A4
-- Barb assigns each minute so can't have end time being the same as previous start time
-- so when we calculated End_time_of_session should have been dateadd(mi, duration - 1, Start_time_of_session)
select
        'Panel' as panel_guest_indicator
        ,Household_number
        ,Barb_date_of_activity
        ,Actual_date_of_session
        ,Set_number
        ,Start_time_of_session
        ,dateadd(mi, -1, End_time_of_session) as End_time_of_session -- Error in the import code I wrote. Didn't account for way Barb do Minute attribution
        ,Duration_of_session
        ,Session_activity_type
        ,Playback_type
        ,DB1_Station_Code
        ,Viewing_platform
        ,Barb_date_of_recording
        ,Actual_Date_of_Recording
        ,Start_time_of_recording
        ,Interactive_Bar_Code_Identifier
        ,VOD_Indicator
        ,VOD_Provider
        ,VOD_Service
        ,VOD_Type
        ,Device_in_use
        ,case when start_time_of_recording is null then start_time_of_session else start_time_of_recording end as broadcast_start_date_time
        ,case when start_time_of_recording is null then end_time_of_session else dateadd(mi, Duration_of_session -1 , start_time_of_recording) end as broadcast_end_date_time -- -1 because of minute attribution
        ,case when VOD_indicator = 1 then 1 else 0 end as VOD_indicator_consolidated -- Combining unknown with Not on-demand. Makes calculating channel level event times easier
        ,count(1) as person_count
        ,sum(male_4_9) as male_4_9
        ,sum(male_10_15) as male_10_15
        ,sum(male_16_19) as male_16_19
        ,sum(male_20_24) as male_20_24
        ,sum(male_25_34) as male_25_34
        ,sum(male_35_44) as male_35_44
        ,sum(male_45_64) as male_45_64
        ,sum(male_65) as male_65
        ,sum(female_4_9) as female_4_9
        ,sum(female_10_15) as female_10_15
        ,sum(female_16_19) as female_16_19
        ,sum(female_20_24) as female_20_24
        ,sum(female_25_34) as female_25_34
        ,sum(female_35_44) as female_35_44
        ,sum(female_45_64) as female_45_64
        ,sum(female_65) as female_65
into
        BARB_temp_Viewing_panel_guest
from
        BARB_transformed_viewing_data
group by
        panel_guest_indicator
        ,Household_number
        ,Barb_date_of_activity
        ,Actual_date_of_session
        ,Set_number
        ,Start_time_of_session
        ,End_time_of_session
        ,Duration_of_session
        ,Session_activity_type
        ,Playback_type
        ,DB1_Station_Code
        ,Viewing_platform
        ,Barb_date_of_recording
        ,Actual_Date_of_Recording
        ,Start_time_of_recording
        ,Interactive_Bar_Code_Identifier
        ,VOD_Indicator
        ,VOD_Provider
        ,VOD_Service
        ,VOD_Type
        ,Device_in_use
        ,broadcast_start_date_time
        ,broadcast_end_date_time
        ,VOD_indicator_consolidated


-- Add guest viewing to the table
insert into BARB_temp_Viewing_panel_guest
select
        'Guest'
        ,Household_number
        ,Barb_date_of_activity
        ,Actual_date_of_session
        ,Set_number
        ,Start_time_of_session
        ,dateadd(mi, -1, End_time_of_session) -- Error in the import code I wrote. Didn't account for way Barb do Minute attribution
        ,Duration_of_session
        ,Session_activity_type
        ,Playback_type
        ,DB1_Station_Code
        ,Viewing_platform
        ,Barb_date_of_recording
        ,Actual_Date_of_Recording
        ,Start_time_of_recording
        ,Interactive_Bar_Code_Identifier
        ,VOD_Indicator
        ,VOD_Provider
        ,VOD_Service
        ,VOD_Type
        ,Device_in_use
        ,case when start_time_of_recording is null then start_time_of_session else start_time_of_recording end
        ,case when start_time_of_recording is null then end_time_of_session else dateadd(mi, Duration_of_session -1 , start_time_of_recording) end  -- -1 because of minute attribution
        ,case when VOD_indicator = 1 then 1 else 0 end  -- Combining unknown with Not on-demand. Makes calculating channel level event times easier
        ,Male_4_9 + Male_10_15 + Male_16_19 + Male_20_24 + Male_25_34 + Male_35_44 + Male_45_64 + Male_65
          + Female_4_9 + Female_10_15 + Female_16_19 + Female_20_24 + Female_25_34 + Female_35_44 + Female_45_64 + Female_65
        ,Male_4_9
        ,Male_10_15
        ,Male_16_19
        ,Male_20_24
        ,Male_25_34
        ,Male_35_44
        ,Male_45_64
        ,Male_65
        ,Female_4_9
        ,Female_10_15
        ,Female_16_19
        ,Female_20_24
        ,Female_25_34
        ,Female_35_44
        ,Female_45_64
        ,Female_65
from
        BARB_PVF07_PV207_Viewing_Record_Guests


create hg index ind_hhd on BARB_temp_Viewing_panel_guest(Household_number)
create lf index ind_set on BARB_temp_Viewing_panel_guest(set_number)
create hg index ind_start on BARB_temp_Viewing_panel_guest(Start_time_of_session)
create hg index ind_end on BARB_temp_Viewing_panel_guest(end_time_of_session)



-- Create combined panel and guest viewing table and order it for processing
-- i.e. finding Channel Event Start/End Times
-- A channel event defined when the channel is changed or the TV is turned on/off (consistent with Skyview approach)

create table BARB_Viewing_panel_guest (
id_row bigint primary key identity
,panel_guest_indicator as varchar(5)
,Household_number int DEFAULT NULL
,Barb_date_of_activity date
,Actual_date_of_session date
,Set_number int DEFAULT NULL
,Start_time_of_session timestamp
,End_time_of_session timestamp
,Duration_of_session int DEFAULT NULL
,Session_activity_type int DEFAULT NULL
,Playback_type varchar(1) DEFAULT NULL
,DB1_Station_Code int DEFAULT NULL
,Viewing_platform int DEFAULT NULL
,Barb_date_of_recording date
,Actual_Date_of_Recording date
,Start_time_of_recording timestamp
,Interactive_Bar_Code_Identifier int DEFAULT NULL
,VOD_Indicator int DEFAULT NULL
,VOD_Provider int DEFAULT NULL
,VOD_Service int DEFAULT NULL
,VOD_Type int DEFAULT NULL
,Device_in_use int DEFAULT NULL
,broadcast_start_date_time timestamp
,broadcast_end_date_time timestamp
,VOD_indicator_consolidated int -- Combining unknown with Not on-demand. Makes calculating channel level event times easier
,channel_event_start_date_time timestamp
,channel_event_end_date_time timestamp
,programme_instance_start_date_time timestamp
,programme_instance_end_date_time timestamp
,person_count int
,Male_4_9 int
,Male_10_15 int
,Male_16_19 int
,Male_20_24 int
,Male_25_34 int
,Male_35_44 int
,Male_45_64 int
,Male_65 int
,Female_4_9 int
,Female_10_15 int
,Female_16_19 int
,Female_20_24 int
,Female_25_34 int
,Female_35_44 int
,Female_45_64 int
,Female_65 int
)


create hg index ind_hhd on BARB_Viewing_panel_guest(Household_number)
create lf index ind_set on BARB_Viewing_panel_guest(set_number)
create hg index ind_start on BARB_Viewing_panel_guest(Start_time_of_session)
create hg index ind_end on BARB_Viewing_panel_guest(end_time_of_session)
create lf index ind_session on BARB_Viewing_panel_guest(Session_activity_type)
create lf index ind_platform on BARB_Viewing_panel_guest(Viewing_platform)
create hg index ind_db1 on BARB_Viewing_panel_guest(db1_station_code)
create hg index ind_bstart on BARB_Viewing_panel_guest(broadcast_start_date_time)
create hg index ind_bend on BARB_Viewing_panel_guest(broadcast_end_date_time)


---- Insert the panel guest viewing into this table ordered by household_number, set_number and start_time_of_session
insert into BARB_Viewing_panel_guest (
panel_guest_indicator, Household_number, Barb_date_of_activity, Actual_date_of_session, Set_number, Start_time_of_session, End_time_of_session
,Duration_of_session, Session_activity_type, Playback_type, DB1_Station_Code, Viewing_platform, Barb_date_of_recording, Actual_Date_of_Recording
,Start_time_of_recording, Interactive_Bar_Code_Identifier, VOD_Indicator, VOD_Provider, VOD_Service, VOD_Type, Device_in_use
,broadcast_start_date_time, broadcast_end_date_time, VOD_indicator_consolidated
,channel_event_start_date_time, channel_event_end_date_time, programme_instance_start_date_time, programme_instance_end_date_time
,person_count
,Male_4_9, Male_10_15, Male_16_19, Male_20_24, Male_25_34, Male_35_44, Male_45_64, Male_65
,Female_4_9, Female_10_15, Female_16_19, Female_20_24, Female_25_34, Female_35_44, Female_45_64, Female_65
)

select
panel_guest_indicator, Household_number, Barb_date_of_activity, Actual_date_of_session, Set_number, Start_time_of_session, End_time_of_session
,Duration_of_session, Session_activity_type, Playback_type, DB1_Station_Code, Viewing_platform, Barb_date_of_recording, Actual_Date_of_Recording
,Start_time_of_recording, Interactive_Bar_Code_Identifier, VOD_Indicator, VOD_Provider, VOD_Service, VOD_Type, Device_in_use
,broadcast_start_date_time, broadcast_end_date_time, VOD_indicator_consolidated
,datetime('1900-01-01 23:59:59'), datetime('1900-01-01 23:59:59'), datetime('1900-01-01 23:59:59'), datetime('1900-01-01 23:59:59') -- these values to be set later
,person_count
,Male_4_9, Male_10_15, Male_16_19, Male_20_24, Male_25_34, Male_35_44, Male_45_64, Male_65
,Female_4_9, Female_10_15, Female_16_19, Female_20_24, Female_25_34, Female_35_44, Female_45_64, Female_65
from
        BARB_temp_Viewing_panel_guest
order by
        household_number
        ,set_number
        ,start_time_of_session


drop table BARB_temp_Viewing_panel_guest

-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
-- B2: Get Event start/end times
-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------

-- Calculate Channel Event Start/End Times
-- A channel event defined when the channel is changed or the TV is turned on/off

-- The viewing table joined to itself, but joined to either the row above or below
-- Barb definition of a session (see doc BARB2010DSP01 Version 3.7C - Panel Viewing File.pdf) is
-- If there is a change for a given tv set in a given household in either channel/session activity/ viewing platform / individuals then must be a new session

-- Existing Skyview uses Vespa like events ie. same as Barb session but ignoring the changes in indivual watching TV
-- So if the row above/below is a different channel/tv set/household/ session activity/ viewing platform then must be either start/end of event

-- Note from data looked like VOD indicator triggered a new session, but have not used this here


-- Identify rows that are Channel Event Starts
select
        v1.household_number
        ,v1.set_number
        ,v1.db1_station_code
        ,v1.session_activity_type
        ,v1.viewing_platform
        ,v1.start_time_of_session as channel_event_start_time
into
        BARB_temp_channel_event_start
from
        BARB_Viewing_panel_guest v1
     inner join
        BARB_Viewing_panel_guest v2
     -- this join matches row_id 1 in table v1 to row_id 0 in table v2
     -- i.e. the first channel event doesn't get a start time. Will deal with this later
     on v1.id_row = v2.id_row+1
where
        v1.household_number <> v2.household_number
        or v1.set_number <> v2.set_number
        or v1.db1_station_code <> v2.db1_station_code
        or v1.session_activity_type <> v2.session_activity_type
        or v1.viewing_platform <> v2.viewing_platform


create hg index ind_hhd on BARB_temp_channel_event_start(household_number)
create lf index ind_set on BARB_temp_channel_event_start(set_number)
create lf index ind_db1 on BARB_temp_channel_event_start(db1_station_code)
create lf index ind_act on BARB_temp_channel_event_start(session_activity_type)
create lf index ind_plat on BARB_temp_channel_event_start(viewing_platform)
create hg index ind_start on BARB_temp_channel_event_start(channel_event_start_time)


-- Update channel event start times
update BARB_Viewing_panel_guest
        set v1.channel_event_start_date_time =
                (select max(s.channel_event_start_time)
                from BARB_temp_channel_event_start s
                where v1.household_number = s.household_number
                and v1.set_number = s.set_number
                and v1.db1_station_code = s.db1_station_code
                and v1.session_activity_type = s.session_activity_type
                and v1.viewing_platform = s.viewing_platform
                and v1.start_time_of_session >= s.channel_event_start_time
                group by s.household_number, s.set_number, s.db1_station_code, s.session_activity_type, s.viewing_platform)
        from BARB_Viewing_panel_guest v1

-- Deal with the missing channel start time for the first event (the first row gets missed when we do the join above)
update BARB_Viewing_panel_guest
        set channel_event_start_date_time =
        (select min(start_time_of_session)
        from BARB_Viewing_panel_guest
        where channel_event_start_date_time is null)
        where channel_event_start_date_time is null


drop table BARB_temp_channel_event_start


-- Identify rows that are Channel Event Ends
-- The following update query can't match the last row_id (if last row_id is 100 then would have to match to row_id 101)
-- i.e. the last channel event doesn't get an end time. Will deal with this later
-- By adding a dummy record at the end this match will work
insert into BARB_Viewing_panel_guest (set_number) select 9999

select
        v1.id_row
        ,v1.household_number
        ,v1.set_number
        ,v1.db1_station_code
        ,v1.session_activity_type
        ,v1.viewing_platform
        ,v1.end_time_of_session as channel_event_end_time
into
        BARB_temp_channel_event_end
from
        BARB_Viewing_panel_guest v1
     inner join
        BARB_Viewing_panel_guest v2
     -- this join can't match the last row_id (if last row_id is 100 then would have to match to row_id 101)
     -- i.e. the last channel event doesn't get an end time. Will deal with this later
     on v1.id_row = v2.id_row-1
where
        v1.household_number <> v2.household_number
        or v1.set_number <> v2.set_number
        or v1.db1_station_code <> v2.db1_station_code
        or v1.session_activity_type <> v2.session_activity_type
        or v1.viewing_platform <> v2.viewing_platform

create hg index ind_row on BARB_temp_channel_event_end(id_row)
create hg index ind_hhd on BARB_temp_channel_event_end(household_number)
create lf index ind_set on BARB_temp_channel_event_end(set_number)
create lf index ind_db1 on BARB_temp_channel_event_end(db1_station_code)
create lf index ind_act on BARB_temp_channel_event_end(session_activity_type)
create lf index ind_plat on BARB_temp_channel_event_end(viewing_platform)
create hg index ind_end on BARB_temp_channel_event_end(channel_event_end_time)

delete from BARB_Viewing_panel_guest where set_number = 9999 -- delete the dummy record that we added


-- Update channel event end times
update BARB_Viewing_panel_guest
        set v1.channel_event_end_date_time =
                (select min(s.channel_event_end_time)
                from BARB_temp_channel_event_end s
                where v1.household_number = s.household_number
                and v1.set_number = s.set_number
                and v1.db1_station_code = s.db1_station_code
                and v1.session_activity_type = s.session_activity_type
                and v1.viewing_platform = s.viewing_platform
                and v1.end_time_of_session <= s.channel_event_end_time
                group by s.household_number, s.set_number, s.db1_station_code, s.session_activity_type, s.viewing_platform)
        from BARB_Viewing_panel_guest v1



drop table BARB_temp_channel_event_end




-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
-- B3: Match viewing to Vespa programme schedule
-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------

-- I have created a table called BARB_Channel_Map
-- I have mapped DB1_station_codes to Service_Keys
-- Note that there is not a 1 to 1 relationship
-- But I have identified the "main" service key that maps to a db1 code by the total viewing duration over the past month or so
-- The main service key identified by main_sk = 'Y'
-- Multiple DB1 codes may feed into a single service key - but this is OK
-- We will need to work out how this table gets updated regularly - out of scope for this code

select
        id_row
        ,panel_guest_indicator
        ,Household_number
        ,Barb_date_of_activity
        ,Actual_date_of_session
        ,Set_number
        ,Start_time_of_session
        ,End_time_of_session
        ,Duration_of_session
        ,Session_activity_type
        ,Playback_type
        ,pvf.DB1_Station_Code
        ,Viewing_platform
        ,Barb_date_of_recording
        ,Actual_Date_of_Recording
        ,Start_time_of_recording
        ,Interactive_Bar_Code_Identifier
        ,VOD_Indicator
        ,VOD_Provider
        ,VOD_Service
        ,VOD_Type
        ,Device_in_use
        ,broadcast_start_date_time
        ,broadcast_end_date_time
        ,VOD_indicator_consolidated
        ,channel_event_start_date_time
        ,channel_event_end_date_time
        ,case
                when pvf.Start_time_of_session >= sch.broadcast_start_date_time_local then pvf.Start_time_of_session
                else sch.broadcast_start_date_time_local
        end as programme_instance_start_date_time
        ,case
                when pvf.end_time_of_session <= sch.broadcast_end_date_time_local then pvf.end_time_of_session
                else sch.broadcast_end_date_time_local
        end as programme_instance_end_date_time
        ,person_count
        ,Male_4_9
        ,Male_10_15
        ,Male_16_19
        ,Male_20_24
        ,Male_25_34
        ,Male_35_44
        ,Male_45_64
        ,Male_65
        ,Female_4_9
        ,Female_10_15
        ,Female_16_19
        ,Female_20_24
        ,Female_25_34
        ,Female_35_44
        ,Female_45_64
        ,Female_65
        ,sch.broadcast_start_date_time_local as broadcast_start_time_local
        ,sch.broadcast_end_date_time_local as broadcast_end_time_local
        ,cm.service_key
        ,sch.channel_name
        ,sch.programme_name
        ,sch.genre_description
        ,sch.sub_genre_description
        ,sch.broadcast_daypart
        ,sch.service_type_description
into BARB_Viewing_panel_guest_programme_instances
from
        BARB_Viewing_panel_guest pvf
     inner join
        BARB_Channel_Map cm
     on pvf.db1_station_code = cm.db1_station_code
     inner join
        sk_prod.VESPA_PROGRAMME_SCHEDULE sch
     on cm.service_key = sch.service_key
     and pvf.broadcast_start_date_time <= sch.broadcast_end_date_time_local
     and pvf.broadcast_end_date_time >= sch.broadcast_start_date_time_local
where
        cm.main_sk = 'Y'



----- Not all events will match agaist the programme schedule
---- See QA below for more details
---- Append these to the programme instance table so have complete set of data

insert into BARB_Viewing_panel_guest_programme_instances
select
        v.id_row
        ,v.panel_guest_indicator
        ,v.Household_number
        ,v.Barb_date_of_activity
        ,v.Actual_date_of_session
        ,v.Set_number
        ,v.Start_time_of_session
        ,v.End_time_of_session
        ,v.Duration_of_session
        ,v.Session_activity_type
        ,v.Playback_type
        ,v.DB1_Station_Code
        ,v.Viewing_platform
        ,v.Barb_date_of_recording
        ,v.Actual_Date_of_Recording
        ,v.Start_time_of_recording
        ,v.Interactive_Bar_Code_Identifier
        ,v.VOD_Indicator
        ,v.VOD_Provider
        ,v.VOD_Service
        ,v.VOD_Type
        ,v.Device_in_use
        ,v.broadcast_start_date_time
        ,v.broadcast_end_date_time
        ,v.VOD_indicator_consolidated
        ,v.channel_event_start_date_time
        ,v.channel_event_end_date_time
        ,'1970-01-01 23:59:59'
        ,'1970-01-01 23:59:59'
        ,v.person_count
        ,v.Male_4_9
        ,v.Male_10_15
        ,v.Male_16_19
        ,v.Male_20_24
        ,v.Male_25_34
        ,v.Male_35_44
        ,v.Male_45_64
        ,v.Male_65
        ,v.Female_4_9
        ,v.Female_10_15
        ,v.Female_16_19
        ,v.Female_20_24
        ,v.Female_25_34
        ,v.Female_35_44
        ,v.Female_45_64
        ,v.Female_65
        ,'1970-01-01 23:59:59'
        ,'1970-01-01 23:59:59'
        ,0
        ,'na'
        ,'na'
        ,'na'
        ,'na'
        ,'na'
        ,'na'
from
        BARB_Viewing_panel_guest v
    left join
        BARB_Viewing_panel_guest_programme_instances i
    on v.id_row = i.id_row
where
        i.id_row is null



-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
-- C: Some QA Checks
-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------

---- These checks done before the final step above is run!!!!!!


---- Number of sessions in the viewing table BEFORE matching with the programme schedule
select Barb_date_of_activity, count(1) as row_count, count(distinct id_row) as id_count
from BARB_Viewing_panel_guest
group by Barb_date_of_activity
-- Result = 1,583,163


---- After matching with the programme schedule
---- Count of station codes that have some/all rows without a matching programme schedule
select
        v.db1_station_code, count(1)
from
        BARB_Viewing_panel_guest v
    left join
        BARB_Viewing_panel_guest_programme_instances i
    on v.id_row = i.id_row
where
        i.id_row is null
group by
        v.db1_station_code

-- RESULTS
-- About 12% (188,742 from 1,583,163) id_rows don't match
-- Most of these are from DB1_Station_Codes that don't existin the Barb Master file
-- So they don't exist in the Channel Mapping table because we don't know what they are
-- Numbers are:
        -- db1 = 9002; 68,548 id_rows
        -- db1 = 9003; 48,896 id_rows
        -- db1 = 9001; 15,339 id_rows
        -- these account for 132,783 or 8% of the 12%
-- A further 31,596 comes from db1 = 4950 which is 'Other Non Itemised Channels'
-- So again don't know what these are so not in the channel mapping table
-- So thats another 2%




-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
-- D: Grant permissions on tables

-- Tables where raw Barb data imported to
grant all on BARB_Panel_Demographic_Data_Home_Characteristics to angeld
grant all on BARB_Panel_Demographic_Data_TV_Sets_Characteristics to angeld
grant all on BARB_Individual_Panel_Member_Details to angeld
grant all on BARB_Panel_Member_Responses_Weights_and_Viewing_Categories to angeld
grant all on BARB_PVF_Viewing_Record_Panel_Members to angeld
grant all on BARB_PVF_Viewing_Record_Guests to angeld

-- Tables where Barb data has been processed - mainly proper formats for dates/times
grant all on BARB_PVF06_Viewing_Record_Panel_Members to angeld
grant all on BARB_PVF07_Viewing_Record_Guests to angeld
grant all on BARB_PV206_Viewing_Record_Panel_Members to angeld
grant all on BARB_PV207_Viewing_Record_Guests to angeld
grant all on BARB_PVF04_Individual_Member_Details to angeld
grant all on BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories to angeld
grant all on BARB_PVF06_PV206_Viewing_Record_Panel_Members to angeld


-- Key tables output from this Skyview code
grant all on BARB_Channel_Map to angeld
grant all on BARB_Viewing_panel_guest to angeld

-- The main output table from this Skyview code
grant all on BARB_Viewing_panel_guest_programme_instances to angeld



