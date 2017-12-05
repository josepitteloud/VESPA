
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

-- C: Grant permissions on tables

-- Issues/bugs
--      1. In A4, the session end times have been incorrected calculated. This is rectified in B1. Ideally this should be resolved.
--      2. In B2 I have used VOD_indicator as part of the definition an an event.
--              The data suggests this is the case, but not consistent with Barb definition.
--      3. In A4, I have not dealt with the 2 days of the year when clocks change
--      4. In A4, there are comments on potential bug when an event spans midnight and the calculated end date time is incorrect.
--              I believe this is OK.

-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
--      A5: Combine the PVF and PV2 viewing data into the same tables



-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
-- A1: Create tables to hold raw Barb data
-- These follow the spec in the Barb documentation (BARB2010DSP01 Version 3.7C - Panel Viewing File.pdf)
-- Note that Barb generate these tables every day. So to use these tables you need to use the data generated on the same day across them all
---- Details of each Household on the Barb panel. There are about 5000
---- Filename, Household_number are the unique fields


IF OBJECT_ID('barb_daily_monitoring') IS NULL
BEGIN
CREATE TABLE barb_daily_monitoring (
id_row bigint primary key identity  --- so we can check if any viewing records are not matched to the schedule
,date_of_sql_run timestamp default NULL
,date_of_interest date default NULL
,nr_of_PVF_rec_tot int default NULL
,nr_of_PVF_rec_panel_mem int default NULL
,nr_of_PVF_rec_guests int default NULL
,nr_of_PV2_rec_tot int default NULL
,nr_of_PV2_rec_panel_mem int default NULL
,nr_of_PV2_rec_guests int default NULL
,nr_of_PVF_rec_not_matching_VESPA int default NULL
,nr_of_PV2_rec_not_matching_VESPA int default NULL
,nr_of_TOT_rec_not_matching_VESPA int default NULL
,nr_of_indiv_member_details_rec int default NULL
,nr_of_TV_char_details_rec int default NULL
,nr_of_individual_weights_rec int default NULL
,nr_of_records_in_viewing_table int default NULL
)
END
;
create or replace variable @date_of_interest date
;
set @date_of_interest=date('2013-07-01') -- use format date('yyyy-mm-dd')
;
-- create or replace variable @date_of_now date
-- ;
-- set @date_of_now=now() -- use format date('yyyy-mm-dd')
-- ;
create or replace variable @nr_of_PVF_members int
;
create or replace variable @nr_of_PVF_guests int
;
create or replace variable @nr_of_PV2_members int
;
create or replace variable @nr_of_PV2_guests int
;
create or replace variable @current_id_row int
;
create or replace variable @nr_of_PVF_rec_not_matching_VESPA int
;
create or replace variable @nr_of_PV2_rec_not_matching_VESPA int
;
create or replace variable @dummy int
;

insert into barb_daily_monitoring(date_of_sql_run, date_of_interest)
values(now(), @date_of_interest)
;
select @current_id_row=(select max(id_row) from barb_daily_monitoring)
;


IF OBJECT_ID('BARB_Panel_Demographic_Data_Home_Characteristics') IS NULL
BEGIN
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
END
ELSE
BEGIN
truncate table BARB_Panel_Demographic_Data_Home_Characteristics
END
;


---- Details of each TV in each Household
---- Filename, , Household_number, Set_number are unique fields
IF OBJECT_ID('BARB_Panel_Demographic_Data_TV_Sets_Characteristics') IS NULL
BEGIN
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
END
ELSE
BEGIN
truncate table BARB_Panel_Demographic_Data_TV_Sets_Characteristics
END

---- Details of each panel member
---- Filename, Household_number, Person_number are unique fields
IF OBJECT_ID('BARB_Individual_Panel_Member_Details') IS NULL
BEGIN
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
END
ELSE
BEGIN
truncate table BARB_Individual_Panel_Member_Details
END



---- Barb apply weight to each panel member to scale up to UK base
---- Filename, Household_number, Person_number are unique fields
IF OBJECT_ID('BARB_Panel_Member_Responses_Weights_and_Viewing_Categories') IS NULL
BEGIN
CREATE TABLE BARB_Panel_Member_Responses_Weights_and_Viewing_Categories (
file_creation_date date, file_creation_time time, file_type varchar(12), file_version int, filename varchar(13)
,Record_Type int DEFAULT NULL
,Household_Number int DEFAULT NULL
,Person_Number int DEFAULT NULL
,Reporting_Panel_Code int DEFAULT NULL
,Date_of_Activity_DB1 int DEFAULT NULL
,Response_Code int DEFAULT NULL
,Processing_Weight double DEFAULT NULL
,Adults_Commercial_TV_Viewing_Sextile int DEFAULT NULL
,ABC1_Adults_Commercial_TV_Viewing_Sextile int DEFAULT NULL
,Adults_Total_Viewing_Sextile int DEFAULT NULL
,ABC1_Adults_Total_Viewing_Sextile int DEFAULT NULL
,Adults_16_34_Commercial_TV_Viewing_Sextile int DEFAULT NULL
,Adults_16_34_Total_Viewing_Sextile int DEFAULT NULL
)
END
ELSE
BEGIN
truncate table BARB_Panel_Member_Responses_Weights_and_Viewing_Categories
END


---- The Viewing Data. Note that this file only contains Live and VOSDAL
---- PV2 files contain other timeshift events
---- See BARB2010DSP01 Version 3.7C - Panel Viewing File.pdf for more details
---- Filename, Household_number, Set_number,  Start_time_of_session are unique fields
IF OBJECT_ID('BARB_PVF_Viewing_Record_Panel_Members') IS NULL
BEGIN
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
END
ELSE
BEGIN
truncate table BARB_PVF_Viewing_Record_Panel_Members
END



IF OBJECT_ID('ind_household_number') IS NOT NULL
DROP INDEX ind_household_number

create hg index ind_household_number on BARB_PVF_Viewing_Record_Panel_Members(household_number)

IF OBJECT_ID('ind_db1') IS NOT NULL
DROP INDEX ind_db1

create hg index ind_db1 on BARB_PVF_Viewing_Record_Panel_Members(db1_station_code)



---- The Barb data also captures any viewing from guests to a Barb household
---- This is captured seperately in this file. Again this file only contains Live and VOSDAL
---- Filename, Household_number, Set_number,  Start_time_of_session are unique fields
IF OBJECT_ID('BARB_PVF_Viewing_Record_Guests') IS NULL
BEGIN
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
END
ELSE
BEGIN
truncate table BARB_PVF_Viewing_Record_Guests
END



---- PV2 viewing files esentially capture timeshifted 1-28 days
---- These files are available the day after the PVF files which contain Live and VOSDAL
---- The fields are the same as the equivilent PVF files
IF OBJECT_ID('BARB_PV2_Viewing_Record_Panel_Members') IS NULL
BEGIN
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
END
ELSE
BEGIN
truncate table BARB_PV2_Viewing_Record_Panel_Members
END


IF OBJECT_ID('BARB_PV2_Viewing_Record_Panel_Guests') IS NULL
BEGIN
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
END
ELSE
BEGIN
truncate table BARB_PV2_Viewing_Record_Panel_Guests
END

;

-- -- Procedure to clean existing data coming from a certain file Barb PVF files
CREATE OR REPLACE PROCEDURE CLEAN_TABLES (@filename varchar(60)) AS
BEGIN

IF OBJECT_ID('BARB_Panel_Demographic_Data_Home_Characteristics') IS NULL
BEGIN
END

END


;

-- -- Procedure to import the raw Barb PVF files
CREATE OR REPLACE PROCEDURE sp_BARB_PVF (@in_filename varchar(60)) AS
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
IF OBJECT_ID('PI_BARB_import') IS NOT NULL
truncate table PI_BARB_import
ELSE
CREATE TABLE PI_BARB_import(
imported_text varchar(1000)
)


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
,CAST(substr(imported_text,1,2) AS Int) -- file type
,CAST(substr(imported_text,3,7) AS Int) -- household nr
,CAST(substr(imported_text,10,2) AS Int) -- person nr
,CAST(substr(imported_text,12,5) AS Int) -- panel code
,CAST(substr(imported_text,17,8) AS Int) -- date of activity
,CAST(substr(imported_text,25,1) AS Int) -- response code
,(CAST(substr(imported_text,26,3) AS double)*1000.0) + CAST(substr(imported_text,29,3) AS double)+ (CAST(substr(imported_text,32,1) AS double)/10.0)-- processing weight
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
,CAST(substr(imported_text,49,1) AS Int) -- person 1
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
,CAST(substr(imported_text,64,1) AS Int) -- person 16
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


SELECT 'Import PVF complete'

END -- of sp_BARB_PVF procedure

;

---- Procedure to import the raw Barb PV2 files
CREATE OR REPLACE PROCEDURE sp_BARB_PV2 (@in_filename varchar(60)) AS

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
IF OBJECT_ID('PI_BARB_import') IS NOT NULL
truncate table PI_BARB_import
ELSE
CREATE TABLE PI_BARB_import(
imported_text varchar(1000)
)

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


SELECT 'Import PV2 complete'

END

;

--EXEC sp_BARB_PVF 'RipoliLe/B20130916r_25.PVF'
--EXEC sp_BARB_PVF 'RipoliLe/B20130916l.PVF'
--EXEC sp_BARB_PVF 'RipoliLe/B20130916_hh23.PVF'
-- EXEC sp_BARB_PVF 'RipoliLe/B20130916c.PVF'
--EXEC sp_BARB_PVF 'RipoliLe/B20130916.PV2'
EXEC sp_BARB_PVF 'RipoliLe/B20130916.PV2'
;

EXEC sp_BARB_PV2 'RipoliLe/B20130916.PV2'
;

commit

--EXEC sp_BARB_PV2 'RipoliLe/B20130916.PV2'

;
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


IF OBJECT_ID('BARB_PVF06_Viewing_Record_Panel_Members') IS NULL
BEGIN
CREATE TABLE BARB_PVF06_Viewing_Record_Panel_Members (
id_row bigint primary key identity  --- so we can check if any viewing records are not matched to the schedule
,Sky_STB_viewing varchar(1) DEFAULT NULL
,Sky_STB_holder_hh varchar(1) DEFAULT NULL
,PVF_PV2 varchar(4) DEFAULT NULL
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
END
ELSE
BEGIN
truncate table BARB_PVF06_Viewing_Record_Panel_Members
END


IF OBJECT_ID('ind_household_number_PVF06') IS NOT NULL
DROP INDEX ind_household_number_PVF06
;
create hg index ind_household_number_PVF06 on BARB_PVF06_Viewing_Record_Panel_Members(household_number) --- check this
;
IF OBJECT_ID('ind_db1_PVF06') IS NOT NULL
DROP INDEX ind_db1_PVF06

create hg index ind_db1_PVF06 on BARB_PVF06_Viewing_Record_Panel_Members(db1_station_code)

IF OBJECT_ID('ind_start_PVF06') IS NOT NULL
DROP INDEX ind_start_PVF06
create hg index ind_start_PVF06 on BARB_PVF06_Viewing_Record_Panel_Members(Start_time_of_session)

IF OBJECT_ID('ind_end_PVF06') IS NOT NULL
DROP INDEX ind_end_PVF06
create hg index ind_end_PVF06 on BARB_PVF06_Viewing_Record_Panel_Members(End_time_of_session)

IF OBJECT_ID('ind_date_PVF06') IS NOT NULL
DROP INDEX ind_date_PVF06
create hg index ind_date_PVF06 on BARB_PVF06_Viewing_Record_Panel_Members(Barb_date_of_activity)


IF OBJECT_ID('BARB_PVF07_Viewing_Record_Guests') IS NULL
BEGIN
CREATE TABLE BARB_PVF07_Viewing_Record_Guests (
id_row bigint primary key identity  --- so we can check if any viewing records are not matched to the schedule
,PVF_PV2 varchar(4) DEFAULT NULL
,Sky_STB_viewing varchar(1) DEFAULT NULL
,Sky_STB_holder_hh varchar(1) DEFAULT NULL
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
END
ELSE
BEGIN
truncate table BARB_PVF07_Viewing_Record_Guests
END


IF OBJECT_ID('ind_household_number_PVF07') IS NOT NULL
DROP INDEX ind_household_number_PVF07
create hg index ind_household_number_PVF07 on BARB_PVF07_Viewing_Record_Guests(Household_number)

IF OBJECT_ID('ind_date_PVF06') IS NOT NULL
DROP INDEX ind_start_PVF07
create hg index ind_start_PVF07 on BARB_PVF07_Viewing_Record_Guests(Start_time_of_session)

IF OBJECT_ID('ind_date_PVF06') IS NOT NULL
DROP INDEX ind_end_PVF07
create hg index ind_end_PVF07 on BARB_PVF07_Viewing_Record_Guests(End_time_of_session)



IF OBJECT_ID('BARB_PVF04_Individual_Member_Details') IS NULL
BEGIN
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
END
ELSE
BEGIN
truncate table BARB_PVF04_Individual_Member_Details
END

IF OBJECT_ID('ind_household_number_PVF04') IS NOT NULL
DROP INDEX ind_household_number_PVF04
create hg index ind_household_number_PVF04 on BARB_PVF04_Individual_Member_Details(Household_number)

IF OBJECT_ID('ind_person_PVF04') IS NOT NULL
DROP INDEX ind_person_PVF04
create lf index ind_person_PVF04 on BARB_PVF04_Individual_Member_Details(person_number)

IF OBJECT_ID('ind_create_PVF04') IS NOT NULL
DROP INDEX ind_create_PVF04
create lf index ind_create_PVF04 on BARB_PVF04_Individual_Member_Details(file_creation_date)



IF OBJECT_ID('BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories') IS NULL
BEGIN
CREATE TABLE BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories (
file_creation_date date, file_creation_time time, file_type varchar(12), file_version int, filename varchar(13)
,Record_Type int DEFAULT NULL
,Household_Number int DEFAULT NULL
,Person_Number int DEFAULT NULL
,Reporting_Panel_Code int DEFAULT NULL
,Date_of_Activity_DB1 date
,Response_Code int DEFAULT NULL
,Processing_Weight double DEFAULT NULL
,Adults_Commercial_TV_Viewing_Sextile int DEFAULT NULL
,ABC1_Adults_Commercial_TV_Viewing_Sextile int DEFAULT NULL
,Adults_Total_Viewing_Sextile int DEFAULT NULL
,ABC1_Adults_Total_Viewing_Sextile int DEFAULT NULL
,Adults_16_34_Commercial_TV_Viewing_Sextile int DEFAULT NULL
,Adults_16_34_Total_Viewing_Sextile int DEFAULT NULL
)
END
ELSE
BEGIN
truncate table BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories
END

IF OBJECT_ID('ind_household_number_PVF05') IS NOT NULL
DROP INDEX ind_household_number_PVF05
create hg index ind_household_number_PVF05 on BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories(Household_Number)

IF OBJECT_ID('ind_person_PVF05') IS NOT NULL
DROP INDEX ind_person_PVF05
create lf index ind_person_PVF05 on BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories(Person_Number)

IF OBJECT_ID('ind_panel_PVF05') IS NOT NULL
DROP INDEX ind_panel_PVF05
create lf index ind_panel_PVF05 on BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories(Reporting_Panel_Code)

IF OBJECT_ID('ind_date_PVF05') IS NOT NULL
DROP INDEX ind_date_PVF05
create lf index ind_date_PVF05 on BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories(Date_of_Activity_DB1)



IF OBJECT_ID('BARB_PV206_Viewing_Record_Panel_Members') IS NULL
BEGIN
CREATE TABLE BARB_PV206_Viewing_Record_Panel_Members (
id_row bigint primary key identity  --- so we can check if any viewing records are not matched to the schedule
,Sky_STB_viewing varchar(1) DEFAULT NULL
,Sky_STB_holder_hh varchar(1) DEFAULT NULL
,PVF_PV2 varchar(4) DEFAULT NULL
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
END
ELSE
BEGIN
truncate table BARB_PV206_Viewing_Record_Panel_Members
END


IF OBJECT_ID('ind_household_number_PV206') IS NOT NULL
DROP INDEX ind_household_number_PV206
create hg index ind_household_number_PV206 on BARB_PV206_Viewing_Record_Panel_Members(household_number)

IF OBJECT_ID('ind_db1_PV206') IS NOT NULL
DROP INDEX ind_db1_PV206
create hg index ind_db1_PV206 on BARB_PV206_Viewing_Record_Panel_Members(db1_station_code)

IF OBJECT_ID('ind_start_PV206') IS NOT NULL
DROP INDEX ind_start_PV206
create hg index ind_start_PV206 on BARB_PV206_Viewing_Record_Panel_Members(Start_time_of_session)

IF OBJECT_ID('ind_end_PV206') IS NOT NULL
DROP INDEX ind_end_PV206
create hg index ind_end_PV206 on BARB_PV206_Viewing_Record_Panel_Members(End_time_of_session)



IF OBJECT_ID('BARB_PV207_Viewing_Record_Guests') IS NULL
BEGIN
CREATE TABLE BARB_PV207_Viewing_Record_Guests (
id_row bigint primary key identity  --- so we can check if any viewing records are not matched to the schedule
,PVF_PV2 varchar(4) DEFAULT NULL
,Sky_STB_viewing varchar(1) DEFAULT NULL
,Sky_STB_holder_hh varchar(1) DEFAULT NULL
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
END
ELSE
BEGIN
truncate table BARB_PV207_Viewing_Record_Guests
END


;

IF OBJECT_ID('ind_household_number_PV207') IS NOT NULL
DROP INDEX ind_household_number_PV207

;
create hg index ind_household_number_PV207 on BARB_PV207_Viewing_Record_Guests(Household_number)

IF OBJECT_ID('ind_start_PV207') IS NOT NULL
DROP INDEX ind_start_PV207

;
create hg index ind_start_PV207 on BARB_PV207_Viewing_Record_Guests(Start_time_of_session)

IF OBJECT_ID('ind__end_PV207') IS NOT NULL
DROP INDEX ind__end_PV207
;
create hg index ind__end_PV207 on BARB_PV207_Viewing_Record_Guests(End_time_of_session)

;

commit

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
---------------------------------------------------------------
----------------------------------------
-- Insert into BARB_PVF06_Viewing_Record_Panel_Members
----------------------------------------
insert into BARB_PVF06_Viewing_Record_Panel_Members
(PVF_PV2,file_creation_date, file_creation_time, file_type, file_version, filename
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
        'PVF',file_creation_date, file_creation_time, file_type, file_version, filename
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


;

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

-- commit;

;

update BARB_PVF06_Viewing_Record_Panel_Members
        set End_time_of_session = dateadd(mi, Duration_of_session-1, Start_time_of_session)-- the -1 is for BARB policy that the same minute cannot be assigned to different events, so if ev start 19:40 and viewed for 21 minutes, end will be at 20:00, and following event will start at 20:01

;

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

;

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

;
set @dummy = @@rowcount

update barb_daily_monitoring
set nr_of_indiv_member_details_rec = @dummy
where id_row=@current_id_row

;

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
        ,Processing_Weight
        ,Adults_Commercial_TV_Viewing_Sextile
        ,ABC1_Adults_Commercial_TV_Viewing_Sextile
        ,Adults_Total_Viewing_Sextile
        ,ABC1_Adults_Total_Viewing_Sextile
        ,Adults_16_34_Commercial_TV_Viewing_Sextile
        ,Adults_16_34_Total_Viewing_Sextile
from BARB_Panel_Member_Responses_Weights_and_Viewing_Categories
-- Leo, join here for the weigh of panel 50 for each individual

;
set @dummy = @@rowcount

update barb_daily_monitoring
set nr_of_individual_weights_rec=@dummy
where id_row=@current_id_row

;

----------------------------------------
-- Insert into BARB_PVF07_Viewing_Record_Guests
----------------------------------------
insert into BARB_PVF07_Viewing_Record_Guests
(PVF_PV2,file_creation_date, file_creation_time, file_type, file_version, filename
,Record_type, Household_number, Barb_date_of_activity, Actual_date_of_session, Set_number
,Start_time_of_session_text, Start_time_of_session, End_time_of_session, Duration_of_session, Session_activity_type
,Playback_type, DB1_Station_Code, Viewing_platform, Barb_date_of_recording, Actual_Date_of_Recording
,Start_time_of_recording_text, Start_time_of_recording
,Male_4_9, Male_10_15, Male_16_19, Male_20_24, Male_25_34, Male_35_44, Male_45_64, Male_65
,Female_4_9, Female_10_15, Female_16_19, Female_20_24, Female_25_34, Female_35_44, Female_45_64, Female_65
,Interactive_Bar_Code_Identifier, VOD_Indicator, VOD_Provider, VOD_Service
,VOD_Type, Device_in_use)
select
        'PVF',file_creation_date, file_creation_time, file_type, file_version, filename
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


;


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


;

update BARB_PVF07_Viewing_Record_Guests
        set End_time_of_session = dateadd(mi, Duration_of_session-1, Start_time_of_session)-- the -1 is for BARB policy that the same minute cannot be assigned to different events, so if ev start 19:40 and viewed for 21 minutes, end will be at 20:00, and following event will start at 20:01



;

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


;


----------------------------------------
-- Insert data into BARB_PV206_Viewing_Record_Panel_Members
----------------------------------------
insert into BARB_PV206_Viewing_Record_Panel_Members
(PVF_PV2,file_creation_date, file_creation_time, file_type, file_version, filename
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
        'PV2',file_creation_date, file_creation_time, file_type, file_version, filename
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


;

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


;

update BARB_PV206_Viewing_Record_Panel_Members
        set End_time_of_session = dateadd(mi, Duration_of_session-1, Start_time_of_session)-- the -1 is for BARB policy that the same minute cannot be assigned to different events, so if ev start 19:40 and viewed for 21 minutes, end will be at 20:00, and following event will start at 20:01


;

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


;

----------------------------------------
-- Insert into BARB_PV207_Viewing_Record_Guests
----------------------------------------
insert into BARB_PV207_Viewing_Record_Guests
(PVF_PV2,file_creation_date, file_creation_time, file_type, file_version, filename
,Record_type, Household_number, Barb_date_of_activity, Actual_date_of_session, Set_number
,Start_time_of_session_text, Start_time_of_session, End_time_of_session, Duration_of_session, Session_activity_type
,Playback_type, DB1_Station_Code, Viewing_platform, Barb_date_of_recording, Actual_Date_of_Recording
,Start_time_of_recording_text, Start_time_of_recording
,Male_4_9, Male_10_15, Male_16_19, Male_20_24, Male_25_34, Male_35_44, Male_45_64, Male_65
,Female_4_9, Female_10_15, Female_16_19, Female_20_24, Female_25_34, Female_35_44, Female_45_64, Female_65
,Interactive_Bar_Code_Identifier, VOD_Indicator, VOD_Provider, VOD_Service
,VOD_Type, Device_in_use)
select
        'PV2',file_creation_date, file_creation_time, file_type, file_version, filename
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


;

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


;

update BARB_PV207_Viewing_Record_Guests
        set End_time_of_session = dateadd(mi, Duration_of_session-1, Start_time_of_session)-- the -1 is for BARB policy that the same minute cannot be assigned to different events, so if ev start 19:40 and viewed for 21 minutes, end will be at 20:00, and following event will start at 20:01


;

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
--      A4.1: Update the Sky_STB_viewing field (viewing data done using a Sky STB)
--      plus update the Sky_STB_holder_hh field, which indicates hether the household has a Sky STB
-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------

IF OBJECT_ID('TV_char_tmp_table') IS NOT NULL
DROP TABLE TV_char_tmp_table

select household_number, set_number, max(reception_capability_code1) as reception_capability_code1
into TV_char_tmp_table
from BARB_Panel_Demographic_Data_TV_Sets_Characteristics
group by household_number, set_number
;


set @dummy = @@rowcount

update barb_daily_monitoring
set nr_of_TV_char_details_rec=@dummy
where id_row=@current_id_row


;

update BARB_PVF06_Viewing_Record_Panel_Members
set Sky_STB_viewing=(case when TV_char.reception_capability_code1=2 then 'Y' else 'N' end)
from
BARB_PVF06_Viewing_Record_Panel_Members view_t
left join
TV_char_tmp_table as TV_char
on view_t.household_number=TV_char.household_number
and view_t.set_number=TV_char.set_number

;

update BARB_PVF07_Viewing_Record_Guests
set Sky_STB_viewing=(case when TV_char.reception_capability_code1=2 then 'Y' else 'N' end)
from
BARB_PVF07_Viewing_Record_Guests view_t
left join
TV_char_tmp_table as TV_char
on view_t.household_number=TV_char.household_number
and view_t.set_number=TV_char.set_number

;

update BARB_PV206_Viewing_Record_Panel_Members
set Sky_STB_viewing=(case when TV_char.reception_capability_code1=2 then 'Y' else 'N' end)
from
BARB_PV206_Viewing_Record_Panel_Members view_t
left join
TV_char_tmp_table as TV_char
on view_t.household_number=TV_char.household_number
and view_t.set_number=TV_char.set_number

;

update BARB_PV207_Viewing_Record_Guests
set Sky_STB_viewing=(case when TV_char.reception_capability_code1=2 then 'Y' else 'N' end)
from
BARB_PV207_Viewing_Record_Guests view_t
left join
TV_char_tmp_table as TV_char
on view_t.household_number=TV_char.household_number
and view_t.set_number=TV_char.set_number

;

IF OBJECT_ID('Sky_STB_holder_hh_tmp_table') IS NOT NULL
DROP TABLE Sky_STB_holder_hh_tmp_table

CREATE TABLE Sky_STB_holder_hh_tmp_table
(
  household_number INT DEFAULT NULL
)

insert into Sky_STB_holder_hh_tmp_table
select distinct household_number
from TV_char_tmp_table
where reception_capability_code1=2
;

update BARB_PVF06_Viewing_Record_Panel_Members
set Sky_STB_holder_hh=(case when TV_char.household_number is NULL then 'N' else 'Y' end)
from
BARB_PVF06_Viewing_Record_Panel_Members view_t
left join
Sky_STB_holder_hh_tmp_table as TV_char
on view_t.household_number=TV_char.household_number

;

update BARB_PVF07_Viewing_Record_Guests
set Sky_STB_holder_hh=(case when TV_char.household_number is NULL then 'N' else 'Y' end)
from
BARB_PVF07_Viewing_Record_Guests view_t
left join
Sky_STB_holder_hh_tmp_table as TV_char
on view_t.household_number=TV_char.household_number

;

update BARB_PV206_Viewing_Record_Panel_Members
set Sky_STB_holder_hh=(case when TV_char.household_number is NULL then 'N' else 'Y' end)
from
BARB_PV206_Viewing_Record_Panel_Members view_t
left join
Sky_STB_holder_hh_tmp_table as TV_char
on view_t.household_number=TV_char.household_number

;

update BARB_PV207_Viewing_Record_Guests
set Sky_STB_holder_hh=(case when TV_char.household_number is NULL then 'N' else 'Y' end)
from
BARB_PV207_Viewing_Record_Guests view_t
left join
Sky_STB_holder_hh_tmp_table as TV_char
on view_t.household_number=TV_char.household_number

;


/*
update BARB_PV206_Viewing_Record_Panel_Members
set Sky_STB_viewing=(case when  reception_capability_code=2 then 'Y' else 'N' end
from
BARB_PV206_Viewing_Record_Panel_Members view_t
left join
(select household_number, max(set_number) as set_number, max(reception_capability_code1) as reception_capability_code1
from BARB_Panel_Demographic_Data_TV_Sets_Characteristics
group by household_number, set_number, reception_capability_code1) as TV_char
on view_t.household_number=TV_char.household_number
and
*/
/*
-- update the channel pack
update BARB_viewing_table
set channel_pack = map.channel_pack
from
BARB_viewing_table view_t
left join
(
select service_key, max(channel_pack) as channel_pack
from
vespa_Analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES
group by service_key
) map
on view_t.service_key = map.service_key
*/

-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
--      A5: Combine the PVF and PV2 viewing data into the same tables
-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------

-- Combine Panel viewing
IF OBJECT_ID('BARB_PVF06_PV206_Viewing_Record_Panel_Members') IS NOT NULL
DROP TABLE BARB_PVF06_PV206_Viewing_Record_Panel_Members

;

select * into BARB_PVF06_PV206_Viewing_Record_Panel_Members from BARB_PVF06_Viewing_Record_Panel_Members
;
set @nr_of_PVF_members = @@rowcount

insert into BARB_PVF06_PV206_Viewing_Record_Panel_Members
select * from BARB_PV206_Viewing_Record_Panel_Members

;
set @nr_of_PV2_members  = @@rowcount

COMMIT

;

-- Combine Guest viewing
IF OBJECT_ID('BARB_PVF07_PV207_Viewing_Record_Guests') IS NOT NULL
DROP TABLE BARB_PVF07_PV207_Viewing_Record_Guests

;

select * into BARB_PVF07_PV207_Viewing_Record_Guests from BARB_PVF07_Viewing_Record_Guests
;
set @nr_of_PVF_guests = @@rowcount

;

insert into BARB_PVF07_PV207_Viewing_Record_Guests
select * from BARB_PV207_Viewing_Record_Guests
;
set @nr_of_PV2_guests = @@rowcount
;

COMMIT

update barb_daily_monitoring
set nr_of_PVF_rec_tot=@nr_of_PVF_members+@nr_of_PVF_guests
,nr_of_PVF_rec_panel_mem=@nr_of_PVF_members
,nr_of_PVF_rec_guests=@nr_of_PVF_guests
,nr_of_PV2_rec_tot=@nr_of_PV2_members+@nr_of_PV2_guests
,nr_of_PV2_rec_panel_mem=@nr_of_PV2_members
,nr_of_PV2_rec_guests=@nr_of_PV2_guests
where id_row=@current_id_row

;

-- BARB_table_output_1 is the output table

IF OBJECT_ID('BARB_table_output_1') IS not NULL
DROP TABLE BARB_table_output_1

;

create table BARB_table_output_1
(
filename varchar(100)
,Sky_STB_viewing varchar(1) DEFAULT NULL
,Sky_STB_holder_hh varchar(1) DEFAULT NULL
,PVF_PV2  varchar(4) DEFAULT NULL
,Household_number int DEFAULT NULL
,Barb_date_of_activity_DB1 date
-- ,Actual_date
,Set_number int DEFAULT NULL
,BARB_Start_Time_of_Session  int default NULL
,Local_Start_Time_of_Session timestamp default NULL
,Local_End_Time_of_Session timestamp default NULL
,Panel_or_guest_flag varchar(8)
,Duration_of_Session int DEFAULT NULL -- minutes
,Session_activity_type int DEFAULT NULL
,Playback_type varchar(1) DEFAULT NULL
,DB1_Station_Code int DEFAULT NULL
,DB1_Station_Name varchar(100)
,Viewing_platform int DEFAULT NULL
,Barb_date_of_recording_DB1 date
-- ,Actual_Date_of_Recording date --- change datatype from Barb
,BARB_Start_time_of_recording int default NULL
,Local_Start_time_of_recording timestamp default NULL
,Local_End_time_of_recording timestamp default NULL
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
,Weigthed_Male_4_9 double DEFAULT NULL
,Weigthed_Male_10_15 double DEFAULT NULL
,Weigthed_Male_16_19 double DEFAULT NULL
,Weigthed_Male_20_24 double DEFAULT NULL
,Weigthed_Male_25_34 double DEFAULT NULL
,Weigthed_Male_35_44 double DEFAULT NULL
,Weigthed_Male_45_64 double DEFAULT NULL
,Weigthed_Male_65 double DEFAULT NULL
,Weigthed_Female_4_9 double DEFAULT NULL
,Weigthed_Female_10_15 double DEFAULT NULL
,Weigthed_Female_16_19 double DEFAULT NULL
,Weigthed_Female_20_24 double DEFAULT NULL
,Weigthed_Female_25_34 double DEFAULT NULL
,Weigthed_Female_35_44 double DEFAULT NULL
,Weigthed_Female_45_64 double DEFAULT NULL
,Weigthed_Female_65 double DEFAULT NULL
,total_people_viewing int DEFAULT NULL
,weighted_total_people_viewing double DEFAULT NULL
,VOD_Indicator int DEFAULT NULL
,VOD_Provider int DEFAULT NULL
,VOD_Service int DEFAULT NULL
,VOD_Type int DEFAULT NULL
,Device_in_use int DEFAULT NULL
-- ,TV_Instance_Start_Date_Time timestamp
-- ,TV_Instance_End_Date_Time timestamp
--,TV_Event_Start_Date_Time timestamp default NULL
--,TV_Event_End_Date_Time timestamp default NULL
,Household_Weight double DEFAULT NULL
,Service_Key  int DEFAULT NULL
,Channel_Name varchar(100)
,cb_row_id bigint DEFAULT NULL
--,row_id bigint primary key identity
/*
1647
*/

)

-- END

;


IF OBJECT_ID('ind_household_number_barb_output_table') IS NOT NULL
DROP INDEX ind_household_number_barb_output_table
;
create hg index ind_household_number_barb_output_table on BARB_table_output_1(Household_number)

;

IF OBJECT_ID('ind_filename_barb_output_table') IS NOT NULL
DROP INDEX ind_filename_barb_output_table
;
create hg index ind_filename_barb_output_table on BARB_table_output_1(filename)

;

-- insert guest viewing info first (age/gender info already available)
insert into BARB_table_output_1(PVF_PV2,Sky_STB_viewing,Sky_STB_holder_hh,Household_number,filename,Viewing_platform,Barb_date_of_recording_DB1,BARB_Start_time_of_recording,Local_Start_time_of_recording, Barb_date_of_activity_DB1,Set_number, BARB_Start_Time_of_Session, Local_Start_Time_of_Session, Local_End_Time_of_Session, Panel_or_guest_flag, Duration_of_Session, Session_activity_type, Playback_type,DB1_Station_Code
,Male_4_9,Male_10_15,Male_16_19,Male_20_24,Male_25_34,Male_35_44,Male_45_64,Male_65,Female_4_9,Female_10_15,Female_16_19,Female_20_24,Female_25_34,Female_35_44,Female_45_64,Female_65
,total_people_viewing
--,TV_Event_Start_Date_Time
--,TV_Event_End_Date_Time
,VOD_Indicator,VOD_Provider,VOD_Service,VOD_Type,Device_in_use
)
select PVF_PV2,Sky_STB_viewing,Sky_STB_holder_hh,Household_number,filename,Viewing_platform,Barb_date_of_recording, cast(Start_time_of_recording_text as int), Start_time_of_recording, Barb_date_of_activity,Set_number,cast(Start_time_of_session_text as int) , Start_Time_of_Session, End_Time_of_Session, 'Guest', Duration_of_Session, Session_activity_type, Playback_type,DB1_Station_Code
,Male_4_9,Male_10_15,Male_16_19,Male_20_24,Male_25_34,Male_35_44,Male_45_64,Male_65,Female_4_9,Female_10_15,Female_16_19,Female_20_24,Female_25_34,Female_35_44,Female_45_64,Female_65
,(Male_4_9+Male_10_15+Male_16_19+Male_20_24+Male_25_34+Male_35_44+Male_45_64+Male_65+Female_4_9+Female_10_15+Female_16_19+Female_20_24+Female_25_34+Female_35_44+Female_45_64+Female_65)
--,Start_Time_of_Session
--,End_time_of_session
,VOD_Indicator,VOD_Provider,VOD_Service,VOD_Type,Device_in_use
from BARB_PVF07_PV207_Viewing_Record_Guests

;

-- insert panel viewing, info on age/gender groups will be updated further down with function proc_BARB_update_fields

insert into BARB_table_output_1(PVF_PV2,Sky_STB_viewing,Sky_STB_holder_hh,Household_number,filename,Viewing_platform,Barb_date_of_recording_DB1,BARB_Start_time_of_recording,Local_Start_time_of_recording, Barb_date_of_activity_DB1,Set_number, BARB_Start_Time_of_Session, Local_Start_Time_of_Session, Local_End_Time_of_Session, Panel_or_guest_flag, Duration_of_Session, Session_activity_type, Playback_type,DB1_Station_Code
,Person_1_viewing,Person_2_viewing,Person_3_viewing,Person_4_viewing,Person_5_viewing,Person_6_viewing,Person_7_viewing,Person_8_viewing,Person_9_viewing,Person_10_viewing,Person_11_viewing,Person_12_viewing,Person_13_viewing,Person_14_viewing,Person_15_viewing,Person_16_viewing
,total_people_viewing
--,TV_Event_Start_Date_Time
--,TV_Event_End_Date_Time
,VOD_Indicator,VOD_Provider,VOD_Service,VOD_Type,Device_in_use
)
select PVF_PV2,Sky_STB_viewing,Sky_STB_holder_hh,Household_number,filename,Viewing_platform,Barb_date_of_recording, cast(Start_time_of_recording_text as int),Start_time_of_recording, Barb_date_of_activity,Set_number,cast(Start_time_of_session_text as int), Start_Time_of_Session, End_Time_of_Session, 'Panel', Duration_of_Session, Session_activity_type, Playback_type,DB1_Station_Code
,Person_1_viewing,Person_2_viewing,Person_3_viewing,Person_4_viewing,Person_5_viewing,Person_6_viewing,Person_7_viewing,Person_8_viewing,Person_9_viewing,Person_10_viewing,Person_11_viewing,Person_12_viewing,Person_13_viewing,Person_14_viewing,Person_15_viewing,Person_16_viewing
,(Person_1_viewing+Person_2_viewing+Person_3_viewing+Person_4_viewing+Person_5_viewing+Person_6_viewing+Person_7_viewing+Person_8_viewing+Person_9_viewing+Person_10_viewing+Person_11_viewing+Person_12_viewing+Person_13_viewing+Person_14_viewing+Person_15_viewing+Person_16_viewing)
--,Start_Time_of_Session
--,End_time_of_session
,VOD_Indicator,VOD_Provider,VOD_Service,VOD_Type,Device_in_use
from BARB_PVF06_PV206_Viewing_Record_Panel_Members

;

-- here we save in a temp table the date range of viewing-----------------------------------------------------------------------------
IF OBJECT_ID('local_time_range') IS not NULL
DROP TABLE local_time_range

select min(Local_Start_Time_of_Session) as min_local_session_timestamp
,CAST(NULL as datetime) as min_local_recording_timestamp -- to be updated below
,CAST(NULL as datetime) as min_local_timestamp -- to be updated below: this contains the minimum between min_local_recording_timestamp and min_local_session_timestamp
,max(Local_End_Time_of_Session) as max_local_timestamp -- it comes of course by the max session timestamp
into local_time_range
from BARB_table_output_1

;

-- here update minimum recording timestamp (we use the coalesce function because there might be the case of no recording)
update local_time_range
set min_local_recording_timestamp = (select min(Local_Start_time_of_recording) from BARB_table_output_1 where Local_Start_time_of_recording is not NULL)

update local_time_range
set min_local_recording_timestamp = min_local_session_timestamp
where min_local_recording_timestamp is NULL

;

update local_time_range
set min_local_timestamp = (case when min_local_recording_timestamp <= min_local_session_timestamp then min_local_recording_timestamp else min_local_session_timestamp end)

;
 -- up to here ok, but check when recording time is null ( so no recording in the whole file)
-- here we save in a temp table the date range of viewing-----------------------------------------------------------------------------
IF OBJECT_ID('local_UTC_conversion_table') IS not NULL
DROP TABLE local_UTC_conversion_table

select utc_day_date,utc_time_hours,local_day_date,local_time_hours,daylight_savings_flag
into local_UTC_conversion_table
from sk_prod.VESPA_CALENDAR
where local_day_date between (select date(min_local_timestamp) from local_time_range) and (select date(max_local_timestamp) from local_time_range)

;

-- -- Procedure to update the gender/age groups, both weighted (panel 50) and normal, from the person_viewing information
CREATE OR REPLACE PROCEDURE proc_BARB_update_fields (@table_to_update varchar(60)) AS
BEGIN

DECLARE @query varchar(10000)
DECLARE @cntr_people integer
DECLARE @cntr_people_string varchar(5)


SET @cntr_people = 1 -- restart the people counter, done outside @cntr_people loop

WHILE @cntr_people <= 16
 BEGIN

SET @cntr_people_string=CAST(@cntr_people as varchar(4))

SET @query = 'update ' || @table_to_update || ' pvf1'
SET @query = @query || ' set male_4_9=coalesce( pvf1.male_4_9,0)+(case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 9) then 1 else 0 end)'
SET @query = @query || ' ,Weigthed_Male_4_9=coalesce( pvf1.Weigthed_Male_4_9,0)+(case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 9) then 1.0 else 0 end)*coalesce(mem.processing_weight, 0)'
SET @query = @query || ' ,Male_10_15=coalesce( pvf1.Male_10_15,0)+(case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 15) then 1 else 0 end)'
SET @query = @query || ' ,Weigthed_Male_10_15=coalesce( pvf1.Weigthed_Male_10_15,0)+(case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 15) then 1.0 else 0 end)*coalesce(mem.processing_weight, 0)'
SET @query = @query || ' ,Male_16_19=coalesce( pvf1.Male_16_19,0)+(case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 19) then 1 else 0 end)'
SET @query = @query || ' ,Weigthed_Male_16_19=coalesce( pvf1.Weigthed_Male_16_19,0)+(case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 19) then 1.0 else 0 end)*coalesce(mem.processing_weight, 0)'
SET @query = @query || ' ,Male_20_24=coalesce( pvf1.Male_20_24,0)+(case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 24) then 1 else 0 end)'
SET @query = @query || ' ,Weigthed_Male_20_24=coalesce( pvf1.Weigthed_Male_20_24,0)+(case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 24) then 1.0 else 0 end)*coalesce(mem.processing_weight, 0)'
SET @query = @query || ' ,Male_25_34=coalesce( pvf1.male_25_34,0)+(case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 34) then 1 else 0 end)'
SET @query = @query || ' ,Weigthed_Male_25_34=coalesce( pvf1.Weigthed_Male_25_34,0)+(case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 34) then 1.0 else 0 end)*coalesce(mem.processing_weight, 0)'
SET @query = @query || ' ,Male_35_44=coalesce( pvf1.Male_35_44,0)+(case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 44) then 1 else 0 end)'
SET @query = @query || ' ,Weigthed_Male_35_44=coalesce( pvf1.Weigthed_Male_35_44,0)+(case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 44) then 1.0 else 0 end)*coalesce(mem.processing_weight, 0)'
SET @query = @query || ' ,Male_45_64=coalesce( pvf1.Male_45_64,0)+(case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 64) then 1 else 0 end)'
SET @query = @query || ' ,Weigthed_Male_45_64=coalesce( pvf1.Weigthed_Male_45_64,0)+(case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 64) then 1.0 else 0 end)*coalesce(mem.processing_weight, 0)'
SET @query = @query || ' ,Male_65=coalesce( pvf1.Male_65,0)+(case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 65) then 1 else 0 end)'
SET @query = @query || ' ,Weigthed_Male_65=coalesce( pvf1.Weigthed_Male_65,0)+(case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 65) then 1.0 else 0 end)*coalesce(mem.processing_weight, 0)'
SET @query = @query || ' ,female_4_9=coalesce( pvf1.female_4_9,0)+(case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 9) then 1 else 0 end)'
SET @query = @query || ' ,Weigthed_Female_4_9=coalesce( pvf1.Weigthed_Female_4_9,0)+(case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 9) then 1.0 else 0 end)*coalesce(mem.processing_weight, 0)'
SET @query = @query || ' ,female_10_15=coalesce( pvf1.female_10_15,0)+(case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 15) then 1 else 0 end)'
SET @query = @query || ' ,Weigthed_Female_10_15=coalesce( pvf1.Weigthed_Female_10_15,0)+(case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 15) then 1.0 else 0 end)*coalesce(mem.processing_weight, 0)'
SET @query = @query || ' ,female_16_19=coalesce( pvf1.female_16_19,0)+(case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 19) then 1 else 0 end)'
SET @query = @query || ' ,Weigthed_Female_16_19=coalesce( pvf1.Weigthed_Female_16_19,0)+(case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 19) then 1.0 else 0 end)*coalesce(mem.processing_weight, 0)'
SET @query = @query || ' ,female_20_24=coalesce( pvf1.female_20_24,0)+(case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 24) then 1 else 0 end)'
SET @query = @query || ' ,Weigthed_Female_20_24=coalesce( pvf1.Weigthed_Female_20_24,0)+(case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 24) then 1.0 else 0 end)*coalesce(mem.processing_weight, 0)'
SET @query = @query || ' ,female_25_34=coalesce( pvf1.female_25_34,0)+(case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 34) then 1 else 0 end)'
SET @query = @query || ' ,Weigthed_Female_25_34=coalesce( pvf1.Weigthed_Female_25_34,0)+(case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 34) then 1.0 else 0 end)*coalesce(mem.processing_weight, 0)'
SET @query = @query || ' ,female_35_44=coalesce( pvf1.female_35_44,0)+(case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 44) then 1 else 0 end)'
SET @query = @query || ' ,Weigthed_Female_35_44=coalesce( pvf1.Weigthed_Female_35_44,0)+(case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 44) then 1.0 else 0 end)*coalesce(mem.processing_weight, 0)'
SET @query = @query || ' ,female_45_64=coalesce( pvf1.female_45_64,0)+(case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 64) then 1 else 0 end)'
SET @query = @query || ' ,Weigthed_Female_45_64=coalesce( pvf1.Weigthed_Female_45_64,0)+(case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 64) then 1.0 else 0 end)*coalesce(mem.processing_weight, 0)'
SET @query = @query || ' ,female_65=coalesce( pvf1.female_65,0)+(case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 65) then 1 else 0 end)'
SET @query = @query || ' ,Weigthed_Female_65=coalesce( pvf1.Weigthed_Female_65,0)+(case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 65) then 1.0 else 0 end)*coalesce(mem.processing_weight, 0)'
SET @query = @query || ' from '
SET @query = @query || '  BARB_table_output_1 pvf1 '
SET @query = @query || '      inner join '
SET @query = @query || ' ( '
SET @query = @query || '    select mem1.sex_code, mem1.household_number, mem1.filename, mem1.date_of_birth, mem1.household_status, wei.processing_weight '
SET @query = @query || '    from '
SET @query = @query || '    BARB_PVF04_Individual_Member_Details mem1 '
SET @query = @query || '              left join '
SET @query = @query || '    BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories wei '
SET @query = @query || '        on  mem1.household_number=wei.household_number '
SET @query = @query || '        and mem1.filename=wei.filename '
SET @query = @query || '        and mem1.person_number=wei.person_number '
SET @query = @query || '        and wei.reporting_panel_code=50 '
SET @query = @query || '    where mem1.person_number=' || @cntr_people_string
SET @query = @query || '    ) mem '
SET @query = @query || '        on  mem.household_number=pvf1.household_number '
SET @query = @query || '        and mem.filename=pvf1.filename '
SET @query = @query || '    where pvf1.person_' || @cntr_people_string || '_viewing=1'
-- SET @query = @query || '    go '

EXECUTE (@query)

-- select (@cntr_people_string)

SET @cntr_people = @cntr_people+1

end -- WHILE @cntr_people <= 16


end -- end of procedure proc_BARB_update_fields

;

commit

exec proc_BARB_update_fields 'BARB_table_output_1'

;

commit


select now(), '1'

-- select top 50 * from BARB_table_output_1
--order by household_number, Local_Start_Time_of_Session

-- update household_weight with the housewife weight (household_status 2 means housewife not head of household, whereas household_status 4 means the person is housewife and head of household)
update BARB_table_output_1 pvf1
set pvf1.Household_Weight=
(
select max(processing_weight) as processing_weight --, mem.person_number, mem.Household_status
from
BARB_PVF04_Individual_Member_Details mem
left join
BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories wei
on mem.household_number=wei.household_number
and mem.filename=wei.filename
and mem.person_number=wei.person_number
where mem.household_number=pvf1.household_number
and wei.Reporting_Panel_Code=50
and (mem.Household_status=2 or mem.Household_status=4)
group by pvf1.household_number
)


;

-- update the total people watching
update BARB_table_output_1
set weighted_total_people_viewing=
coalesce(Weigthed_Male_4_9,0)+coalesce(Weigthed_Male_10_15,0)+coalesce(Weigthed_Male_16_19,0)+coalesce(Weigthed_Male_20_24,0)+coalesce(Weigthed_Male_25_34,0)+coalesce(Weigthed_Male_35_44,0)+coalesce(Weigthed_Male_45_64,0)+coalesce(Weigthed_Male_65,0)+
coalesce(Weigthed_Female_4_9,0)+coalesce(Weigthed_Female_10_15,0)+coalesce(Weigthed_Female_16_19,0)+coalesce(Weigthed_Female_20_24,0)+coalesce(Weigthed_Female_25_34,0)+coalesce(Weigthed_Female_35_44,0)+coalesce(Weigthed_Female_45_64,0)+coalesce(Weigthed_Female_65,0)

;

-- select top 50 * from BARB_table_output_1
-- order by household_number, Local_Start_Time_of_Session


-- update service key, channel name and DB1_Station_Name with information from the table BARB_Channel_Map

update BARB_table_output_1 pvf1
set pvf1.service_key=cm.service_key, pvf1.Channel_Name=cm.sk_name, pvf1.DB1_Station_Name=cm.db1_name
from
BARB_table_output_1 pvf1
left join
(
select db1_station_code, service_key, sk_name, db1_name
from
vespa_analysts.BARB_Channel_Map
where
main_sk = 'Y'
) cm
on pvf1.DB1_Station_Code=cm.db1_station_code


select now(), '2'


;

-- better to update here local_end_time_of_recording


-- update end time of recording: start time of recording plus duration of session.

update BARB_table_output_1
set Local_End_time_of_recording = dateadd(mi, barb_t.Duration_of_Session-1, barb_t.Local_Start_time_of_recording)-- the -1 is for BARB policy that the same minute cannot be assigned to different events, so if ev start 19:40 and viewed for 21 minutes, end will be at 20:00, and following event will start at 20:01
from
BARB_table_output_1 barb_t
where Local_Start_time_of_recording is not null


;

IF OBJECT_ID('BARB_table_output_1_ordered') IS not NULL
DROP TABLE BARB_table_output_1_ordered

;

create table BARB_table_output_1_ordered
(
filename varchar(100)
,Sky_STB_viewing varchar(1) DEFAULT NULL
,Sky_STB_holder_hh varchar(1) DEFAULT NULL
,PVF_PV2 varchar(4) DEFAULT NULL
,Household_number int DEFAULT NULL
,Barb_date_of_activity_DB1 date
-- ,Actual_date
,Set_number int DEFAULT NULL
,BARB_Start_Time_of_Session  int default NULL
,Local_Start_Time_of_Session timestamp default NULL
,Local_End_Time_of_Session timestamp default NULL
,Panel_or_guest_flag varchar(8)
,Duration_of_Session int DEFAULT NULL -- minutes
,Session_activity_type int DEFAULT NULL
,Playback_type varchar(1) DEFAULT NULL
,DB1_Station_Code int DEFAULT NULL
,DB1_Station_Name varchar(100)
,Viewing_platform int DEFAULT NULL
,Barb_date_of_recording_DB1 date
-- ,Actual_Date_of_Recording date --- change datatype from Barb
,BARB_Start_time_of_recording int default NULL
,Local_Start_time_of_recording timestamp default NULL
,Local_End_time_of_recording timestamp default NULL
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
,Weigthed_Male_4_9 double DEFAULT NULL
,Weigthed_Male_10_15 double DEFAULT NULL
,Weigthed_Male_16_19 double DEFAULT NULL
,Weigthed_Male_20_24 double DEFAULT NULL
,Weigthed_Male_25_34 double DEFAULT NULL
,Weigthed_Male_35_44 double DEFAULT NULL
,Weigthed_Male_45_64 double DEFAULT NULL
,Weigthed_Male_65 double DEFAULT NULL
,Weigthed_Female_4_9 double DEFAULT NULL
,Weigthed_Female_10_15 double DEFAULT NULL
,Weigthed_Female_16_19 double DEFAULT NULL
,Weigthed_Female_20_24 double DEFAULT NULL
,Weigthed_Female_25_34 double DEFAULT NULL
,Weigthed_Female_35_44 double DEFAULT NULL
,Weigthed_Female_45_64 double DEFAULT NULL
,Weigthed_Female_65 double DEFAULT NULL
,total_people_viewing int DEFAULT NULL
,weighted_total_people_viewing double DEFAULT NULL
,VOD_Indicator int DEFAULT NULL
,VOD_Provider int DEFAULT NULL
,VOD_Service int DEFAULT NULL
,VOD_Type int DEFAULT NULL
,Device_in_use int DEFAULT NULL
-- ,TV_Instance_Start_Date_Time timestamp
-- ,TV_Instance_End_Date_Time timestamp
,TV_Event_Start_Date_Time timestamp default NULL
,TV_Event_End_Date_Time timestamp default NULL
,Household_Weight double DEFAULT NULL
,Service_Key  int DEFAULT NULL
,Channel_Name varchar(100)
,cb_row_id bigint DEFAULT NULL
,row_id bigint primary key identity
/*
1647
*/

)


;

insert into BARB_table_output_1_ordered (set_number) VALUES('8888')

;

insert into BARB_table_output_1_ordered
(
filename
,PVF_PV2
,Sky_STB_viewing
,Sky_STB_holder_hh
,Household_number
,Barb_date_of_activity_DB1
-- ,Actual_date
,Set_number
,BARB_Start_Time_of_Session
,Local_Start_Time_of_Session
,Local_End_Time_of_Session
,Panel_or_guest_flag
,Duration_of_Session
,Session_activity_type
,Playback_type
,DB1_Station_Code
,DB1_Station_Name
,Viewing_platform
,Barb_date_of_recording_DB1
-- ,Actual_Date_of_Recording date --- change datatype from Barb
,BARB_Start_time_of_recording
,Local_Start_time_of_recording
,Local_End_time_of_recording
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
,Weigthed_Male_4_9
,Weigthed_Male_10_15
,Weigthed_Male_16_19
,Weigthed_Male_20_24
,Weigthed_Male_25_34
,Weigthed_Male_35_44
,Weigthed_Male_45_64
,Weigthed_Male_65
,Weigthed_Female_4_9
,Weigthed_Female_10_15
,Weigthed_Female_16_19
,Weigthed_Female_20_24
,Weigthed_Female_25_34
,Weigthed_Female_35_44
,Weigthed_Female_45_64
,Weigthed_Female_65
,total_people_viewing
,weighted_total_people_viewing
,VOD_Indicator
,VOD_Provider
,VOD_Service
,VOD_Type
,Device_in_use
--,TV_Instance_Start_Date_Time
--,TV_Instance_End_Date_Time
--,TV_Event_Start_Date_Time
--,TV_Event_End_Date_Time
,Household_Weight
,Service_Key
,Channel_Name
,cb_row_id
)
select
filename
,PVF_PV2
,Sky_STB_viewing
,Sky_STB_holder_hh
,Household_number
,Barb_date_of_activity_DB1
-- ,Actual_date
,Set_number
,BARB_Start_Time_of_Session
,Local_Start_Time_of_Session
,Local_End_Time_of_Session
,Panel_or_guest_flag
,Duration_of_Session
,Session_activity_type
,Playback_type
,DB1_Station_Code
,DB1_Station_Name
,Viewing_platform
,Barb_date_of_recording_DB1
-- ,Actual_Date_of_Recording date --- change datatype from Barb
,BARB_Start_time_of_recording
,Local_Start_time_of_recording
,Local_End_time_of_recording
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
,Weigthed_Male_4_9
,Weigthed_Male_10_15
,Weigthed_Male_16_19
,Weigthed_Male_20_24
,Weigthed_Male_25_34
,Weigthed_Male_35_44
,Weigthed_Male_45_64
,Weigthed_Male_65
,Weigthed_Female_4_9
,Weigthed_Female_10_15
,Weigthed_Female_16_19
,Weigthed_Female_20_24
,Weigthed_Female_25_34
,Weigthed_Female_35_44
,Weigthed_Female_45_64
,Weigthed_Female_65
,total_people_viewing
,weighted_total_people_viewing
,VOD_Indicator
,VOD_Provider
,VOD_Service
,VOD_Type
,Device_in_use
-- ,TV_Instance_Start_Date_Time timestamp
-- ,TV_Instance_End_Date_Time timestamp
--,TV_Event_Start_Date_Time
--,TV_Event_End_Date_Time
,Household_Weight
,Service_Key
,Channel_Name
,cb_row_id
from BARB_table_output_1
order by Panel_or_guest_flag, household_number, set_number, Local_start_time_of_session

;

IF OBJECT_ID('BARB_temp_channel_event_start') IS not NULL
DROP TABLE BARB_temp_channel_event_start

;

select
        v1.household_number
        ,v1.Panel_or_guest_flag
        ,v1.set_number
        ,v1.db1_station_code
        ,v1.session_activity_type
        ,v1.viewing_platform
        ,v1.Local_Start_Time_of_Session as channel_event_start_time
into
        BARB_temp_channel_event_start
from
        BARB_table_output_1_ordered v1
     inner join
        BARB_table_output_1_ordered v2
     -- this join matches row_id 1 in table v1 to row_id 0 in table v2
     -- i.e. the first channel event doesn't get a start time. Will deal with this later -- Leo solved it!!!
     on v1.row_id = v2.row_id+1
where
        v1.Local_Start_Time_of_Session > dateadd(mi, 1, v2.Local_End_Time_of_Session)
        or v1.household_number <> v2.household_number
        or v1.Panel_or_guest_flag <> v2.Panel_or_guest_flag
        or v1.set_number <> v2.set_number
        or v1.db1_station_code <> v2.db1_station_code
        or v1.session_activity_type <> v2.session_activity_type
        or v1.viewing_platform <> v2.viewing_platform

;


delete from BARB_table_output_1_ordered where set_number = 8888 -- delete the dummy record that we added

;
---------------------------------
-- Update channel event start times
update BARB_table_output_1_ordered
        set v1.TV_Event_Start_Date_Time =
                (select max(s.channel_event_start_time)
                from BARB_temp_channel_event_start s
                where v1.household_number = s.household_number
                and v1.set_number = s.set_number
                and v1.Panel_or_guest_flag = s.Panel_or_guest_flag
                and v1.db1_station_code = s.db1_station_code
                and v1.session_activity_type = s.session_activity_type
                and v1.viewing_platform = s.viewing_platform
                and v1.Local_Start_Time_of_Session >= s.channel_event_start_time
                group by s.household_number, s.set_number, s.db1_station_code, s.session_activity_type, s.viewing_platform)
        from BARB_table_output_1_ordered v1
/*
        inner join
(select max(s.channel_event_start_time)
                from BARB_temp_channel_event_start s
                 group by s.household_number, s.set_number, s.db1_station_code, s.session_activity_type, s.viewing_platform)
               on v1.household_number = s.household_number
                and v1.set_number = s.set_number
                and v1.db1_station_code = s.db1_station_code
                and v1.session_activity_type = s.session_activity_type
                and v1.viewing_platform = s.viewing_platform
                and v1.Local_Start_Time_of_Session >= s.channel_event_start_time
*/



-- Identify rows that are Channel Event Ends
-- The following update query can't match the last row_id (if last row_id is 100 then would have to match to row_id 101)
-- i.e. the last channel event doesn't get an end time. Will deal with this later
-- By adding a dummy record at the end this match will work

;

insert into BARB_table_output_1_ordered (set_number) VALUES('9999')

;

IF OBJECT_ID('BARB_temp_channel_event_end') IS not NULL
DROP TABLE BARB_temp_channel_event_end

;

select
--        v1.row_id
        v1.Panel_or_guest_flag
        ,v1.household_number
        ,v1.set_number
        ,v1.db1_station_code
        ,v1.session_activity_type
        ,v1.viewing_platform
        ,v1.Local_End_Time_of_Session as channel_event_end_time
into
        BARB_temp_channel_event_end
from
        BARB_table_output_1_ordered v1
     inner join
        BARB_table_output_1_ordered v2
     -- this join can't match the last row_id (if last row_id is 100 then would have to match to row_id 101)
     -- i.e. the last channel event doesn't get an end time. Will deal with this later
     on v1.row_id = v2.row_id-1
where
        v2.Local_Start_Time_of_Session > dateadd(mi, 1, v1.Local_End_Time_of_Session)
        or v1.household_number <> v2.household_number
        or v1.Panel_or_guest_flag <> v2.Panel_or_guest_flag
        or v1.set_number <> v2.set_number
        or v1.db1_station_code <> v2.db1_station_code
        or v1.session_activity_type <> v2.session_activity_type
        or v1.viewing_platform <> v2.viewing_platform

;

--create hg index ind_row on BARB_temp_channel_event_end(row_id)
create hg index ind_hhd on BARB_temp_channel_event_end(household_number)
create lf index ind_set on BARB_temp_channel_event_end(set_number)
create lf index ind_db1 on BARB_temp_channel_event_end(db1_station_code)
create lf index ind_act on BARB_temp_channel_event_end(session_activity_type)
create lf index ind_plat on BARB_temp_channel_event_end(viewing_platform)
create hg index ind_end on BARB_temp_channel_event_end(channel_event_end_time)

;

delete from BARB_table_output_1_ordered where set_number = 9999 -- delete the dummy record that we added



select now(), '3'


;

-- Update channel event end times
update BARB_table_output_1_ordered
        set v1.TV_Event_End_Date_Time =
                (select min(s.channel_event_end_time)
                from BARB_temp_channel_event_end s
                where v1.household_number = s.household_number
                and v1.Panel_or_guest_flag = s.Panel_or_guest_flag
                and v1.set_number = s.set_number
                and v1.db1_station_code = s.db1_station_code
                and v1.session_activity_type = s.session_activity_type
                and v1.viewing_platform = s.viewing_platform
                and v1.Local_End_Time_of_Session <= s.channel_event_end_time
                group by s.household_number, s.set_number, s.db1_station_code, s.session_activity_type, s.viewing_platform)
        from BARB_table_output_1_ordered v1

;

-- at this stage we have the tv event start and end time. Let's calculate now at programme instance level

IF OBJECT_ID('BARB_viewing_table') IS not NULL
DROP TABLE BARB_viewing_table

;

create table BARB_viewing_table
(
filename varchar(100)
,Sky_STB_viewing varchar(1) DEFAULT NULL
,Sky_STB_holder_hh varchar(1) DEFAULT NULL
,PVF_PV2 varchar(4) DEFAULT NULL
,Household_number int DEFAULT NULL
,Barb_date_of_activity_DB1 date
-- ,Actual_date
,Set_number int DEFAULT NULL
,Panel_or_guest_flag varchar(8)
,Duration_of_Session int DEFAULT NULL -- minutes
,Session_activity_type int DEFAULT NULL
,Playback_type varchar(1) DEFAULT NULL
,DB1_Station_Code int DEFAULT NULL
,DB1_Station_Name varchar(100)
,Viewing_platform int DEFAULT NULL
,Barb_date_of_recording_DB1 date
-- ,Actual_Date_of_Recording date --- change datatype from Barb
,BARB_Start_time_of_recording int default NULL
,Local_Start_time_of_recording timestamp default NULL
,Local_End_time_of_recording timestamp default NULL
,UTC_Start_time_of_recording timestamp default NULL
,UTC_End_time_of_recording timestamp default NULL
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
,Weigthed_Male_4_9 double DEFAULT NULL
,Weigthed_Male_10_15 double DEFAULT NULL
,Weigthed_Male_16_19 double DEFAULT NULL
,Weigthed_Male_20_24 double DEFAULT NULL
,Weigthed_Male_25_34 double DEFAULT NULL
,Weigthed_Male_35_44 double DEFAULT NULL
,Weigthed_Male_45_64 double DEFAULT NULL
,Weigthed_Male_65 double DEFAULT NULL
,Weigthed_Female_4_9 double DEFAULT NULL
,Weigthed_Female_10_15 double DEFAULT NULL
,Weigthed_Female_16_19 double DEFAULT NULL
,Weigthed_Female_20_24 double DEFAULT NULL
,Weigthed_Female_25_34 double DEFAULT NULL
,Weigthed_Female_35_44 double DEFAULT NULL
,Weigthed_Female_45_64 double DEFAULT NULL
,Weigthed_Female_65 double DEFAULT NULL
,total_people_viewing int DEFAULT NULL
,weighted_total_people_viewing double DEFAULT NULL
,VOD_Indicator int DEFAULT NULL
,VOD_Provider int DEFAULT NULL
,VOD_Service int DEFAULT NULL
,VOD_Type int DEFAULT NULL
,Device_in_use int DEFAULT NULL
,broadcast_start_date_time_local timestamp default NULL
,broadcast_end_date_time_local timestamp default NULL
,broadcast_start_date_time_UTC timestamp default NULL
,broadcast_end_date_time_UTC timestamp default NULL
,BARB_Start_Time_of_Session  int default NULL
,UTC_Start_Time_of_Session timestamp default NULL
,UTC_End_Time_of_Session timestamp default NULL
,Local_Start_Time_of_Session timestamp default NULL
,Local_End_Time_of_Session timestamp default NULL
,Local_TV_Event_Start_Date_Time timestamp default NULL
,Local_TV_Event_End_Date_Time timestamp default NULL
,Local_TV_Instance_Start_Date_Time timestamp default NULL
,Local_TV_Instance_End_Date_Time timestamp default NULL
,Local_BARB_Instance_Start_Date_Time timestamp default NULL
,Local_BARB_Instance_End_Date_Time timestamp default NULL
,UTC_TV_Event_Start_Date_Time timestamp default NULL
,UTC_TV_Event_End_Date_Time timestamp default NULL
,UTC_TV_Instance_Start_Date_Time timestamp default NULL
,UTC_TV_Instance_End_Date_Time timestamp default NULL
,UTC_BARB_Instance_Start_Date_Time timestamp default NULL
,UTC_BARB_Instance_End_Date_Time timestamp default NULL
,TV_Instance_sequence_id int default NULL
,BARB_Instance_duration int default NULL
,TV_event_duration int default NULL
,TV_instance_duration int default NULL
,Household_Weight double DEFAULT NULL
,Service_Key  int DEFAULT NULL
,Channel_Name varchar(100)
,cb_row_id bigint DEFAULT NULL
,row_id bigint primary key identity
,programme_name varchar(255) default NULL -- from here, fields from sk_prod.VESPA_PROGRAMME_SCHEDULE
,genre_description varchar(20) default NULL
,sub_genre_description varchar(20) default NULL
,broadcast_daypart varchar(20) default NULL
,episode_number smallint DEFAULT NULL
,episodes_in_series smallint DEFAULT NULL
,three_d_flag int default NULL
,true_hd_flag int default NULL
,wide_screen_flag int default NULL -- to here, fields from sk_prod.VESPA_PROGRAMME_SCHEDULE
,channel_pack varchar(200) default NULL
)

;

/*

IF OBJECT_ID('PRG_schedule_tmp_schedule') IS not NULL
DROP TABLE PRG_schedule_tmp_schedule
-- Leooooo -- make sure fields are unique (take max)
;

-- select only the fields we need, and only restricted to the time range we need
            select service_key, broadcast_start_date_time_local, broadcast_end_date_time_local
                ,programme_name
                ,genre_description
                ,sub_genre_description
                ,broadcast_daypart
                ,episode_number
                ,episodes_in_series
                ,three_d_flag
                ,true_hd_flag
                ,wide_screen_flag
            into PRG_schedule_tmp_schedule
            from
            sk_prod.VESPA_PROGRAMME_SCHEDULE
            where broadcast_start_date_time_local >= (select min_local_timestamp from local_time_range)
            and broadcast_end_date_time_local <= (select max_local_timestamp from local_time_range)

;

*/

insert into BARB_viewing_table
(
filename
,PVF_PV2
,Sky_STB_viewing
,Sky_STB_holder_hh
,Household_number
,Barb_date_of_activity_DB1
-- ,Actual_date
,Set_number
,BARB_Start_Time_of_Session
,Local_Start_Time_of_Session
,Local_End_Time_of_Session
,Panel_or_guest_flag
,Duration_of_Session
,Session_activity_type
,Playback_type
,DB1_Station_Code
,DB1_Station_Name
,Viewing_platform
,Barb_date_of_recording_DB1
-- ,Actual_Date_of_Recording date --- change datatype from Barb
,BARB_Start_time_of_recording
,Local_Start_time_of_recording
,Local_End_time_of_recording
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
,Weigthed_Male_4_9
,Weigthed_Male_10_15
,Weigthed_Male_16_19
,Weigthed_Male_20_24
,Weigthed_Male_25_34
,Weigthed_Male_35_44
,Weigthed_Male_45_64
,Weigthed_Male_65
,Weigthed_Female_4_9
,Weigthed_Female_10_15
,Weigthed_Female_16_19
,Weigthed_Female_20_24
,Weigthed_Female_25_34
,Weigthed_Female_35_44
,Weigthed_Female_45_64
,Weigthed_Female_65
,total_people_viewing
,weighted_total_people_viewing
,VOD_Indicator
,VOD_Provider
,VOD_Service
,VOD_Type
,Device_in_use
,broadcast_start_date_time_local -- Leo: to remove
,broadcast_end_date_time_local -- Leo: to remove
,broadcast_start_date_time_UTC
,broadcast_end_date_time_UTC
,Local_TV_Instance_Start_Date_Time
,Local_TV_Instance_End_Date_Time
,Local_TV_Event_Start_Date_Time
,Local_TV_Event_End_Date_Time
,Household_Weight
,Service_Key
,Channel_Name
,cb_row_id
,programme_name
,genre_description
,sub_genre_description
,broadcast_daypart
,episode_number
,episodes_in_series
,three_d_flag
,true_hd_flag
,wide_screen_flag
)
select
filename
,PVF_PV2
,Sky_STB_viewing
,Sky_STB_holder_hh
,Household_number
,Barb_date_of_activity_DB1
-- ,Actual_date
,Set_number
,BARB_Start_Time_of_Session
,Local_Start_Time_of_Session
,Local_End_Time_of_Session
,Panel_or_guest_flag
,Duration_of_Session
,Session_activity_type
,Playback_type
,DB1_Station_Code
,DB1_Station_Name
,Viewing_platform
,Barb_date_of_recording_DB1
-- ,Actual_Date_of_Recording date --- change datatype from Barb
,BARB_Start_time_of_recording
,Local_Start_time_of_recording
,Local_End_time_of_recording
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
,Weigthed_Male_4_9
,Weigthed_Male_10_15
,Weigthed_Male_16_19
,Weigthed_Male_20_24
,Weigthed_Male_25_34
,Weigthed_Male_35_44
,Weigthed_Male_45_64
,Weigthed_Male_65
,Weigthed_Female_4_9
,Weigthed_Female_10_15
,Weigthed_Female_16_19
,Weigthed_Female_20_24
,Weigthed_Female_25_34
,Weigthed_Female_35_44
,Weigthed_Female_45_64
,Weigthed_Female_65
,total_people_viewing
,weighted_total_people_viewing
,VOD_Indicator
,VOD_Provider
,VOD_Service
,VOD_Type
,Device_in_use
,broadcast_start_date_time_local
,broadcast_end_date_time_local
,broadcast_start_date_time_UTC
,broadcast_end_date_time_UTC
        ,case
                when pvf.TV_Event_Start_Date_Time >= sch.broadcast_start_date_time_local then pvf.TV_Event_Start_Date_Time
                else sch.broadcast_start_date_time_local
        end as TV_Instance_Start_Date_Time
        ,case
                when pvf.TV_Event_End_Date_Time < sch.broadcast_end_date_time_local then pvf.TV_Event_End_Date_Time
                else dateadd(mi, -1, sch.broadcast_end_date_time_local) -- when the end time of instance comes from VESPA, we decrement by 1 to make it consistent with BARB policy
        end as TV_Instance_End_Date_Time
,pvf.TV_Event_Start_Date_Time
,pvf.TV_Event_End_Date_Time
,Household_Weight
,pvf.Service_Key
,pvf.Channel_Name
,pvf.cb_row_id
,sch.programme_name
,sch.genre_description
,sch.sub_genre_description
,sch.broadcast_daypart
,sch.episode_number
,sch.episodes_in_series
,cast(sch.three_d_flag as int)
,cast(sch.true_hd_flag as int)
,cast(sch.wide_screen_flag as int)
--into BARB_viewing_table
from
     BARB_table_output_1_ordered pvf
          left join
     sk_prod.VESPA_PROGRAMME_SCHEDULE sch
     on pvf.service_key = sch.service_key
     and pvf.Local_Start_Time_of_Session < sch.broadcast_end_date_time_local
     and pvf.Local_End_Time_of_Session >= sch.broadcast_start_date_time_local
where pvf.Local_Start_time_of_recording is null -- probably we can use this to limit the number of fields (check if it gives the same number of records
-- order by pvf.Panel_or_guest_flag, pvf.household_number, pvf.TV_Event_Start_Date_Time -- pvf.Panel_or_guest_flag
--     and pvf.TV_Event_Start_Date_Time < sch.broadcast_end_date_time_local -- needless
--     and pvf.TV_Event_End_Date_Time > sch.broadcast_start_date_time_local -- needless

--     and pvf.Local_Start_Time_of_Session < sch.broadcast_end_date_time_local
--     and pvf.Local_End_Time_of_Session > sch.broadcast_start_date_time_local
--, sch.broadcast_start_date_time_local

select now(),@@rowcount, '4'
/*
     and pvf.Local_Start_Time_of_Session < sch.broadcast_end_date_time_local
     and pvf.Local_End_Time_of_Session > sch.broadcast_start_date_time_local


     and coalesce(pvf.Local_Start_time_of_recording,pvf.Local_Start_Time_of_Session) < sch.broadcast_end_date_time_local
     and coalesce(pvf.Local_End_time_of_recording,pvf.Local_End_Time_of_Session) > sch.broadcast_start_date_time_local

     and (case when pvf.Local_Start_time_of_recording is not null then pvf.Local_Start_time_of_recording else pvf.Local_Start_Time_of_Session end) < sch.broadcast_end_date_time_local
     and (case when pvf.Local_End_time_of_recording is not null then pvf.Local_End_time_of_recording else pvf.Local_End_Time_of_Session end) > sch.broadcast_start_date_time_local
*/

;


insert into BARB_viewing_table
(
filename
,PVF_PV2
,Sky_STB_viewing
,Sky_STB_holder_hh
,Household_number
,Barb_date_of_activity_DB1
-- ,Actual_date
,Set_number
,BARB_Start_Time_of_Session
,Local_Start_Time_of_Session
,Local_End_Time_of_Session
,Panel_or_guest_flag
,Duration_of_Session
,Session_activity_type
,Playback_type
,DB1_Station_Code
,DB1_Station_Name
,Viewing_platform
,Barb_date_of_recording_DB1
-- ,Actual_Date_of_Recording date --- change datatype from Barb
,BARB_Start_time_of_recording
,Local_Start_time_of_recording
,Local_End_time_of_recording
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
,Weigthed_Male_4_9
,Weigthed_Male_10_15
,Weigthed_Male_16_19
,Weigthed_Male_20_24
,Weigthed_Male_25_34
,Weigthed_Male_35_44
,Weigthed_Male_45_64
,Weigthed_Male_65
,Weigthed_Female_4_9
,Weigthed_Female_10_15
,Weigthed_Female_16_19
,Weigthed_Female_20_24
,Weigthed_Female_25_34
,Weigthed_Female_35_44
,Weigthed_Female_45_64
,Weigthed_Female_65
,total_people_viewing
,weighted_total_people_viewing
,VOD_Indicator
,VOD_Provider
,VOD_Service
,VOD_Type
,Device_in_use
,broadcast_start_date_time_local -- Leo: to remove
,broadcast_end_date_time_local -- Leo: to remove
,broadcast_start_date_time_UTC
,broadcast_end_date_time_UTC
,Local_TV_Instance_Start_Date_Time
,Local_TV_Instance_End_Date_Time
,Local_TV_Event_Start_Date_Time
,Local_TV_Event_End_Date_Time
,Household_Weight
,Service_Key
,Channel_Name
,cb_row_id
,programme_name
,genre_description
,sub_genre_description
,broadcast_daypart
,episode_number
,episodes_in_series
,three_d_flag
,true_hd_flag
,wide_screen_flag
)
select
filename
,PVF_PV2
,Sky_STB_viewing
,Sky_STB_holder_hh
,Household_number
,Barb_date_of_activity_DB1
-- ,Actual_date
,Set_number
,BARB_Start_Time_of_Session
,Local_Start_Time_of_Session
,Local_End_Time_of_Session
,Panel_or_guest_flag
,Duration_of_Session
,Session_activity_type
,Playback_type
,DB1_Station_Code
,DB1_Station_Name
,Viewing_platform
,Barb_date_of_recording_DB1
-- ,Actual_Date_of_Recording date --- change datatype from Barb
,BARB_Start_time_of_recording
,Local_Start_time_of_recording
,Local_End_time_of_recording
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
,Weigthed_Male_4_9
,Weigthed_Male_10_15
,Weigthed_Male_16_19
,Weigthed_Male_20_24
,Weigthed_Male_25_34
,Weigthed_Male_35_44
,Weigthed_Male_45_64
,Weigthed_Male_65
,Weigthed_Female_4_9
,Weigthed_Female_10_15
,Weigthed_Female_16_19
,Weigthed_Female_20_24
,Weigthed_Female_25_34
,Weigthed_Female_35_44
,Weigthed_Female_45_64
,Weigthed_Female_65
,total_people_viewing
,weighted_total_people_viewing
,VOD_Indicator
,VOD_Provider
,VOD_Service
,VOD_Type
,Device_in_use
,broadcast_start_date_time_local
,broadcast_end_date_time_local
,broadcast_start_date_time_UTC
,broadcast_end_date_time_UTC
,case when pvf.Local_Start_time_of_recording >= sch.broadcast_start_date_time_local then pvf.Local_Start_Time_of_Session
                else dateadd(mi, datediff(mi, pvf.Local_Start_Time_of_Session, pvf.Local_Start_time_of_recording),sch.broadcast_start_date_time_local) end
,case when pvf.Local_End_time_of_recording < sch.broadcast_end_date_time_local then pvf.Local_End_Time_of_Session
                else dateadd(mi, -1,dateadd(mi, datediff(mi, pvf.Local_Start_time_of_recording, pvf.Local_Start_Time_of_Session),sch.broadcast_end_date_time_local) ) end
,pvf.TV_Event_Start_Date_Time
,pvf.TV_Event_End_Date_Time
,Household_Weight
,pvf.Service_Key
,pvf.Channel_Name
,pvf.cb_row_id
,sch.programme_name
,sch.genre_description
,sch.sub_genre_description
,sch.broadcast_daypart
,sch.episode_number
,sch.episodes_in_series
,cast(sch.three_d_flag as int)
,cast(sch.true_hd_flag as int)
,cast(sch.wide_screen_flag as int)
--into BARB_viewing_table
from
     BARB_table_output_1_ordered pvf
          left join
     sk_prod.VESPA_PROGRAMME_SCHEDULE sch
     on pvf.service_key = sch.service_key
     and pvf.Local_Start_time_of_recording < sch.broadcast_end_date_time_local
     and pvf.Local_End_time_of_recording >= sch.broadcast_start_date_time_local
where pvf.Local_Start_time_of_recording is not null -- probably we can use this to limit the number of fields (check if it gives the same number of records


select now(), @@rowcount, '4.5'


-- we need to add a shift to the broadcast time to calculate correctly instance start and end
-- the shift, in minutes, is datediff(mi, Local_Start_Time_of_Session, Local_Start_time_of_recording)
-- so we must add this shif to the prg schedule, when it is the case.
/*
update BARB_viewing_table
set TV_Instance_Start_Date_Time = (case when pvf.Local_Start_time_of_recording >= sch.broadcast_start_date_time_local then pvf.Local_Start_Time_of_Session
                else dateadd(mi, datediff(mi, pvf.Local_Start_Time_of_Session, pvf.Local_Start_time_of_recording),sch.broadcast_start_date_time_local) end)
,TV_Instance_End_Date_Time = (case when pvf.Local_End_time_of_recording <= sch.broadcast_end_date_time_local then pvf.Local_End_Time_of_Session
                else dateadd(mi, -1,
                dateadd(mi, datediff(mi, pvf.Local_Start_Time_of_Session, pvf.Local_Start_time_of_recording),sch.broadcast_end_date_time_local)
                ) end)
from
     BARB_table_output_1_ordered pvf
          left join
     PRG_schedule_tmp_schedule sch
     on pvf.service_key = sch.service_key
     and pvf.Local_Start_time_of_recording < sch.broadcast_end_date_time_local
     and pvf.Local_End_time_of_recording > sch.broadcast_start_date_time_local
where pvf.Local_Start_time_of_recording is not null
*/


/*
local_time_range') IS not NULL
DROP TABLE local_time_range

select min(Local_Start_Time_of_Session) as min_local_session_timestamp
*/
----------------------------------------------------------------------------------------------------------------------------
;
--,nr_of_PVF_rec_not_matching_VESPA int default NULL lullaby
--,nr_of_PV2_rec_not_matching_VESPA int default NULL

set @nr_of_PVF_rec_not_matching_VESPA=(select count(1) from BARB_viewing_table where broadcast_start_date_time_local is null and PVF_PV2='PVF')

set @nr_of_PV2_rec_not_matching_VESPA=(select count(1) from BARB_viewing_table where broadcast_start_date_time_local is null and PVF_PV2='PV2')

update barb_daily_monitoring
set nr_of_PVF_rec_not_matching_VESPA=@nr_of_PVF_rec_not_matching_VESPA
,nr_of_PV2_rec_not_matching_VESPA=@nr_of_PV2_rec_not_matching_VESPA
,nr_of_TOT_rec_not_matching_VESPA=@nr_of_PVF_rec_not_matching_VESPA+@nr_of_PV2_rec_not_matching_VESPA
where id_row=@current_id_row

;
-- update the channel pack
update BARB_viewing_table
set channel_pack = map.channel_pack
from
BARB_viewing_table view_t
left join
(
select service_key, max(channel_pack) as channel_pack
from
vespa_Analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES
group by service_key
) map
on view_t.service_key = map.service_key

;

-- fill NULL TV instance values with BARB event data
update BARB_viewing_table
set Local_TV_Instance_Start_Date_Time = view_t.Local_Start_Time_of_Session
, Local_TV_Instance_End_Date_Time = view_t.Local_End_Time_of_Session
from
BARB_viewing_table view_t
where Local_TV_Instance_Start_Date_Time IS NULL
OR Local_TV_Instance_End_Date_Time IS NULL

;

IF OBJECT_ID('temp_sequenced_viewing_table') IS not NULL
DROP TABLE temp_sequenced_viewing_table

-- since row_number or rank is not supported in update/delete activities, we create a temp table to make a join
select Panel_or_guest_flag, household_number, set_number, db1_station_code, session_activity_type, viewing_platform, Local_Start_Time_of_Session
,Local_TV_Event_Start_Date_Time, Local_TV_Event_End_Date_Time, Local_TV_Instance_Start_Date_Time, Local_TV_Instance_End_Date_Time, row_number() over (partition by Panel_or_guest_flag, household_number, set_number, db1_station_code, session_activity_type, viewing_platform, /*Local_Start_Time_of_Session,*/ Local_TV_Event_Start_Date_Time order by Local_Start_Time_of_Session, Local_TV_Instance_Start_Date_Time) as instance_sequence
into temp_sequenced_viewing_table
from BARB_viewing_table

;

-- update instance sequence id
update BARB_viewing_table
set TV_Instance_sequence_id = seq.instance_sequence
from
BARB_viewing_table view_t
left join
temp_sequenced_viewing_table seq
on
view_t.Panel_or_guest_flag = seq.Panel_or_guest_flag
and
view_t.household_number = seq.household_number
and
view_t.set_number = seq.set_number
and
view_t.db1_station_code = seq.db1_station_code
and
view_t.session_activity_type = seq.session_activity_type
and
view_t.viewing_platform = seq.viewing_platform
and
view_t.Local_Start_Time_of_Session = seq.Local_Start_Time_of_Session
and
view_t.Local_TV_Event_Start_Date_Time = seq.Local_TV_Event_Start_Date_Time
and
view_t.Local_TV_Event_End_Date_Time = seq.Local_TV_Event_End_Date_Time
and
view_t.Local_TV_Instance_Start_Date_Time = seq.Local_TV_Instance_Start_Date_Time
and
view_t.Local_TV_Instance_End_Date_Time = seq.Local_TV_Instance_End_Date_Time

;

-- here we update BARB_Instance_Start_Date_Time and BARB_Instance_End_Date_Time
-- BARB_Instance_Start_Date_Time which will be the maximum among Local_Start_Time_of_Session, TV_Event_Start_Date_Time, TV_Instance_Start_Date_Time
-- BARB_Instance_End_Date_Time which will be the minimum among Local_End_Time_of_Session, TV_Event_End_Date_Time, TV_Instance_End_Date_Time

update BARB_viewing_table
set Local_BARB_Instance_Start_Date_Time = CASE WHEN coalesce(view_t.Local_Start_Time_of_Session,'') >= coalesce(view_t.Local_TV_Event_Start_Date_Time,'') THEN
    CASE WHEN coalesce(view_t.Local_Start_Time_of_Session,'') >= coalesce(view_t.Local_TV_Instance_Start_Date_Time,'') THEN coalesce(view_t.Local_Start_Time_of_Session,'') ELSE coalesce(view_t.Local_TV_Instance_Start_Date_Time,'') END
ELSE
    CASE WHEN coalesce(view_t.Local_TV_Event_Start_Date_Time,'') >= coalesce(view_t.Local_TV_Instance_Start_Date_Time,'') THEN coalesce(view_t.Local_TV_Event_Start_Date_Time,'') ELSE coalesce(view_t.Local_TV_Instance_Start_Date_Time,'') END
END
, Local_BARB_Instance_End_Date_Time = CASE WHEN coalesce(view_t.Local_End_Time_of_Session,'') <= coalesce(view_t.Local_TV_Event_End_Date_Time,'') THEN
    CASE WHEN coalesce(view_t.Local_End_Time_of_Session,'') <= coalesce(view_t.Local_TV_Instance_End_Date_Time,'') THEN coalesce(view_t.Local_End_Time_of_Session,'') ELSE coalesce(view_t.Local_TV_Instance_End_Date_Time,'') END
ELSE
    CASE WHEN coalesce(view_t.Local_TV_Event_End_Date_Time,'') <= coalesce(view_t.Local_TV_Instance_End_Date_Time,'') THEN coalesce(view_t.Local_TV_Event_End_Date_Time,'') ELSE coalesce(view_t.Local_TV_Instance_End_Date_Time,'') END
END
from
BARB_viewing_table view_t

;

-- here we calculate all durations: BARB instance, TV event and TV instance (simple difference: end time-start time)
---- the +1 is for BARB policy that the same minute cannot be assigned to different events, so if ev start 19:40 and viewed for 21 minutes, end will be at 20:00, and following event will start at 20:01
-- so to have the actual event duration we must add 1
update BARB_viewing_table
set BARB_Instance_duration = datediff(mi,Local_BARB_Instance_Start_Date_Time,Local_BARB_Instance_End_Date_Time)+1
,TV_event_duration = datediff(mi,Local_TV_Event_Start_Date_Time,Local_TV_Event_End_Date_Time)+1
,TV_instance_duration = datediff(mi,Local_TV_Instance_Start_Date_Time,Local_TV_Instance_End_Date_Time)+1

select now(), '5'

;

-- here update the UTC times: we convert local day and time to UTC using data in sk_prod.VESPA_CALENDAR

-- we create a temp table containing only UTC/Local conversion data for the short period we are interested in
update BARB_viewing_table
set UTC_Start_Time_of_Session = datetime(utc_day_date || ' ' || utc_time_hours || ':' || datepart(mi, view_t.Local_Start_Time_of_Session) || ':00.000000')
from
BARB_viewing_table view_t
inner join
local_UTC_conversion_table loc
on local_day_date = date(view_t.Local_Start_Time_of_Session)
and local_time_hours = datepart(hh, view_t.Local_Start_Time_of_Session)
where Local_Start_Time_of_Session is not null

;

update BARB_viewing_table
set UTC_End_Time_of_Session = datetime(utc_day_date || ' ' || utc_time_hours || ':' || datepart(mi, view_t.Local_End_Time_of_Session) || ':00.000000')
from
BARB_viewing_table view_t
inner join
local_UTC_conversion_table loc
on local_day_date = date(view_t.Local_End_Time_of_Session)
and local_time_hours = datepart(hh, view_t.Local_End_Time_of_Session)
where Local_End_Time_of_Session is not null

;


update BARB_viewing_table
set UTC_Start_time_of_recording = datetime(utc_day_date || ' ' || utc_time_hours || ':' || datepart(mi, view_t.Local_Start_time_of_recording) || ':00.000000')
from
BARB_viewing_table view_t
inner join
local_UTC_conversion_table loc
on local_day_date = date(view_t.Local_Start_time_of_recording)
and local_time_hours = datepart(hh, view_t.Local_Start_time_of_recording)
where Local_Start_time_of_recording is not null

;

update BARB_viewing_table
set UTC_End_time_of_recording = datetime(utc_day_date || ' ' || utc_time_hours || ':' || datepart(mi, view_t.Local_End_time_of_recording) || ':00.000000')
from
BARB_viewing_table view_t
inner join
local_UTC_conversion_table loc
on local_day_date = date(view_t.Local_End_time_of_recording)
and local_time_hours = datepart(hh, view_t.Local_End_time_of_recording)
where Local_End_time_of_recording is not null

;

update BARB_viewing_table
set UTC_TV_Event_Start_Date_Time = datetime(utc_day_date || ' ' || utc_time_hours || ':' || datepart(mi, view_t.Local_TV_Event_Start_Date_Time) || ':00.000000')
from
BARB_viewing_table view_t
inner join
local_UTC_conversion_table loc
on local_day_date = date(view_t.Local_TV_Event_Start_Date_Time)
and local_time_hours = datepart(hh, view_t.Local_TV_Event_Start_Date_Time)
where Local_TV_Event_Start_Date_Time is not null

;

update BARB_viewing_table
set UTC_TV_Event_End_Date_Time = datetime(utc_day_date || ' ' || utc_time_hours || ':' || datepart(mi, view_t.Local_TV_Event_End_Date_Time) || ':00.000000' )
from
BARB_viewing_table view_t
inner join
local_UTC_conversion_table loc
on local_day_date = date(view_t.Local_TV_Event_End_Date_Time)
and local_time_hours = datepart(hh, view_t.Local_TV_Event_End_Date_Time)
where view_t.Local_TV_Event_End_Date_Time is not null

;

update BARB_viewing_table
set UTC_TV_Instance_Start_Date_Time = datetime(utc_day_date || ' ' || utc_time_hours || ':' || datepart(mi, view_t.Local_TV_Instance_Start_Date_Time) || ':00.000000' )
from
BARB_viewing_table view_t
inner join
local_UTC_conversion_table loc
on local_day_date = date(view_t.Local_TV_Instance_Start_Date_Time)
and local_time_hours = datepart(hh, view_t.Local_TV_Instance_Start_Date_Time)
where view_t.Local_TV_Instance_Start_Date_Time is not null

;


update BARB_viewing_table
set UTC_TV_Instance_End_Date_Time = datetime(utc_day_date || ' ' || utc_time_hours || ':' || datepart(mi, view_t.Local_TV_Instance_End_Date_Time) || ':00.000000' )
from
BARB_viewing_table view_t
inner join
local_UTC_conversion_table loc
on local_day_date = date(view_t.Local_TV_Instance_End_Date_Time)
and local_time_hours = datepart(hh, view_t.Local_TV_Instance_End_Date_Time)
where view_t.Local_TV_Instance_End_Date_Time is not null

;

update BARB_viewing_table
set UTC_BARB_Instance_Start_Date_Time = datetime(utc_day_date || ' ' || utc_time_hours || ':' || datepart(mi, view_t.Local_BARB_Instance_Start_Date_Time) || ':00.000000' )
from
BARB_viewing_table view_t
inner join
local_UTC_conversion_table loc
on local_day_date = date(view_t.Local_BARB_Instance_Start_Date_Time)
and local_time_hours = datepart(hh, view_t.Local_BARB_Instance_Start_Date_Time)
where view_t.Local_BARB_Instance_Start_Date_Time is not null

;


update BARB_viewing_table
set UTC_BARB_Instance_End_Date_Time = datetime(utc_day_date || ' ' || utc_time_hours || ':' || datepart(mi, view_t.Local_BARB_Instance_End_Date_Time) || ':00.000000' )
from
BARB_viewing_table view_t
inner join
local_UTC_conversion_table loc
on local_day_date = date(view_t.Local_BARB_Instance_End_Date_Time)
and local_time_hours = datepart(hh, view_t.Local_BARB_Instance_End_Date_Time)
where view_t.Local_BARB_Instance_End_Date_Time is not null

;

set @dummy = (select count(1) from BARB_viewing_table)

update barb_daily_monitoring
set nr_of_records_in_viewing_table = @dummy
where id_row=@current_id_row

;


select now(), '6'

/*
,UTC_BARB_Instance_End_Date_Time timestamp default NULL
*/

/*
IF OBJECT_ID('latest_BARB_viewing_table_rec') IS not NULL
DROP TABLE latest_BARB_viewing_table_rec

;

select *
into latest_BARB_viewing_table_rec
from BARB_viewing_table


IF OBJECT_ID('latest_BARB_viewing_table') IS not NULL
DROP TABLE latest_BARB_viewing_table

;

select *
into latest_BARB_viewing_table
from BARB_viewing_table

grant select on latest_BARB_viewing_table to vespa_group_low_security


grant select on BARB_viewing_table to vespa_group_low_security
*/

