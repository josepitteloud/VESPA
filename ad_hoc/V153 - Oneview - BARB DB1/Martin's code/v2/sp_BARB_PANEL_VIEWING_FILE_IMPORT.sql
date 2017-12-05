

create PROCEDURE sp_BARB_PANEL_VIEWING_FILE_IMPORT (@in_filename varchar(60)) AS

BEGIN
/*
Martin Neighbours
5/2/13

Usage
EXEC sp_BARB_PANEL_VIEWING_FILE_IMPORT 'Jim/BARB/B20120227.PVF'

Note this is incremental

*/


DECLARE @query varchar(3000)
DECLARE @file_creation_date date
DECLARE @file_creation_time time
DECLARE @file_type  varchar(12)
DECLARE @File_Version Int
DECLARE @audit_row_count bigint
DECLARE @filename varchar(13)

DELETE FROM MN_BARB_import
-- import raw data

SET @query = 'LOAD TABLE MN_BARB_import (imported_text '
SET @query = @query || ' ''\n'' ) '
SET @query = @query || ' FROM ''/ETL013/prod/sky/olive/data/share/clarityq/export/' || @in_filename || ''' QUOTES OFF ESCAPES OFF NOTIFY 1000'

EXECUTE (@query)

SET @audit_row_count = (Select count(1) from MN_BARB_IMPORT)

-- file doesn't exist

IF @audit_row_count = 0
BEGIN
        SELECT now(), @file_creation_Date, @file_creation_time, @file_type, @file_version, @filename,'File not found ' || @in_filename, @audit_row_count

        RETURN
END

IF @file_type <> 'DSP01.V03.05'
BEGIN

        SELECT now(), @file_creation_Date, @file_creation_time, @file_type, @file_version, @filename,'File type DSP01.V03.05, file type imported ' || @file_type, 0

        RETURN
END

-- parse out the data records


SET @file_creation_date = (SELECT CAST(substr(imported_text,7,8) AS Date)
                                FROM MN_BARB_import
                                WHERE substr(imported_text,1,2) = '01')

SET @file_creation_time = (SELECT CAST(substr(imported_text,15,2) || ':' || substr(imported_text,17,2) || ':' || substr(imported_text,19,2)  AS Time)
                                FROM MN_BARB_import
                                WHERE substr(imported_text,1,2) = '01')

SET @file_type = (SELECT substr(imported_text,21,12)
                                FROM MN_BARB_import
                                WHERE substr(imported_text,1,2) = '01')

SET @File_Version = (SELECT CAST(substr(imported_text,33,3) AS Int)
                                FROM MN_BARB_import
                                WHERE substr(imported_text,1,2) = '01')

SET @Filename = (SELECT substr(imported_text,36,13)
                                FROM MN_BARB_import
                                WHERE substr(imported_text,1,2) = '01')
/*
CREATE TABLE BARB_PANEL_VIEWING_FILE_HOME_CHARACTERISTICS
                (file_creation_date date,
                file_creation_time time,
                file_type varchar(12),
                file_version int,
                filename varchar(13),
                Household_number Int DEFAULT NULL,
                Date_Valid_For Int DEFAULT NULL,
                Panel_membership_status Int DEFAULT NULL,
                No_of_TV_Sets Int DEFAULT NULL,
                No_of_VCRs Int DEFAULT NULL,
                No_of_PVRs Int DEFAULT NULL,
                No_of_DVDs Int DEFAULT NULL,
                No_of_People Int DEFAULT NULL,
                Social_Class Varchar(2) DEFAULT NULL,
                Presence_of_Children Int DEFAULT NULL,
                Demographic_cell_1 Int DEFAULT NULL,
                BBC_Region_code Int DEFAULT NULL,
                BBC_ITV_Area_Segment Int DEFAULT NULL,
                S4C_Segment Int DEFAULT NULL,
                Language_Spoken_at_Home Int DEFAULT NULL,
                Welsh_Speaking_Home Int DEFAULT NULL,
                Number_of_DVD_Recorders Int DEFAULT NULL,
                Number_of_DVD_Players_not_recorders Int DEFAULT NULL,
                Number_of_Sky_plus_PVRs Int DEFAULT NULL,
                Number_of_other_PVRs Int DEFAULT NULL,
                Broadband Int DEFAULT NULL,
                BBC_Sub_Reporting_Region Int DEFAULT NULL)

CREATE TABLE BARB_PANEL_VIEWING_FILE_TV_SET_CHARACTERISTICS
                (file_creation_date date,
                file_creation_time time,
                file_type varchar(12),
                file_version int,
                filename varchar(13),
                Household_number Int DEFAULT NULL,
                Date_Valid_for_DB1 Date DEFAULT NULL,
                Set_Membership_Status Int DEFAULT NULL,
                Set_number Int DEFAULT NULL,
                Teletext Int DEFAULT NULL,
                Main_Location Int DEFAULT NULL,
                Analogue_Terrestrial Int DEFAULT NULL,
                Digital_Terrestrial Int DEFAULT NULL,
                Analogue_Satellite Int DEFAULT NULL,
                Digital_Satellite Int DEFAULT NULL,
                Analogue_Cable Int DEFAULT NULL,
                Digital_Cable Int DEFAULT NULL,
                Blank_for_future_platforms Varchar(6) DEFAULT NULL,
                VCR_present Int DEFAULT NULL,
                Sky_plus_PVR_present Int DEFAULT NULL,
                Other_PVR_present Int DEFAULT NULL,
                DVD_Player_only_present Int DEFAULT NULL,
                DVD_Recorder_present Int DEFAULT NULL,
                HD_reception Int DEFAULT NULL,
                Reception_Capability_Code_1 Int DEFAULT NULL,
                Reception_Capability_Code_2 Int DEFAULT NULL,
                Reception_Capability_Code_3 Int DEFAULT NULL,
                Reception_Capability_Code_4 Int DEFAULT NULL,
                Reception_Capability_Code_5 Int DEFAULT NULL,
                Reception_Capability_Code_6 Int DEFAULT NULL,
                Reception_Capability_Code_7 Int DEFAULT NULL,
                Reception_Capability_Code_8 Int DEFAULT NULL,
                Reception_Capability_Code_9 Int DEFAULT NULL,
                Reception_Capability_Code_10 Int DEFAULT NULL)

CREATE TABLE BARB_PANEL_VIEWING_FILE_INDIVIDAL_PANEL_MEMBER_DETAILS
        (file_creation_date date,
        file_creation_time time,
        file_type varchar(12),
        file_version int,
        filename varchar(13),
        Household_number Int DEFAULT NULL,
        Date_valid_for_DB1 Date DEFAULT NULL,
        Person_membership_status Int DEFAULT NULL,
        Person_number Int DEFAULT NULL,
        Sex_code Int DEFAULT NULL,
        Date_of_birth Date DEFAULT NULL,
        Marital_status Int DEFAULT NULL,
        Household_status Int DEFAULT NULL,
        Working_status Int DEFAULT NULL,
        Terminal_age_of_education Int DEFAULT NULL,
        Welsh_Language_code Int DEFAULT NULL,
        Gaelic_language_code Int DEFAULT NULL,
        Dependency_of_Children Int DEFAULT NULL,
        Life_stage_12_classifications Int DEFAULT NULL,
        Ethnic_Origin Int DEFAULT NULL)

CREATE TABLE BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY
        (file_creation_date date,
        file_creation_time time,
        file_type varchar(12),
        file_version int,
        filename varchar(13),
        Household_Number Int DEFAULT NULL,
        Person_Number Int DEFAULT NULL,
        Reporting_Panel_Code Int DEFAULT NULL,
        Date_of_Activity_DB1 Date DEFAULT NULL,
        Response_Code Int DEFAULT NULL,
        Processing_Weight DECIMAL(7,4) DEFAULT NULL,
        Adults_Commercial_TV_Viewing_Sextile Int DEFAULT NULL,
        ABC1_Adults_Commercial_TV_Viewing_Sextile Int DEFAULT NULL,
        Adults_Total_Viewing_Sextile Int DEFAULT NULL,
        ABC1_Adults_Total_Viewing_Sextile Int DEFAULT NULL,
        Adults_16_34_Commercial_TV_Viewing_Sextile Int DEFAULT NULL,
        Adults_16_34_Total_Viewing_Sextile Int DEFAULT NULL)

CREATE TABLE BARB_PANEL_VIEWING_FILE_VIEWING_RECORD_PANEL_MEMBERS
        (file_creation_date date,
        file_creation_time time,
        file_type varchar(12),
        file_version int,
        filename varchar(13),
        Household_number Int DEFAULT NULL,
        Date_of_Activity_DB1 Date DEFAULT NULL,
        Set_number Int DEFAULT NULL,
        Start_time_of_session Int DEFAULT NULL,
        Duration_of_session Int DEFAULT NULL,
        Session_activity_type Int DEFAULT NULL,
        Playback_type Varchar(1) DEFAULT NULL,
        DB1_Station_Code Varchar(5) DEFAULT NULL,
        Viewing_platform Int DEFAULT NULL,
        Date_of_Recording_DB1 Date DEFAULT NULL,
        Start_time_of_recording Int DEFAULT NULL,
        Person_1_viewing Int DEFAULT NULL,
        Person_2_viewing Int DEFAULT NULL,
        Person_3_viewing Int DEFAULT NULL,
        Person_4_viewing Int DEFAULT NULL,
        Person_5_viewing Int DEFAULT NULL,
        Person_6_viewing Int DEFAULT NULL,
        Person_7_viewing Int DEFAULT NULL,
        Person_8_viewing Int DEFAULT NULL,
        Person_9_viewing Int DEFAULT NULL,
        Person_10_viewing Int DEFAULT NULL,
        Person_11_viewing Int DEFAULT NULL,
        Person_12_viewing Int DEFAULT NULL,
        Person_13_viewing Int DEFAULT NULL,
        Person_14_viewing Int DEFAULT NULL,
        Person_15_viewing Int DEFAULT NULL,
        Person_16_viewing Int DEFAULT NULL,
        Interactive_Bar_Code_Identifier Int DEFAULT NULL)

CREATE TABLE BARB_PANEL_VIEWING_FILE_VIEWING_RECORD_GUESTS
        (file_creation_date date,
        file_creation_time time,
        file_type varchar(12),
        file_version int,
        filename varchar(13),
        Household_number Int DEFAULT NULL,
        Date_of_Activity_DB1 Date DEFAULT NULL,
        Set_number Int DEFAULT NULL,
        Start_time_of_session Int DEFAULT NULL,
        Duration_of_session Int DEFAULT NULL,
        Session_activity_type Int DEFAULT NULL,
        Playback_type Varchar(1) DEFAULT NULL,
        DB1_Station_Code Varchar(5) DEFAULT NULL,
        Viewing_platform Int DEFAULT NULL,
        Date_of_Recording_DB1 Date DEFAULT NULL,
        Start_time_of_recording Int DEFAULT NULL,
        Male_4_9 Int DEFAULT NULL,
        Male_10_15 Int DEFAULT NULL,
        Male_16_19 Int DEFAULT NULL,
        Male_20_24 Int DEFAULT NULL,
        Male_25_34 Int DEFAULT NULL,
        Male_35_44 Int DEFAULT NULL,
        Male_45_64 Int DEFAULT NULL,
        Male_65_plus Int DEFAULT NULL,
        Female_4_9 Int DEFAULT NULL,
        Female_10_15 Int DEFAULT NULL,
        Female_16_19 Int DEFAULT NULL,
        Female_20_24 Int DEFAULT NULL,
        Female_25_34 Int DEFAULT NULL,
        Female_35_44 Int DEFAULT NULL,
        Female_45_64 Int DEFAULT NULL,
        Female_65_plus Int DEFAULT NULL,
        Interactive_Bar_Code_Identifier Int DEFAULT NULL)
*/

INSERT INTO BARB_PANEL_VIEWING_FILE_HOME_CHARACTERISTICS
        (file_creation_date,
        file_creation_time,
        file_type,
        file_version,
        filename,
        Household_number,
        Date_Valid_For,
        Panel_membership_status,
        No_of_TV_Sets,
        No_of_VCRs,
        No_of_PVRs,
        No_of_DVDs,
        No_of_People,
        Social_Class,
        Presence_of_Children,
        Demographic_cell_1,
        BBC_Region_code,
        BBC_ITV_Area_Segment,
        S4C_Segment,
        Language_Spoken_at_Home,
        Welsh_Speaking_Home,
        Number_of_DVD_Recorders,
        Number_of_DVD_Players_not_recorders,
        Number_of_Sky_plus_PVRs,
        Number_of_other_PVRs,
        Broadband,
        BBC_Sub_Reporting_Region,
        )
SELECT  @file_creation_date,
        @file_creation_time,
        @file_type,
        @file_version,
        @filename,
        CAST(substr(imported_text,3,7) AS Int) AS Household_number,
        CAST(substr(imported_text,10,8) AS Int) AS Date_Valid_For,
        CAST(substr(imported_text,18,1) AS Int) AS Panel_membership_status,
        CAST(substr(imported_text,19,2) AS Int) AS No_of_TV_Sets,
        CAST(substr(imported_text,21,1) AS Int) AS No_of_VCRs,
        CAST(substr(imported_text,22,1) AS Int) AS No_of_PVRs,
        CAST(substr(imported_text,23,1) AS Int) AS No_of_DVDs,
        CAST(substr(imported_text,25,2) AS Int) AS No_of_People,
        substr(imported_text,27,2) AS Social_Class,
        CAST(substr(imported_text,31,1) AS Int) AS Presence_of_Children,
        CAST(substr(imported_text,32,2) AS Int) AS Demographic_cell_1,
        CAST(substr(imported_text,40,3) AS Int) AS BBC_Region_code,
        CAST(substr(imported_text,43,2) AS Int) AS BBC_ITV_Area_Segment,
        CAST(substr(imported_text,45,2) AS Int) AS S4C_Segment,
        CAST(substr(imported_text,47,1) AS Int) AS Language_Spoken_at_Home,
        CAST(substr(imported_text,48,1) AS Int) AS Welsh_Speaking_Home,
        CAST(substr(imported_text,49,1) AS Int) AS Number_of_DVD_Recorders,
        CAST(substr(imported_text,50,1) AS Int) AS Number_of_DVD_Players_not_recorders,
        CAST(substr(imported_text,51,1) AS Int) AS Number_of_Sky_plus_PVRs,
        CAST(substr(imported_text,52,1) AS Int) AS Number_of_other_PVRs,
        CAST(substr(imported_text,53,1) AS Int) AS Broadband,
        CAST(substr(imported_text,54,2) AS Int) AS BBC_Sub_Reporting_Region
FROM MN_BARB_IMPORT
WHERE substr(imported_text,1,2) = '02'

INSERT INTO BARB_PANEL_VIEWING_FILE_TV_SET_CHARACTERISTICS
        (file_creation_date,
        file_creation_time,
        file_type,
        file_version,
        filename,
        Household_number,
        Date_Valid_for_DB1,
        Set_Membership_Status,
        Set_number,
        Teletext,
        Main_Location,
        Analogue_Terrestrial,
        Digital_Terrestrial,
        Analogue_Satellite,
        Digital_Satellite,
        Analogue_Cable,
        Digital_Cable,
        Blank_for_future_platforms,
        VCR_present,
        Sky_plus_PVR_present,
        Other_PVR_present,
        DVD_Player_only_present,
        DVD_Recorder_present,
        HD_reception,
        Reception_Capability_Code_1,
        Reception_Capability_Code_2,
        Reception_Capability_Code_3,
        Reception_Capability_Code_4,
        Reception_Capability_Code_5,
        Reception_Capability_Code_6,
        Reception_Capability_Code_7,
        Reception_Capability_Code_8,
        Reception_Capability_Code_9,
        Reception_Capability_Code_10)
SELECT  @file_creation_date,
        @file_creation_time,
        @file_type,
        @file_version,
        @filename,
        CAST(substr(imported_text,3,7) AS Int) AS Household_number,
        CAST(substr(imported_text,10,8) AS Date) AS Date_Valid_for_DB1,
        CAST(substr(imported_text,18,1) AS Int) AS Set_Membership_Status,
        CAST(substr(imported_text,19,2) AS Int) AS Set_number,
        CAST(substr(imported_text,21,1) AS Int) AS Teletext,
        CAST(substr(imported_text,22,1) AS Int) AS Main_Location,
        CAST(substr(imported_text,23,1) AS Int) AS Analogue_Terrestrial,
        CAST(substr(imported_text,24,1) AS Int) AS Digital_Terrestrial,
        CAST(substr(imported_text,25,1) AS Int) AS Analogue_Satellite,
        CAST(substr(imported_text,26,1) AS Int) AS Digital_Satellite,
        CAST(substr(imported_text,27,1) AS Int) AS Analogue_Cable,
        CAST(substr(imported_text,28,1) AS Int) AS Digital_Cable,
        substr(imported_text,29,6) AS Blank_for_future_platforms,
        CAST(substr(imported_text,35,1) AS Int) AS VCR_present,
        CAST(substr(imported_text,36,1) AS Int) AS Sky_plus_PVR_present,
        CAST(substr(imported_text,37,1) AS Int) AS Other_PVR_present,
        CAST(substr(imported_text,38,1) AS Int) AS DVD_Player_only_present,
        CAST(substr(imported_text,39,1) AS Int) AS DVD_Recorder_present,
        CAST(substr(imported_text,40,1) AS Int) AS HD_reception,
        CAST(substr(imported_text,41,3) AS Int) AS Reception_Capability_Code_1,
        CAST(substr(imported_text,44,3) AS Int) AS Reception_Capability_Code_2,
        CAST(substr(imported_text,47,3) AS Int) AS Reception_Capability_Code_3,
        CAST(substr(imported_text,50,3) AS Int) AS Reception_Capability_Code_4,
        CAST(substr(imported_text,53,3) AS Int) AS Reception_Capability_Code_5,
        CAST(substr(imported_text,56,3) AS Int) AS Reception_Capability_Code_6,
        CAST(substr(imported_text,59,3) AS Int) AS Reception_Capability_Code_7,
        CAST(substr(imported_text,62,3) AS Int) AS Reception_Capability_Code_8,
        CAST(substr(imported_text,65,3) AS Int) AS Reception_Capability_Code_9,
        CAST(substr(imported_text,68,3) AS Int) AS Reception_Capability_Code_10
FROM MN_BARB_IMPORT
WHERE substr(imported_text,1,2) = '03'


INSERT INTO BARB_PANEL_VIEWING_FILE_INDIVIDAL_PANEL_MEMBER_DETAILS
        (file_creation_date,
        file_creation_time,
        file_type,
        file_version,
        filename,
        Household_number,
        Date_valid_for_DB1,
        Person_membership_status,
        Person_number,
        Sex_code,
        Date_of_birth,
        Marital_status,
        Household_status,
        Working_status,
        Terminal_age_of_education,
        Welsh_Language_code,
        Gaelic_language_code,
        Dependency_of_Children,
        Life_stage_12_classifications,
        Ethnic_Origin)
SELECT  @file_creation_date,
        @file_creation_time,
        @file_type,
        @file_version,
        @filename,
        CAST(substr(imported_text,3,7) AS Int) AS Household_number,
        CAST(substr(imported_text,10,8) AS Date) AS Date_valid_for_DB1,
        CAST(substr(imported_text,18,1) AS Int) AS Person_membership_status,
        CAST(substr(imported_text,19,2) AS Int) AS Person_number,
        CAST(substr(imported_text,21,1) AS Int) AS Sex_code,
        CAST(substr(imported_text,22,8) AS Date) AS Date_of_birth,
        CAST(substr(imported_text,30,1) AS Int) AS Marital_status,
        CAST(substr(imported_text,31,1) AS Int) AS Household_status,
        CAST(substr(imported_text,32,1) AS Int) AS Working_status,
        CAST(substr(imported_text,33,1) AS Int) AS Terminal_age_of_education,
        CAST(substr(imported_text,34,1) AS Int) AS Welsh_Language_code,
        CAST(substr(imported_text,35,1) AS Int) AS Gaelic_language_code,
        CAST(substr(imported_text,36,1) AS Int) AS Dependency_of_Children,
        CAST(substr(imported_text,37,2) AS Int) AS Life_stage_12_classifications,
        CAST(substr(imported_text,39,2) AS Int) AS Ethnic_Origin
FROM MN_BARB_IMPORT
WHERE substr(imported_text,1,2) = '04'


INSERT INTO BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY
        (file_creation_date,
        file_creation_time,
        file_type,
        file_version,
        filename,
        Household_Number,
        Person_Number,
        Reporting_Panel_Code,
        Date_of_Activity_DB1,
        Response_Code,
        Processing_Weight,
        Adults_Commercial_TV_Viewing_Sextile,
        ABC1_Adults_Commercial_TV_Viewing_Sextile,
        Adults_Total_Viewing_Sextile,
        ABC1_Adults_Total_Viewing_Sextile,
        Adults_16_34_Commercial_TV_Viewing_Sextile,
        Adults_16_34_Total_Viewing_Sextile)
SELECT  @file_creation_date,
        @file_creation_time,
        @file_type,
        @file_version,
        @filename,
        CAST(substr(imported_text,3,7) AS Int) AS Household_Number,
        CAST(substr(imported_text,10,2) AS Int) AS Person_Number,
        CAST(substr(imported_text,12,5) AS Int) AS Reporting_Panel_Code,
        CAST(substr(imported_text,17,8) AS Date) AS Date_of_Activity_DB1,
        CAST(substr(imported_text,25,1) AS Int) AS Response_Code,
        CAST(substr(imported_text,26,7) AS DECIMAL(11,4))/10000 AS Processing_Weight,
        CAST(substr(imported_text,33,1) AS Int) AS Adults_Commercial_TV_Viewing_Sextile,
        CAST(substr(imported_text,34,1) AS Int) AS ABC1_Adults_Commercial_TV_Viewing_Sextile,
        CAST(substr(imported_text,35,1) AS Int) AS Adults_Total_Viewing_Sextile,
        CAST(substr(imported_text,36,1) AS Int) AS ABC1_Adults_Total_Viewing_Sextile,
        CAST(substr(imported_text,37,1) AS Int) AS Adults_16_34_Commercial_TV_Viewing_Sextile,
        CAST(substr(imported_text,38,1) AS Int) AS Adults_16_34_Total_Viewing_Sextile
FROM MN_BARB_IMPORT
WHERE substr(imported_text,1,2) = '05'

INSERT INTO BARB_PANEL_VIEWING_FILE_VIEWING_RECORD_PANEL_MEMBERS
        (file_creation_date,
        file_creation_time,
        file_type,
        file_version,
        filename,
        Household_number,
        Date_of_Activity_DB1,
        Set_number,
        Start_time_of_session,
        Duration_of_session,
        Session_activity_type,
        Playback_type,
        DB1_Station_Code,
        Viewing_platform,
        Date_of_Recording_DB1,
        Start_time_of_recording,
        Person_1_viewing,
        Person_2_viewing,
        Person_3_viewing,
        Person_4_viewing,
        Person_5_viewing,
        Person_6_viewing,
        Person_7_viewing,
        Person_8_viewing,
        Person_9_viewing,
        Person_10_viewing,
        Person_11_viewing,
        Person_12_viewing,
        Person_13_viewing,
        Person_14_viewing,
        Person_15_viewing,
        Person_16_viewing,
        Interactive_Bar_Code_Identifier)
SELECT  @file_creation_date,
        @file_creation_time,
        @file_type,
        @file_version,
        @filename,
        CAST(substr(imported_text,3,7) AS Int) AS Household_number,
        CAST(substr(imported_text,10,8) AS Date) AS Date_of_Activity_DB1,
        CAST(substr(imported_text,18,2) AS Int) AS Set_number,
        CAST(substr(imported_text,20,4) AS Int) AS Start_time_of_session,
        CAST(substr(imported_text,24,4) AS Int) AS Duration_of_session,
        CAST(substr(imported_text,28,2) AS Int) AS Session_activity_type,
        substr(imported_text,30,1) AS Playback_type,
        substr(imported_text,31,5) AS DB1_Station_Code,
        CAST(substr(imported_text,36,1) AS Int) AS Viewing_platform,
        CAST(substr(imported_text,37,8) AS Date) AS Date_of_Recording_DB1,
        CAST(substr(imported_text,45,4) AS Int) AS Start_time_of_recording,
        CAST(substr(imported_text,49,1) AS Int) AS Person_1_viewing,
        CAST(substr(imported_text,50,1) AS Int) AS Person_2_viewing,
        CAST(substr(imported_text,51,1) AS Int) AS Person_3_viewing,
        CAST(substr(imported_text,52,1) AS Int) AS Person_4_viewing,
        CAST(substr(imported_text,53,1) AS Int) AS Person_5_viewing,
        CAST(substr(imported_text,54,1) AS Int) AS Person_6_viewing,
        CAST(substr(imported_text,55,1) AS Int) AS Person_7_viewing,
        CAST(substr(imported_text,56,1) AS Int) AS Person_8_viewing,
        CAST(substr(imported_text,57,1) AS Int) AS Person_9_viewing,
        CAST(substr(imported_text,58,1) AS Int) AS Person_10_viewing,
        CAST(substr(imported_text,59,1) AS Int) AS Person_11_viewing,
        CAST(substr(imported_text,60,1) AS Int) AS Person_12_viewing,
        CAST(substr(imported_text,61,1) AS Int) AS Person_13_viewing,
        CAST(substr(imported_text,62,1) AS Int) AS Person_14_viewing,
        CAST(substr(imported_text,63,1) AS Int) AS Person_15_viewing,
        CAST(substr(imported_text,64,1) AS Int) AS Person_16_viewing,
        CAST(substr(imported_text,65,9) AS Int) AS Interactive_Bar_Code_Identifier
FROM MN_BARB_IMPORT
WHERE substr(imported_text,1,2) = '06'

INSERT INTO BARB_PANEL_VIEWING_FILE_VIEWING_RECORD_GUESTS
        (file_creation_date,
        file_creation_time,
        file_type,
        file_version,
        filename,
        Household_number,
        Date_of_Activity_DB1,
        Set_number,
        Start_time_of_session,
        Duration_of_session,
        Session_activity_type,
        Playback_type,
        DB1_Station_Code,
        Viewing_platform,
        Date_of_Recording_DB1,
        Start_time_of_recording,
        Male_4_9,
        Male_10_15,
        Male_16_19,
        Male_20_24,
        Male_25_34,
        Male_35_44,
        Male_45_64,
        Male_65_plus,
        Female_4_9,
        Female_10_15,
        Female_16_19,
        Female_20_24,
        Female_25_34,
        Female_35_44,
        Female_45_64,
        Female_65_plus,
        Interactive_Bar_Code_Identifier)
SELECT  @file_creation_date,
        @file_creation_time,
        @file_type,
        @file_version,
        @filename,
        CAST(substr(imported_text,3,7) AS Int) AS Household_number,
        CAST(substr(imported_text,10,8) AS Date) AS Date_of_Activity_DB1,
        CAST(substr(imported_text,18,2) AS Int) AS Set_number,
        CAST(substr(imported_text,20,4) AS Int) AS Start_time_of_session,
        CAST(substr(imported_text,24,4) AS Int) AS Duration_of_session,
        CAST(substr(imported_text,28,2) AS Int) AS Session_activity_type,
        substr(imported_text,30,1) AS Playback_type,
        substr(imported_text,31,5) AS DB1_Station_Code,
        CAST(substr(imported_text,36,1) AS Int) AS Viewing_platform,
        CAST(substr(imported_text,37,8) AS Date) AS Date_of_Recording_DB1,
        CAST(substr(imported_text,45,4) AS Int) AS Start_time_of_recording,
        CAST(substr(imported_text,49,2) AS Int) AS Male_4_9,
        CAST(substr(imported_text,51,2) AS Int) AS Male_10_15,
        CAST(substr(imported_text,53,2) AS Int) AS Male_16_19,
        CAST(substr(imported_text,55,2) AS Int) AS Male_20_24,
        CAST(substr(imported_text,57,2) AS Int) AS Male_25_34,
        CAST(substr(imported_text,59,2) AS Int) AS Male_35_44,
        CAST(substr(imported_text,61,2) AS Int) AS Male_45_64,
        CAST(substr(imported_text,63,2) AS Int) AS Male_65_plus,
        CAST(substr(imported_text,65,2) AS Int) AS Female_4_9,
        CAST(substr(imported_text,67,2) AS Int) AS Female_10_15,
        CAST(substr(imported_text,69,2) AS Int) AS Female_16_19,
        CAST(substr(imported_text,71,2) AS Int) AS Female_20_24,
        CAST(substr(imported_text,73,2) AS Int) AS Female_25_34,
        CAST(substr(imported_text,75,2) AS Int) AS Female_35_44,
        CAST(substr(imported_text,77,2) AS Int) AS Female_45_64,
        CAST(substr(imported_text,79,2) AS Int) AS Female_65_plus,
        CAST(substr(imported_text,81,9) AS Int) AS Interactive_Bar_Code_Identifier
FROM MN_BARB_IMPORT
WHERE substr(imported_text,1,2) = '07'

SELECT 'Import complete'

END
