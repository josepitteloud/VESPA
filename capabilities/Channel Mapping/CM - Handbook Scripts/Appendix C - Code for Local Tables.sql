/* Appendix C – Code for Local Tables
This is the code to create the basic tables and the stored procedure in your schema.
*/

-- audit table for channel map validation (see Diagnostic Checks)

CREATE TABLE "channel_map_updates" (
        "audit_time"         timestamp DEFAULT NULL,
        "version"            int DEFAULT NULL,
        "test_ref"           int DEFAULT NULL,
        "test_area"          varchar(255) DEFAULT NULL,
        "test"               varchar(255) DEFAULT NULL,
        "result"             int DEFAULT NULL
);

-- BARB master file tables.  
CREATE TABLE "BARB_MASTER_FILE_LOG_STATIONS_REPORTING_RECORD" (
        "File_Creation_date" date DEFAULT NULL,
        "File_Creation_time" time DEFAULT NULL,
        "File_Type"          varchar(12) DEFAULT NULL,
        "File_Version"       int DEFAULT NULL,
        "Filename"           varchar(13) DEFAULT NULL,
        "Log_Station_Code"   int DEFAULT NULL,
        "DB2_Station_Code"   int DEFAULT NULL,
        "Log_Station_Name"   varchar(30) DEFAULT NULL,
        "Log_Station_Short_Name" varchar(8) DEFAULT NULL,
        "Log_Station_15_Char_Name" varchar(15) DEFAULT NULL,
        "Area_Geography"     int DEFAULT NULL,
        "Area_Flags"         varchar(4) DEFAULT NULL,
        "Primary_Reporting_Panel_Code" int DEFAULT NULL,
        "Reporting_Start_Date" date DEFAULT NULL,
        "Reporting_End_Date" date DEFAULT NULL,
        "Sales_House_1"      int DEFAULT NULL,
        "Sales_House_2"      int DEFAULT NULL,
        "Sales_House_3"      int DEFAULT NULL,
        "Sales_House_4"      int DEFAULT NULL,
        "Sales_House_5"      int DEFAULT NULL,
        "Sales_House_6"      int DEFAULT NULL,
        "Broadcast_Group_Id" int DEFAULT NULL,
        "Station_Genre_Type" varchar(5) DEFAULT NULL
);

CREATE TABLE "BARB_MASTER_FILE_SPLIT_STATIONS_REPORTING_RECORD" (
        "File_Creation_date" date DEFAULT NULL,
        "File_Creation_time" time DEFAULT NULL,
        "File_Type"          varchar(12) DEFAULT NULL,
        "File_Version"       int DEFAULT NULL,
        "Filename"           varchar(13) DEFAULT NULL,
        "Log_Station_Code"   int DEFAULT NULL,
        "Split_Transmission_Indicator" int DEFAULT NULL,
        "Split_Station_Name" varchar(30) DEFAULT NULL,
        "Split_Station_Short_Name" varchar(8) DEFAULT NULL,
        "Split_Station_15_Char_Name" varchar(15) DEFAULT NULL,
        "Split_Area_Factor"  numeric(6, 5) DEFAULT NULL,
        "Reporting_Start_Date" date DEFAULT NULL,
        "Reporting_End_Date" date DEFAULT NULL,
        "Panel_Code"         int DEFAULT NULL
);

REATE TABLE BARB_MASTER_FILE_BROADCAST_GROUP_RECORD(
    File_Creation_date   date DEFAULT NULL,
    File_Creation_time   time DEFAULT NULL,
    File_Type            varchar(12) DEFAULT NULL,
    File_Version         integer DEFAULT NULL,
    Filename             varchar(13) DEFAULT NULL,
    Broadcast_Group_Id   integer DEFAULT NULL,
    Broadcast_Group_Name varchar(30) DEFAULT NULL,
    Broadcast_Group_Short_Name varchar(8) DEFAULT NULL,
    Broadcast_Group_15_Char_Name varchar(15) DEFAULT NULL,
    Reporting_Start_Date date DEFAULT NULL,
    Reporting_End_Date   date DEFAULT NULL
)

CREATE TABLE BARB_MASTER_FILE_PANEL_REPORTING_RECORD(
    File_Creation_date   date DEFAULT NULL,
    File_Creation_time   time DEFAULT NULL,
    File_Type            varchar(12) DEFAULT NULL,
    File_Version         integer DEFAULT NULL,
    Filename             varchar(13) DEFAULT NULL,
    Panel_Code           integer DEFAULT NULL,
    Panel_Name           varchar(30) DEFAULT NULL,
    Panel_Medium_Name    varchar(15) DEFAULT NULL,
    Panel_Short_Name     varchar(8) DEFAULT NULL,
    Macro_region         varchar(1) DEFAULT NULL,
    Used_in_DB2          varchar(1) DEFAULT NULL,
    Panel_Start_Date     date DEFAULT NULL,
    Panel_End_Date       date DEFAULT NULL
)

CREATE TABLE BARB_MASTER_FILE_SALES_HOUSE_RECORD(
    File_Creation_date   date DEFAULT NULL,
    File_Creation_time   time DEFAULT NULL,
    File_Type            varchar(12) DEFAULT NULL,
    File_Version         integer DEFAULT NULL,
    Filename             varchar(13) DEFAULT NULL,
    Sales_House_Identifier integer DEFAULT NULL,
    Sales_House_Name     varchar(30) DEFAULT NULL,
    Sales_House_Short_Name varchar(8) DEFAULT NULL,
    Sales_House_15_Char_Name varchar(15) DEFAULT NULL,
    Reporting_Start_Date date DEFAULT NULL,
    Reporting_End_Date   date DEFAULT NULL
)

CREATE TABLE "MN_BARB_import" (
        "imported_text"      varchar(8000) DEFAULT NULL
);

-- stored procedure to import BARB master file data.  
create PROCEDURE sp_BARB_MASTER_FILE_IMPORT (@in_filename varchar(60)) AS
BEGIN
DECLARE @file_creation_date date
DECLARE @file_creation_time time
DECLARE @file_type  varchar(12)
DECLARE @File_Version Int
DECLARE @audit_row_count bigint
DECLARE @query varchar(3000)
DECLARE @filename varchar(13)

-- clear down the data first
DELETE FROM MN_BARB_import
DELETE FROM BARB_MASTER_FILE_LOG_STATIONS_REPORTING_RECORD
DELETE FROM BARB_MASTER_FILE_SPLIT_STATIONS_REPORTING_RECORD
DELETE FROM BARB_MASTER_FILE_PANEL_REPORTING_RECORD
DELETE FROM BARB_MASTER_FILE_SALES_HOUSE_RECORD
DELETE FROM BARB_MASTER_FILE_BROADCAST_GROUP_RECORD
-- import the raw data as a string
SET @query = 'LOAD TABLE MN_BARB_import (imported_text '
SET @query = @query || ' ''\n'' ) '
SET @query = @query || ' FROM ''/ETL013/prod/sky/olive/data/share/clarityq/export/' || @in_filename || ''' QUOTES OFF ESCAPES OFF NOTIFY 1000'

EXECUTE (@query)

SET @audit_row_count = (Select count(1) from MN_BARB_IMPORT)
-- if file doesn't exist
IF @audit_row_count = 0
BEGIN
        SELECT now(), @file_creation_Date, @file_creation_time, @file_type, @file_version, @filename,'File not found ' || @in_filename, @audit_row_count
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
-- wrong format (this may change – check the BARB file)
-- IF @file_type <> 'DSP14.V04.01'
IF @file_type <> 'DSP14.V04.02'
BEGIN
        SELECT now(), @file_creation_Date, @file_creation_time, @file_type, @file_version, @filename,'File type DSP14.V03.03, file type imported ' || @file_type, 0
        RETURN
END
--Populate the tables, we are interested in sections 04, 06, 07, 10 and 11 from Master file
      INSERT INTO BARB_MASTER_FILE_LOG_STATIONS_REPORTING_RECORD
                (file_creation_date,
                file_creation_time,
                file_type,
                file_version,
                filename,
                Log_Station_Code,
                DB2_Station_Code,
                Log_Station_Name,
                Log_Station_Short_Name,
                Log_Station_15_Char_Name,
                Area_Geography,
                Area_Flags,
                Primary_Reporting_Panel_Code,
                Reporting_Start_Date,
                Reporting_End_Date,
                Sales_House_1,
                Sales_House_2,
                Sales_House_3,
                Sales_House_4,
                Sales_House_5,
                Sales_House_6,
                Broadcast_Group_Id,
                Station_Genre_Type)
        SELECT
                @file_creation_date,
                @file_creation_time,
                @file_type,
                @file_version,
                @filename,
                CAST(substr(imported_text,3,5) AS Int) AS Log_Station_Code,
                CAST(substr(imported_text,8,5) AS Int) AS DB2_Station_Code,
                substr(imported_text,13,30) AS Log_Station_Name,
                substr(imported_text,43,8) AS Log_Station_Short_Name,
                substr(imported_text,51,15) AS Log_Station_15_Char_Name,
                CAST(substr(imported_text,66,1) AS Int) AS Area_Geography,
                substr(imported_text,67,4) AS Area_Flags,
                CAST(substr(imported_text,71,5) AS Int) AS Primary_Reporting_Panel_Code,
                CAST(substr(imported_text,76,8) AS Date) AS Reporting_Start_Date,
                CAST(substr(imported_text,84,8) AS Date) AS Reporting_End_Date,
                CAST(substr(imported_text,92,5) AS Int) AS Sales_House_1,
                CAST(substr(imported_text,97,5) AS Int) AS Sales_House_2,
                CAST(substr(imported_text,102,5) AS Int) AS Sales_House_3,
                CAST(substr(imported_text,107,5) AS Int) AS Sales_House_4,
                CAST(substr(imported_text,112,5) AS Int) AS Sales_House_5,
                CAST(substr(imported_text,117,5) AS Int) AS Sales_House_6,
                CAST(substr(imported_text,122,5) AS Int) AS Broadcast_Group_Id,
                substr(imported_text,127,5) AS Station_Genre_Type
        FROM MN_BARB_import
        WHERE substr(imported_text,1,2) = '04'

      INSERT INTO BARB_MASTER_FILE_SPLIT_STATIONS_REPORTING_RECORD
                (file_creation_date,
                file_creation_time,
                file_type,
                file_version,
                filename,
                Log_Station_Code,
                Split_Transmission_Indicator,
                Split_Station_Name,
                Split_Station_Short_Name,
                Split_Station_15_Char_Name,
                Split_Area_Factor,
                Reporting_Start_Date,
                Reporting_End_Date,
                Panel_Code)
        SELECT
                @file_creation_date,
                @file_creation_time,
                @file_type,
                @file_version,
                @filename,
                CAST(substr(imported_text,3,5) AS Int) AS Log_Station_Code,
                CAST(substr(imported_text,8,2) AS Int) AS Split_Transmission_Indicator,
                substr(imported_text,10,30) AS Split_Station_Name,
                substr(imported_text,40,8) AS Split_Station_Short_Name,
                substr(imported_text,48,15) AS Split_Station_15_Char_Name,
                CAST(substr(imported_text,63,6) AS decimal(6,5)) AS Split_Area_Factor,
                CAST(substr(imported_text,69,8) AS Date) AS Reporting_Start_Date,
                CAST(substr(imported_text,77,8) AS Date) AS Reporting_End_Date,
                CAST(substr(imported_text,85,5) AS Int) AS Panel_Code
        FROM MN_BARB_import
        WHERE substr(imported_text,1,2) = '06'

INSERT INTO BARB_MASTER_FILE_PANEL_REPORTING_RECORD
                (file_creation_date,
                file_creation_time,
                file_type,
                file_version,
                filename,
                Panel_Code,
                Panel_Name,
                Panel_Medium_Name,
                Panel_Short_Name,
                Macro_region,
                Used_in_DB2,
                Panel_Start_Date,
                Panel_End_Date)
        SELECT
                @file_creation_date,
                @file_creation_time,
                @file_type,
                @file_version,
                @filename,
                CAST(substr(imported_text,3,5) AS Int) AS Panel_Code,
                substr(imported_text,8,30) AS Panel_Name,
                substr(imported_text,38,15) AS Panel_Medium_Name,
                substr(imported_text,53,8) AS Panel_Short_Name,
                substr(imported_text,61,1) AS Macro_region,
                substr(imported_text,62,1) AS Used_in_DB2,
                CAST(substr(imported_text,63,8) AS Date) AS Panel_Start_Date,
                CAST(substr(imported_text,71,8) AS Date) AS Panel_End_Date
        FROM MN_BARB_import
        WHERE substr(imported_text,1,2) = '07'

      INSERT INTO BARB_MASTER_FILE_SALES_HOUSE_RECORD
                (file_creation_date,
                file_creation_time,
                file_type,
                file_version,
                filename,
                Sales_House_Identifier,
                Sales_House_Name,
                Sales_House_Short_Name,
                Sales_House_15_Char_Name,
                Reporting_Start_Date,
                Reporting_End_Date)
        SELECT
                @file_creation_date,
                @file_creation_time,
                @file_type,
                @file_version,
                @filename,
                CAST(substr(imported_text,3,5) AS Int) AS Sales_House_Identifier,
                substr(imported_text,8,30) AS Sales_House_Name,
                substr(imported_text,38,8) AS Sales_House_Short_Name,
                substr(imported_text,46,15) AS Sales_House_15_Char_Name,
                CAST(substr(imported_text,61,8) AS Date) AS Reporting_Start_Date,
                CAST(substr(imported_text,69,8) AS Date) AS Reporting_End_Date
        FROM MN_BARB_import
        WHERE substr(imported_text,1,2) = '10'

      INSERT INTO BARB_MASTER_FILE_BROADCAST_GROUP_RECORD
                (file_creation_date,
                file_creation_time,
                file_type,
                file_version,
                filename,
                Broadcast_Group_Id,
                Broadcast_Group_Name,
                Broadcast_Group_Short_Name,
                Broadcast_Group_15_Char_Name,
                Reporting_Start_Date,
                Reporting_End_Date)
        SELECT
                @file_creation_date,
                @file_creation_time,
                @file_type,
                @file_version,
                @filename,
                CAST(substr(imported_text,3,5) AS Int) AS Broadcast_Group_Id,
                substr(imported_text,8,30) AS Broadcast_Group_Name,
                substr(imported_text,38,8) AS Broadcast_Group_Short_Name,
                substr(imported_text,46,15) AS Broadcast_Group_15_Char_Name,
                CAST(substr(imported_text,61,8) AS Date) AS Reporting_Start_Date,
                CAST(substr(imported_text,69,8) AS Date) AS Reporting_End_Date
        FROM MN_BARB_import
        WHERE substr(imported_text,1,2) = '11'
END
