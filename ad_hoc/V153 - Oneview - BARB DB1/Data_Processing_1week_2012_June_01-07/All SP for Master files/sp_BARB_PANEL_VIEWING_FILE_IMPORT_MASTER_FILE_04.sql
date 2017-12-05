
/*Analyst: Patrick Igonor
 Date : 08th of July 2013
 Lead : Claudio Lima
 */

CREATE TABLE "PI_BARB_IMPORT_MASTER_04"("imported_text" varchar (10000));

--Creating the tables of Interest in other to obtain the DB1_Station_Code and the Log_Station_Code ----

CREATE TABLE "BARB_DB1_Stations_Reporting_Record_04"
                ("file_creation_date" date,
                "file_creation_time" time,
                "file_type" varchar(12),
                "file_version" int,
                "filename" varchar(13),
                "DB1_Station_Code" Int DEFAULT NULL,
                "DB2_Station_Code" Int DEFAULT NULL,
                "DB1_Station_Name" Varchar(30) DEFAULT NULL,
                "DB1_Station_Medium_Name" Varchar(15) DEFAULT NULL,
                "DB1_Station_Short_Name" Varchar(8) DEFAULT NULL,
                "Exclude_from_Total_TV" Varchar(1),
                "Exclude_from_Comm_TV" Varchar(1),
                "Area_Geography" Int DEFAULT NULL,
                "Transmission_Format" Int DEFAULT NULL,
                "Reporting_Start_Date" Int DEFAULT NULL,
                "Reportng_End_Date" Int DEFAULT NULL)
;

CREATE TABLE "BARB_Log_Station_Relationship_to_DB1_Station_Record_04"
                ("file_creation_date" date,
                "file_creation_time" time,
                "file_type" varchar(12),
                "file_version" int,
                "filename" varchar(13),
                "Log_Station_Code" Int DEFAULT NULL,
                "DB1_Station_Code" Int DEFAULT NULL,
                "Relationship_Start_Date" Varchar(8)DEFAULT NULL,
                "Relationship_End_Date" Varchar(8)DEFAULT NULL)
;

CREATE TABLE "BARB_Split_Stations_Reporting_Record_04"
        ("file_creation_date" date,
        "file_creation_time" time,
        "file_type" varchar(12),
        "file_version" int,
        "filename" varchar(13),
        "Log_Station_Code" Int DEFAULT NULL,
        "Split_Transmission_Indicator" Int DEFAULT NULL,
        "Split_Station_Name" Varchar(30) DEFAULT NULL,
        "Split_Station_Short_Name" Varchar(8) DEFAULT NULL,
        "Split_Station_15_Char_Name" Varchar(15) DEFAULT NULL,
        "Split_Area_Factor" Int DEFAULT NULL,
        "Reporting_Start_Date" Varchar(8) DEFAULT NULL,
        "Reportng_End_Date" Varchar(8) DEFAULT NULL,
        "Panel_Code" Int DEFAULT NULL)
;

create PROCEDURE sp_PI_BARB_IMPORT_MASTER_04 (@in_filename varchar(60)) AS

BEGIN


DECLARE @query varchar(3000)
DECLARE @file_creation_date date
DECLARE @file_creation_time time
DECLARE @file_type  varchar(12)
DECLARE @File_Version Int
DECLARE @audit_row_count bigint
DECLARE @filename varchar(13)

DELETE FROM PI_BARB_IMPORT_MASTER_04
-- import raw data

SET @query = 'LOAD TABLE PI_BARB_IMPORT_MASTER_04 (imported_text '
SET @query = @query || ' ''\n'' ) '
SET @query = @query || ' FROM ''/ETL013/prod/sky/olive/data/share/clarityq/export/' || @in_filename || ''' QUOTES OFF ESCAPES OFF NOTIFY 1000'


EXECUTE (@query)

SET @audit_row_count = (Select count(1) from PI_BARB_IMPORT_MASTER_04)

-- file doesn't exist

IF @audit_row_count = 0
BEGIN
        SELECT now(), @file_creation_Date, @file_creation_time, @file_type, @file_version, @filename,'File not found ' || @in_filename, @audit_row_count

        RETURN
END

IF @file_type <> 'DSP14.V03.02'
BEGIN

        SELECT now(), @file_creation_Date, @file_creation_time, @file_type, @file_version, @filename,'File type DSP14.V03.02, file type imported ' || @file_type, 0

        RETURN
END

-- Data records


SET @file_creation_date = (SELECT CAST(substr(imported_text,7,8) AS Date)
                                FROM PI_BARB_IMPORT_MASTER_04
                                WHERE substr(imported_text,1,2) = '01')

SET @file_creation_time = (SELECT CAST(substr(imported_text,15,2) || ':' || substr(imported_text,17,2) || ':' || substr(imported_text,19,2)  AS Time)
                                FROM PI_BARB_IMPORT_MASTER_04
                                WHERE substr(imported_text,1,2) = '01')

SET @file_type = (SELECT substr(imported_text,21,12)
                                FROM PI_BARB_IMPORT_MASTER_04
                                WHERE substr(imported_text,1,2) = '01')

SET @File_Version = (SELECT CAST(substr(imported_text,33,3) AS Int)
                                FROM PI_BARB_IMPORT_MASTER_04
                                WHERE substr(imported_text,1,2) = '01')

SET @Filename = (SELECT substr(imported_text,36,13)
                                FROM PI_BARB_IMPORT_MASTER_04
                                WHERE substr(imported_text,1,2) = '01')



---Inserting data into the tables created above ----

INSERT INTO BARB_DB1_Stations_Reporting_Record_04
                (file_creation_date,
                file_creation_time,
                file_type,
                file_version,
                filename,
                DB1_Station_Code,
                DB2_Station_Code,
                DB1_Station_Name,
                DB1_Station_Medium_Name,
                DB1_Station_Short_Name,
                Exclude_from_Total_TV,
                Exclude_from_Comm_TV,
                Area_Geography,
                Transmission_Format,
                Reporting_Start_Date,
                Reportng_End_Date)
SELECT  @file_creation_date,
        @file_creation_time,
        @file_type,
        @file_version,
        @filename,
        CAST(substr(imported_text,3,5) AS Int) AS DB1_Station_Code,
        CAST(substr(imported_text,8,5) AS Int) AS DB2_Station_Code,
        substr(imported_text,13,30) AS DB1_Station_Name,
        substr(imported_text,43,15) AS DB1_Station_Medium_Name,
        substr(imported_text,58,8) AS DB1_Station_Short_Name,
        substr(imported_text,66,1) AS Exclude_from_Total_TV,
        substr(imported_text,67,1) AS Exclude_from_Comm_TV,
        CAST(substr(imported_text,68,1) AS Int) AS Area_Geography,
        CAST(substr(imported_text,73,2) AS Int) AS Transmission_Format,
        CAST(substr(imported_text,74,8) AS Int) AS Reporting_Start_Date,
        CAST(substr(imported_text,82,8) AS Int) AS Reportng_End_Date
FROM PI_BARB_IMPORT_MASTER_04
WHERE substr(imported_text,1,2) = '02'

INSERT INTO BARB_Log_Station_Relationship_to_DB1_Station_Record_04
                (file_creation_date,
                file_creation_time,
                file_type,
                file_version,
                filename,
                Log_Station_Code,
                DB1_Station_Code,
                Relationship_Start_Date,
                Relationship_End_Date)
SELECT  @file_creation_date,
        @file_creation_time,
        @file_type,
        @file_version,
        @filename,
        CAST(substr(imported_text,3,5) AS Int) AS Log_Station_Code,
        CAST(substr(imported_text,8,5) AS Int) AS DB1_Station_Code,
        substr(imported_text,13,8) AS Relationship_Start_Date,
        substr(imported_text,21,8) AS Relationship_End_Date
FROM PI_BARB_IMPORT_MASTER_04
WHERE substr(imported_text,1,2) = '05'


INSERT INTO BARB_Split_Stations_Reporting_Record_04
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
        Reportng_End_Date,
        Panel_Code)
SELECT  @file_creation_date,
        @file_creation_time,
        @file_type,
        @file_version,
        @filename,
        CAST(substr(imported_text,3,5) AS Int) AS Log_Station_Code,
        CAST(substr(imported_text,8,2) AS Int) AS Split_Transmission_Indicator,
        substr(imported_text,10,30) AS Split_Station_Name,
        substr(imported_text,40,8) AS Split_Station_Short_Name,
        substr(imported_text,48,15) AS Split_Station_15_Char_Name,
        CAST(substr(imported_text,63,6) AS Int) AS Split_Area_Factor,
        substr(imported_text,69,8) AS Reporting_Start_Date,
        substr(imported_text,77,8) AS Reporting_End_Date,
        CAST(substr(imported_text,85,5) AS Int) AS Panel_Code
FROM PI_BARB_IMPORT_MASTER_04
WHERE substr(imported_text,1,2) = '06'

SELECT 'Import complete'

END

--Running the Stored procedure i.e sp_PI_BARB_IMPORT_MASTER_04

EXEC sp_PI_BARB_IMPORT_MASTER_04 'PatrickI/MAS_Files/B20120604.MAS'

select top 10* from BARB_Log_Station_Relationship_to_DB1_Station_Record_04
select count(*) from BARB_Log_Station_Relationship_to_DB1_Station_Record_04
