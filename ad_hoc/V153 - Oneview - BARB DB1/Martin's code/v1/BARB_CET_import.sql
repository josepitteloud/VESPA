create PROCEDURE BARB_CET_IMPORT (@in_filename varchar(60)) AS

-- Martin Neighbours
-- 19/9/12

-- BARB CET file.  Note that this is only for the import of the original file
-- and is intended to process one day at a time, hence it will clear down the input files first
-- note that creation date, time, file type, version and name are in the header record
-- This code will not adjust for BST in 2013, so will need to updated if still used then

/*
2013-02-04
Added in channel map version into updates

Amend to deal with the end of british summer time - barb clock goes up to 30
Amend to deal with duplicates - base on utc_spot_start_date_time as local will potentially not work
*/

BEGIN

DECLARE @file_creation_date date
DECLARE @file_creation_time time
DECLARE @file_type  varchar(12)
DECLARE @File_Version Int
DECLARE @audit_row_count bigint
DECLARE @query varchar(3000)
DECLARE @filename varchar(13)

--DECLARE @filename varchar(13)
--SET @filename = 'B20120227.CE3'

-- clear down the data first

DELETE FROM MN_BARB_import
DELETE FROM BARB_CET

-- import the raw data as a string
SET @query = 'LOAD TABLE MN_BARB_import (imported_text '
SET @query = @query || ' ''\n'' ) '
SET @query = @query || ' FROM ''/ETL013/prod/sky/olive/data/share/clarityq/export/' || @in_filename || ''' QUOTES OFF ESCAPES OFF NOTIFY 1000'

EXECUTE (@query)

SET @audit_row_count = (Select count(1) from MN_BARB_IMPORT)

-- file doesn't exist

IF @audit_row_count = 0
BEGIN
        INSERT INTO BARB_CET_AUDIT_LOG
                (Audit_time_stamp,
                File_Creation_date,
                File_Creation_time,
                File_Type,
                File_Version,
                Filename,
                Audit_action,
                Audit_count
                )
        SELECT now(), @file_creation_Date, @file_creation_time, @file_type, @file_version, @filename,'File not found ' || @in_filename, @audit_row_count

        SELECT *
        FROM BARB_CET_AUDIT_LOG
        WHERE Audit_action = 'File not found ' || @in_filename

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

-- already imported

IF EXISTS  (SELECT distinct filename FROM BARB_CET_LOG
        WHERE filename = @filename)
BEGIN
        INSERT INTO BARB_CET_AUDIT_LOG
                (Audit_time_stamp,
                File_Creation_date,
                File_Creation_time,
                File_Type,
                File_Version,
                Filename,
                Audit_action,
                Audit_count
                )
        SELECT now(), @file_creation_Date, @file_creation_time, @file_type, @file_version, @filename,'File already been imported', 0

        SELECT *
        FROM BARB_CET_AUDIT_LOG
        WHERE filename = @filename

        RETURN
END

-- wrong format

IF @file_type <> 'DSP10.V03.03'
BEGIN
        INSERT INTO BARB_CET_AUDIT_LOG
                (Audit_time_stamp,
                File_Creation_date,
                File_Creation_time,
                File_Type,
                File_Version,
                Filename,
                Audit_action,
                Audit_count
                )
        SELECT now(), @file_creation_Date, @file_creation_time, @file_type, @file_version, @filename,'File type DSP10.V03.03, file type imported ' || @file_type, 0

        SELECT *
        FROM BARB_CET_AUDIT_LOG
        WHERE filename = @filename

        RETURN
END

-- Log the imported data

INSERT INTO BARB_CET_AUDIT_LOG
        (Audit_time_stamp,
        File_Creation_date,
        File_Creation_time,
        File_Type,
        File_Version,
        Filename,
        Audit_action,
        Audit_count
        )
SELECT now(), @file_creation_Date, @file_creation_time, @file_type, @file_version, @filename,'Raw data import', @audit_row_count

SET @audit_row_count = 0


        INSERT INTO BARB_CET
                (file_creation_date,
                file_creation_time,
                file_type,
                file_version,
                filename,
                Insert_delete_amend_code,
                Date_of_Transmission,
                Reporting_panel_code,
                Log_Station_Code_for_Break,
                Break_Split_Transmission_Indicator,
                Break_Platform_Indicator,
                Break_Start_Time,
                Spot_Break_Total_Duration,
                Break_Type,
                Spot_Type,
                Broadcasters_Spot_Number,
                Station_code,
                Log_Station_code_for_spot,
                Split_Transmission_Indicator,
                Spot_Platform_Indicator,
                HD_Simulcast_Spot_Platform_Indicator,
                Spot_start_time,
                Spot_duration,
                Clearcast_Commercial_No,
                Sales_House_Brand_Description,
                Preceding_Programme_Name,
                Succeeding_Programme_Name,
                Sales_House_Identifier,
                Campaign_Approval_ID,
                Campaign_Approval_ID_version_number,
                Interactive_Spot_Platform_Indicator,
                local_Break_Start_date_Time,
                local_date_of_transmission,
                local_spot_Start_date_Time)
        SELECT
                @file_creation_date,
                @file_creation_time,
                @file_type,
                @file_version,
                @filename,
                substr(imported_text,3,1) AS Insert_delete_amend_code,
                CAST(substr(imported_text,4,8) AS Date) AS Date_of_Transmission,
                CAST(substr(imported_text,12,5) AS Int) AS Reporting_panel_code,
                CAST(substr(imported_text,17,5) AS Int) AS Log_Station_Code_for_Break,
                CAST(substr(imported_text,22,2) AS Int) AS Break_Split_Transmission_Indicator,
                substr(imported_text,24,2) AS Break_Platform_Indicator,
                substr(imported_text,26,6) AS Break_Start_Time,
                CAST(substr(imported_text,32,5) AS Int) AS Spot_Break_Total_Duration,
                substr(imported_text,37,2) AS Break_Type,
                substr(imported_text,39,2) AS Spot_Type,
                CAST(substr(imported_text,41,12) AS Int) AS Broadcasters_Spot_Number,
                CAST(substr(imported_text,53,5) AS Int) AS Station_code,
                CAST(substr(imported_text,58,5) AS Int) AS Log_Station_code_for_spot,
                CAST(substr(imported_text,63,2) AS Int) AS Split_Transmission_Indicator,
                substr(imported_text,65,2) AS Spot_Platform_Indicator,
                substr(imported_text,67,2) AS HD_Simulcast_Spot_Platform_Indicator,
                substr(imported_text,69,6) AS Spot_start_time,
                CAST(substr(imported_text,75,5) AS Int) AS Spot_duration,
                substr(imported_text,80,15) AS Clearcast_Commercial_No,
                substr(imported_text,95,35) AS Sales_House_Brand_Description,
                substr(imported_text,130,40) AS Preceding_Programme_Name,
                substr(imported_text,170,40) AS Succeeding_Programme_Name,
                CAST(substr(imported_text,210,5) AS Int) AS Sales_House_Identifier,
                substr(imported_text,215,10) AS Campaign_Approval_ID,
                substr(imported_text,225,5) AS Campaign_Approval_ID_version_number,
                substr(imported_text,230,2) AS Interactive_Spot_Platform_Indicator,
                CASE WHEN substr (imported_text,26,2) in ('24','25','26','27','28','29','30')
                     THEN CAST(substr(imported_text,4,8) || ' ' || '0' || cast(substr (imported_text,26,2) as integer)-24 ||':' || substr (imported_text,28,2) || ':' || substr(imported_text,30,2) as datetime) + 1
                     ELSE CAST (substr(imported_text,4,8) || ' ' || substr(imported_text,26,2) || ':' || substr(imported_text,28,2) || ':' || substr(imported_text,30,2) AS datetime)
                END AS local_Break_Start_date_time,
                CASE WHEN substr (imported_text,26,2) in ('24','25','26','27','28','29','30')
                     THEN dateadd(dd,1,CAST(substr(imported_text,4,8) as date))
                     ELSE CAST (substr(imported_text,4,8) as date)
                END AS local_date_of_transmission,
                CASE WHEN substr (imported_text,69,2) in ('24','25','26','27','28','29','30')
                     THEN CAST(substr(imported_text,4,8) || ' ' || '0'|| cast(substr (imported_text,69,2) as integer)-24 ||':' || substr (imported_text,71,2) || ':' || substr(imported_text,73,2) AS datetime) + 1
                     ELSE CAST(substr(imported_text,4,8) || ' ' || substr(imported_text,69,2) || ':' || substr(imported_text,71,2) || ':' || substr(imported_text,73,2) AS datetime)
               END AS local_spot_Start_date_Time
        FROM MN_BARB_import
        WHERE substr(imported_text,1,2) = '02'


        -- adjust for british summer summer to create UTC = GMT

        UPDATE BARB_CET
                SET utc_break_start_date_time =
                        CASE
                                WHEN dateformat(local_break_start_date_time,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,-1,local_break_start_date_time)
                                WHEN dateformat(local_break_start_date_time,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,-1,local_break_start_date_time)
                                WHEN dateformat(local_break_start_date_time,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,-1,local_break_start_date_time)
                                ELSE local_break_start_date_time
                        END,
                    utc_spot_start_date_time = CASE
                                WHEN dateformat(local_spot_start_date_time,'YYYY-MM-DD-HH') between '2010-03-28-01' and '2010-10-31-02' then dateadd(hh,-1,local_spot_start_date_time)
                                WHEN dateformat(local_spot_start_date_time,'YYYY-MM-DD-HH') between '2011-03-27-01' and '2011-10-30-02' then dateadd(hh,-1,local_spot_start_date_time)
                                WHEN dateformat(local_spot_start_date_time,'YYYY-MM-DD-HH') between '2012-03-25-01' and '2012-10-28-02' then dateadd(hh,-1,local_spot_start_date_time)
                                ELSE local_spot_start_date_time
                        END


        -- BARB clock runs to 30:59:59 at the end of British Summer time using the old clock
        -- at this stage, we only want to adjust the time for the ads that ran on the relevant transmission day
        UPDATE BARB_CET
                SET utc_break_start_date_time =
                CASE
                        WHEN date_of_transmission = '2010-10-30' and local_break_start_date_time between '2010-10-31 02:00:00' and '2010-10-31 06:59:59' then dateadd(hh,-1, local_break_start_date_time)
                        WHEN date_of_transmission = '2011-10-29' and local_break_start_date_time between '2011-10-30 02:00:00' and '2011-10-30 06:59:59' then dateadd(hh,-1, local_break_start_date_time)
                        WHEN date_of_transmission = '2012-10-27' and local_break_start_date_time between '2012-10-28 02:00:00' and '2012-10-28 06:59:59' then dateadd(hh,-1, local_break_start_date_time)
                        ELSE utc_break_start_date_time
                END,
                utc_spot_start_date_time =
                CASE
                        WHEN date_of_transmission = '2010-10-30' and local_spot_start_date_time between '2010-10-31 02:00:00' and '2010-10-31 06:59:59' then dateadd(hh,-1, local_spot_start_date_time)
                        WHEN date_of_transmission = '2011-10-29' and local_spot_start_date_time between '2011-10-30 02:00:00' and '2011-10-30 06:59:59' then dateadd(hh,-1, local_spot_start_date_time)
                        WHEN date_of_transmission = '2012-10-27' and local_spot_start_date_time between '2012-10-28 02:00:00' and '2012-10-28 06:59:59' then dateadd(hh,-1, local_spot_start_date_time)
                        ELSE utc_spot_start_date_time
                END,
                local_break_start_date_time =
                CASE
                        WHEN date_of_transmission = '2010-10-30' and local_break_start_date_time between '2010-10-31 02:00:00' and '2010-10-31 06:59:59' then dateadd(hh,-1, local_break_start_date_time)
                        WHEN date_of_transmission = '2011-10-29' and local_break_start_date_time between '2011-10-30 02:00:00' and '2011-10-30 06:59:59' then dateadd(hh,-1, local_break_start_date_time)
                        WHEN date_of_transmission = '2012-10-27' and local_break_start_date_time between '2012-10-28 02:00:00' and '2012-10-28 06:59:59' then dateadd(hh,-1, local_break_start_date_time)
                        ELSE local_break_start_date_time
                END,
                local_spot_start_date_time =
                CASE
                        WHEN date_of_transmission = '2010-10-30' and local_spot_start_date_time between '2010-10-31 02:00:00' and '2010-10-31 06:59:59' then dateadd(hh,-1, local_spot_start_date_time)
                        WHEN date_of_transmission = '2011-10-29' and local_spot_start_date_time between '2011-10-30 02:00:00' and '2011-10-30 06:59:59' then dateadd(hh,-1, local_spot_start_date_time)
                        WHEN date_of_transmission = '2012-10-27' and local_spot_start_date_time between '2012-10-28 02:00:00' and '2012-10-28 06:59:59' then dateadd(hh,-1, local_spot_start_date_time)
                        ELSE local_spot_start_date_time
                END

        UPDATE BARB_CET
                SET utc_break_start_date_time =
                CASE
                        WHEN date_of_transmission = '2010-03-27' and local_break_start_date_time between '2010-03-28 01:00:00' and '2010-03-28 06:59:59' then local_break_start_date_time
                        WHEN date_of_transmission = '2011-03-26' and local_break_start_date_time between '2011-03-27 01:00:00' and '2011-03-27 06:59:59' then local_break_start_date_time
                        WHEN date_of_transmission = '2012-03-24' and local_break_start_date_time between '2012-03-25 01:00:00' and '2012-03-25 06:59:59' then local_break_start_date_time
                        ELSE utc_break_start_date_time
                END,
                utc_spot_start_date_time =
                CASE
                        WHEN date_of_transmission = '2010-03-27' and local_spot_start_date_time between '2010-03-28 01:00:00' and '2010-03-28 06:59:59' then local_spot_start_date_time
                        WHEN date_of_transmission = '2011-03-26' and local_spot_start_date_time between '2011-03-27 01:00:00' and '2011-03-27 06:59:59' then local_spot_start_date_time
                        WHEN date_of_transmission = '2012-03-24' and local_spot_start_date_time between '2012-03-25 01:00:00' and '2012-03-25 06:59:59' then local_spot_start_date_time
                        ELSE utc_spot_start_date_time
                END,
                local_break_start_date_time =
                CASE
                        WHEN date_of_transmission = '2010-03-27' and local_break_start_date_time between '2010-03-28 01:00:00' and '2010-03-28 06:59:59' then dateadd(hh,1, local_break_start_date_time)
                        WHEN date_of_transmission = '2011-03-26' and local_break_start_date_time between '2011-03-27 01:00:00' and '2011-03-27 06:59:59' then dateadd(hh,1, local_break_start_date_time)
                        WHEN date_of_transmission = '2012-03-24' and local_break_start_date_time between '2012-03-25 01:00:00' and '2012-03-25 06:59:59' then dateadd(hh,1, local_break_start_date_time)
                        ELSE local_break_start_date_time
                END,
                local_spot_start_date_time =
                CASE
                        WHEN date_of_transmission = '2010-03-27' and local_spot_start_date_time between '2010-03-28 01:00:00' and '2010-03-28 06:59:59' then dateadd(hh,1, local_spot_start_date_time)
                        WHEN date_of_transmission = '2011-03-26' and local_spot_start_date_time between '2011-03-27 01:00:00' and '2011-03-27 06:59:59' then dateadd(hh,1, local_spot_start_date_time)
                        WHEN date_of_transmission = '2012-03-24' and local_spot_start_date_time between '2012-03-25 01:00:00' and '2012-03-25 06:59:59' then dateadd(hh,1, local_spot_start_date_time)
                        ELSE local_spot_start_date_time
                END




        -- Log record
        SET @audit_row_count = @@rowcount
        INSERT INTO BARB_CET_AUDIT_LOG
                (Audit_time_stamp,
                File_Creation_date,
                File_Creation_time,
                File_Type,
                File_Version,
                Filename,
                Audit_action,
                Audit_count
                )
        SELECT now(), @file_creation_Date, @file_creation_time, @file_type, @file_version, @filename,'Records parsed to CET', @audit_row_count

        -- import log file

        INSERT INTO BARB_CET_LOG
        SELECT
                @file_creation_date,
                @file_creation_time,
                @file_type,
                @file_version,
                @filename,
                CAST(substr(imported_text,3,8) AS Date) AS Date_of_Transmission,
                CAST(substr(imported_text,11,5) AS Int) AS Log_Station_Code,
                substr(imported_text,16,1) AS Indicator_of_Log_Received_or_Rejected,
                CAST(substr(imported_text,17,5) AS Int) AS Total_Spot_Records,
                CAST(substr(imported_text,22,6) AS Int) AS Total_number_of_Spots,
                CAST(substr(imported_text,28,5) AS Int) AS Total_Commercial_Duration
        FROM MN_BARB_import
        WHERE substr(imported_text,1,2) = '95'

        ----------------------------------------------------------
        -- expand the data using the barb_spot_map to service keys
        -- filter on the platform indicator
        ----------------------------------------------------------

        -- process delete records first

        DELETE BARB_master_spot_data
        FROM BARB_master_spot_data msd
        JOIN (SELECT
                        skb.service_key,
                        skb.log_station_code,
                        skb.sti_code,
                        cet.Clearcast_Commercial_No,
                        cet.local_spot_start_date_time
                FROM BARB_CET cet
                JOIN BARB_PLATFORM_INDICATOR platform
                        on cet.spot_platform_indicator = platform.hex_code
                JOIN VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_BARB skb  -- implicit expansion of the table through a many to many join
                        on cet.log_station_code_for_spot = skb.log_station_code
                        and cet.split_transmission_indicator = skb.sti_code
                        and cet.reporting_panel_code = skb.panel_code
                        and cet.local_date_of_transmission between skb.effective_from AND skb.effective_to
                WHERE platform.on_sky_platform = 1 AND
                        skb.service_key is not null
                        AND cet.Insert_delete_amend_code ='D'
                ) delrecs
        ON msd.service_key = delrecs.service_key
                AND msd.local_spot_start_date_time = delrecs.local_spot_start_date_time
                AND msd.log_station_code = delrecs.log_station_code
                AND msd.sti_code = delrecs.sti_code
                AND msd.clearcast_commercial_no = delrecs.clearcast_commercial_no

        -- Log record
        SET @audit_row_count = @@rowcount

        INSERT INTO BARB_CET_AUDIT_LOG
                (Audit_time_stamp,
                File_Creation_date,
                File_Creation_time,
                File_Type,
                File_Version,
                Filename,
                Audit_action,
                Audit_count
                )
        SELECT now(), @file_creation_Date, @file_creation_time, @file_type, @file_version, @filename,'Records deleted from master file', @audit_row_count


        -- once this is done anything else is new and ok to add in

        INSERT INTO BARB_master_spot_data
        (
                service_key,
                log_station_code,
                sti_code,
                barb_date_of_transmission,
                local_date_of_transmission,
                local_break_start_date_time,
                local_break_end_date_time,
                utc_break_start_date_time,
                utc_break_end_date_time,
                spot_break_total_duration,
                break_type,
                spot_type,
                barb_spot_start_time,
                local_spot_start_date_time,
                local_spot_end_date_time,
                utc_spot_start_date_time,
                utc_spot_end_date_time,
                spot_duration,
                Clearcast_Commercial_No,
                Sales_House_Brand_Description,
                Preceding_Programme_Name,
                Succeeding_Programme_Name,
                Sales_House_Identifier,
                Campaign_Approval_ID,
                Campaign_Approval_ID_version_number,
                Interactive_Spot_Platform_Indicator,
                file_creation_date,
                file_creation_time,
                file_type,
                file_version,
                filename,
                channel_map_version
        )
        SELECT
                skb.service_key,
                skb.log_station_code,
                skb.sti_code,
                cet.date_of_transmission,
                cet.local_date_of_transmission,
                cet.local_break_start_date_time,
                dateadd(ss,cet.spot_break_total_duration,cet.local_break_start_date_time),
                cet.utc_break_start_date_time,
                dateadd(ss,cet.spot_break_total_duration,cet.utc_break_start_date_time),
                cet.spot_break_total_duration,
                cet.break_type,
                cet.spot_type,
                cet.spot_start_time,
                cet.local_spot_start_date_time,
                dateadd(ss,cet.spot_duration,cet.local_spot_start_date_time),
                cet.utc_spot_start_date_time,
                dateadd(ss,cet.spot_duration,cet.utc_spot_start_date_time),
                cet.spot_duration,
                cet.Clearcast_Commercial_No,
                cet.Sales_House_Brand_Description,
                cet.Preceding_Programme_Name,
                cet.Succeeding_Programme_Name,
                cet.Sales_House_Identifier,
                cet.Campaign_Approval_ID,
                cet.Campaign_Approval_ID_version_number,
                cet.Interactive_Spot_Platform_Indicator,
                cet.file_creation_date,
                cet.file_creation_time,
                cet.file_type,
                cet.file_version,
                cet.filename,
                skb.version
        FROM BARB_CET cet
        JOIN BARB_PLATFORM_INDICATOR platform
                on cet.spot_platform_indicator = platform.hex_code
        JOIN VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_BARB skb  -- implicit expansion of the table through a many to many join
                on cet.log_station_code_for_spot = skb.log_station_code
                and cet.split_transmission_indicator = skb.sti_code
                and cet.reporting_panel_code = skb.panel_code
                and cet.local_date_of_transmission between skb.effective_from AND skb.effective_to
        WHERE platform.on_sky_platform = 1 AND
                skb.service_key is not null
                AND cet.Insert_delete_amend_code IN (' ','I')

        -- Log record
        SET @audit_row_count = @@rowcount
        INSERT INTO BARB_CET_AUDIT_LOG
                (Audit_time_stamp,
                File_Creation_date,
                File_Creation_time,
                File_Type,
                File_Version,
                Filename,
                Audit_action,
                Audit_count
                )
        SELECT now(), @file_creation_Date, @file_creation_time, @file_type, @file_version, @filename,'Records Inserted to master file', @audit_row_count


      -- diagnostic check
        INSERT INTO BARB_CET_AUDIT_LOG
                (Audit_time_stamp,
                File_Creation_date,
                File_Creation_time,
                File_Type,
                File_Version,
                Filename,
                Audit_action,
                Audit_count
                )
        SELECT now(), @file_creation_Date, @file_creation_time, @file_type, @file_version, @filename,'Duplicate record count', ( SELECT count(1) FROM (SELECT service_key, utc_spot_start_date_time, count(1) as no_dupes
                                                                                                                                                FROM BARB_master_spot_data
                                                                                                                                                group by service_key, utc_spot_start_date_time
                                                                                                                                                having count(1) > 1) a                                                                                                                                              )

-- note this will reprocess all records on any date of transmission

        DELETE FROM BARB_SPOT_RANK
        DELETE FROM BARB_SPOT_RANK_TOTAL

        INSERT INTO BARB_SPOT_RANK
        (service_key, local_break_start_date_time, local_spot_start_date_time, spot_rank)
        SELECT  bm.service_key,
                bm.local_break_start_date_time,
                bm.local_spot_start_date_time,
                rank() over (partition by bm.service_key, bm.local_break_start_date_time order by bm.local_spot_start_date_time asc) as spot_rank
        FROM BARB_MASTER_SPOT_DATA bm
        JOIN (  SELECT distinct skb.service_key, cet.date_of_transmission
                FROM BARB_CET cet
                JOIN BARB_PLATFORM_INDICATOR platform
                        on cet.spot_platform_indicator = platform.hex_code
                JOIN VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_BARB skb  -- implicit expansion of the table through a many to many join
                        on cet.log_station_code_for_spot = skb.log_station_code
                        and cet.split_transmission_indicator = skb.sti_code
                        and cet.reporting_panel_code = skb.panel_code
                WHERE platform.on_sky_platform = 1 AND
                        skb.service_key is not null AND
                        cet.local_date_of_transmission between skb.effective_from AND skb.effective_to
                        AND cet.Insert_delete_amend_code IN (' ','I')
                ) cet
                ON bm.service_key = cet.service_key and bm.barb_date_of_transmission = cet.date_of_transmission

        INSERT INTO BARB_SPOT_RANK_TOTAL
        (service_key, local_break_start_date_time, no_spots_in_break)
        SELECT  service_key,
                local_break_start_date_time,
                max(spot_rank) as spot_rank
        FROM BARB_SPOT_RANK
        GROUP BY  service_key,
                  local_break_start_date_time

        UPDATE BARB_MASTER_SPOT_DATA
        SET bm.spot_position_in_break = sr.spot_rank
        FROM BARB_MASTER_SPOT_DATA bm
        JOIN BARB_SPOT_RANK sr
             ON bm.service_key = sr.service_key and bm.local_spot_start_date_time = sr.local_spot_start_date_time

        UPDATE BARB_MASTER_SPOT_DATA
        SET bm.no_spots_in_break = sr.no_spots_in_break
        FROM BARB_MASTER_SPOT_DATA bm
        JOIN BARB_SPOT_RANK_TOTAL sr
             ON bm.service_key = sr.service_key and bm.local_break_start_date_time = sr.local_break_start_date_time

        SET @audit_row_count = (SELECT count(1) FROM BARB_SPOT_RANK)
        INSERT INTO BARB_CET_AUDIT_LOG
                (Audit_time_stamp,
                File_Creation_date,
                File_Creation_time,
                File_Type,
                File_Version,
                Filename,
                Audit_action,
                Audit_count
                )
        SELECT now(), @file_creation_Date, @file_creation_time, @file_type, @file_version, @filename,'Rankings updated', @audit_row_count

        SELECT *
        FROM BARB_CET_AUDIT_LOG
        WHERE filename = @filename

END
