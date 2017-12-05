/*###############################################################################
# Created on:   23/01/2017
# Created by:   Jason Thompson (JT)
# Description:  Channel Mapping process - process raw Barb data
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Requires the daily Barb files to be delivered to the FTP site: 194.202.213.25
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 23/01/2016  JT   Initial version
#
###############################################################################*/

/*###############################################################################
# Updates the Barb tables: BARB_MASTER_FILE_SALES_HOUSE_RECORD
#                           BARB_MASTER_FILE_BROADCAST_GROUP_RECORD
#
###############################################################################*/


create or replace procedure CM_Process_Barb_Table_Setup

as begin

IF NOT EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'CM_99_RAW_BARB_MAS')
begin
    create table CM_99_Raw_Barb_MAS (import_text varchar(10000))
    commit
end

IF NOT EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'BARB_MASTER_FILE_SALES_HOUSE_RECORD')
begin
    CREATE TABLE BARB_MASTER_FILE_SALES_HOUSE_RECORD (
        Filename varchar(50) DEFAULT NULL
        ,Sales_House_Identifier int DEFAULT NULL
        ,Sales_House_Name varchar(50) DEFAULT NULL
        ,Sales_House_Short_Name varchar(8) DEFAULT NULL
        ,Sales_House_15_Char_Name varchar(15) DEFAULT NULL
        ,Reporting_Start_Date date DEFAULT NULL
        ,Reporting_End_Date date DEFAULT NULL)
    commit

    --grant select on BARB_MASTER_FILE_SALES_HOUSE_RECORD to public
    commit
end


IF NOT EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = 'BARB_MASTER_FILE_BROADCAST_GROUP_RECORD')
begin
    CREATE TABLE BARB_MASTER_FILE_BROADCAST_GROUP_RECORD (
        Filename varchar(50) DEFAULT NULL
        ,Broadcast_Group_Id int DEFAULT NULL
        ,Broadcast_Group_Name varchar(50) DEFAULT NULL
        ,Broadcast_Group_Short_Name varchar(8) DEFAULT NULL
        ,Broadcast_Group_15_Char_Name varchar(15) DEFAULT NULL
        ,Reporting_Start_Date date DEFAULT NULL
        ,Reporting_End_Date date DEFAULT NULL)
    commit

    --grant select on BARB_MASTER_FILE_BROADCAST_GROUP_RECORD to public
    commit
end


end



create or replace procedure CM_Process_Barb_MAS_Data
    @mas_filename varchar(50) = null --- the name of the MAS file to process

as begin

    declare @varSQL               text

---- Import raw MAS data
    truncate table CM_99_Raw_Barb_MAS
    commit

    set @varSQL = '
                    load table CM_99_Raw_Barb_MAS (
                        import_text
                        ''\n''
                    )
                    FROM ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Channel_Mapping_Process/_Process_inputs_/##^1^##''
                    SKIP 1
                    QUOTES ON
                    ESCAPES OFF
                  '
    execute( replace(@varSQL, '##^1^##', @mas_filename) )
    commit


---- Refresh table BARB_MASTER_FILE_SALES_HOUSE_RECORD
truncate table BARB_MASTER_FILE_SALES_HOUSE_RECORD
commit

INSERT INTO BARB_MASTER_FILE_SALES_HOUSE_RECORD
        (Filename, Sales_House_Identifier, Sales_House_Name, Sales_House_Short_Name,
        Sales_House_15_Char_Name, Reporting_Start_Date, Reporting_End_Date)

SELECT
@mas_filename
,CAST(substr(import_text,3,5) AS Int)
,substr(import_text,8,30)
,substr(import_text,38,8)
,substr(import_text,46,15)
,date(substr(import_text,61,4) || '-' || substr(import_text,65,2) || '-' || substr(import_text,67,2))
,date(substr(import_text,69,4) || '-' || substr(import_text,73,2) || '-' || substr(import_text,75,2))
FROM CM_99_Raw_Barb_MAS
WHERE substr(import_text,1,2) = '10'

commit


---- Refresh table BARB_MASTER_FILE_BROADCAST_GROUP_RECORD
truncate table BARB_MASTER_FILE_BROADCAST_GROUP_RECORD
commit

INSERT INTO BARB_MASTER_FILE_BROADCAST_GROUP_RECORD
        (Filename, Broadcast_Group_Id, Broadcast_Group_Name, Broadcast_Group_Short_Name,
        Broadcast_Group_15_Char_Name, Reporting_Start_Date, Reporting_End_Date)

SELECT
@mas_filename
,CAST(substr(import_text,3,5) AS Int)
,substr(import_text,8,30)
,substr(import_text,38,8)
,substr(import_text,46,15)
,date(substr(import_text,61,4) || '-' || substr(import_text,65,2) || '-' || substr(import_text,67,2))
,date(substr(import_text,69,4) || '-' || substr(import_text,73,2) || '-' || substr(import_text,75,2))
FROM CM_99_Raw_Barb_MAS
WHERE substr(import_text,1,2) = '11'

commit

end

