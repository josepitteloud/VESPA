--List your procedures
create procedure neighbom.sp_BARB_MASTER_FILE_IMPORT_V14_02( @in_filename varchar(60) )
-- Martin Neighbours
-- 19/9/12
-- BARB MASTER file.  Note that this is only for the import of the original file
-- This will clear down all files and replace with new, no log files required
-- amended to enable load of V14
-- note additional record types are available in this version and have not yet been incorporated
as
begin
  declare @file_creation_date date
  declare @file_creation_time time
  declare @file_type varchar(12)
  declare @File_Version integer
  declare @audit_row_count bigint
  declare @query varchar(3000)
  declare @filename varchar(13)
  -- clear down the data first
  delete from MN_BARB_import
  delete from BARB_MASTER_FILE_DB1_STATIONS_REPORTING_RECORD
  delete from BARB_MASTER_FILE_DB2_STATIONS_REPORTING_RECORD
  delete from BARB_MASTER_FILE_LOG_STATIONS_REPORTING_RECORD
  delete from BARB_MASTER_FILE_LOG_STATION_RELATIONSHIP_TO_DB1_RECORD
  delete from BARB_MASTER_FILE_SPLIT_STATIONS_REPORTING_RECORD
  delete from BARB_MASTER_FILE_PANEL_REPORTING_RECORD
  delete from BARB_MASTER_FILE_MACRO_PANEL_RELATIONSHIP_RECORD
  delete from BARB_MASTER_FILE_DB2_PANEL_STATIONS_REPORTING_RECORD
  delete from BARB_MASTER_FILE_SALES_HOUSE_RECORD
  delete from BARB_MASTER_FILE_BROADCAST_GROUP_RECORD
  delete from BARB_MASTER_FILE_AUDIENCE_CATEGORY_RECORD
  delete from BARB_MASTER_FILE_COMMERCIAL_LENGTH_RATE_FACTORS_RECORD
  delete from BARB_MASTER_FILE_COMMERCIAL_LENGTH_RATE_FACTORS_RECORD_UNPVT
  delete from BARB_MASTER_FILE_VOD_PROVIDER_RECORD
  delete from BARB_MASTER_FILE_VOD_SERVICE_RECORD
  delete from BARB_MASTER_FILE_VOD_TYPE_RECORD
  delete from BARB_MASTER_FILE_DEVICE_IN_USE_TYPE_RECORD
  -- import the raw data as a string
  set @query = 'LOAD TABLE MN_BARB_import (imported_text '
  set @query = @query || ' '' '' ) '
  set @query = @query || ' FROM ''/ETL013/prod/sky/olive/data/share/clarityq/export/' || @in_filename || ''' QUOTES OFF ESCAPES OFF NOTIFY 1000'
  execute(@query)
  set @audit_row_count = (select count(1) from MN_BARB_IMPORT)
  -- file doesn't exist
  if @audit_row_count = 0
    begin
      select now(),@file_creation_Date,@file_creation_time,@file_type,@file_version,@filename,'File not found ' || @in_filename,@audit_row_count
      return
    end
  -- parse out the data records
  set @file_creation_date = (select convert(date,substr(imported_text,7,8))
      from MN_BARB_import
      where substr(imported_text,1,2) = '01')
  set @file_creation_time = (select convert(time,substr(imported_text,15,2) || ':' || substr(imported_text,17,2) || ':' || substr(imported_text,19,2))
      from MN_BARB_import
      where substr(imported_text,1,2) = '01')
  set @file_type = (select substr(imported_text,21,12)
      from MN_BARB_import
      where substr(imported_text,1,2) = '01')
  set @File_Version = (select convert(integer,substr(imported_text,33,3))
      from MN_BARB_import
      where substr(imported_text,1,2) = '01')
  set @Filename = (select substr(imported_text,36,13)
      from MN_BARB_import
      where substr(imported_text,1,2) = '01')
  -- wrong format
  if @file_type <> 'DSP14.V04.02'
    begin
      select now(),@file_creation_Date,@file_creation_time,@file_type,@file_version,@filename,'File type DSP10.V03.03, file type imported ' || @file_type,0
      return
    end
  insert into BARB_MASTER_FILE_DB1_STATIONS_REPORTING_RECORD
    ( file_creation_date,
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
    Exclude_from_Commercial_TV,
    Area_Geography,
    Area_Flags,
    Transmission_Format,
    Reporting_Start_Date,
    Reporting_End_Date )
    select @file_creation_date,
      @file_creation_time,
      @file_type,
      @file_version,
      @filename,
      DB1_Station_Code=convert(integer,substr(imported_text,3,5)),
      DB2_Station_Code=convert(integer,substr(imported_text,8,5)),
      DB1_Station_Name=substr(imported_text,13,30),
      DB1_Station_Medium_Name=substr(imported_text,43,15),
      DB1_Station_Short_Name=substr(imported_text,58,8),
      Exclude_from_Total_TV=substr(imported_text,66,1),
      Exclude_from_Commercial_TV=substr(imported_text,67,1),
      Area_Geography=convert(integer,substr(imported_text,68,1)),
      Area_Flags=substr(imported_text,69,4),
      Transmission_Format=convert(integer,substr(imported_text,73,1)),
      Reporting_Start_Date=convert(date,substr(imported_text,74,8)),
      Reporting_End_Date=convert(date,substr(imported_text,82,8))
      from MN_BARB_import
      where substr(imported_text,1,2) = '02'
  insert into BARB_MASTER_FILE_DB2_STATIONS_REPORTING_RECORD
    ( file_creation_date,
    file_creation_time,
    file_type,
    file_version,
    filename,
    DB2_Station_Code,
    DB2_Station_Name,
    DB2_Station_Short_Name,
    DB2_Station_15_Char_Name,
    DB2_Station_Code_of_Parent_channel,
    Staggercast_Delay_Minutes,
    Reporting_Start_Date,
    Reporting_End_Date )
    select @file_creation_date,
      @file_creation_time,
      @file_type,
      @file_version,
      @filename,
      DB2_Station_Code=convert(integer,substr(imported_text,3,5)),
      DB2_Station_Name=substr(imported_text,8,30),
      DB2_Station_Short_Name=substr(imported_text,38,8),
      DB2_Station_15_Char_Name=substr(imported_text,46,15),
      DB2_Station_Code_of_Parent_channel=convert(integer,substr(imported_text,61,5)),
      Staggercast_Delay_Minutes=convert(integer,substr(imported_text,66,4)),
      Reporting_Start_Date=convert(date,substr(imported_text,70,8)),
      Reporting_End_Date=convert(date,substr(imported_text,78,8))
      from MN_BARB_import
      where substr(imported_text,1,2) = '03'
  insert into BARB_MASTER_FILE_LOG_STATIONS_REPORTING_RECORD
    ( file_creation_date,
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
    Station_Genre_Type )
    select @file_creation_date,
      @file_creation_time,
      @file_type,
      @file_version,
      @filename,
      Log_Station_Code=convert(integer,substr(imported_text,3,5)),
      DB2_Station_Code=convert(integer,substr(imported_text,8,5)),
      Log_Station_Name=substr(imported_text,13,30),
      Log_Station_Short_Name=substr(imported_text,43,8),
      Log_Station_15_Char_Name=substr(imported_text,51,15),
      Area_Geography=convert(integer,substr(imported_text,66,1)),
      Area_Flags=substr(imported_text,67,4),
      Primary_Reporting_Panel_Code=convert(integer,substr(imported_text,71,5)),
      Reporting_Start_Date=convert(date,substr(imported_text,76,8)),
      Reporting_End_Date=convert(date,substr(imported_text,84,8)),
      Sales_House_1=convert(integer,substr(imported_text,92,5)),
      Sales_House_2=convert(integer,substr(imported_text,97,5)),
      Sales_House_3=convert(integer,substr(imported_text,102,5)),
      Sales_House_4=convert(integer,substr(imported_text,107,5)),
      Sales_House_5=convert(integer,substr(imported_text,112,5)),
      Sales_House_6=convert(integer,substr(imported_text,117,5)),
      Broadcast_Group_Id=convert(integer,substr(imported_text,122,5)),
      Station_Genre_Type=substr(imported_text,127,5)
      from MN_BARB_import
      where substr(imported_text,1,2) = '04'
  insert into BARB_MASTER_FILE_LOG_STATION_RELATIONSHIP_TO_DB1_RECORD
    ( file_creation_date,
    file_creation_time,
    file_type,
    file_version,
    filename,
    Log_Station_Code,
    DB1_Station_Code,
    Relationship_Start_Date,
    Relationship_End_Date )
    select @file_creation_date,
      @file_creation_time,
      @file_type,
      @file_version,
      @filename,
      Log_Station_Code=convert(integer,substr(imported_text,3,5)),
      DB1_Station_Code=convert(integer,substr(imported_text,8,5)),
      Relationship_Start_Date=convert(date,substr(imported_text,13,8)),
      Relationship_End_Date=convert(date,substr(imported_text,21,8))
      from MN_BARB_import
      where substr(imported_text,1,2) = '05'
  insert into BARB_MASTER_FILE_SPLIT_STATIONS_REPORTING_RECORD
    ( file_creation_date,
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
    Panel_Code )
    select @file_creation_date,
      @file_creation_time,
      @file_type,
      @file_version,
      @filename,
      Log_Station_Code=convert(integer,substr(imported_text,3,5)),
      Split_Transmission_Indicator=convert(integer,substr(imported_text,8,2)),
      Split_Station_Name=substr(imported_text,10,30),
      Split_Station_Short_Name=substr(imported_text,40,8),
      Split_Station_15_Char_Name=substr(imported_text,48,15),
      Split_Area_Factor=convert(decimal(6,5),substr(imported_text,63,6)),
      Reporting_Start_Date=convert(date,substr(imported_text,69,8)),
      Reporting_End_Date=convert(date,substr(imported_text,77,8)),
      Panel_Code=convert(integer,substr(imported_text,85,5))
      from MN_BARB_import
      where substr(imported_text,1,2) = '06'
  insert into BARB_MASTER_FILE_PANEL_REPORTING_RECORD
    ( file_creation_date,
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
    Panel_End_Date )
    select @file_creation_date,
      @file_creation_time,
      @file_type,
      @file_version,
      @filename,
      Panel_Code=convert(integer,substr(imported_text,3,5)),
      Panel_Name=substr(imported_text,8,30),
      Panel_Medium_Name=substr(imported_text,38,15),
      Panel_Short_Name=substr(imported_text,53,8),
      Macro_region=substr(imported_text,61,1),
      Used_in_DB2=substr(imported_text,62,1),
      Panel_Start_Date=convert(date,substr(imported_text,63,8)),
      Panel_End_Date=convert(date,substr(imported_text,71,8))
      from MN_BARB_import
      where substr(imported_text,1,2) = '07'
  insert into BARB_MASTER_FILE_MACRO_PANEL_RELATIONSHIP_RECORD
    ( file_creation_date,
    file_creation_time,
    file_type,
    file_version,
    filename,
    Macro_Panel_Code,
    DB1_Panel_Code,
    Relationship_Start_Date,
    Relationship_End_Date )
    select @file_creation_date,
      @file_creation_time,
      @file_type,
      @file_version,
      @filename,
      Macro_Panel_Code=convert(integer,substr(imported_text,3,5)),
      DB1_Panel_Code=convert(integer,substr(imported_text,8,5)),
      Relationship_Start_Date=convert(date,substr(imported_text,13,8)),
      Relationship_End_Date=convert(date,substr(imported_text,21,8))
      from MN_BARB_import
      where substr(imported_text,1,2) = '08'
  /*
version 4_1
alter table BARB_MASTER_FILE_DB2_PANEL_STATIONS_REPORTING_RECORD
ADD targeted_advertising varchar(1);
*/
  insert into BARB_MASTER_FILE_DB2_PANEL_STATIONS_REPORTING_RECORD
    ( file_creation_date,
    file_creation_time,
    file_type,
    file_version,
    filename,
    Reporting_Panel_Code,
    DB2_Station_Code,
    Log_Station_Code,
    Reporting_Start_Date,
    Reporting_End_Date,
    Reported_in_IBT,
    Reported_in_Programme_file,
    Reported_in_Spots_file,
    Reported_in_Breaks_file,
    Reported_in_Sponsorship_file,
    targeted_advertising )
    select @file_creation_date,
      @file_creation_time,
      @file_type,
      @file_version,
      @filename,
      Reporting_Panel_Code=convert(integer,substr(imported_text,3,5)),
      DB2_Station_Code=convert(integer,substr(imported_text,8,5)),
      Log_Station_Code=convert(integer,substr(imported_text,13,5)),
      Reporting_Start_Date=convert(date,substr(imported_text,18,8)),
      Reporting_End_Date=convert(date,substr(imported_text,26,8)),
      Reported_in_IBT=substr(imported_text,34,1),
      Reported_in_Programme_file=substr(imported_text,35,1),
      Reported_in_Spots_file=substr(imported_text,36,1),
      Reported_in_Breaks_file=substr(imported_text,37,1),
      Reported_in_Sponsorship_file=substr(imported_text,38,1),
      targeted_advertising=substr(imported_text,39,1) -- v4_1
      from MN_BARB_import
      where substr(imported_text,1,2) = '09'
  insert into BARB_MASTER_FILE_SALES_HOUSE_RECORD
    ( file_creation_date,
    file_creation_time,
    file_type,
    file_version,
    filename,
    Sales_House_Identifier,
    Sales_House_Name,
    Sales_House_Short_Name,
    Sales_House_15_Char_Name,
    Reporting_Start_Date,
    Reporting_End_Date )
    select @file_creation_date,
      @file_creation_time,
      @file_type,
      @file_version,
      @filename,
      Sales_House_Identifier=convert(integer,substr(imported_text,3,5)),
      Sales_House_Name=substr(imported_text,8,30),
      Sales_House_Short_Name=substr(imported_text,38,8),
      Sales_House_15_Char_Name=substr(imported_text,46,15),
      Reporting_Start_Date=convert(date,substr(imported_text,61,8)),
      Reporting_End_Date=convert(date,substr(imported_text,69,8))
      from MN_BARB_import
      where substr(imported_text,1,2) = '10'
  insert into BARB_MASTER_FILE_BROADCAST_GROUP_RECORD
    ( file_creation_date,
    file_creation_time,
    file_type,
    file_version,
    filename,
    Broadcast_Group_Id,
    Broadcast_Group_Name,
    Broadcast_Group_Short_Name,
    Broadcast_Group_15_Char_Name,
    Reporting_Start_Date,
    Reporting_End_Date )
    select @file_creation_date,
      @file_creation_time,
      @file_type,
      @file_version,
      @filename,
      Broadcast_Group_Id=convert(integer,substr(imported_text,3,5)),
      Broadcast_Group_Name=substr(imported_text,8,30),
      Broadcast_Group_Short_Name=substr(imported_text,38,8),
      Broadcast_Group_15_Char_Name=substr(imported_text,46,15),
      Reporting_Start_Date=convert(date,substr(imported_text,61,8)),
      Reporting_End_Date=convert(date,substr(imported_text,69,8))
      from MN_BARB_import
      where substr(imported_text,1,2) = '11'
  insert into BARB_MASTER_FILE_AUDIENCE_CATEGORY_RECORD
    ( file_creation_date,
    file_creation_time,
    file_type,
    file_version,
    filename,
    Audience_Category_No,
    BARB_Audience_Category_Code,
    Audience_Description,
    Audience_Short_Description,
    Reporting_Start_Date,
    Reporting_End_Date,
    Audience_Category_1_Arithmetic_Operator,
    Audience_Category_2_Arithmetic_Operator,
    Audience_Category_3_Arithmetic_Operator,
    Audience_Category_4_Arithmetic_Operator,
    Audience_Category_5_Arithmetic_Operator,
    Audience_Category_6_Arithmetic_Operator,
    Audience_Category_7_Arithmetic_Operator,
    Audience_Category_8_Arithmetic_Operator,
    Audience_Category_9_Arithmetic_Operator,
    Audience_Category_10_Arithmetic_Operator,
    Audience_Category_11_Arithmetic_Operator,
    Audience_Category_12_Arithmetic_Operator,
    Audience_Category_13_Arithmetic_Operator,
    Audience_Category_14_Arithmetic_Operator,
    Audience_Category_15_Arithmetic_Operator,
    Audience_Category_16_Arithmetic_Operator,
    Audience_Category_17_Arithmetic_Operator,
    Audience_Category_18_Arithmetic_Operator,
    Audience_Category_19_Arithmetic_Operator,
    Audience_Category_20_Arithmetic_Operator,
    Audience_Category_21_Arithmetic_Operator,
    Audience_Category_22_Arithmetic_Operator,
    Audience_Category_23_Arithmetic_Operator,
    Audience_Category_24_Arithmetic_Operator,
    Audience_Category_25_Arithmetic_Operator,
    Audience_Category_26_Arithmetic_Operator,
    Audience_Category_27_Arithmetic_Operator,
    Audience_Category_28_Arithmetic_Operator,
    Audience_Category_29_Arithmetic_Operator,
    Audience_Category_30_Arithmetic_Operator,
    Audience_Category_31_Arithmetic_Operator,
    Audience_Category_32_Arithmetic_Operator,
    Audience_Category_33_Arithmetic_Operator,
    Audience_Category_34_Arithmetic_Operator,
    Audience_Category_35_Arithmetic_Operator,
    Audience_Category_36_Arithmetic_Operator,
    Audience_Category_37_Arithmetic_Operator,
    Audience_Category_38_Arithmetic_Operator,
    Audience_Category_39_Arithmetic_Operator,
    Audience_Category_40_Arithmetic_Operator,
    Audience_Category_41_Arithmetic_Operator,
    Audience_Category_42_Arithmetic_Operator,
    Audience_Category_43_Arithmetic_Operator,
    Audience_Category_44_Arithmetic_Operator,
    Audience_Category_45_Arithmetic_Operator,
    Audience_Category_46_Arithmetic_Operator,
    Audience_Category_47_Arithmetic_Operator,
    Audience_Category_48_Arithmetic_Operator,
    Audience_Category_49_Arithmetic_Operator,
    Audience_Category_50_Arithmetic_Operator,
    Audience_Category_51_Arithmetic_Operator,
    Audience_Category_52_Arithmetic_Operator,
    Audience_Category_53_Arithmetic_Operator,
    Audience_Category_54_Arithmetic_Operator,
    Audience_Category_55_Arithmetic_Operator,
    Audience_Category_56_Arithmetic_Operator,
    Audience_Category_57_Arithmetic_Operator,
    Audience_Category_58_Arithmetic_Operator,
    Audience_Category_59_Arithmetic_Operator,
    Audience_Category_60_Arithmetic_Operator,
    Audience_Category_61_Arithmetic_Operator,
    Audience_Category_62_Arithmetic_Operator,
    Audience_Category_63_Arithmetic_Operator,
    Audience_Category_64_Arithmetic_Operator,
    Audience_Category_65_Arithmetic_Operator,
    Audience_Category_66_Arithmetic_Operator,
    Audience_Category_67_Arithmetic_Operator,
    Audience_Category_68_Arithmetic_Operator,
    Audience_Category_69_Arithmetic_Operator,
    Audience_Category_70_Arithmetic_Operator,
    Audience_Category_71_Arithmetic_Operator )
    select @file_creation_date,
      @file_creation_time,
      @file_type,
      @file_version,
      @filename,
      Audience_Category_No=convert(integer,substr(imported_text,3,5)),
      BARB_Audience_Category_Code=convert(integer,substr(imported_text,8,6)),
      Audience_Description=substr(imported_text,14,30),
      Audience_Short_Description=substr(imported_text,44,15),
      Reporting_Start_Date=convert(date,substr(imported_text,59,8)),
      Reporting_End_Date=convert(date,substr(imported_text,67,8)),
      Audience_Category_1_Arithmetic_Operator=substr(imported_text,75,1),
      Audience_Category_2_Arithmetic_Operator=substr(imported_text,76,1),
      Audience_Category_3_Arithmetic_Operator=substr(imported_text,77,1),
      Audience_Category_4_Arithmetic_Operator=substr(imported_text,78,1),
      Audience_Category_5_Arithmetic_Operator=substr(imported_text,79,1),
      Audience_Category_6_Arithmetic_Operator=substr(imported_text,80,1),
      Audience_Category_7_Arithmetic_Operator=substr(imported_text,81,1),
      Audience_Category_8_Arithmetic_Operator=substr(imported_text,82,1),
      Audience_Category_9_Arithmetic_Operator=substr(imported_text,83,1),
      Audience_Category_10_Arithmetic_Operator=substr(imported_text,84,1),
      Audience_Category_11_Arithmetic_Operator=substr(imported_text,85,1),
      Audience_Category_12_Arithmetic_Operator=substr(imported_text,86,1),
      Audience_Category_13_Arithmetic_Operator=substr(imported_text,87,1),
      Audience_Category_14_Arithmetic_Operator=substr(imported_text,88,1),
      Audience_Category_15_Arithmetic_Operator=substr(imported_text,89,1),
      Audience_Category_16_Arithmetic_Operator=substr(imported_text,90,1),
      Audience_Category_17_Arithmetic_Operator=substr(imported_text,91,1),
      Audience_Category_18_Arithmetic_Operator=substr(imported_text,92,1),
      Audience_Category_19_Arithmetic_Operator=substr(imported_text,93,1),
      Audience_Category_20_Arithmetic_Operator=substr(imported_text,94,1),
      Audience_Category_21_Arithmetic_Operator=substr(imported_text,95,1),
      Audience_Category_22_Arithmetic_Operator=substr(imported_text,96,1),
      Audience_Category_23_Arithmetic_Operator=substr(imported_text,97,1),
      Audience_Category_24_Arithmetic_Operator=substr(imported_text,98,1),
      Audience_Category_25_Arithmetic_Operator=substr(imported_text,99,1),
      Audience_Category_26_Arithmetic_Operator=substr(imported_text,100,1),
      Audience_Category_27_Arithmetic_Operator=substr(imported_text,101,1),
      Audience_Category_28_Arithmetic_Operator=substr(imported_text,102,1),
      Audience_Category_29_Arithmetic_Operator=substr(imported_text,103,1),
      Audience_Category_30_Arithmetic_Operator=substr(imported_text,104,1),
      Audience_Category_31_Arithmetic_Operator=substr(imported_text,105,1),
      Audience_Category_32_Arithmetic_Operator=substr(imported_text,106,1),
      Audience_Category_33_Arithmetic_Operator=substr(imported_text,107,1),
      Audience_Category_34_Arithmetic_Operator=substr(imported_text,108,1),
      Audience_Category_35_Arithmetic_Operator=substr(imported_text,109,1),
      Audience_Category_36_Arithmetic_Operator=substr(imported_text,110,1),
      Audience_Category_37_Arithmetic_Operator=substr(imported_text,111,1),
      Audience_Category_38_Arithmetic_Operator=substr(imported_text,112,1),
      Audience_Category_39_Arithmetic_Operator=substr(imported_text,113,1),
      Audience_Category_40_Arithmetic_Operator=substr(imported_text,114,1),
      Audience_Category_41_Arithmetic_Operator=substr(imported_text,115,1),
      Audience_Category_42_Arithmetic_Operator=substr(imported_text,116,1),
      Audience_Category_43_Arithmetic_Operator=substr(imported_text,117,1),
      Audience_Category_44_Arithmetic_Operator=substr(imported_text,118,1),
      Audience_Category_45_Arithmetic_Operator=substr(imported_text,119,1),
      Audience_Category_46_Arithmetic_Operator=substr(imported_text,120,1),
      Audience_Category_47_Arithmetic_Operator=substr(imported_text,121,1),
      Audience_Category_48_Arithmetic_Operator=substr(imported_text,122,1),
      Audience_Category_49_Arithmetic_Operator=substr(imported_text,123,1),
      Audience_Category_50_Arithmetic_Operator=substr(imported_text,124,1),
      Audience_Category_51_Arithmetic_Operator=substr(imported_text,125,1),
      Audience_Category_52_Arithmetic_Operator=substr(imported_text,126,1),
      Audience_Category_53_Arithmetic_Operator=substr(imported_text,127,1),
      Audience_Category_54_Arithmetic_Operator=substr(imported_text,128,1),
      Audience_Category_55_Arithmetic_Operator=substr(imported_text,129,1),
      Audience_Category_56_Arithmetic_Operator=substr(imported_text,130,1),
      Audience_Category_57_Arithmetic_Operator=substr(imported_text,131,1),
      Audience_Category_58_Arithmetic_Operator=substr(imported_text,132,1),
      Audience_Category_59_Arithmetic_Operator=substr(imported_text,133,1),
      Audience_Category_60_Arithmetic_Operator=substr(imported_text,134,1),
      Audience_Category_61_Arithmetic_Operator=substr(imported_text,135,1),
      Audience_Category_62_Arithmetic_Operator=substr(imported_text,136,1),
      Audience_Category_63_Arithmetic_Operator=substr(imported_text,137,1),
      Audience_Category_64_Arithmetic_Operator=substr(imported_text,138,1),
      Audience_Category_65_Arithmetic_Operator=substr(imported_text,139,1),
      Audience_Category_66_Arithmetic_Operator=substr(imported_text,140,1),
      Audience_Category_67_Arithmetic_Operator=substr(imported_text,141,1),
      Audience_Category_68_Arithmetic_Operator=substr(imported_text,142,1),
      Audience_Category_69_Arithmetic_Operator=substr(imported_text,143,1),
      Audience_Category_70_Arithmetic_Operator=substr(imported_text,144,1),
      Audience_Category_71_Arithmetic_Operator=substr(imported_text,145,1)
      from MN_BARB_import
      where substr(imported_text,1,2) = '12'
  insert into BARB_MASTER_FILE_COMMERCIAL_LENGTH_RATE_FACTORS_RECORD
    ( file_creation_date,
    file_creation_time,
    file_type,
    file_version,
    filename,
    Sales_House_Identifier,
    Date_active_from,
    Date_active_to,
    Log_Station_Code,
    Base_Duration_Length,
    Commercial_Duration_1,
    Factor_1,
    Commercial_Duration_2,
    Factor_2,
    Commercial_Duration_3,
    Factor_3,
    Commercial_Duration_4,
    Factor_4,
    Commercial_Duration_5,
    Factor_5,
    Commercial_Duration_6,
    Factor_6,
    Commercial_Duration_7,
    Factor_7,
    Commercial_Duration_8,
    Factor_8,
    Commercial_Duration_9,
    Factor_9,
    Commercial_Duration_10,
    Factor_10,
    Commercial_Duration_11,
    Factor_11,
    Commercial_Duration_12,
    Factor_12,
    Commercial_Duration_13,
    Factor_13,
    Commercial_Duration_14,
    Factor_14,
    Commercial_Duration_15,
    Factor_15,
    Commercial_Duration_16,
    Factor_16,
    Commercial_Duration_17,
    Factor_17,
    Commercial_Duration_18,
    Factor_18,
    Commercial_Duration_19,
    Factor_19,
    Commercial_Duration_20,
    Factor_20,
    Commercial_Duration_21,
    Factor_21,
    Commercial_Duration_22,
    Factor_22,
    Commercial_Duration_23,
    Factor_23,
    Commercial_Duration_24,
    Factor_24,
    Commercial_Duration_25,
    Factor_25,
    Commercial_Duration_26,
    Factor_26,
    Commercial_Duration_27,
    Factor_27,
    Commercial_Duration_28,
    Factor_28,
    Commercial_Duration_29,
    Factor_29,
    Commercial_Duration_30,
    Factor_30,
    Commercial_Duration_31,
    Factor_31,
    Commercial_Duration_32,
    Factor_32,
    Commercial_Duration_33,
    Factor_33,
    Commercial_Duration_34,
    Factor_34,
    Commercial_Duration_35,
    Factor_35,
    Commercial_Duration_36,
    Factor_36,
    Commercial_Duration_37,
    Factor_37,
    Commercial_Duration_38,
    Factor_38,
    Commercial_Duration_39,
    Factor_39,
    Commercial_Duration_40,
    Factor_40,
    Commercial_Duration_41,
    Factor_41,
    Commercial_Duration_42,
    Factor_42,
    Commercial_Duration_43,
    Factor_43,
    Commercial_Duration_44,
    Factor_44,
    Commercial_Duration_45,
    Factor_45,
    Commercial_Duration_46,
    Factor_46,
    Commercial_Duration_47,
    Factor_47,
    Commercial_Duration_48,
    Factor_48,
    Commercial_Duration_49,
    Factor_49,
    Commercial_Duration_50,
    Factor_50,
    Commercial_Duration_51,
    Factor_51,
    Commercial_Duration_52,
    Factor_52,
    Commercial_Duration_53,
    Factor_53,
    Commercial_Duration_54,
    Factor_54,
    Commercial_Duration_55,
    Factor_55,
    Commercial_Duration_56,
    Factor_56,
    Commercial_Duration_57,
    Factor_57,
    Commercial_Duration_58,
    Factor_58,
    Commercial_Duration_59,
    Factor_59,
    Commercial_Duration_60,
    Factor_60,
    Commercial_Duration_61,
    Factor_61,
    Commercial_Duration_62,
    Factor_62,
    Commercial_Duration_63,
    Factor_63,
    Commercial_Duration_64,
    Factor_64,
    Commercial_Duration_65,
    Factor_65,
    Commercial_Duration_66,
    Factor_66,
    Commercial_Duration_67,
    Factor_67,
    Commercial_Duration_68,
    Factor_68,
    Commercial_Duration_69,
    Factor_69,
    Commercial_Duration_70,
    Factor_70,
    Commercial_Duration_71,
    Factor_71,
    Commercial_Duration_72,
    Factor_72,
    Commercial_Duration_73,
    Factor_73,
    Commercial_Duration_74,
    Factor_74,
    Commercial_Duration_75,
    Factor_75,
    Commercial_Duration_76,
    Factor_76,
    Commercial_Duration_77,
    Factor_77,
    Commercial_Duration_78,
    Factor_78,
    Commercial_Duration_79,
    Factor_79,
    Commercial_Duration_80,
    Factor_80,
    Commercial_Duration_81,
    Factor_81,
    Commercial_Duration_82,
    Factor_82,
    Commercial_Duration_83,
    Factor_83,
    Commercial_Duration_84,
    Factor_84,
    Commercial_Duration_85,
    Factor_85,
    Commercial_Duration_86,
    Factor_86,
    Commercial_Duration_87,
    Factor_87,
    Commercial_Duration_88,
    Factor_88,
    Commercial_Duration_89,
    Factor_89,
    Commercial_Duration_90,
    Factor_90 )
    select @file_creation_date,
      @file_creation_time,
      @file_type,
      @file_version,
      @filename,
      Sales_House_Identifier=convert(integer,substr(imported_text,3,5)),
      Date_active_from=convert(date,substr(imported_text,8,8)),
      Date_active_to=case when convert(date,substr(imported_text,16,8)) is null then convert(date,'2999-12-31') else convert(date,substr(imported_text,16,8)) end, -- BARB uses NULL dates
      Log_Station_Code=convert(integer,substr(imported_text,24,5)),
      Base_Duration_Length=convert(integer,substr(imported_text,29,5)),
      Commercial_Duration_1=convert(integer,substr(imported_text,34,5)),
      Factor_1=convert(decimal(7,3),substr(imported_text,39,7)),
      Commercial_Duration_2=convert(integer,substr(imported_text,46,5)),
      Factor_2=convert(decimal(7,3),substr(imported_text,51,7)),
      Commercial_Duration_3=convert(integer,substr(imported_text,58,5)),
      Factor_3=convert(decimal(7,3),substr(imported_text,63,7)),
      Commercial_Duration_4=convert(integer,substr(imported_text,70,5)),
      Factor_4=convert(decimal(7,3),substr(imported_text,75,7)),
      Commercial_Duration_5=convert(integer,substr(imported_text,82,5)),
      Factor_5=convert(decimal(7,3),substr(imported_text,87,7)),
      Commercial_Duration_6=convert(integer,substr(imported_text,94,5)),
      Factor_6=convert(decimal(7,3),substr(imported_text,99,7)),
      Commercial_Duration_7=convert(integer,substr(imported_text,106,5)),
      Factor_7=convert(decimal(7,3),substr(imported_text,111,7)),
      Commercial_Duration_8=convert(integer,substr(imported_text,118,5)),
      Factor_8=convert(decimal(7,3),substr(imported_text,123,7)),
      Commercial_Duration_9=convert(integer,substr(imported_text,130,5)),
      Factor_9=convert(decimal(7,3),substr(imported_text,135,7)),
      Commercial_Duration_10=convert(integer,substr(imported_text,142,5)),
      Factor_10=convert(decimal(7,3),substr(imported_text,147,7)),
      Commercial_Duration_11=convert(integer,substr(imported_text,154,5)),
      Factor_11=convert(decimal(7,3),substr(imported_text,159,7)),
      Commercial_Duration_12=convert(integer,substr(imported_text,166,5)),
      Factor_12=convert(decimal(7,3),substr(imported_text,171,7)),
      Commercial_Duration_13=convert(integer,substr(imported_text,178,5)),
      Factor_13=convert(decimal(7,3),substr(imported_text,183,7)),
      Commercial_Duration_14=convert(integer,substr(imported_text,190,5)),
      Factor_14=convert(decimal(7,3),substr(imported_text,195,7)),
      Commercial_Duration_15=convert(integer,substr(imported_text,202,5)),
      Factor_15=convert(decimal(7,3),substr(imported_text,207,7)),
      Commercial_Duration_16=convert(integer,substr(imported_text,214,5)),
      Factor_16=convert(decimal(7,3),substr(imported_text,219,7)),
      Commercial_Duration_17=convert(integer,substr(imported_text,226,5)),
      Factor_17=convert(decimal(7,3),substr(imported_text,231,7)),
      Commercial_Duration_18=convert(integer,substr(imported_text,238,5)),
      Factor_18=convert(decimal(7,3),substr(imported_text,243,7)),
      Commercial_Duration_19=convert(integer,substr(imported_text,250,5)),
      Factor_19=convert(decimal(7,3),substr(imported_text,255,7)),
      Commercial_Duration_20=convert(integer,substr(imported_text,262,5)),
      Factor_20=convert(decimal(7,3),substr(imported_text,267,7)),
      Commercial_Duration_21=convert(integer,substr(imported_text,274,5)),
      Factor_21=convert(decimal(7,3),substr(imported_text,279,7)),
      Commercial_Duration_22=convert(integer,substr(imported_text,286,5)),
      Factor_22=convert(decimal(7,3),substr(imported_text,291,7)),
      Commercial_Duration_23=convert(integer,substr(imported_text,298,5)),
      Factor_23=convert(decimal(7,3),substr(imported_text,303,7)),


      Commercial_Duration_24=convert(integer,substr(imported_text,310,5)),
      Factor_24=convert(decimal(7,3),substr(imported_text,315,7)),
      Commercial_Duration_25=convert(integer,substr(imported_text,322,5)),
      Factor_25=convert(decimal(7,3),substr(imported_text,327,7)),
      Commercial_Duration_26=convert(integer,substr(imported_text,334,5)),
      Factor_26=convert(decimal(7,3),substr(imported_text,339,7)),
      Commercial_Duration_27=convert(integer,substr(imported_text,346,5)),
      Factor_27=convert(decimal(7,3),substr(imported_text,351,7)),
      Commercial_Duration_28=convert(integer,substr(imported_text,358,5)),
      Factor_28=convert(decimal(7,3),substr(imported_text,363,7)),
      Commercial_Duration_29=convert(integer,substr(imported_text,370,5)),
      Factor_29=convert(decimal(7,3),substr(imported_text,375,7)),
      Commercial_Duration_30=convert(integer,substr(imported_text,382,5)),
      Factor_30=convert(decimal(7,3),substr(imported_text,387,7)),
      Commercial_Duration_31=convert(integer,substr(imported_text,394,5)),
      Factor_31=convert(decimal(7,3),substr(imported_text,399,7)),
      Commercial_Duration_32=convert(integer,substr(imported_text,406,5)),
      Factor_32=convert(decimal(7,3),substr(imported_text,411,7)),
      Commercial_Duration_33=convert(integer,substr(imported_text,418,5)),
      Factor_33=convert(decimal(7,3),substr(imported_text,423,7)),
      Commercial_Duration_34=convert(integer,substr(imported_text,430,5)),
      Factor_34=convert(decimal(7,3),substr(imported_text,435,7)),
      Commercial_Duration_35=convert(integer,substr(imported_text,442,5)),
      Factor_35=convert(decimal(7,3),substr(imported_text,447,7)),
      Commercial_Duration_36=convert(integer,substr(imported_text,454,5)),
      Factor_36=convert(decimal(7,3),substr(imported_text,459,7)),
      Commercial_Duration_37=convert(integer,substr(imported_text,466,5)),
      Factor_37=convert(decimal(7,3),substr(imported_text,471,7)),
      Commercial_Duration_38=convert(integer,substr(imported_text,478,5)),
      Factor_38=convert(decimal(7,3),substr(imported_text,483,7)),
      Commercial_Duration_39=convert(integer,substr(imported_text,490,5)),
      Factor_39=convert(decimal(7,3),substr(imported_text,495,7)),
      Commercial_Duration_40=convert(integer,substr(imported_text,502,5)),
      Factor_40=convert(decimal(7,3),substr(imported_text,507,7)),
      Commercial_Duration_41=convert(integer,substr(imported_text,514,5)),
      Factor_41=convert(decimal(7,3),substr(imported_text,519,7)),
      Commercial_Duration_42=convert(integer,substr(imported_text,526,5)),
      Factor_42=convert(decimal(7,3),substr(imported_text,531,7)),
      Commercial_Duration_43=convert(integer,substr(imported_text,538,5)),
      Factor_43=convert(decimal(7,3),substr(imported_text,543,7)),
      Commercial_Duration_44=convert(integer,substr(imported_text,550,5)),
      Factor_44=convert(decimal(7,3),substr(imported_text,555,7)),
      Commercial_Duration_45=convert(integer,substr(imported_text,562,5)),
      Factor_45=convert(decimal(7,3),substr(imported_text,567,7)),
      Commercial_Duration_46=convert(integer,substr(imported_text,574,5)),
      Factor_46=convert(decimal(7,3),substr(imported_text,579,7)),
      Commercial_Duration_47=convert(integer,substr(imported_text,586,5)),
      Factor_47=convert(decimal(7,3),substr(imported_text,591,7)),
      Commercial_Duration_48=convert(integer,substr(imported_text,598,5)),
      Factor_48=convert(decimal(7,3),substr(imported_text,603,7)),
      Commercial_Duration_49=convert(integer,substr(imported_text,610,5)),
      Factor_49=convert(decimal(7,3),substr(imported_text,615,7)),
      Commercial_Duration_50=convert(integer,substr(imported_text,622,5)),
      Factor_50=convert(decimal(7,3),substr(imported_text,627,7)),
      Commercial_Duration_51=convert(integer,substr(imported_text,634,5)),
      Factor_51=convert(decimal(7,3),substr(imported_text,639,7)),
      Commercial_Duration_52=convert(integer,substr(imported_text,646,5)),
      Factor_52=convert(decimal(7,3),substr(imported_text,651,7)),
      Commercial_Duration_53=convert(integer,substr(imported_text,658,5)),
      Factor_53=convert(decimal(7,3),substr(imported_text,663,7)),
      Commercial_Duration_54=convert(integer,substr(imported_text,670,5)),
      Factor_54=convert(decimal(7,3),substr(imported_text,675,7)),
      Commercial_Duration_55=convert(integer,substr(imported_text,682,5)),
      Factor_55=convert(decimal(7,3),substr(imported_text,687,7)),
      Commercial_Duration_56=convert(integer,substr(imported_text,694,5)),
      Factor_56=convert(decimal(7,3),substr(imported_text,699,7)),
      Commercial_Duration_57=convert(integer,substr(imported_text,706,5)),
      Factor_57=convert(decimal(7,3),substr(imported_text,711,7)),
      Commercial_Duration_58=convert(integer,substr(imported_text,718,5)),
      Factor_58=convert(decimal(7,3),substr(imported_text,723,7)),
      Commercial_Duration_59=convert(integer,substr(imported_text,730,5)),
      Factor_59=convert(decimal(7,3),substr(imported_text,735,7)),
      Commercial_Duration_60=convert(integer,substr(imported_text,742,5)),
      Factor_60=convert(decimal(7,3),substr(imported_text,747,7)),
      Commercial_Duration_61=convert(integer,substr(imported_text,754,5)),
      Factor_61=convert(decimal(7,3),substr(imported_text,759,7)),
      Commercial_Duration_62=convert(integer,substr(imported_text,766,5)),
      Factor_62=convert(decimal(7,3),substr(imported_text,771,7)),
      Commercial_Duration_63=convert(integer,substr(imported_text,778,5)),
      Factor_63=convert(decimal(7,3),substr(imported_text,783,7)),
      Commercial_Duration_64=convert(integer,substr(imported_text,790,5)),
      Factor_64=convert(decimal(7,3),substr(imported_text,795,7)),
      Commercial_Duration_65=convert(integer,substr(imported_text,802,5)),
      Factor_65=convert(decimal(7,3),substr(imported_text,807,7)),
      Commercial_Duration_66=convert(integer,substr(imported_text,814,5)),
      Factor_66=convert(decimal(7,3),substr(imported_text,819,7)),
      Commercial_Duration_67=convert(integer,substr(imported_text,826,5)),
      Factor_67=convert(decimal(7,3),substr(imported_text,831,7)),
      Commercial_Duration_68=convert(integer,substr(imported_text,838,5)),
      Factor_68=convert(decimal(7,3),substr(imported_text,843,7)),
      Commercial_Duration_69=convert(integer,substr(imported_text,850,5)),
      Factor_69=convert(decimal(7,3),substr(imported_text,855,7)),
      Commercial_Duration_70=convert(integer,substr(imported_text,862,5)),
      Factor_70=convert(decimal(7,3),substr(imported_text,867,7)),
      Commercial_Duration_71=convert(integer,substr(imported_text,874,5)),
      Factor_71=convert(decimal(7,3),substr(imported_text,879,7)),
      Commercial_Duration_72=convert(integer,substr(imported_text,886,5)),
      Factor_72=convert(decimal(7,3),substr(imported_text,891,7)),
      Commercial_Duration_73=convert(integer,substr(imported_text,898,5)),
      Factor_73=convert(decimal(7,3),substr(imported_text,903,7)),
      Commercial_Duration_74=convert(integer,substr(imported_text,910,5)),
      Factor_74=convert(decimal(7,3),substr(imported_text,915,7)),
      Commercial_Duration_75=convert(integer,substr(imported_text,922,5)),
      Factor_75=convert(decimal(7,3),substr(imported_text,927,7)),
      Commercial_Duration_76=convert(integer,substr(imported_text,934,5)),
      Factor_76=convert(decimal(7,3),substr(imported_text,939,7)),
      Commercial_Duration_77=convert(integer,substr(imported_text,946,5)),
      Factor_77=convert(decimal(7,3),substr(imported_text,951,7)),
      Commercial_Duration_78=convert(integer,substr(imported_text,958,5)),
      Factor_78=convert(decimal(7,3),substr(imported_text,963,7)),
      Commercial_Duration_79=convert(integer,substr(imported_text,970,5)),
      Factor_79=convert(decimal(7,3),substr(imported_text,975,7)),
      Commercial_Duration_80=convert(integer,substr(imported_text,982,5)),
      Factor_80=convert(decimal(7,3),substr(imported_text,987,7)),
      Commercial_Duration_81=convert(integer,substr(imported_text,994,5)),
      Factor_81=convert(decimal(7,3),substr(imported_text,999,7)),
      Commercial_Duration_82=convert(integer,substr(imported_text,1006,5)),
      Factor_82=convert(decimal(7,3),substr(imported_text,1011,7)),
      Commercial_Duration_83=convert(integer,substr(imported_text,1018,5)),
      Factor_83=convert(decimal(7,3),substr(imported_text,1023,7)),
      Commercial_Duration_84=convert(integer,substr(imported_text,1030,5)),
      Factor_84=convert(decimal(7,3),substr(imported_text,1035,7)),
      Commercial_Duration_85=convert(integer,substr(imported_text,1042,5)),
      Factor_85=convert(decimal(7,3),substr(imported_text,1047,7)),
      Commercial_Duration_86=convert(integer,substr(imported_text,1054,5)),
      Factor_86=convert(decimal(7,3),substr(imported_text,1059,7)),
      Commercial_Duration_87=convert(integer,substr(imported_text,1066,5)),
      Factor_87=convert(decimal(7,3),substr(imported_text,1071,7)),
      Commercial_Duration_88=convert(integer,substr(imported_text,1078,5)),
      Factor_88=convert(decimal(7,3),substr(imported_text,1083,7)),
      Commercial_Duration_89=convert(integer,substr(imported_text,1090,5)),
      Factor_89=convert(decimal(7,3),substr(imported_text,1095,7)),
      Commercial_Duration_90=convert(integer,substr(imported_text,1102,5)),
      Factor_90=convert(decimal(7,3),substr(imported_text,1107,7))
      from MN_BARB_import
      where substr(imported_text,1,2) = '13'
  /*
VOD
4_0


CREATE TABLE BARB_MASTER_FILE_VOD_PROVIDER_RECORD
(
File_creation_date date,
File_creation_time time,
File_Type varchar(12),
File_version int,
Filename varchar(13),
VOD_PROVIDER_IDENTIFIER INT,
DATE_ACTIVE_FROM DATE,
DATE_ACTIVE_TO DATE,
VOD_PROVIDER_DESCRIPTION VARCHAR(30)
);


DROP TABLE BARB_MASTER_FILE_VOD_SERVICE_RECORD;

CREATE TABLE BARB_MASTER_FILE_VOD_SERVICE_RECORD
(
File_creation_date date,
File_creation_time time,
File_Type varchar(12),
File_version int,
Filename varchar(13),
VOD_SERVICE_IDENTIFIER INT,
DATE_ACTIVE_FROM DATE,
DATE_ACTIVE_TO DATE,
VOD_SERVICE_TYPE_DESCRIPTION VARCHAR(30),
BROADCAST_GROUP_ID INT);

DROP TABLE BARB_MASTER_FILE_VOD_TYPE_RECORD

CREATE TABLE BARB_MASTER_FILE_VOD_TYPE_RECORD
(
File_creation_date date,
File_creation_time time,
File_Type varchar(12),
File_version int,
Filename varchar(13),
VOD_TYPE_IDENTIFIER INT,
DATE_ACTIVE_FROM DATE,
DATE_ACTIVE_TO DATE,
VOD_TYPE_DESCRIPTION VARCHAR(30),
);

*/
  insert into BARB_MASTER_FILE_VOD_PROVIDER_RECORD
    ( file_creation_date,
    file_creation_time,
    file_type,
    file_version,
    filename,
    VOD_PROVIDER_IDENTIFIER,
    DATE_ACTIVE_FROM,
    DATE_ACTIVE_TO,
    VOD_PROVIDER_DESCRIPTION )
    select @file_creation_date,
      @file_creation_time,
      @file_type,
      @file_version,
      @filename,
      VOD_PROVIDER_IDENTIFIER=convert(integer,substr(imported_text,3,5)),
      DATE_ACTIVE_FROM=case when convert(date,substr(imported_text,8,8)) is null then convert(date,'2999-12-31') else convert(date,substr(imported_text,8,8)) end, -- BARB uses NULL dates
      DATE_ACTIVE_TO=case when convert(date,substr(imported_text,16,8)) is null then convert(date,'2999-12-31') else convert(date,substr(imported_text,16,8)) end,
      VOD_PROVIDER_DESCRIPTION=substr(imported_text,24,30)
      from MN_BARB_import
      where substr(imported_text,1,2) = '14'
  insert into BARB_MASTER_FILE_VOD_SERVICE_RECORD
    ( file_creation_date,
    file_creation_time,
    file_type,
    file_version,
    filename,
    VOD_SERVICE_IDENTIFIER,
    DATE_ACTIVE_FROM,
    DATE_ACTIVE_TO,
    VOD_SERVICE_TYPE_DESCRIPTION,
    BROADCAST_GROUP_ID )
    select @file_creation_date,
      @file_creation_time,
      @file_type,
      @file_version,
      @filename,
      VOD_SERVICE_IDENTIFIER=convert(integer,substr(imported_text,3,5)),
      DATE_ACTIVE_FROM=case when convert(date,substr(imported_text,8,8)) is null then convert(date,'2999-12-31') else convert(date,substr(imported_text,8,8)) end, -- BARB uses NULL dates
      DATE_ACTIVE_TO=case when convert(date,substr(imported_text,16,8)) is null then convert(date,'2999-12-31') else convert(date,substr(imported_text,16,8)) end,
      VOD_SERVICE_TYPE_DESCRIPTION=substr(imported_text,24,30),
      BROADCAST_GROUP_ID=convert(integer,substr(imported_text,54,5))
      from MN_BARB_import
      where substr(imported_text,1,2) = '15'
  insert into BARB_MASTER_FILE_VOD_TYPE_RECORD
    ( file_creation_date,
    file_creation_time,
    file_type,
    file_version,
    filename,
    VOD_TYPE_IDENTIFIER,
    DATE_ACTIVE_FROM,
    DATE_ACTIVE_TO,
    VOD_TYPE_DESCRIPTION )
    select @file_creation_date,
      @file_creation_time,
      @file_type,
      @file_version,
      @filename,
      VOD_TYPE_IDENTIFIER=convert(integer,substr(imported_text,3,5)),
      DATE_ACTIVE_FROM=case when convert(date,substr(imported_text,8,8)) is null then convert(date,'2999-12-31') else convert(date,substr(imported_text,8,8)) end, -- BARB uses NULL dates
      DATE_ACTIVE_TO=case when convert(date,substr(imported_text,16,8)) is null then convert(date,'2999-12-31') else convert(date,substr(imported_text,16,8)) end,
      VOD_TYPE_DESCRIPTION=substr(imported_text,24,30)
      from MN_BARB_import
      where substr(imported_text,1,2) = '16'
  /*
4_2



CREATE TABLE BARB_MASTER_FILE_DEVICE_IN_USE_TYPE_RECORD
(
File_creation_date date,
File_creation_time time,
File_Type varchar(12),
File_version int,
Filename varchar(13),
DEVICE_IN_USE_TYPE_IDENTIER INT,
DATE_ACTIVE_FROM DATE,
DATE_ACTIVE_TO DATE,
DEVICE_IN_USE_NAME VARCHAR(30));


*/
  insert into BARB_MASTER_FILE_DEVICE_IN_USE_TYPE_RECORD
    ( file_creation_date,
    file_creation_time,
    file_type,
    file_version,
    filename,
    DEVICE_IN_USE_TYPE_IDENTIER,
    DATE_ACTIVE_FROM,
    DATE_ACTIVE_TO,
    DEVICE_IN_USE_NAME )
    select @file_creation_date,
      @file_creation_time,
      @file_type,
      @file_version,
      @filename,
      DEVICE_IN_USE_TYPE_IDENTIER=convert(integer,substr(imported_text,3,4)),
      DATE_ACTIVE_FROM=case when convert(date,substr(imported_text,8,8)) is null then convert(date,'2999-12-31') else convert(date,substr(imported_text,8,8)) end, -- BARB uses NULL dates
      DATE_ACTIVE_TO=case when convert(date,substr(imported_text,16,8)) is null then convert(date,'2999-12-31') else convert(date,substr(imported_text,16,8)) end,
      DEVICE_IN_USE_NAME=substr(imported_text,23,30)
      from MN_BARB_import
      where substr(imported_text,1,2) = '17'
  create table #mn_temp(
    n integer null default null,
    )
  declare @i integer
  set @i = 1
  while @i < 91
    begin
      insert into #mn_temp values( @i )
      set @i = @i+1
    end
  insert into BARB_MASTER_FILE_COMMERCIAL_LENGTH_RATE_FACTORS_RECORD_UNPVT
    ( File_Creation_date,
    File_Creation_time,
    File_Type,
    File_Version,
    Filename,
    sales_house_identifier,
    date_active_from,
    date_active_to,
    log_station_code,
    base_duration_length,
    commercial_duration,
    factor )
    select File_Creation_date,
      File_Creation_time,
      File_Type,
      File_Version,
      Filename,
      sales_house_identifier,
      date_active_from,
      date_active_to,
      log_station_code,
      base_duration_length,
      commercial_duration,
      factor
      from(select File_Creation_date,
          File_Creation_time,
          File_Type,
          File_Version,
          Filename,
          sales_house_identifier,
          date_active_from,
          date_active_to,
          log_station_code,
          base_duration_length,
          n,
          commercial_duration=case n
          when 1 then commercial_duration_1
          when 2 then commercial_duration_2
          when 1 then commercial_duration_1
          when 2 then commercial_duration_2
          when 3 then commercial_duration_3
          when 4 then commercial_duration_4
          when 5 then commercial_duration_5
          when 6 then commercial_duration_6
          when 7 then commercial_duration_7
          when 8 then commercial_duration_8
          when 9 then commercial_duration_9
          when 10 then commercial_duration_10
          when 11 then commercial_duration_11
          when 12 then commercial_duration_12
          when 13 then commercial_duration_13
          when 14 then commercial_duration_14
          when 15 then commercial_duration_15
          when 16 then commercial_duration_16
          when 17 then commercial_duration_17
          when 18 then commercial_duration_18
          when 19 then commercial_duration_19
          when 20 then commercial_duration_20
          when 21 then commercial_duration_21
          when 22 then commercial_duration_22
          when 23 then commercial_duration_23
          when 24 then commercial_duration_24
          when 25 then commercial_duration_25
          when 26 then commercial_duration_26
          when 27 then commercial_duration_27
          when 28 then commercial_duration_28
          when 29 then commercial_duration_29
          when 30 then commercial_duration_30
          when 31 then commercial_duration_31
          when 32 then commercial_duration_32
          when 33 then commercial_duration_33
          when 34 then commercial_duration_34
          when 35 then commercial_duration_35
          when 36 then commercial_duration_36
          when 37 then commercial_duration_37
          when 38 then commercial_duration_38
          when 39 then commercial_duration_39
          when 40 then commercial_duration_40
          when 41 then commercial_duration_41
          when 42 then commercial_duration_42
          when 43 then commercial_duration_43
          when 44 then commercial_duration_44
          when 45 then commercial_duration_45
          when 46 then commercial_duration_46
          when 47 then commercial_duration_47
          when 48 then commercial_duration_48
          when 49 then commercial_duration_49
          when 50 then commercial_duration_50
          when 51 then commercial_duration_51
          when 52 then commercial_duration_52
          when 53 then commercial_duration_53
          when 54 then commercial_duration_54
          when 55 then commercial_duration_55
          when 56 then commercial_duration_56
          when 57 then commercial_duration_57
          when 58 then commercial_duration_58
          when 59 then commercial_duration_59
          when 60 then commercial_duration_60
          when 61 then commercial_duration_61
          when 62 then commercial_duration_62
          when 63 then commercial_duration_63
          when 64 then commercial_duration_64
          when 65 then commercial_duration_65
          when 66 then commercial_duration_66
          when 67 then commercial_duration_67
          when 68 then commercial_duration_68
          when 69 then commercial_duration_69
          when 70 then commercial_duration_70
          when 71 then commercial_duration_71
          when 72 then commercial_duration_72
          when 73 then commercial_duration_73
          when 74 then commercial_duration_74
          when 75 then commercial_duration_75
          when 76 then commercial_duration_76
          when 77 then commercial_duration_77
          when 78 then commercial_duration_78
          when 79 then commercial_duration_79
          when 80 then commercial_duration_80
          when 81 then commercial_duration_81
          when 82 then commercial_duration_82
          when 83 then commercial_duration_83
          when 84 then commercial_duration_84
          when 85 then commercial_duration_85
          when 86 then commercial_duration_86
          when 87 then commercial_duration_87
          when 88 then commercial_duration_88
          when 89 then commercial_duration_89
          when 90 then commercial_duration_90 end,
          factor=case n
          when 1 then factor_1
          when 2 then factor_2
          when 3 then factor_3
          when 4 then factor_4
          when 5 then factor_5
          when 6 then factor_6
          when 7 then factor_7
          when 8 then factor_8
          when 9 then factor_9
          when 10 then factor_10
          when 11 then factor_11
          when 12 then factor_12
          when 13 then factor_13
          when 14 then factor_14
          when 15 then factor_15
          when 16 then factor_16
          when 17 then factor_17
          when 18 then factor_18
          when 19 then factor_19
          when 20 then factor_20
          when 21 then factor_21
          when 22 then factor_22
          when 23 then factor_23
          when 24 then factor_24
          when 25 then factor_25
          when 26 then factor_26
          when 27 then factor_27
          when 28 then factor_28
          when 29 then factor_29
          when 30 then factor_30
          when 31 then factor_31
          when 32 then factor_32
          when 33 then factor_33
          when 34 then factor_34
          when 35 then factor_35
          when 36 then factor_36
          when 37 then factor_37
          when 38 then factor_38
          when 39 then factor_39
          when 40 then factor_40
          when 41 then factor_41
          when 42 then factor_42
          when 43 then factor_43
          when 44 then factor_44
          when 45 then factor_45
          when 46 then factor_46
          when 47 then factor_47
          when 48 then factor_48
          when 49 then factor_49
          when 50 then factor_50
          when 51 then factor_51
          when 52 then factor_52
          when 53 then factor_53
          when 54 then factor_54
          when 55 then factor_55
          when 56 then factor_56
          when 57 then factor_57
          when 58 then factor_58
          when 59 then factor_59
          when 60 then factor_60
          when 61 then factor_61
          when 62 then factor_62
          when 63 then factor_63
          when 64 then factor_64
          when 65 then factor_65
          when 66 then factor_66
          when 67 then factor_67
          when 68 then factor_68
          when 69 then factor_69
          when 70 then factor_70
          when 71 then factor_71
          when 72 then factor_72
          when 73 then factor_73
          when 74 then factor_74
          when 75 then factor_75
          when 76 then factor_76
          when 77 then factor_77
          when 78 then factor_78
          when 79 then factor_79
          when 80 then factor_80
          when 81 then factor_81
          when 82 then factor_82
          when 83 then factor_83
          when 84 then factor_84
          when 85 then factor_85
          when 86 then factor_86
          when 87 then factor_87
          when 88 then factor_88
          when 89 then factor_89
          when 90 then factor_90 end
          from BARB_MASTER_FILE_COMMERCIAL_LENGTH_RATE_FACTORS_RECORD as clrfr
            cross join(select * from #mn_temp) as nums( n ) ) as a
      where commercial_duration is not null
end
