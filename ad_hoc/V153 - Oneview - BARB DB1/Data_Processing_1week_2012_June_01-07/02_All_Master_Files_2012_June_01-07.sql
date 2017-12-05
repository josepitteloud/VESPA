/* Combining all data into one table */

-- Table_1 --BARB_Log_Station_Relationship_to_DB1_Station_Record_02_2012_06_01_07

select * into BARB_Log_Station_Relationship_to_DB1_Station_Record_02_2012_06_01_07 from BARB_Log_Station_Relationship_to_DB1_Station_Record_01
union all
select * from BARB_Log_Station_Relationship_to_DB1_Station_Record_02
union all
select * from BARB_Log_Station_Relationship_to_DB1_Station_Record_03
union all
select * from BARB_Log_Station_Relationship_to_DB1_Station_Record_04
union all
select * from BARB_Log_Station_Relationship_to_DB1_Station_Record_05
union all
select * from BARB_Log_Station_Relationship_to_DB1_Station_Record_06
union all
select * from BARB_Log_Station_Relationship_to_DB1_Station_Record_07
--7,413 Row(s) affected

select top 100* from BARB_Log_Station_Relationship_to_DB1_Station_Record_02_2012_06_01_07

select distinct  Log_Station_Code
                ,DB1_Station_Code
                ,Relationship_Start_Date
                ,Relationship_End_Date
into BARB_Log_Station_Relationship_to_DB1_Station_Record_02_2012_06_01_07_distinct from BARB_Log_Station_Relationship_to_DB1_Station_Record_02_2012_06_01_07
--1,059 Row(s) affected



select * from BARB_Log_Station_Relationship_to_DB1_Station_Record_02_2012_06_01_07_Recoded
where DB1_Station_Code = 04882


--Adding the preceeding zero unto DB1_Station_Code
select  log_Station_Code
        ,cast(DB1_Station_Code as Varchar)
--        ,'00000' ||DB1_Station_Code as New_Station_Code
        ,cast(right('00000' ||DB1_Station_Code,5) as Varchar) as Station_Code
        ,Relationship_Start_Date
        ,Relationship_End_Date
into BARB_Log_Station_Relationship_to_DB1_Station_Record_02_2012_06_01_07_Recoded
from BARB_Log_Station_Relationship_to_DB1_Station_Record_02_2012_06_01_07_distinct
--1,059 Row(s) affected
