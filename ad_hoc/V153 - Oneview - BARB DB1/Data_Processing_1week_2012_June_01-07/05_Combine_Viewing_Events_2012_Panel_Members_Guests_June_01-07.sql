                                /*
Analyst : Patrick Igonor
Date    : 15th of July 2013
Lead    : Claudio Lima
*/

--Copy Claudio's table into my Schema ---
select * into DB1_Station_Code_TO_Channel_Name_Updated from limac.DB1_Station_Code_TO_Channel_Name
--328 Row(s) affected

--Tables of interest -
select top 10* from BARB_VIEWING_RECORD_PANEL_MEMBERS_2012_06_01_07_Code_Descr;
select top 10* from BARB_VIEWING_RECORD_PANEL_GUESTS_2012_06_01_07_Code_Descr;
select top 10* from DB1_Station_Code_TO_Channel_Name_Updated


--Updating the DB1_Station_Code_TO_Channel_Name_Updated table with Service_Keys and Channel names with programme names from the Programme_Schedule table
select a.*,b.vespa_name
into DB1_Station_Code_TO_Channel_Name_Updated_Final_Vespa_Name
from DB1_Station_Code_TO_Channel_Name_Updated_Final a
left join VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES b
on a.new_service_key = b.service_key
and b.effective_from <= '2012-06-01'
and b.effective_to >= '2012-06-07'


select   DB1_Station_Code
        ,Service_Key
        ,case  when Service_Key = 6391  then 6000
               when Service_Key = 6365  then 6155
               when Service_Key = 4044  then 1627
               when Service_Key = 1629  then 1629
               when Service_Key = 4043  then 3360
               when Service_Key = 3825  then 2303
               when Service_Key = 4087  then 1873
               when Service_Key = 4058  then 1801
               when Service_Key = 4075  then 1625
               when Service_Key = 1675  then 1670
               when Service_Key = 1839  then 1839
               when Service_Key = 2156  then 2153
               when Service_Key = 2017  then 2006
               when Service_Key = 4061  then 1402
               when Service_Key = 2018  then 2018
               when Service_Key = 2019  then 2019
               when Service_Key = 4076  then 1628
               when Service_Key = 5701  then 5701
               when Service_Key = 1857  then 1857
               when Service_Key = 4014  then 1001
               when Service_Key = 5605  then 5605
               when Service_Key = 1372  then 1372
               when Service_Key = 4053  then 1412
               when Service_Key = 4021  then 1409
               when Service_Key = 3300  then 1668
               when Service_Key = 4019  then 1002
               when Service_Key = 2304  then 2903
               when Service_Key = 2205  then 2205
               when Service_Key = 4066  then 2201
               when Service_Key = 3207  then 3207
               when Service_Key = 4033  then 1814
               when Service_Key = 6505  then 6503
               when Service_Key = 2502  then 2502
               when Service_Key = 4016  then 1816
               when Service_Key = 4074  then 2505
               when Service_Key = 2513  then 2513
               when Service_Key = 1360  then 3730
               when Service_Key = 1823  then 1823
               when Service_Key = 2611  then 2611
               when Service_Key = 2020  then 2020
               when Service_Key = 4420  then 4420
               when Service_Key = 4018  then 1808
               when Service_Key = 3358  then 3358
               when Service_Key = 4025  then 1847
               when Service_Key = 2075  then 2153
               when Service_Key = 4064  then 1753
               when Service_Key = 4080  then 1842
               when Service_Key = 4063  then 1752
               when Service_Key = 4015  then 1815
               when Service_Key = 4073  then 4073
               when Service_Key = 2061  then 2061
               when Service_Key = 3590  then 3590
               when Service_Key = 1845  then 1845
               when Service_Key = 3802  then 3802
               when Service_Key = 4081  then 1702
               when Service_Key = 1858  then 1858
               when Service_Key = 1889  then 1889
               when Service_Key = 2325  then 2325
               when Service_Key = 5601  then 5601
               when Service_Key = 3028  then 3028
               when Service_Key = 1833  then 1833
               when Service_Key = 2703  then 2703
               when Service_Key = 5609  then 5609
               when Service_Key = 1879  then 1879
               when Service_Key = 2512  then 2512
               when Service_Key = 1890  then 1890
               when Service_Key = 4069  then 1846
               when Service_Key = 4062  then 1818
               when Service_Key = 3760  then 3760
               when Service_Key = 6533  then 6260
               when Service_Key = 3781  then 3781
               when Service_Key = 3771  then 3771
               when Service_Key = 1853  then 1853
               when Service_Key = 1877  then 1877
               when Service_Key = 4421  then 4421
               when Service_Key = 4071  then 1881
               when Service_Key = 6532  then 6240
               when Service_Key = 3780  then 3780
               when Service_Key = 1841  then 1841
               when Service_Key = 1813  then 1813
               when Service_Key = 3408  then 3408
               when Service_Key = 1844  then 1844
               when Service_Key = 2509  then 2509
               when Service_Key = 1371  then 1371
               else Service_Key end as New_Service_Key
              ,Channel_Name
into DB1_Station_Code_TO_Channel_Name_Updated_Final
from DB1_Station_Code_TO_Channel_Name_Updated
--328 Row(s) affected

--Picking the channels from the table VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES
select a.*,b.vespa_name
into DB1_Station_Code_TO_Channel_Name_Updated_Final_Vespa_Name
from DB1_Station_Code_TO_Channel_Name_Updated_Final a
left join VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES b
on a.new_service_key = b.service_key
and b.effective_from <= '2012-06-01'
and b.effective_to >= '2012-06-07'
--328 Row(s) affected

----Member_Panel_Viewing Events table-----------------

select
         MB.Household_number
        ,MB.Set_number
        ,MB.Date_of_Activity_DB1
        ,MB.Event_Start_Date_Time
        ,MB.Event_End_Date_Time
        ,MB.Duration_of_session
        ,MB.DB1_Station_Code
        ,MB.Date_of_Recording_DB1
        ,MB.Start_time_of_recording
        ,MB.Recording_Start_Date_Time
        ,MB.Recording_End_Date_Time
        ,MB.Person_1_viewing
        ,MB.Person_2_viewing
        ,MB.Person_3_viewing
        ,MB.Person_4_viewing
        ,MB.Person_5_viewing
        ,MB.Person_6_viewing
        ,MB.Person_7_viewing
        ,MB.Person_8_viewing
        ,MB.Person_9_viewing
        ,MB.Person_10_viewing
        ,MB.Person_11_viewing
        ,MB.Person_12_viewing
        ,MB.Person_13_viewing
        ,MB.Person_14_viewing
        ,MB.Person_15_viewing
        ,MB.Person_16_viewing
        ,MB.Session_activity_type_
        ,MB.Playback_type_
        ,MB.Viewing_platform_
        ,MB.Interactive_Bar_Code_Identifier
        ,CN.Service_Key
        ,CN.New_Service_Key
        ,CN.Channel_Name
        ,CN.vespa_name
into DB1_Station_Code_TO_Channel_Name_Members
from BARB_VIEWING_RECORD_PANEL_MEMBERS_2012_06_01_07_Code_Descr MB
left join DB1_Station_Code_TO_Channel_Name_Updated_Final_Vespa_Name CN
on  MB.DB1_Station_Code = CN.DB1_Station_Code
--830,257 Row(s) affected

--Adding the Sky_Vieing field unto the above table
Alter table DB1_Station_Code_TO_Channel_Name_Members add Sky_Viewing bit default 0

-- Updating the DB1_Station_Code_TO_Channel_Name_Members table with Sky_Viewing
update DB1_Station_Code_TO_Channel_Name_Members GD
set Sky_Viewing = 1
    from BARB_PANEL_VIEWING_FILE_TV_SET_CHARACTERISTICS_2012_06_01_07_Code_Descr ST
    where GD.Household_number =ST.Household_number
    and GD.Set_number =ST.Set_number
    and(ST.reception_capability_code_1_ = 'Sky'
    or ST.reception_capability_code_2_ = 'Sky'
    or ST.reception_capability_code_3_ = 'Sky')
--409,529 Row(s) affected

---Guests_Panel_Viewing Events table--------
select
         GT.Household_number
        ,GT.Set_number
        ,GT.Date_of_Activity_DB1
        ,GT.Event_Start_Date_Time
        ,GT.Event_End_Date_Time
        ,GT.Duration_of_session
        ,GT.DB1_Station_Code
        ,GT.Date_of_Recording_DB1
        ,GT.Start_time_of_recording
        ,GT.Recording_Start_Date_Time
        ,GT.Recording_End_Date_Time
        ,GT.Male_4_9
        ,GT.Male_10_15
        ,GT.Male_16_19
        ,GT.Male_20_24
        ,GT.Male_25_34
        ,GT.Male_35_44
        ,GT.Male_45_64
        ,GT.Male_65_plus
        ,GT.Female_4_9
        ,GT.Female_10_15
        ,GT.Female_16_19
        ,GT.Female_20_24
        ,GT.Female_25_34
        ,GT.Female_35_44
        ,GT.Female_45_64
        ,GT.Female_65_plus
        ,GT.Session_activity_type_
        ,GT.Playback_type_
        ,GT.Viewing_platform_
        ,GT.Interactive_Bar_Code_Identifier
        ,CN.Service_Key
        ,CN.New_Service_Key
        ,CN.Channel_Name
        ,CN.vespa_name
into DB1_Station_Code_TO_Channel_Name_Guests
from BARB_VIEWING_RECORD_PANEL_GUESTS_2012_06_01_07_Code_Descr GT
left join DB1_Station_Code_TO_Channel_Name_Updated_Final_Vespa_Name CN
on GT.DB1_Station_Code = CN.DB1_Station_Code
--54,188 Row(s) affected

--Adding the Sky_Vieing field unto the above table
alter table DB1_Station_Code_TO_Channel_Name_Guests add Sky_Viewing bit default 0

-- Updating the DB1_Station_Code_TO_Channel_Name_Guests table with Sky_Viewing
update DB1_Station_Code_TO_Channel_Name_Guests GD
set Sky_Viewing = 1
    from BARB_PANEL_VIEWING_FILE_TV_SET_CHARACTERISTICS_2012_06_01_07_Code_Descr ST
    where GD.Household_number =ST.Household_number
    and GD.Set_number =ST.Set_number
    and(ST.reception_capability_code_1_ = 'Sky'
    or ST.reception_capability_code_2_ = 'Sky'
    or ST.reception_capability_code_3_ = 'Sky')
--27,717 Row(s) affected

--Combining both the Panel_Members and the Guests Viewing events tables into One table ---

select   coalesce(MB.Household_number,GT.Household_number)as Household_number
        ,coalesce(MB.Set_number,GT.Set_number)as Set_number
        ,coalesce(MB.Date_of_Activity_DB1,GT.Date_of_Activity_DB1)as Date_of_Activity_DB1
        ,coalesce(MB.Event_Start_Date_Time,GT.Event_Start_Date_Time)as Event_Start_Date_Time
        ,coalesce(MB.Event_End_Date_Time,GT.Event_End_Date_Time)as Event_End_Date_Time
        ,coalesce(MB.Duration_of_session, GT.Duration_of_session) as Duration_of_session
        ,coalesce(MB.DB1_Station_Code,GT.DB1_Station_Code) as DB1_Station_Code
        ,coalesce(MB.Date_of_Recording_DB1,GT.Date_of_Recording_DB1)as Date_of_Recording_DB1
        ,coalesce(MB.Start_time_of_recording,GT.Start_time_of_recording) as Start_time_of_recording
        ,coalesce(MB.Recording_Start_Date_Time,GT.Recording_Start_Date_Time)as Recording_Start_Date_Time
        ,coalesce(MB.Recording_End_Date_Time,GT.Recording_End_Date_Time)as Recording_End_Date_Time
        ,coalesce(MB.Interactive_Bar_Code_Identifier,GT.Interactive_Bar_Code_Identifier)as Interactive_Bar_Code_Identifier
        ,coalesce(MB.Person_1_viewing,0) as Person_1_viewing
        ,coalesce(MB.Person_2_viewing,0) as Person_2_viewing
        ,coalesce(MB.Person_3_viewing,0) as Person_3_viewing
        ,coalesce(MB.Person_4_viewing,0) as Person_4_viewing
        ,coalesce(MB.Person_5_viewing,0) as Person_5_viewing
        ,coalesce(MB.Person_6_viewing,0) as Person_6_viewing
        ,coalesce(MB.Person_7_viewing,0) as Person_7_viewing
        ,coalesce(MB.Person_8_viewing,0) as Person_8_viewing
        ,coalesce(MB.Person_9_viewing,0) as Person_9_viewing
        ,coalesce(MB.Person_10_viewing,0) as Person_10_viewing
        ,coalesce(MB.Person_11_viewing,0) as Person_11_viewing
        ,coalesce(MB.Person_12_viewing,0) as Person_12_viewing
        ,coalesce(MB.Person_13_viewing,0) as Person_13_viewing
        ,coalesce(MB.Person_14_viewing,0) as Person_14_viewing
        ,coalesce(MB.Person_15_viewing,0) as Person_15_viewing
        ,coalesce(MB.Person_16_viewing,0) as Person_16_viewing
        ,coalesce(MB.Person_1_viewing,0)+coalesce(MB.Person_2_viewing,0)+coalesce(MB.Person_3_viewing,0)
        +coalesce(MB.Person_4_viewing,0)+coalesce(MB.Person_5_viewing,0)+coalesce(MB.Person_6_viewing,0)
        +coalesce(MB.Person_7_viewing,0)+coalesce(MB.Person_8_viewing,0)+coalesce(MB.Person_9_viewing,0)
        +coalesce(MB.Person_10_viewing,0)+coalesce(MB.Person_11_viewing,0)+coalesce(MB.Person_12_viewing,0)
        +coalesce(MB.Person_13_viewing,0)+coalesce(MB.Person_14_viewing,0)+coalesce(MB.Person_15_viewing,0)
        +coalesce(MB.Person_16_viewing,0) as Number_Panel_Members
        ,coalesce(MB.Session_activity_type_,GT.Session_activity_type_) as Session_activity_type_
        ,coalesce(MB.Playback_type_,GT.Playback_type_) as Playback_type_
        ,coalesce(MB.Viewing_platform_,GT.Viewing_platform_) as Viewing_platform_
        ,coalesce(MB.Sky_Viewing,GT.Sky_Viewing) as Sky_Viewing
        ,coalesce(MB.Service_Key,GT.Service_Key) as Service_Key
        ,coalesce(MB.New_Service_Key,GT.New_Service_Key) as New_Service_Key
        ,coalesce(MB.Channel_Name,GT.Channel_Name) as Channel_Name
        ,coalesce(MB.vespa_name,GT.vespa_Name) as vespa_name
        ,coalesce(GT.Male_4_9,0) as Male_4_9
        ,coalesce(GT.Male_10_15,0) as Male_10_15
        ,coalesce(GT.Male_16_19,0) as Male_16_19
        ,coalesce(GT.Male_20_24,0) as Male_20_24
        ,coalesce(GT.Male_25_34,0) as Male_25_34
        ,coalesce(GT.Male_35_44,0) as Male_35_44
        ,coalesce(GT.Male_45_64,0) as Male_45_64
        ,coalesce(GT.Male_65_plus,0) as Male_65_plus
        ,coalesce(GT.Female_4_9,0) as Female_4_9
        ,coalesce(GT.Female_10_15,0) as Female_10_15
        ,coalesce(GT.Female_16_19,0) as Female_16_19
        ,coalesce(GT.Female_20_24,0) as Female_20_24
        ,coalesce(GT.Female_25_34,0) as Female_25_34
        ,coalesce(GT.Female_35_44,0) as Female_35_44
        ,coalesce(GT.Female_45_64,0) as Female_45_64
        ,coalesce(GT.Female_65_plus,0) as Female_65_plus
        ,coalesce(GT.Male_4_9,0)+coalesce(GT.Male_10_15,0)+coalesce(GT.Male_16_19,0)
        +coalesce(GT.Male_20_24,0)+coalesce(GT.Male_25_34,0)+coalesce(GT.Male_35_44,0)+coalesce(GT.Male_45_64,0)
        +coalesce(GT.Male_65_plus,0)+coalesce(GT.Female_4_9,0)+coalesce(GT.Female_10_15,0)
        +coalesce(GT.Female_16_19,0)+coalesce(GT.Female_20_24,0)+coalesce(GT.Female_25_34,0)
        +coalesce(GT.Female_35_44,0)+coalesce(GT.Female_45_64,0)+coalesce(GT.Female_65_plus,0) as Number_Guests
into Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07
from DB1_Station_Code_TO_Channel_Name_Members MB
full join DB1_Station_Code_TO_Channel_Name_Guests GT
on MB.Household_number = GT.Household_number
and MB.Set_number = GT.Set_number
and MB.Event_Start_Date_Time = GT.Event_Start_Date_Time
--846,474 Row(s) affected
select top 10* from Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07
select top 1000* from Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07
--Note ---There are 73,134 null Service Keys...this is due to the fact that, there are DB1_Station_Codes that are not in the master file (The file that has both DB1_Station_Code and Log_Station_Code)

--Checks
select DB1_Station_Code from Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07 where Service_Key is null --73,134
select distinct DB1_Station_Code from Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07 where Service_Key is null --22

--How many DB1_Station_Codes from the Panel Members are missing in the master table
select count(distinct A.DB1_Station_Code)
from BARB_VIEWING_RECORD_PANEL_MEMBERS_2012_06_01_07_Code_Descr A
left join BARB_Log_Station_Relationship_to_DB1_Station_Record_02_2012_06_01_07_Recoded B
on A.DB1_Station_Code = B.Station_Code
where B.Station_Code is null --8
--Checking the log stations with null service keys
select count(distinct log_station_code) from vespa_analysts.channel_map_dev_service_key_barb
where service_key is null --252
--Checking the number of events affected----
select DB1_Station_Code,Date_of_Activity_DB1,count(*)as Num_Events from Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07
where DB1_Station_Code in ('00990','04882','04950','04901','04344','04904','04907','04162')
and Date_of_Activity_DB1 >= '2012-06-01'
group by DB1_Station_Code,Date_of_Activity_DB1
order by DB1_Station_Code,Date_of_Activity_DB1

--There are 8 DB1_Station_Code NOT in the Master file (The master file has the relationship between the DB1_Station_Code and log_Station_Code)
select DB1_Station_Code,count(*)as Num_Events from Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07
where DB1_Station_Code in ('00990','04882','04950','04901','04344','04904','04907','04162')
--and Date_of_Activity_DB1 >= '2012-06-01'
group by DB1_Station_Code

--Selecting the log_station_code without service_keys
select distinct log_Station_Code from BARB_Log_Station_Relationship_to_DB1_Station_Record_02_2012_06_01_07_Recoded
where Station_Code in ('04974','06780','06785','06786','06787','06788','06789','06790','06861','06880','06881','06882','06883','06886')

--Checking that indeed there are no service keys for the above log_station_codes --
select * from VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_BARB
where log_station_code in (4974,6780,6785,6786,6787,6788,6789,6790,6861,6880,6881,6882,6883,6886)
--------------------------------------------------------------------------------------------------------------------------------------------------------
--Creating indexes to speed up the process
create hg index idx101 on Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07(service_key,Event_Start_Date_Time,Event_End_Date_Time,Recording_Start_Date_Time,Recording_End_Date_Time);

----Combining the above table to the Programme Schedule Table in order to obtain the Programme Instances---

Select CB.Household_number
      ,CB.Set_number
      ,CB.Date_of_Activity_DB1
      ,CB.Event_Start_Date_Time
      ,CB.Event_End_Date_Time
      ,CB.Duration_of_session
      ,CB.DB1_Station_Code
      ,CB.Date_of_Recording_DB1
      ,CB.Start_time_of_recording
      ,CB.Person_1_viewing
      ,CB.Person_2_viewing
      ,CB.Person_3_viewing
      ,CB.Person_4_viewing
      ,CB.Person_5_viewing
      ,CB.Person_6_viewing
      ,CB.Person_7_viewing
      ,CB.Person_8_viewing
      ,CB.Person_9_viewing
      ,CB.Person_10_viewing
      ,CB.Person_11_viewing
      ,CB.Person_12_viewing
      ,CB.Person_13_viewing
      ,CB.Person_14_viewing
      ,CB.Person_15_viewing
      ,CB.Person_16_viewing
      ,CB.Number_Panel_Members
      ,CB.Session_activity_type_
      ,CB.Playback_type_
      ,CB.Viewing_platform_
      ,CB.Sky_Viewing
      ,CB.New_Service_Key
      ,CB.Channel_Name
      ,CB.vespa_name
      ,CB.Male_4_9
      ,CB.Male_10_15
      ,CB.Male_16_19
      ,CB.Male_20_24
      ,CB.Male_25_34
      ,CB.Male_35_44
      ,CB.Male_45_64
      ,CB.Male_65_plus
      ,CB.Female_4_9
      ,CB.Female_10_15
      ,CB.Female_16_19
      ,CB.Female_20_24
      ,CB.Female_25_34
      ,CB.Female_35_44
      ,CB.Female_45_64
      ,CB.Female_65_plus
      ,CB.Number_Guests
      ,CB.Interactive_Bar_Code_Identifier
      ,SK.programme_name
      ,SK.genre_description
      ,SK.sub_genre_description
      ,SK.synopsis
      ,SK.broadcast_start_date_time_utc
      ,SK.broadcast_end_date_time_utc
into Final_Combination_Panel_Members_Guest_with_Prog_Instances
from Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07 CB
left join SK_Prod.VESPA_PROGRAMME_SCHEDULE  SK
on CB.New_Service_Key = SK.service_key
where SK.broadcast_end_date_time_utc >= coalesce(CB.Recording_Start_Date_Time,CB.Event_Start_Date_Time)
and SK.broadcast_start_date_time_utc <= coalesce(CB.Recording_End_Date_Time,CB.Event_End_Date_Time)
--2,070,384 Row(s) affected

--Inserting the missing viewing events with null service keys into the final table--
Insert into Final_Combination_Panel_Members_Guest_with_Prog_Instances
(      Household_number
      ,Set_number
      ,Date_of_Activity_DB1
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
      ,Number_Panel_Members
      ,Session_activity_type_
      ,Playback_type_
      ,Viewing_platform_
      ,Sky_Viewing
      ,New_Service_Key
      ,Channel_Name
      ,vespa_name
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
      ,Number_Guests
      ,Interactive_Bar_Code_Identifier
)
select
       Household_number
      ,Set_number
      ,Date_of_Activity_DB1
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
      ,Number_Panel_Members
      ,Session_activity_type_
      ,Playback_type_
      ,Viewing_platform_
      ,Sky_Viewing
      ,New_Service_Key
      ,Channel_Name
      ,vespa_name
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
      ,Number_Guests
      ,Interactive_Bar_Code_Identifier
from Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07
where New_Service_Key is null
--73,134 Row(s) affected
select top 50* from Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups where New_Service_Key is null


--Final deduplication of the above table --NB Please note that I have used the where option here because all the programmes that are nulls have service keys, meaning that they are coming from the Programme_Schedule table
Select row_number () over (partition by Household_number, Set_number, Event_Start_Date_Time,broadcast_start_date_time_utc order by channel_name) as Row_Order
      ,*
into Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups
from Final_Combination_Panel_Members_Guest_with_Prog_Instances
--2,143,518 Row(s) affected

--Deleting the duplicates from the above table
delete from Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups where Row_Order > 1
--939,724 Row(s) affected

--Checks
select count(*) from Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups
-- 1,203,794

*************** --The next task is to bring in the weights into the final table...The Household_Weight and the Total_Individual_Weights *********************************


--Checking the two tables below for the number of records they each hold ----(These tables contain the weights and the household_status)

select count(*) from BARB_VIEWING_FILE_INDIVIDUAL_PANEL_MEMBER_DETAILS_2012_06_01_07_Code_Descr
--97,293

select count(*) from BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY_2012_06_01_07_CODE_DESCR
--853,833

select count(*) from BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY_2012_06_01_07_CODE_DESCR
where Reporting_Panel_Code = 50
--86,240

--Checking the two tables of interest...
select count(*) from
             (select distinct Household_number,Person_number from BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY_2012_06_01_07_CODE_DESCR ) P
--13,704

select count(*) from
             (select distinct Household_number,Person_number from BARB_VIEWING_FILE_INDIVIDUAL_PANEL_MEMBER_DETAILS_2012_06_01_07_Code_Descr ) P
--14,108

select count(*) from
             (select distinct Household_number,Person_number from BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY_2012_06_01_07_CODE_DESCR where Reporting_Panel_Code = 50  ) P
--12,826


---Selecting distinct records for the Panel Member Details table
select   Household_number
        ,Person_number
        ,Date_valid_for_DB1
        ,max(case when Household_status = 2 or Household_status = 4 then 1 else 0 end) as House_Wife
into Local_Individual_Panel_Member_Details
from BARB_VIEWING_FILE_INDIVIDUAL_PANEL_MEMBER_DETAILS_2012_06_01_07_Code_Descr
group by Household_number
        ,Person_number
        ,Date_valid_for_DB1
--97,293 Row(s) affected

---Selecting distinct records for the WEIGHT_VIEWING_CATEGORY table & doing some checks too Min weight, Max weight, you would expect same Min and Max per person within a household)--
Select   Household_number
        ,Person_number
        ,Date_of_Activity_DB1
        ,Processing_Weight
        ,Max(Processing_Weight)as Max_Weight
        ,Min(Processing_Weight)as Min_Weight
        ,Max_Weight - Min_Weight as Diff_Weight
into Local_WEIGHT_VIEWING_CATEGORY
from BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY_2012_06_01_07_CODE_DESCR
where Reporting_Panel_Code = 50
group by Household_number
        ,Person_number
        ,Date_of_Activity_DB1
        ,Processing_Weight
--86,240 Row(s) affected

---Combining the two tables above into one single table so we can have identify house_wife and also see the weights per person / per household

Select   PMD.Household_number
        ,PMD.Person_number
        ,PMD.Date_valid_for_DB1
        ,PMD.House_Wife
        ,WE.Date_of_Activity_DB1
        ,WE.Processing_Weight
into Combined_WEIGHT_PANEL_MEMBER_DETAILS
from Local_Individual_Panel_Member_Details PMD
left join Local_WEIGHT_VIEWING_CATEGORY WE
on PMD.Household_number = WE.Household_number
and PMD.Person_number = WE.Person_number
and PMD.Date_valid_for_DB1 = WE.Date_of_Activity_DB1
--97,293 Row(s) affected


--Adding the Household_Weights and Total_Individual_Weights as fields to the Final table (Final_Combination_Panel_Members_Guest_w_Prog_Instance_dedups)
Alter table Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups add Household_Weight decimal(7,4) default 0;
Alter table Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups add Total_Individual_Weight decimal(7,4) default 0;

--Updating the final table with Household_Weights----
Update Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups as PM
   set PM.Household_Weight = WP.Processing_Weight
        from Combined_WEIGHT_PANEL_MEMBER_DETAILS WP
      where WP.Household_number = PM.Household_number
      and WP.Date_of_Activity_DB1 = PM.Date_of_Activity_DB1
    and WP.House_Wife = 1
--1,148,279 Row(s) affected



--Duplicating the Viewing events by Individual in order to obtain the Viewers for each household per day per event

select distinct Household_number, Set_number, Date_of_Activity_DB1,Event_Start_Date_Time, 1 as Person_Viewing into People_Viewing_Combined from Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups
where Person_1_viewing <> 0
union all
select distinct Household_number, Set_number, Date_of_Activity_DB1,Event_Start_Date_Time, 2 as Person_Viewing from Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups
where Person_2_viewing <> 0
union all
select distinct Household_number, Set_number, Date_of_Activity_DB1,Event_Start_Date_Time, 3 as Person_Viewing from Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups
where Person_3_viewing <> 0
union all
select distinct Household_number, Set_number, Date_of_Activity_DB1,Event_Start_Date_Time, 4 as Person_Viewing from Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups
where Person_4_viewing <> 0
union all
select distinct Household_number, Set_number, Date_of_Activity_DB1,Event_Start_Date_Time, 5 as Person_Viewing from Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups
where Person_5_viewing <> 0
union all
select distinct Household_number, Set_number, Date_of_Activity_DB1,Event_Start_Date_Time, 6 as Person_Viewing from Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups
where Person_6_viewing <> 0
union all
select distinct Household_number, Set_number, Date_of_Activity_DB1,Event_Start_Date_Time, 7 as Person_Viewing from Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups
where Person_7_viewing <> 0
union all
select distinct Household_number, Set_number, Date_of_Activity_DB1,Event_Start_Date_Time, 8 as Person_Viewing from Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups
where Person_8_viewing <> 0
union all
select distinct Household_number, Set_number, Date_of_Activity_DB1,Event_Start_Date_Time, 9 as Person_Viewing from Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups
where Person_9_viewing <> 0
union all
select distinct Household_number, Set_number, Date_of_Activity_DB1,Event_Start_Date_Time, 10 as Person_Viewing from Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups
where Person_10_viewing <> 0
union all
select distinct Household_number, Set_number, Date_of_Activity_DB1,Event_Start_Date_Time, 11 as Person_Viewing from Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups
where Person_11_viewing <> 0
union all
select distinct Household_number, Set_number, Date_of_Activity_DB1,Event_Start_Date_Time, 12 as Person_Viewing from Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups
where Person_12_viewing <> 0
union all
select distinct Household_number, Set_number, Date_of_Activity_DB1,Event_Start_Date_Time, 13 as Person_Viewing from Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups
where Person_13_viewing <> 0
union all
select distinct Household_number, Set_number, Date_of_Activity_DB1,Event_Start_Date_Time, 14 as Person_Viewing from Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups
where Person_14_viewing <> 0
union all
select distinct Household_number, Set_number, Date_of_Activity_DB1,Event_Start_Date_Time, 15 as Person_Viewing from Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups
where Person_15_viewing <> 0
union all
select distinct Household_number, Set_number, Date_of_Activity_DB1,Event_Start_Date_Time, 16 as Person_Viewing from Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups
where Person_16_viewing <> 0
--1,120,472 Row(s) affected

**--Checks ****
select top 100* from People_Viewing_Combined
select top 100* from Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups

--Creating Indexes to speed up the join
create hg index idx19 on People_Viewing_Combined(Household_number, Set_number, Date_of_Activity_DB1,Event_Start_Date_Time,Person_Viewing);
create hg index idx30 on BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY_2012_06_01_07_CODE_DESCR(Reporting_Panel_Code,Date_of_Activity_db1,Household_number,Person_number);

--Bringing in the sum of the weights (which is basically the individual weights)
select
         VE.Household_number
        ,VE.Set_number
        ,VE.Date_of_Activity_db1
        ,VE.Event_Start_Date_Time
        ,sum(WE.Processing_Weight)as Total_Individual_Weight
into #Updates
from People_Viewing_Combined VE
inner join BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY_2012_06_01_07_CODE_DESCR WE
      on VE.Date_of_Activity_db1 = WE.Date_of_Activity_db1
     and VE.Household_number = WE.Household_number
     and VE.Person_Viewing = WE.Person_number
     and WE.Reporting_Panel_Code = 50
group by VE.Household_number
        ,VE.Set_number
        ,VE.Date_of_Activity_db1
        ,VE.Event_Start_Date_Time
--790,735 Row(s) affected
select top 10* from #Updates
--Updating the final table with Total_Individual_Weights--
select top 10* from Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups
  Update Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups as PM
     set PM.Total_Individual_Weight = upd.Total_Individual_Weight
    from #Updates as upd
   where PM.Household_number = upd.Household_number
     and PM.Set_number = upd.Set_number
     and PM.Date_of_Activity_DB1 = upd.Date_of_Activity_DB1
     and PM.Event_Start_Date_Time = upd.Event_Start_Date_Time
     and PM.Number_Panel_Members > 0
-- 1,126,527 Row(s) affected


***************************************************************************************************************************************
--Checks

select * from BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY_2012_06_01_07_CODE_DESCR
where Household_number = 33600
and Reporting_Panel_Code = 50

select * from Combined_WEIGHT_PANEL_MEMBER_DETAILS
where Household_number = 33600

select top 1000 * from Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups
where Household_number = 33600

select * from #Updates
where Household_number = 33600

*******--confirming why we have zero weights *******
select count(*) from (
select   distinct Household_number
from     Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups
group by Household_number)P
--5,696

        select distinct Household_number into igonorp.Quick_Table_Checks from BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY_2012_06_01_07_CODE_DESCR
         where Reporting_Panel_Code = 50
          group by Household_number
--5,608

--Checking the number of Households in Final table that are null in the Weights table
select  distinct A.Household_number
from Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups A
left join Quick_Table_Checks B
on A.Household_number = B.Household_number
where B.Household_number is null
--216

--Taking a few Household_numbers from the above table and checking them on the weights table as an example ---
select * from BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY_2012_06_01_07_CODE_DESCR
where Household_number in (20506,25637,22335,23966,30172,27520,12524,14247,12528,14251,14905)
and Reporting_Panel_Code = 50

***************************************************************************************************************************************
--Granting Priviledges --

grant all on Combined_WEIGHT_PANEL_MEMBER_DETAILS to limac;
grant all on Local_Individual_Panel_Member_Details to limac;
grant all on Local_WEIGHT_VIEWING_CATEGORY to limac;
grant all on DB1_Station_Code_TO_Channel_Name_Updated to limac;
grant all on DB1_Station_Code_TO_Channel_Name_Updated_Final to limac;
grant all on Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07 to limac;
grant all on DB1_Station_Code_TO_Channel_Name_Members to limac;
grant all on DB1_Station_Code_TO_Channel_Name_Guests to limac;
grant all on Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups to limac;
grant all on Final_Combination_Panel_Members_Guest_with_Prog_Instances to limac;
grant all on DB1_Station_Code_TO_Channel_Name_Updated_Final_Vespa_Name to limac;
grant all on Quick_Table_Checks to limac;
commit;

grant all on Combined_WEIGHT_PANEL_MEMBER_DETAILS to thompsonja;
grant all on Local_Individual_Panel_Member_Details to thompsonja;
grant all on Local_WEIGHT_VIEWING_CATEGORY to thompsonja;
grant all on DB1_Station_Code_TO_Channel_Name_Updated to thompsonja;
grant all on DB1_Station_Code_TO_Channel_Name_Updated_Final to thompsonja;
grant all on Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07 to thompsonja;
grant all on DB1_Station_Code_TO_Channel_Name_Members to thompsonja;
grant all on DB1_Station_Code_TO_Channel_Name_Guests to thompsonja;
grant all on Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups to thompsonja;
grant all on Final_Combination_Panel_Members_Guest_with_Prog_Instances to thompsonja;
grant all on DB1_Station_Code_TO_Channel_Name_Updated_Final_Vespa_Name to thompsonja;
grant all on Quick_Table_Checks to thompsonja;
commit;

select top 100* from Quick_Table_Checks
