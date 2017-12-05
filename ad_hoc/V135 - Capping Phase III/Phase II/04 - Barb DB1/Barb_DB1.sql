--Title :Analysis of the Barb DB1 data for the purpose of Capping Calibration
--Author: Patrick Igonor
--Lead Analyst : Jason Thompson


--*** Tables of Interest

select top 10* from thompsonja.BARB_PVF06_Viewing_Record_Panel_Members
select top 10* from thompsonja.BARB_PVF_Viewing_Record_Guests
select top 10* from thompsonja.BARB_Individual_Panel_Member_Details
select top 10* from thompsonja.BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories
select top 10* from thompsonja.BARB_Panel_Demographic_Data_TV_Sets_Characteristics

---Adding the Panel Members table to my own Schema ----
select * into BARB_PVF06_Viewing_Record_Panel_Members_format from thompsonja.BARB_PVF06_Viewing_Record_Panel_Members
--1,317,064 Row(s) affected

--Transformation of the GUEST Data to obtain start and end times of events
select  file_creation_date
       ,file_creation_time
       ,file_type
       ,file_version
       ,filename
       ,Record_type
       ,Household_number
       ,Date_of_Activity_DB1
       ,cast(Date_of_Activity_DB1 as varchar(10)) as Date_of_Activity_DB1_V
       ,cast(left(Date_of_Activity_DB1_V,4) || '-'|| substr(Date_of_Activity_DB1_V,5,2) || '-'|| right(Date_of_Activity_DB1_V,2)as date) as Date_of_Activity_DB1_N
       ,Set_number
       ,Start_time_of_session
       ,cast(cast(floor(1.0*Start_time_of_session/100) as int) % 24 as int) as hour
       ,case when Start_time_of_session >= 2400 then Dateadd(Day,1,Date_of_Activity_DB1_N)else Date_of_Activity_DB1_N end as New_Date_of_Activity_DB1
       ,cast(Start_time_of_session as int) % 100 as min
       ,cast(hour || ':' || min as time) as Hour_Min
       ,cast(New_Date_of_Activity_DB1 || ' ' || Hour_Min as Datetime) as Event_Start_Date_Time
       ,Dateadd(minute,Duration_of_session,Event_Start_Date_Time) as Event_End_Date_Time
       ,Duration_of_session
       ,Session_activity_type
       ,Playback_type
       ,DB1_Station_Code
       ,Viewing_platform
       ,Date_of_Recording_DB1
       ,cast(Date_of_Recording_DB1 as varchar(10)) as Date_of_Recording_DB1_V
       ,cast(left(Date_of_Recording_DB1_V,4) || '-'|| substr(Date_of_Recording_DB1_V,5,2) || '-'|| right(Date_of_Recording_DB1_V,2)as date) as Date_of_Recording_DB1_N
       ,Start_time_of_recording
       ,cast(cast(floor(1.0*Start_time_of_recording/100) as int) % 24 as int) as hour_2
       ,case when Start_time_of_recording >= 2400 then Dateadd(Day,1,Date_of_Recording_DB1_N)else Date_of_Recording_DB1_N end as New_Date_of_Recording_DB1
       ,cast(Start_time_of_recording as int) % 100 as min_2
       ,cast(hour_2 || ':' || min_2 as time) as Hour_Min_2
       ,cast(New_Date_of_Recording_DB1 || ' ' || Hour_Min_2 as Datetime) as Recording_Start_Date_Time
       ,Dateadd(minute,Duration_of_session,Recording_Start_Date_Time) as Recording_End_Date_Time
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
       ,Interactive_Bar_Code_Identifier
into BARB_PVF_Viewing_Record_Guests_Format
from thompsonja.BARB_PVF_Viewing_Record_Guests
--31,068 Row(s) affected


--Adding the Sky_Viewing field into the Panel Member table
alter table BARB_PVF06_Viewing_Record_Panel_Members_format add Sky_Viewing bit default 0;

--Updatng the Panel and the Guest tables with Sky Viewing Information
update BARB_PVF06_Viewing_Record_Panel_Members_format MB
set Sky_Viewing = 1
    from thompsonja.BARB_Panel_Demographic_Data_TV_Sets_Characteristics ST
    where MB.Household_number =ST.Household_number
    and MB.Set_number =ST.Set_number
    and(ST.Reception_Capability_Code1  = 2
     or ST.Reception_Capability_code2  = 2
     or ST.Reception_Capability_code3  = 2
     or ST.Reception_Capability_code4  = 2
     or ST.Reception_Capability_code5  = 2
     or ST.Reception_Capability_code6  = 2
     or ST.Reception_Capability_code7  = 2
     or ST.Reception_Capability_code8  = 2
     or ST.Reception_Capability_code9  = 2
     or ST.Reception_Capability_code10 = 2)
;
--613,135 Row(s) affected

--Adding the Sky_Vieing field into the Guest table
alter table BARB_PVF_Viewing_Record_Guests_Format add Sky_Viewing bit default 0;

--Updatng the Panel and the Guest tables with Sky Viewing Information
update BARB_PVF_Viewing_Record_Guests_Format GT
set Sky_Viewing = 1
    from thompsonja.BARB_Panel_Demographic_Data_TV_Sets_Characteristics ST
    where GT.Household_number =ST.Household_number
    and GT.Set_number =ST.Set_number
    and(ST.Reception_Capability_Code1  = 2
     or ST.Reception_Capability_code2  = 2
     or ST.Reception_Capability_code3  = 2
     or ST.Reception_Capability_code4  = 2
     or ST.Reception_Capability_code5  = 2
     or ST.Reception_Capability_code6  = 2
     or ST.Reception_Capability_code7  = 2
     or ST.Reception_Capability_code8  = 2
     or ST.Reception_Capability_code9  = 2
     or ST.Reception_Capability_code10 = 2)
--16,251 Row(s) affected

--Trnasforming the Panel Members DB1 station code for conformity with the guest table ... (The guest one has a preceding 0 but the Panel one doesn't)
select   *
        ,cast(DB1_Station_Code as Varchar) as New_Station_C
        ,cast(right('00000' ||New_Station_C,5) as Varchar) as Station_Code
into  BARB_PVF06_Viewing_Record_Panel_Members_format_Final
from BARB_PVF06_Viewing_Record_Panel_Members_format
--1,317,064 Row(s) affected

--Combining both the Panel_Members and the Guests Viewing events tables into One table ---

select   coalesce(MB.Household_number,GT.Household_number)as Household_number
        ,coalesce(MB.Set_number,GT.Set_number)as Set_number
        ,coalesce(MB.Barb_date_of_activity,GT.Date_of_Activity_DB1_N)as Date_of_Activity_DB1
        ,coalesce(MB.Start_time_of_session,GT.Event_Start_Date_Time)as Event_Start_Date_Time
        ,coalesce(MB.End_time_of_session,GT.Event_End_Date_Time)as Event_End_Date_Time
        ,coalesce(MB.Duration_of_session, GT.Duration_of_session) as Duration_of_session
        ,coalesce(MB.Station_Code,GT.DB1_Station_Code) as DB1_Station_Code
        ,coalesce(MB.Interactive_Bar_Code_Identifier,GT.Interactive_Bar_Code_Identifier)as Interactive_Bar_Code_Identifier
        ,coalesce(MB.Person_1_viewing,0)  as Person_1_viewing
        ,coalesce(MB.Person_2_viewing,0)  as Person_2_viewing
        ,coalesce(MB.Person_3_viewing,0)  as Person_3_viewing
        ,coalesce(MB.Person_4_viewing,0)  as Person_4_viewing
        ,coalesce(MB.Person_5_viewing,0)  as Person_5_viewing
        ,coalesce(MB.Person_6_viewing,0)  as Person_6_viewing
        ,coalesce(MB.Person_7_viewing,0)  as Person_7_viewing
        ,coalesce(MB.Person_8_viewing,0)  as Person_8_viewing
        ,coalesce(MB.Person_9_viewing,0)  as Person_9_viewing
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
        ,coalesce(MB.Session_activity_type,GT.Session_activity_type) as Session_activity_type_
        ,coalesce(MB.Playback_type,GT.Playback_type) as Playback_type_
        ,coalesce(MB.Viewing_platform,GT.Viewing_platform) as Viewing_platform_
        ,coalesce(MB.Sky_Viewing,GT.Sky_Viewing) as Sky_Viewing
        ,coalesce(GT.Male_4_9,0)     as Male_4_9
        ,coalesce(GT.Male_10_15,0)   as Male_10_15
        ,coalesce(GT.Male_16_19,0)   as Male_16_19
        ,coalesce(GT.Male_20_24,0)   as Male_20_24
        ,coalesce(GT.Male_25_34,0)   as Male_25_34
        ,coalesce(GT.Male_35_44,0)   as Male_35_44
        ,coalesce(GT.Male_45_64,0)   as Male_45_64
        ,coalesce(GT.Male_65,0)      as Male_65
        ,coalesce(GT.Female_4_9,0)   as Female_4_9
        ,coalesce(GT.Female_10_15,0) as Female_10_15
        ,coalesce(GT.Female_16_19,0) as Female_16_19
        ,coalesce(GT.Female_20_24,0) as Female_20_24
        ,coalesce(GT.Female_25_34,0) as Female_25_34
        ,coalesce(GT.Female_35_44,0) as Female_35_44
        ,coalesce(GT.Female_45_64,0) as Female_45_64
        ,coalesce(GT.Female_65,0)    as Female_65
        ,coalesce(GT.Male_4_9,0)+coalesce(GT.Male_10_15,0)+coalesce(GT.Male_16_19,0)
        +coalesce(GT.Male_20_24,0)+coalesce(GT.Male_25_34,0)+coalesce(GT.Male_35_44,0)+coalesce(GT.Male_45_64,0)
        +coalesce(GT.Male_65,0)+coalesce(GT.Female_4_9,0)+coalesce(GT.Female_10_15,0)
        +coalesce(GT.Female_16_19,0)+coalesce(GT.Female_20_24,0)+coalesce(GT.Female_25_34,0)
        +coalesce(GT.Female_35_44,0)+coalesce(GT.Female_45_64,0)+coalesce(GT.Female_65,0) as Number_Guests
into  Combined_Members_Guest_Details
from BARB_PVF06_Viewing_Record_Panel_Members_format_Final MB
full join BARB_PVF_Viewing_Record_Guests_Format GT
on MB.Household_number = GT.Household_number
and MB.Set_number = GT.Set_number
and MB.Start_time_of_session = GT.Event_Start_Date_Time
--1,325,861 Row(s) affected
select top 10* from Combined_Members_Guest_Details
**************************************************************************************************************************************************
--Checking Overlaps
select count(*) from Combined_Members_Guest_Details
where Number_Panel_Members > 0 and Number_Guests = 0
--count() 1,294,793 --Member only

select count(*) from Combined_Members_Guest_Details
where Number_Panel_Members = 0 and Number_Guests > 0
--count() 8,797 --Guest only

select count(*) from Combined_Members_Guest_Details
where Number_Panel_Members > 0 and Number_Guests > 0
--count() 22,271 --Overlaps

select count(*) from Combined_Members_Guest_Details
--count() 1,325,861 Total after the combination of Members and Guests

**************************************************************************************************************************************************

--Transforming the Date_valid_for_Db1 into a date format for easy comparisons
select *
       ,cast(Date_valid_for_DB1 as varchar(10)) as Date_valid_for_DB1_V
       ,cast(left(Date_valid_for_DB1_V,4) || '-'|| substr(Date_valid_for_DB1_V,5,2) || '-'|| right(Date_valid_for_DB1_V,2)as date) as Date_valid_for_DB1_N
into BARB_Individual_Panel_Member_Details_format
from thompsonja.BARB_Individual_Panel_Member_Details
--181,452 Row(s) affected

--Trying to bring in the Housewife Weights which is the weight of the Household
*******--Household_status = 2 indicates 'Housewife and NOT Head of Household' *************
*******--and Household_status = 4 indicates 'Both housewife and head of household' ********

---Selecting distinct records for the Panel Member Details table
select   Household_number
        ,Person_number
        ,Date_valid_for_DB1_N
        ,max(case when Household_status = 2 or Household_status = 4 then 1 else 0 end) as House_Wife
into Local_Individual_Panel_Member_Details_format
from BARB_Individual_Panel_Member_Details_format
group by Household_number
        ,Person_number
        ,Date_valid_for_DB1_N

--181,452 Row(s) affected

---Checks----------------------------------------------------------------------
select count (distinct Household_number),count (distinct Person_number) ,Date_valid_for_DB1_N, count(*)
from Local_Individual_Panel_Member_Details_format
where House_Wife = 1
group by Date_valid_for_DB1_N

select * from BARB_Individual_Panel_Member_Details_format
where Date_valid_for_DB1_N ='2013-09-29'
-------------------------------------------------------------------------------
---Selecting distinct records for the WEIGHT_VIEWING_CATEGORY table & doing some checks too Min weight, Max weight, you would expect same Min and Max per person within a household)--
Select   Household_number
        ,Person_number
        ,Date_of_Activity_DB1
        ,Processing_Weight
        ,Max(Processing_Weight)as Max_Weight
        ,Min(Processing_Weight)as Min_Weight
        ,Max_Weight - Min_Weight as Diff_Weight
into Local_WEIGHT_VIEWING_CATEGORY_format
from thompsonja.BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories
where Reporting_Panel_Code = 50
group by Household_number
        ,Person_number
        ,Date_of_Activity_DB1
        ,Processing_Weight
--164,505 Row(s) affected
select top 10* from Local_WEIGHT_VIEWING_CATEGORY_format
---Combining the two tables above into one single table so we can have identify house_wife and also see the weights per person / per household

Select   PMD.Household_number
        ,PMD.Person_number
        ,PMD.Date_valid_for_DB1_N
        ,PMD.House_Wife
        ,WE.Date_of_Activity_DB1
        ,WE.Processing_Weight
into Combined_WEIGHT_PANEL_MEMBER_DETAILS_Format
from Local_Individual_Panel_Member_Details_format PMD
left join Local_WEIGHT_VIEWING_CATEGORY_format WE
on PMD.Household_number = WE.Household_number
and PMD.Person_number = WE.Person_number
and PMD.Date_valid_for_DB1_N = WE.Date_of_Activity_DB1
--181,452 Row(s) affected
select top 100* from Combined_WEIGHT_PANEL_MEMBER_DETAILS_Format

--Adding the Household_Weights
Alter table Combined_Members_Guest_Details add Household_Weight int default 0;

--Updating the final table with Household_Weights----
Update Combined_Members_Guest_Details as PM
   set PM.Household_Weight = WP.Processing_Weight
        from Combined_WEIGHT_PANEL_MEMBER_DETAILS_Format WP
      where WP.Household_number = PM.Household_number
      and WP.Date_of_Activity_DB1 = PM.Date_of_Activity_DB1
    and WP.House_Wife = 1
--1,295,033 Row(s) affected


select * into BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories_format from thompsonja.BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories
--1,616,589 Row(s) affected

--Checks --*******************************************************************
Select top 100* from Combined_Members_Guest_Details
where Date_of_Activity_DB1 = '2013-09-29'

select Sky_Viewing, count(*) from Combined_Members_Guest_Details
group by Sky_Viewing
--Sky_Viewing      count()
--     0          707,741
--     1          618,120

select count(*) from Combined_Members_Guest_Details
--count() 1,325,861

select sum(Household_Weight) from Combined_Members_Guest_Details
where Sky_Viewing = 1 and Date_of_Activity_DB1 = '2013-09-29'


select Date_of_Activity_DB1, count(*)
from Combined_Members_Guest_Details
where Date_of_Activity_DB1  > '2013-09-22' and Sky_Viewing = 1
group by Date_of_Activity_DB1
order by Date_of_Activity_DB1

--Date_of_Activity_DB1    count()
--2013-09-23              43,505
--2013-09-24              41,316
--2013-09-25              43,275
--2013-09-26              41,612
--2013-09-27              41,488
--2013-09-28              46,337
--2013-09-29              47,965

--Select ing all the data to be exported to Excel ---
select * from Combined_Members_Guest_Details
where Date_of_Activity_DB1  > '2013-09-22' and Sky_Viewing = 1

--Checking for live events
select session_activity_type_, playback_type_, count(*)
from Combined_Members_Guest_Details
group by session_activity_type_, playback_type_

--To check for live viewing ---- 1 and 13 are the Live_Viewings
--         ,   when Session_activity_type = 1 then 'Live viewing (Excl Targeted Advertising)'
--             when Session_activity_type = 4 then 'Un-coded Playback'
--             when Session_activity_type = 5 then 'Time-shifted/coded playback (2-7 days) (Excl Targeted Advertising)'
--             when Session_activity_type = 6 then 'Teletext'
--             when Session_activity_type = 7 then 'Interactive'
--             when Session_activity_type = 8 then 'EPG'
--             when Session_activity_type = 9 then 'Interactive (include in Live Viewing)'
--             when Session_activity_type = 11 then 'VOSDAL (Excl Targeted Advertising)'
--             when Session_activity_type = 12 then 'Interactive Playback (include in VOSDAL)'
--             when Session_activity_type = 13 then 'Live Viewing - Targeted Advertising'
--             when Session_activity_type = 14 then 'Time-shifted/coded playback (2-7 days) - Targeted Advertising'
--             when Session_activity_type = 15 then 'VOSDAL - Targeted Advertising'
--             when Session_activity_type = 19 then 'Other (e.g. Play Station)'

--Creating a table
create table Barb_Viewing_Events( Household_number int,Set_number int,Event_Start_Date_Time datetime, Event_End_Date_Time datetime,Date_of_Activity_DB1 date,Final_Grouping int,  Playback_type_ int,  Household_Weight bigint)
;

-- Importing Raw data
LOAD TABLE Barb_Viewing_Events ( Household_number,Set_number,Event_Start_Date_Time,Event_End_Date_Time,Date_of_Activity_DB1,Final_Grouping,Playback_type_,Household_Weight '\n' )
FROM '/ETL013/prod/sky/olive/data/share/clarityq/export/PatrickI/Barb/Barb_Household_Viewing_Events.csv' QUOTES OFF ESCAPES OFF NOTIFY 1000 DELIMITED BY ',' START ROW ID 2
;

select   Date_of_Activity_DB1
        ,Household_number
        ,Set_number
        ,Household_Weight
        ,Final_Grouping
        ,min(Event_Start_Date_Time) as New_Event_Start_Date_Time
        ,max(Event_End_Date_Time) as New_Event_End_Date_Time
        ,datediff(second,New_Event_Start_Date_Time,New_Event_End_Date_Time) as Duration
        ,hour(New_Event_Start_Date_Time) Event_Start_Hour
into Final_Barb_Viewing_Events
from Barb_Viewing_Events
where Playback_type_ is null
group by Date_of_Activity_DB1
        ,Household_number
        ,Set_number
        ,Household_Weight
        ,Final_Grouping
--207,912 Row(s) affected

---Dealing with the weights ---
select Household_number,Processing_Weight
into #Distinct_HH
from Combined_WEIGHT_PANEL_MEMBER_DETAILS_Format
where Date_of_Activity_DB1 = '2013-09-23' and Sky_Viewing = 1 and House_Wife = 1
group by Household_number,Processing_Weight
--2047 Row(s) affected

select top 1000* from Combined_WEIGHT_PANEL_MEMBER_DETAILS_Format
where House_Wife = 1
select sum(Processing_Weight) as Total_Weight
from #Distinct_HH
--97,428,179

Alter table Final_Barb_Viewing_Events add Total_Weights bigint

Update Final_Barb_Viewing_Events
set Total_Weights = 97428179

select *
       ,(Household_Weight*9403911)/Total_Weights as Weights
into Final_Barb_Viewing_Events_Weights
from Final_Barb_Viewing_Events

select top 10* from #Distinct_HH
select Date_of_Activity_DB1,sum(Weights)as Sum_Weights from Final_Barb_Viewing_Events_Weights
group by Date_of_Activity_DB1


--Pivoting
--1.
select Date_of_Activity_DB1
      ,Event_Start_Hour
      ,sum(Duration*Weights) Sum_Dur
      ,Sum_Dur / 3600 as Duration_Hours
 from Final_Barb_Viewing_Events_Weights
where Weights is not null
group by Date_of_Activity_DB1,Event_Start_Hour


--2. --Household level ----
select  Date_of_Activity_DB1
       ,Event_Start_Hour
       ,Household_number
       ,Weights
into Table_1
from Final_Barb_Viewing_Events_Weights
where Weights is not null
group by Date_of_Activity_DB1
        ,Event_Start_Hour
        ,Household_number
        ,Weights
;
--------------------------------------------
select  Date_of_Activity_DB1
       ,Event_Start_Hour
       ,sum(Weights) as Sum_Weights
into Table_2
from Table_1
where Weights is not null
group by Date_of_Activity_DB1
        ,Event_Start_Hour

--Joining them together
select A.Date_of_Activity_DB1
     , A.Event_Start_Hour
     ,sum(Duration*Weights) / B.Sum_Weights as Avg_Dur
     ,Avg_Dur / 60
from Final_Barb_Viewing_Events_Weights A
inner join Table_2 B
on  A.Date_of_Activity_DB1 = B.Date_of_Activity_DB1
and A.Event_Start_Hour = B.Event_Start_Hour
group by A.Date_of_Activity_DB1
        ,A.Event_Start_Hour
        ,B.Sum_Weights

select Household_number,sum(Household_Weight) from Final_Barb_Viewing_Events_Weights
group by Household_number


select *
       ,cast(Date_valid_for_DB1 as varchar(10)) as Date_valid_for_DB1_V
       ,cast(left(Date_valid_for_DB1_V,4) || '-'|| substr(Date_valid_for_DB1_V,5,2) || '-'|| right(Date_valid_for_DB1_V,2)as date) as Date_valid_for_DB1_N
into BARB_Panel_Demographic_Data_TV_Sets_Characteristics_format
from thompsonja.BARB_Panel_Demographic_Data_TV_Sets_Characteristics

alter table Combined_WEIGHT_PANEL_MEMBER_DETAILS_Format add Sky_Viewing bit default 0;

--Updatng the Panel and the Guest tables with Sky Viewing Information
update Combined_WEIGHT_PANEL_MEMBER_DETAILS_Format GT
set Sky_Viewing = 1
    from BARB_Panel_Demographic_Data_TV_Sets_Characteristics_format ST
    where GT.Household_number =ST.Household_number
    and GT.Date_of_Activity_DB1 = ST.Date_Valid_for_DB1_N
    and(ST.Reception_Capability_Code1  = 2
     or ST.Reception_Capability_code2  = 2
     or ST.Reception_Capability_code3  = 2
     or ST.Reception_Capability_code4  = 2
     or ST.Reception_Capability_code5  = 2
     or ST.Reception_Capability_code6  = 2
     or ST.Reception_Capability_code7  = 2
     or ST.Reception_Capability_code8  = 2
     or ST.Reception_Capability_code9  = 2
     or ST.Reception_Capability_code10 = 2)
select top 10* from BARB_Panel_Demographic_Data_TV_Sets_Characteristics_format


