/*
Analyst : Patrick Igonor
Date    : 11th of July 2013
Lead    : Claudio Lima
*/

--Tables of interest -

select top 10* from Final_Barb_Viewing_Event_Vespa_Metadata_2012_06_01_07_dedups; --- Members
select top 10* from Final_Barb_Viewing_Event_Vespa_Metadata_2012_06_01_07_Guests_dedups; ---Guests


--Matching the two tables above together into one table based on Household_number, Set_number,Broadcast_Start_Date_Time, Event_Start_Time

select   MB.Household_number
        ,MB.Set_number
        ,MB.Date_of_Activity_DB1
        ,MB.Event_Start_Date_Time
        ,MB.Event_End_Date_Time
        ,MB.Duration_of_session
        ,MB.DB1_Station_Code
        ,MB.Date_of_Recording_DB1
        ,MB.Start_time_of_recording
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
        ,MB.Session_activity_type
        ,MB.Playback_type
        ,MB.Viewing_platform
        ,MB.log_Station_Code
        ,MB.Relationship_Start_Date
        ,MB.Relationship_End_Date
        ,MB.service_key
        ,MB.effective_from
        ,MB.effective_to
        ,MB.CHANNEL_NAME
        ,MB.EPG_NAME
        ,MB.programme_name
        ,MB.genre_description
        ,MB.sub_genre_description
        ,MB.synopsis
        ,MB.broadcast_start_date_time_utc
        ,MB.broadcast_end_date_time_utc
        ,MB.Sky_Viewing
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
        ,GT.Interactive_Bar_Code_Identifier
into drop table  Combined_Panel_Members_Guest_1week_2012_June_01_07
from Final_Barb_Viewing_Event_Vespa_Metadata_2012_06_01_07_dedups MB
full join igonorp.Final_Barb_Viewing_Event_Vespa_Metadata_2012_06_01_07_Guests_dedups GT
on MB.Household_number = GT.Household_number
and MB.Set_number = GT.Set_number
and MB.date_of_activity_db1 = GT.date_of_activity_db1
and MB.broadcast_start_date_time_utc = GT.broadcast_start_date_time_utc
and MB.Event_Start_Date_Time = GT.Event_Start_Date_Time

--

--Granting Access ---
grant all on Combined_Panel_Members_Guest_1week_2012_June_01_07 to limac;

--checks
select top 10* from Combined_Panel_Members_Guest_1week_2012_June_01_07

--Checks --
select count(*) from Final_Barb_Viewing_Event_Vespa_Metadata_2012_06_01_07_dedups
--937,136

select count(*) from Final_Barb_Viewing_Event_Vespa_Metadata_2012_06_01_07_Guests_dedups
--62,015

-----------------------------------------------------------------------------------------

--Average daily number of hours per TV set and Household_number
select   Date_of_Activity_DB1
        ,1.0*Sum(Sum_Dur) / Sum(Set_number) / 60 as Avg_Viewing_Hrs_TV_Set
        ,1.0*Sum(Sum_Dur) / Count(Household_number) / 60 as Avg_Viewing_Hrs_HH
from
(
   Select   Household_number
           ,Set_number
           ,Date_of_Activity_DB1
           ,sum(Duration_of_session) as Sum_Dur
from Combined_Panel_Members_Guest_1week_2012_June_01_07
where Date_of_Activity_DB1 >= '2012-06-01'
group by Household_number,Set_number,Date_of_Activity_DB1
 )P
group by Date_of_Activity_DB1
order by Date_of_Activity_DB1


--Breakdown of Total number of viewing hours per Genre

select   genre_description
        ,sum(Duration_of_session) as Sum_Dur
        ,Sum_Dur*1.0 / 60 as Total_No_Viewing_Hours
from Combined_Panel_Members_Guest_1week_2012_June_01_07
where Date_of_Activity_DB1 >= '2012-06-01'
group by genre_description
order by genre_description

