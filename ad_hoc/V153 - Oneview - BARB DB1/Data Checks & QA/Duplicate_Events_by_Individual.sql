
select Household_number
,Set_number
,Date_of_Activity_DB1
,Event_Start_Date_Time
,Event_End_Date_Time
,Duration_of_session
,DB1_Station_Code
,Date_of_Recording_DB1
,Start_time_of_recording
,Recording_Start_Date_Time
,Recording_End_Date_Time
,Interactive_Bar_Code_Identifier
,Number_Panel_Members
,Session_activity_type_
,Playback_type_
,Viewing_platform_
,Sky_Viewing
,Service_Key
,Channel_Name
,Number_Guests
,Viewer
into BARB_Viewing_Events_1_7_June_2012
from (
select *,1 as Viewer
from igonorp.Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07 
where person_1_viewing = 1
UNION
select *,2 as Viewer
from igonorp.Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07 
where person_2_viewing = 1
UNION
select *,3 as Viewer
from igonorp.Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07 
where person_3_viewing = 1
UNION
select *,4 as Viewer
from igonorp.Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07 
where person_4_viewing = 1
UNION
select *,5 as Viewer
from igonorp.Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07 
where person_5_viewing = 1
UNION
select *,6 as Viewer
from igonorp.Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07 
where person_6_viewing = 1
UNION
select *,7 as Viewer
from igonorp.Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07 
where person_7_viewing = 1
UNION
select *,8 as Viewer
from igonorp.Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07 
where person_8_viewing = 1
UNION
select *,9 as Viewer
from igonorp.Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07 
where person_9_viewing = 1
UNION
select *,10 as Viewer
from igonorp.Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07 
where person_10_viewing = 1
UNION
select *,11 as Viewer
from igonorp.Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07 
where person_11_viewing = 1
UNION
select *,12 as Viewer
from igonorp.Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07 
where person_12_viewing = 1
UNION
select *,13 as Viewer
from igonorp.Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07 
where person_13_viewing = 1
UNION
select *,14 as Viewer
from igonorp.Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07 
where person_14_viewing = 1
UNION
select *,15 as Viewer
from igonorp.Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07 
where person_15_viewing = 1
UNION
select *,16 as Viewer
from igonorp.Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07 
where person_16_viewing = 1
) t
-- 1,121,650

create hg index idx1 on BARB_Viewing_Events_1_7_June_2012(date_of_activity_db1,household_number,viewer)

create hg index idx3 on igonorp.BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY_2012_06_01_07_CODE_DESCR(reporting_panel_description,date_of_activity_db1,household_number,person_number)


select VE.date_of_activity_db1
        ,sum(W.processing_weight)
from BARB_Viewing_Events_1_7_June_2012 VE
inner join igonorp.BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY_2012_06_01_07_CODE_DESCR W
on VE.date_of_activity_db1 = W.date_of_activity_db1
and VE.household_number = W.household_number
and VE.viewer = W.person_number
and W.reporting_panel_description = 'BBC Network'
group by VE.date_of_activity_db1
order by VE.date_of_activity_db1