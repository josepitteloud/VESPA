/*

Analytics for BARB DB1 - week of 1-7 June 2012

*/

-- Have a look
select top 1000 * from igonorp.Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07

-- Have a look at viewing events with only guests
select top 1000 * 
from igonorp.Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07
where number_panel_members = 0
and number_guests > 0

-- Have a look at weights
select top 1000 * from igonorp.BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY_2012_06_01_07_CODE_DESCR

-- Summary information of panels for 1 June
select replace(reporting_panel_description,'\x1a ','') as reporting_panel_description
        ,count(*) as Number_Individuals
        ,count(distinct household_number) as Number_HHs
        ,sum(processing_weight)*1000 as Sum_of_Weights
        ,avg(processing_weight)*1000 as Avg_Weight
        ,min(processing_weight)*1000 as Min_Weight
        ,median(processing_weight)*1000 as Med_Weight
        ,max(processing_weight)*1000 as Max_Weight
from igonorp.BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY_2012_06_01_07_CODE_DESCR
where date_of_activity_db1 = '2012-06-01'
and reporting_panel_description is not null
group by reporting_panel_description
order by 2 desc,1

-- Summary of BARB panel viewing for week
select HH.date_of_activity_db1
        ,HH.Number_Events
        ,HH.Number_HHs
        ,HH.Number_TV_Sets
        ,IND.Number_Panel_Members
        ,HH.Total_Viewing_hours
        ,HH.Average_Daily_Viewing_Per_HH
        ,HH.Average_Daily_Viewing_Per_TV_Set
        ,HH.Total_Viewing_hours/IND.Number_Panel_Members as Average_Daily_Viewing_Per_Panel_Member
from (
select date_of_activity_db1
        ,count(*) as Number_Events
        ,count(distinct household_number) as Number_HHs
        ,count(distinct str(household_number)+'-'+str(set_number)) as Number_TV_Sets
        ,sum(duration_of_session)/60.0 as Total_Viewing_hours
        ,Total_Viewing_hours*1.0/Number_HHs as Average_Daily_Viewing_Per_HH
        ,Total_Viewing_hours*1.0/Number_TV_Sets as Average_Daily_Viewing_Per_TV_Set
from igonorp.Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07
group by date_of_activity_db1
) HH
INNER JOIN
(
select date_of_activity_db1
        ,sum(Daily_Number_Panel_Members) as Number_Panel_Members
from (
select date_of_activity_db1
        ,household_number
        ,max(person_1_viewing)+max(person_2_viewing)+max(person_3_viewing)+max(person_4_viewing)
        +max(person_5_viewing)+max(person_6_viewing)+max(person_7_viewing)+max(person_8_viewing)
        +max(person_9_viewing)+max(person_10_viewing)+max(person_11_viewing)+max(person_12_viewing)
        +max(person_13_viewing)+max(person_14_viewing)+max(person_15_viewing)+max(person_16_viewing) 
        as Daily_Number_Panel_Members
from igonorp.Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07 
where Number_Panel_Members > 0       
group by date_of_activity_db1,household_number
) t
group by date_of_activity_db1
) IND
on HH.date_of_activity_db1 = IND.date_of_activity_db1
order by 1



select household_number,person_number,date_of_activity_db1,processing_weight 
from igonorp.BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY_2012_06_01_07_CODE_DESCR
where reporting_panel_description = 'BBC Network'


-- Summary of UK base viewing for week
select date_of_activity_db1
        ,count(*) as Number_Events
        ,count(distinct household_number) as Number_HHs
        ,count(distinct str(household_number)+'-'+str(set_number)) as Number_TV_Sets
        ,sum(duration_of_session)/60.0 as Total_Viewing_hours
        ,Total_Viewing_hours*1.0/Number_HHs as Average_Daily_Viewing_Per_HH
        ,Total_Viewing_hours*1.0/Number_TV_Sets as Average_Daily_Viewing_Per_TV_Set
from igonorp.Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07 VE
inner join igonorp.BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY_2012_06_01_07_CODE_DESCR W
on VE.household_number = W.household_number
and 
group by date_of_activity_db1
order by date_of_activity_db1


-- Distribution of viewing per number of individuals
select Number_Panel_Members+Number_Guests as Number_Viewers
        ,count(*) AS Number_Events 
        ,sum(duration_of_session)/60.0 as Total_Viewing_hours
from igonorp.Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07
group by Number_Panel_Members+Number_Guests
order by Number_Panel_Members+Number_Guests



select reception_capability_code_1_,reception_capability_code_2_,reception_capability_code_3_,count(*)
from igonorp.New_BARB_PANEL_VIEWING_FILE_TV_SET_CHARACTERISTICS
group by reception_capability_code_1_,reception_capability_code_2_,reception_capability_code_3_ 
order by reception_capability_code_1_,reception_capability_code_2_,reception_capability_code_3_ 

select session_Activity_type_
        ,count(*) AS Number_Events 
        ,sum(duration_of_session)/60.0 as Total_Viewing_hours
from igonorp.Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07
group by session_Activity_type_
order by 3 desc


select top 10 * from igonorp.Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07


-- Summary of BARB panel viewing for week
select HH.sky_viewing
        ,HH.date_of_activity_db1
        ,HH.Number_Events
        ,HH.Number_HHs
        ,HH.Number_TV_Sets
        ,IND.Number_Panel_Members
        ,HH.Total_Viewing_hours
        ,HH.Average_Daily_Viewing_Per_HH
        ,HH.Average_Daily_Viewing_Per_TV_Set
        ,HH.Total_Viewing_hours/IND.Number_Panel_Members as Average_Daily_Viewing_Per_Panel_Member
from (
select sky_viewing
        ,date_of_activity_db1
        ,count(*) as Number_Events
        ,count(distinct household_number) as Number_HHs
        ,count(distinct str(household_number)+'-'+str(set_number)) as Number_TV_Sets
        ,sum(duration_of_session)/60.0 as Total_Viewing_hours
        ,Total_Viewing_hours*1.0/Number_HHs as Average_Daily_Viewing_Per_HH
        ,Total_Viewing_hours*1.0/Number_TV_Sets as Average_Daily_Viewing_Per_TV_Set
from igonorp.Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07
group by sky_viewing
        ,date_of_activity_db1
) HH
INNER JOIN
(
select sky_viewing
        ,date_of_activity_db1
        ,sum(Daily_Number_Panel_Members) as Number_Panel_Members
from (
select sky_viewing
        ,date_of_activity_db1
        ,household_number
        ,max(person_1_viewing)+max(person_2_viewing)+max(person_3_viewing)+max(person_4_viewing)
        +max(person_5_viewing)+max(person_6_viewing)+max(person_7_viewing)+max(person_8_viewing)
        +max(person_9_viewing)+max(person_10_viewing)+max(person_11_viewing)+max(person_12_viewing)
        +max(person_13_viewing)+max(person_14_viewing)+max(person_15_viewing)+max(person_16_viewing) 
        as Daily_Number_Panel_Members
from igonorp.Combined_DB1_Station_Code_TO_Channel_Name_Members_1week_2012_June_01_07 
where Number_Panel_Members > 0       
group by sky_viewing
        ,date_of_activity_db1
        ,household_number
) t
group by sky_viewing
        ,date_of_activity_db1
) IND
on HH.sky_viewing = IND.sky_viewing
and HH.date_of_activity_db1 = IND.date_of_activity_db1
order by 1,2