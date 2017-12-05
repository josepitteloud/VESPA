
-- -- Procedure proc_BARB_update_fields, used to to update the gender/age groups, both weighted (currently panel 50) and normal, from the person_viewing information
-- in the BARB_table_output_1 table
-- the procedure will iterate through the 16 people (maximum) of each household and update the aggregate age/gender data
-- and it will do so in admirable fashion (this is writer's opinion)
CREATE OR REPLACE PROCEDURE proc_BARB_update_fields  AS
BEGIN

DECLARE @query varchar(10000)
DECLARE @cntr_people integer
DECLARE @cntr_people_string varchar(5)


SET @cntr_people = 1 -- restart the people counter, done outside @cntr_people loop

WHILE @cntr_people <= 16
 BEGIN

SET @cntr_people_string=CAST(@cntr_people as varchar(4))

SET @query = 'update ripolile.BARB_table_output_1 pvf1'
SET @query = @query || ' set male_4_9=coalesce( pvf1.male_4_9,0)+(case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 9) then 1 else 0 end)'
SET @query = @query || ' ,Weigthed_Male_4_9=coalesce( pvf1.Weigthed_Male_4_9,0)+(case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 9) then 1.0 else 0 end)*coalesce(mem.processing_weight, 0)'
SET @query = @query || ' ,Male_10_15=coalesce( pvf1.Male_10_15,0)+(case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 15) then 1 else 0 end)'
SET @query = @query || ' ,Weigthed_Male_10_15=coalesce( pvf1.Weigthed_Male_10_15,0)+(case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 15) then 1.0 else 0 end)*coalesce(mem.processing_weight, 0)'
SET @query = @query || ' ,Male_16_19=coalesce( pvf1.Male_16_19,0)+(case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 19) then 1 else 0 end)'
SET @query = @query || ' ,Weigthed_Male_16_19=coalesce( pvf1.Weigthed_Male_16_19,0)+(case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 19) then 1.0 else 0 end)*coalesce(mem.processing_weight, 0)'
SET @query = @query || ' ,Male_20_24=coalesce( pvf1.Male_20_24,0)+(case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 24) then 1 else 0 end)'
SET @query = @query || ' ,Weigthed_Male_20_24=coalesce( pvf1.Weigthed_Male_20_24,0)+(case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 24) then 1.0 else 0 end)*coalesce(mem.processing_weight, 0)'
SET @query = @query || ' ,Male_25_34=coalesce( pvf1.male_25_34,0)+(case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 34) then 1 else 0 end)'
SET @query = @query || ' ,Weigthed_Male_25_34=coalesce( pvf1.Weigthed_Male_25_34,0)+(case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 34) then 1.0 else 0 end)*coalesce(mem.processing_weight, 0)'
SET @query = @query || ' ,Male_35_44=coalesce( pvf1.Male_35_44,0)+(case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 44) then 1 else 0 end)'
SET @query = @query || ' ,Weigthed_Male_35_44=coalesce( pvf1.Weigthed_Male_35_44,0)+(case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 44) then 1.0 else 0 end)*coalesce(mem.processing_weight, 0)'
SET @query = @query || ' ,Male_45_64=coalesce( pvf1.Male_45_64,0)+(case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 64) then 1 else 0 end)'
SET @query = @query || ' ,Weigthed_Male_45_64=coalesce( pvf1.Weigthed_Male_45_64,0)+(case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 64) then 1.0 else 0 end)*coalesce(mem.processing_weight, 0)'
SET @query = @query || ' ,Male_65=coalesce( pvf1.Male_65,0)+(case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 65) then 1 else 0 end)'
SET @query = @query || ' ,Weigthed_Male_65=coalesce( pvf1.Weigthed_Male_65,0)+(case when mem.sex_code = 1 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 65) then 1.0 else 0 end)*coalesce(mem.processing_weight, 0)'
SET @query = @query || ' ,female_4_9=coalesce( pvf1.female_4_9,0)+(case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 9) then 1 else 0 end)'
SET @query = @query || ' ,Weigthed_Female_4_9=coalesce( pvf1.Weigthed_Female_4_9,0)+(case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 4 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 9) then 1.0 else 0 end)*coalesce(mem.processing_weight, 0)'
SET @query = @query || ' ,female_10_15=coalesce( pvf1.female_10_15,0)+(case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 15) then 1 else 0 end)'
SET @query = @query || ' ,Weigthed_Female_10_15=coalesce( pvf1.Weigthed_Female_10_15,0)+(case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 10 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 15) then 1.0 else 0 end)*coalesce(mem.processing_weight, 0)'
SET @query = @query || ' ,female_16_19=coalesce( pvf1.female_16_19,0)+(case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 19) then 1 else 0 end)'
SET @query = @query || ' ,Weigthed_Female_16_19=coalesce( pvf1.Weigthed_Female_16_19,0)+(case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 16 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 19) then 1.0 else 0 end)*coalesce(mem.processing_weight, 0)'
SET @query = @query || ' ,female_20_24=coalesce( pvf1.female_20_24,0)+(case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 24) then 1 else 0 end)'
SET @query = @query || ' ,Weigthed_Female_20_24=coalesce( pvf1.Weigthed_Female_20_24,0)+(case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 20 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 24) then 1.0 else 0 end)*coalesce(mem.processing_weight, 0)'
SET @query = @query || ' ,female_25_34=coalesce( pvf1.female_25_34,0)+(case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 34) then 1 else 0 end)'
SET @query = @query || ' ,Weigthed_Female_25_34=coalesce( pvf1.Weigthed_Female_25_34,0)+(case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 25 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 34) then 1.0 else 0 end)*coalesce(mem.processing_weight, 0)'
SET @query = @query || ' ,female_35_44=coalesce( pvf1.female_35_44,0)+(case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 44) then 1 else 0 end)'
SET @query = @query || ' ,Weigthed_Female_35_44=coalesce( pvf1.Weigthed_Female_35_44,0)+(case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 35 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 44) then 1.0 else 0 end)*coalesce(mem.processing_weight, 0)'
SET @query = @query || ' ,female_45_64=coalesce( pvf1.female_45_64,0)+(case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 64) then 1 else 0 end)'
SET @query = @query || ' ,Weigthed_Female_45_64=coalesce( pvf1.Weigthed_Female_45_64,0)+(case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 45 and datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) <= 64) then 1.0 else 0 end)*coalesce(mem.processing_weight, 0)'
SET @query = @query || ' ,female_65=coalesce( pvf1.female_65,0)+(case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 65) then 1 else 0 end)'
SET @query = @query || ' ,Weigthed_Female_65=coalesce( pvf1.Weigthed_Female_65,0)+(case when mem.sex_code = 2 and (datediff(yy, mem.date_of_birth, date(pvf1.Local_start_time_of_session)) >= 65) then 1.0 else 0 end)*coalesce(mem.processing_weight, 0)'
SET @query = @query || ' from '
SET @query = @query || '  ripolile.BARB_table_output_1 pvf1 '
SET @query = @query || '      inner join '
SET @query = @query || ' ( '
SET @query = @query || '    select mem1.sex_code, mem1.household_number, mem1.date_of_birth, mem1.household_status, wei.processing_weight '
SET @query = @query || '    from '
SET @query = @query || '    ripolile.BARB_PVF04_Individual_Member_Details mem1 '
SET @query = @query || '              left join '
SET @query = @query || '    ripolile.BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories wei '
SET @query = @query || '        on  mem1.household_number=wei.household_number '
SET @query = @query || '        and mem1.person_number=wei.person_number '
SET @query = @query || '        and wei.reporting_panel_code=50 '
SET @query = @query || '    where mem1.person_number=' || @cntr_people_string
SET @query = @query || '    ) mem '
SET @query = @query || '        on  mem.household_number=pvf1.household_number '
SET @query = @query || '    where pvf1.person_' || @cntr_people_string || '_viewing=1'
-- SET @query = @query || '    go '

EXECUTE (@query)

-- select (@cntr_people_string)

SET @cntr_people = @cntr_people+1

end -- WHILE @cntr_people <= 16


end -- end of procedure proc_BARB_update_fields

GO




IF OBJECT_ID('ripolile.BARB_PVF04_Individual_Member_Details') IS NOT NULL
drop table ripolile.BARB_PVF04_Individual_Member_Details

CREATE TABLE ripolile.BARB_PVF04_Individual_Member_Details (
filename varchar(100)
,Record_type int DEFAULT NULL
,date_of_birth date DEFAULT NULL
,Household_number int DEFAULT NULL
,Person_membership_status int DEFAULT NULL
,Person_number int DEFAULT NULL
,Sex_code int DEFAULT NULL
,Date_valid_for_DB1 date DEFAULT NULL
,Marital_status int DEFAULT NULL
,Household_status int DEFAULT NULL
,Working_status int DEFAULT NULL
)
GO
create hg index ind_household_number_PVF04 on ripolile.BARB_PVF04_Individual_Member_Details(Household_number)
GO
create lf index ind_person_PVF04 on ripolile.BARB_PVF04_Individual_Member_Details(person_number)
GO

IF OBJECT_ID('ripolile.BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories') IS NOT NULL
drop table ripolile.BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories
GO
CREATE TABLE ripolile.BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories (
filename varchar(100)
,Record_Type int DEFAULT NULL
,Household_Number int DEFAULT NULL
,Person_Number int DEFAULT NULL
,Reporting_Panel_Code int DEFAULT NULL
,Processing_Weight double DEFAULT NULL
)
GO

create hg index ind_household_number_PVF05 on ripolile.BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories(Household_Number)
GO
create lf index ind_person_PVF05 on ripolile.BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories(Person_Number)
GO
create lf index ind_panel_PVF05 on ripolile.BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories(Reporting_Panel_Code)
GO

IF OBJECT_ID('ripolile.BARB_Panel_Demographic_Data_TV_Sets_Characteristics') IS not NULL
drop table ripolile.BARB_Panel_Demographic_Data_TV_Sets_Characteristics
GO
CREATE TABLE ripolile.BARB_Panel_Demographic_Data_TV_Sets_Characteristics (
Household_number int DEFAULT NULL
,Set_number int DEFAULT NULL
,Reception_Capability_Code1 int DEFAULT NULL
,Reception_Capability_Code2 int DEFAULT NULL
,Reception_Capability_Code3 int DEFAULT NULL
,Reception_Capability_Code4 int DEFAULT NULL
,Reception_Capability_Code5 int DEFAULT NULL
,Reception_Capability_Code6 int DEFAULT NULL
,Reception_Capability_Code7 int DEFAULT NULL
,Reception_Capability_Code8 int DEFAULT NULL
,Reception_Capability_Code9 int DEFAULT NULL
,Reception_Capability_Code10 int DEFAULT NULL
)

GO

create hg index ind_household_number_tvchar on ripolile.BARB_Panel_Demographic_Data_TV_Sets_Characteristics(Household_Number)
GO

IF OBJECT_ID('ripolile.BARB_PVF_Viewing_Record_Panel_Members') IS not NULL
drop table ripolile.BARB_PVF_Viewing_Record_Panel_Members
GO
CREATE TABLE ripolile.BARB_PVF_Viewing_Record_Panel_Members (
filename varchar(100)
,Record_type int DEFAULT NULL
,Household_number int DEFAULT NULL
,Date_of_Activity_DB1 date DEFAULT NULL
,Set_number int DEFAULT NULL
,Start_time_of_session int DEFAULT NULL
,Duration_of_session int DEFAULT NULL
,Session_activity_type int DEFAULT NULL
,Playback_type int DEFAULT NULL
,DB1_Station_Code int DEFAULT NULL
,Viewing_platform int DEFAULT NULL
,Date_of_Recording_DB1 date DEFAULT NULL
,Start_time_of_recording int DEFAULT NULL
,Person_1_viewing int DEFAULT NULL
,Person_2_viewing int DEFAULT NULL
,Person_3_viewing int DEFAULT NULL
,Person_4_viewing int DEFAULT NULL
,Person_5_viewing int DEFAULT NULL
,Person_6_viewing int DEFAULT NULL
,Person_7_viewing int DEFAULT NULL
,Person_8_viewing int DEFAULT NULL
,Person_9_viewing int DEFAULT NULL
,Person_10_viewing int DEFAULT NULL
,Person_11_viewing int DEFAULT NULL
,Person_12_viewing int DEFAULT NULL
,Person_13_viewing int DEFAULT NULL
,Person_14_viewing int DEFAULT NULL
,Person_15_viewing int DEFAULT NULL
,Person_16_viewing int DEFAULT NULL
,Interactive_Bar_Code_Identifier int DEFAULT NULL
,VOD_Indicator int DEFAULT NULL
,VOD_Provider int DEFAULT NULL
,VOD_Service int DEFAULT NULL
,VOD_Type int DEFAULT NULL
,Device_in_use int DEFAULT NULL
,cb_row_id bigint DEFAULT NULL
)
GO
create hg index ind_household_number on ripolile.BARB_PVF_Viewing_Record_Panel_Members(household_number)
GO


IF OBJECT_ID('ripolile.BARB_PVF_Viewing_Record_Guests') IS not NULL
drop table ripolile.BARB_PVF_Viewing_Record_Guests
GO
CREATE TABLE ripolile.BARB_PVF_Viewing_Record_Guests (
filename varchar(100)
,Record_type int DEFAULT NULL
,Household_number int DEFAULT NULL
,Date_of_Activity_DB1 date DEFAULT NULL
,Set_number int DEFAULT NULL
,Start_time_of_session int DEFAULT NULL
,Duration_of_session int DEFAULT NULL
,Session_activity_type int DEFAULT NULL
,Playback_type int DEFAULT NULL
,DB1_Station_Code int DEFAULT NULL
,Viewing_platform int DEFAULT NULL
,Date_of_Recording_DB1 date DEFAULT NULL
,Start_time_of_recording int DEFAULT NULL
,Male_4_9 int DEFAULT NULL
,Male_10_15 int DEFAULT NULL
,Male_16_19 int DEFAULT NULL
,Male_20_24 int DEFAULT NULL
,Male_25_34 int DEFAULT NULL
,Male_35_44 int DEFAULT NULL
,Male_45_64 int DEFAULT NULL
,Male_65 int DEFAULT NULL
,Female_4_9 int DEFAULT NULL
,Female_10_15 int DEFAULT NULL
,Female_16_19 int DEFAULT NULL
,Female_20_24 int DEFAULT NULL
,Female_25_34 int DEFAULT NULL
,Female_35_44 int DEFAULT NULL
,Female_45_64 int DEFAULT NULL
,Female_65 int DEFAULT NULL
,Interactive_Bar_Code_Identifier int DEFAULT NULL
,VOD_Indicator int DEFAULT NULL
,VOD_Provider int DEFAULT NULL
,VOD_Service int DEFAULT NULL
,VOD_Type int DEFAULT NULL
,Device_in_use int DEFAULT NULL
,cb_row_id bigint DEFAULT NULL
)
GO
create hg index ind_household_number_guests on ripolile.BARB_PVF_Viewing_Record_Guests(household_number)
GO

IF OBJECT_ID('ripolile.BARB_PV2_Viewing_Record_Panel_Members') IS NOT NULL
drop table ripolile.BARB_PV2_Viewing_Record_Panel_Members
GO
CREATE TABLE ripolile.BARB_PV2_Viewing_Record_Panel_Members (
filename varchar(100)
,Record_type int DEFAULT NULL
,Household_number int DEFAULT NULL
,Date_of_Activity_DB1 date DEFAULT NULL
,Set_number int DEFAULT NULL
,Start_time_of_session int DEFAULT NULL
,Duration_of_session int DEFAULT NULL
,Session_activity_type int DEFAULT NULL
,Playback_type int DEFAULT NULL
,DB1_Station_Code int DEFAULT NULL
,Viewing_platform int DEFAULT NULL
,Date_of_Recording_DB1 date DEFAULT NULL
,Start_time_of_recording int DEFAULT NULL
,Person_1_viewing int DEFAULT NULL
,Person_2_viewing int DEFAULT NULL
,Person_3_viewing int DEFAULT NULL
,Person_4_viewing int DEFAULT NULL
,Person_5_viewing int DEFAULT NULL
,Person_6_viewing int DEFAULT NULL
,Person_7_viewing int DEFAULT NULL
,Person_8_viewing int DEFAULT NULL
,Person_9_viewing int DEFAULT NULL
,Person_10_viewing int DEFAULT NULL
,Person_11_viewing int DEFAULT NULL
,Person_12_viewing int DEFAULT NULL
,Person_13_viewing int DEFAULT NULL
,Person_14_viewing int DEFAULT NULL
,Person_15_viewing int DEFAULT NULL
,Person_16_viewing int DEFAULT NULL
,Interactive_Bar_Code_Identifier int DEFAULT NULL
,VOD_Indicator int DEFAULT NULL
,VOD_Provider int DEFAULT NULL
,VOD_Service int DEFAULT NULL
,VOD_Type int DEFAULT NULL
,Device_in_use int DEFAULT NULL
,cb_row_id bigint DEFAULT NULL
)
GO
create hg index ind_household_number_PV2 on ripolile.BARB_PV2_Viewing_Record_Panel_Members(household_number)
GO

IF OBJECT_ID('ripolile.BARB_PV2_Viewing_Record_Guests') IS NOT NULL
drop table ripolile.BARB_PV2_Viewing_Record_Guests
GO

CREATE TABLE ripolile.BARB_PV2_Viewing_Record_Guests (
filename varchar(100)
,Record_type int DEFAULT NULL
,Household_number int DEFAULT NULL
,Date_of_Activity_DB1 date DEFAULT NULL
,Set_number int DEFAULT NULL
,Start_time_of_session int DEFAULT NULL
,Duration_of_session int DEFAULT NULL
,Session_activity_type int DEFAULT NULL
,Playback_type int DEFAULT NULL
,DB1_Station_Code int DEFAULT NULL
,Viewing_platform int DEFAULT NULL
,Date_of_Recording_DB1 date DEFAULT NULL
,Start_time_of_recording int DEFAULT NULL
,Male_4_9 int DEFAULT NULL
,Male_10_15 int DEFAULT NULL
,Male_16_19 int DEFAULT NULL
,Male_20_24 int DEFAULT NULL
,Male_25_34 int DEFAULT NULL
,Male_35_44 int DEFAULT NULL
,Male_45_64 int DEFAULT NULL
,Male_65 int DEFAULT NULL
,Female_4_9 int DEFAULT NULL
,Female_10_15 int DEFAULT NULL
,Female_16_19 int DEFAULT NULL
,Female_20_24 int DEFAULT NULL
,Female_25_34 int DEFAULT NULL
,Female_35_44 int DEFAULT NULL
,Female_45_64 int DEFAULT NULL
,Female_65 int DEFAULT NULL
,Interactive_Bar_Code_Identifier int DEFAULT NULL
,VOD_Indicator int DEFAULT NULL
,VOD_Provider int DEFAULT NULL
,VOD_Service int DEFAULT NULL
,VOD_Type int DEFAULT NULL
,Device_in_use int DEFAULT NULL
,cb_row_id bigint DEFAULT NULL
)
GO
create hg index ind_household_number_PV2_guests on ripolile.BARB_PV2_Viewing_Record_Guests(household_number)
GO

IF OBJECT_ID('ripolile.BARB_PVF06_Viewing_Record_Panel_Members') IS NOT NULL
drop table ripolile.BARB_PVF06_Viewing_Record_Panel_Members
GO

CREATE TABLE ripolile.BARB_PVF06_Viewing_Record_Panel_Members (
Sky_STB_viewing varchar(1) DEFAULT NULL
,Sky_STB_holder_hh varchar(1) DEFAULT NULL
,PVF_PV2 varchar(4) DEFAULT NULL
,filename varchar(100)
,Record_type int DEFAULT NULL
,Household_number int DEFAULT NULL
,Barb_date_of_activity date -- New field
,Actual_date_of_session date  --- change datatype from Barb. If Barb start time > 24:00 then add 1 to this date
,Set_number int DEFAULT NULL
,Start_time_of_session_text varchar(6)--- change datatype from Barb to make it easier to convert to timestamp later. Working field
,Start_time_of_session timestamp  --- New field
,End_time_of_session timestamp --- new field
,Duration_of_session int DEFAULT NULL
,Session_activity_type int DEFAULT NULL
,Playback_type int DEFAULT NULL
,DB1_Station_Code int DEFAULT NULL
,Viewing_platform int DEFAULT NULL
,Barb_date_of_recording date
,Actual_Date_of_Recording date --- change datatype from Barb
,Start_time_of_recording_text varchar(6) --- change datatype from Barb to make it easier to convert to timestamp later. Working field
,Start_time_of_recording timestamp --- new field
,Person_1_viewing int DEFAULT NULL
,Person_2_viewing int DEFAULT NULL
,Person_3_viewing int DEFAULT NULL
,Person_4_viewing int DEFAULT NULL
,Person_5_viewing int DEFAULT NULL
,Person_6_viewing int DEFAULT NULL
,Person_7_viewing int DEFAULT NULL
,Person_8_viewing int DEFAULT NULL
,Person_9_viewing int DEFAULT NULL
,Person_10_viewing int DEFAULT NULL
,Person_11_viewing int DEFAULT NULL
,Person_12_viewing int DEFAULT NULL
,Person_13_viewing int DEFAULT NULL
,Person_14_viewing int DEFAULT NULL
,Person_15_viewing int DEFAULT NULL
,Person_16_viewing int DEFAULT NULL
,Interactive_Bar_Code_Identifier int DEFAULT NULL
,VOD_Indicator int DEFAULT NULL
,VOD_Provider int DEFAULT NULL
,VOD_Service int DEFAULT NULL
,VOD_Type int DEFAULT NULL
,Device_in_use int DEFAULT NULL
,cb_row_id bigint DEFAULT NULL
)
GO

IF OBJECT_ID('ripolile.BARB_PVF07_Viewing_Record_Guests') IS NOT NULL
drop table ripolile.BARB_PVF07_Viewing_Record_Guests
GO
CREATE TABLE ripolile.BARB_PVF07_Viewing_Record_Guests (
Sky_STB_viewing varchar(1) DEFAULT NULL
,Sky_STB_holder_hh varchar(1) DEFAULT NULL
,PVF_PV2 varchar(4) DEFAULT NULL
,filename varchar(100)
,Record_type int DEFAULT NULL
,Household_number int DEFAULT NULL
,Barb_date_of_activity date -- New field
,Actual_date_of_session date  --- change datatype from Barb. If Barb start time > 24:00 then add 1 to this date
,Set_number int DEFAULT NULL
,Start_time_of_session_text varchar(6)--- change datatype from Barb to make it easier to convert to timestamp later. Working field
,Start_time_of_session timestamp  --- New field
,End_time_of_session timestamp --- new field
,Duration_of_session int DEFAULT NULL
,Session_activity_type int DEFAULT NULL
,Playback_type int DEFAULT NULL
,DB1_Station_Code int DEFAULT NULL
,Viewing_platform int DEFAULT NULL
,Barb_date_of_recording date
,Actual_Date_of_Recording date --- change datatype from Barb
,Start_time_of_recording_text varchar(6) --- change datatype from Barb to make it easier to convert to timestamp later. Working field
,Start_time_of_recording timestamp --- new field
,Male_4_9 int DEFAULT NULL
,Male_10_15 int DEFAULT NULL
,Male_16_19 int DEFAULT NULL
,Male_20_24 int DEFAULT NULL
,Male_25_34 int DEFAULT NULL
,Male_35_44 int DEFAULT NULL
,Male_45_64 int DEFAULT NULL
,Male_65 int DEFAULT NULL
,Female_4_9 int DEFAULT NULL
,Female_10_15 int DEFAULT NULL
,Female_16_19 int DEFAULT NULL
,Female_20_24 int DEFAULT NULL
,Female_25_34 int DEFAULT NULL
,Female_35_44 int DEFAULT NULL
,Female_45_64 int DEFAULT NULL
,Female_65 int DEFAULT NULL
,Interactive_Bar_Code_Identifier int DEFAULT NULL
,VOD_Indicator int DEFAULT NULL
,VOD_Provider int DEFAULT NULL
,VOD_Service int DEFAULT NULL
,VOD_Type int DEFAULT NULL
,Device_in_use int DEFAULT NULL
,cb_row_id bigint DEFAULT NULL
)
GO

IF OBJECT_ID('ripolile.BARB_PV206_Viewing_Record_Panel_Members') IS NOT NULL
drop table ripolile.BARB_PV206_Viewing_Record_Panel_Members
GO
CREATE TABLE ripolile.BARB_PV206_Viewing_Record_Panel_Members (
Sky_STB_viewing varchar(1) DEFAULT NULL
,Sky_STB_holder_hh varchar(1) DEFAULT NULL
,PVF_PV2 varchar(4) DEFAULT NULL
,filename varchar(100)
,Record_type int DEFAULT NULL
,Household_number int DEFAULT NULL
,Barb_date_of_activity date -- New field
,Actual_date_of_session date  --- change datatype from Barb. If Barb start time > 24:00 then add 1 to this date
,Set_number int DEFAULT NULL
,Start_time_of_session_text varchar(6)--- change datatype from Barb to make it easier to convert to timestamp later. Working field
,Start_time_of_session timestamp  --- New field
,End_time_of_session timestamp --- new field
,Duration_of_session int DEFAULT NULL
,Session_activity_type int DEFAULT NULL
,Playback_type int DEFAULT NULL
,DB1_Station_Code int DEFAULT NULL
,Viewing_platform int DEFAULT NULL
,Barb_date_of_recording date
,Actual_Date_of_Recording date --- change datatype from Barb
,Start_time_of_recording_text varchar(6) --- change datatype from Barb to make it easier to convert to timestamp later. Working field
,Start_time_of_recording timestamp --- new field
,Person_1_viewing int DEFAULT NULL
,Person_2_viewing int DEFAULT NULL
,Person_3_viewing int DEFAULT NULL
,Person_4_viewing int DEFAULT NULL
,Person_5_viewing int DEFAULT NULL
,Person_6_viewing int DEFAULT NULL
,Person_7_viewing int DEFAULT NULL
,Person_8_viewing int DEFAULT NULL
,Person_9_viewing int DEFAULT NULL
,Person_10_viewing int DEFAULT NULL
,Person_11_viewing int DEFAULT NULL
,Person_12_viewing int DEFAULT NULL
,Person_13_viewing int DEFAULT NULL
,Person_14_viewing int DEFAULT NULL
,Person_15_viewing int DEFAULT NULL
,Person_16_viewing int DEFAULT NULL
,Interactive_Bar_Code_Identifier int DEFAULT NULL
,VOD_Indicator int DEFAULT NULL
,VOD_Provider int DEFAULT NULL
,VOD_Service int DEFAULT NULL
,VOD_Type int DEFAULT NULL
,Device_in_use int DEFAULT NULL
,cb_row_id bigint DEFAULT NULL
)
GO

IF OBJECT_ID('ripolile.BARB_PV207_Viewing_Record_Guests') IS NOT NULL
drop table ripolile.BARB_PV207_Viewing_Record_Guests
GO
CREATE TABLE ripolile.BARB_PV207_Viewing_Record_Guests (
Sky_STB_viewing varchar(1) DEFAULT NULL
,Sky_STB_holder_hh varchar(1) DEFAULT NULL
,PVF_PV2 varchar(4) DEFAULT NULL
,filename varchar(100)
,Record_type int DEFAULT NULL
,Household_number int DEFAULT NULL
,Barb_date_of_activity date -- New field
,Actual_date_of_session date  --- change datatype from Barb. If Barb start time > 24:00 then add 1 to this date
,Set_number int DEFAULT NULL
,Start_time_of_session_text varchar(6)--- change datatype from Barb to make it easier to convert to timestamp later. Working field
,Start_time_of_session timestamp  --- New field
,End_time_of_session timestamp --- new field
,Duration_of_session int DEFAULT NULL
,Session_activity_type int DEFAULT NULL
,Playback_type int DEFAULT NULL
,DB1_Station_Code int DEFAULT NULL
,Viewing_platform int DEFAULT NULL
,Barb_date_of_recording date
,Actual_Date_of_Recording date --- change datatype from Barb
,Start_time_of_recording_text varchar(6) --- change datatype from Barb to make it easier to convert to timestamp later. Working field
,Start_time_of_recording timestamp --- new field
,Male_4_9 int DEFAULT NULL
,Male_10_15 int DEFAULT NULL
,Male_16_19 int DEFAULT NULL
,Male_20_24 int DEFAULT NULL
,Male_25_34 int DEFAULT NULL
,Male_35_44 int DEFAULT NULL
,Male_45_64 int DEFAULT NULL
,Male_65 int DEFAULT NULL
,Female_4_9 int DEFAULT NULL
,Female_10_15 int DEFAULT NULL
,Female_16_19 int DEFAULT NULL
,Female_20_24 int DEFAULT NULL
,Female_25_34 int DEFAULT NULL
,Female_35_44 int DEFAULT NULL
,Female_45_64 int DEFAULT NULL
,Female_65 int DEFAULT NULL
,Interactive_Bar_Code_Identifier int DEFAULT NULL
,VOD_Indicator int DEFAULT NULL
,VOD_Provider int DEFAULT NULL
,VOD_Service int DEFAULT NULL
,VOD_Type int DEFAULT NULL
,Device_in_use int DEFAULT NULL
,cb_row_id bigint DEFAULT NULL
)
GO

create hg index ind_household_number_PVF06 on ripolile.BARB_PVF06_Viewing_Record_Panel_Members(household_number) --- check this
GO
create hg index ind_db1_PVF06 on ripolile.BARB_PVF06_Viewing_Record_Panel_Members(db1_station_code)
GO

create DTTM index ind_start_PVF06 on ripolile.BARB_PVF06_Viewing_Record_Panel_Members(Start_time_of_session)
GO

create DTTM index ind_end_PVF06 on ripolile.BARB_PVF06_Viewing_Record_Panel_Members(End_time_of_session)

GO
create hg index ind_household_number_PVF07 on ripolile.BARB_PVF07_Viewing_Record_Guests(Household_number)
GO

create DTTM index ind_start_PVF07 on ripolile.BARB_PVF07_Viewing_Record_Guests(Start_time_of_session)
GO

create DTTM index ind_end_PVF07 on ripolile.BARB_PVF07_Viewing_Record_Guests(End_time_of_session)

GO
create hg index ind_household_number_PV206 on ripolile.BARB_PV206_Viewing_Record_Panel_Members(household_number)
GO

create hg index ind_db1_PV206 on ripolile.BARB_PV206_Viewing_Record_Panel_Members(db1_station_code)
GO

create hg index ind_start_PV206 on ripolile.BARB_PV206_Viewing_Record_Panel_Members(Start_time_of_session)
GO

create hg index ind_end_PV206 on ripolile.BARB_PV206_Viewing_Record_Panel_Members(End_time_of_session)

GO


create hg index ind_household_number_PV207 on ripolile.BARB_PV207_Viewing_Record_Guests(Household_number)
GO

create DTTM index ind_start_PV207 on ripolile.BARB_PV207_Viewing_Record_Guests(Start_time_of_session)
GO

create DTTM index ind_end_PV207 on ripolile.BARB_PV207_Viewing_Record_Guests(End_time_of_session)

GO

IF OBJECT_ID('ripolile.BARB_PVF06_PV206_Viewing_Record_Panel_Members') IS NOT NULL
DROP TABLE ripolile.BARB_PVF06_PV206_Viewing_Record_Panel_Members
GO
CREATE TABLE ripolile.BARB_PVF06_PV206_Viewing_Record_Panel_Members (
Sky_STB_viewing varchar(1) DEFAULT NULL
,Sky_STB_holder_hh varchar(1) DEFAULT NULL
,PVF_PV2 varchar(4) DEFAULT NULL
,filename varchar(100)
,Record_type int DEFAULT NULL
,Household_number int DEFAULT NULL
,Barb_date_of_activity date -- New field
,Actual_date_of_session date  --- change datatype from Barb. If Barb start time > 24:00 then add 1 to this date
,Set_number int DEFAULT NULL
,Start_time_of_session_text varchar(6)--- change datatype from Barb to make it easier to convert to timestamp later. Working field
,Start_time_of_session timestamp  --- New field
,End_time_of_session timestamp --- new field
,Duration_of_session int DEFAULT NULL
,Session_activity_type int DEFAULT NULL
,Playback_type int DEFAULT NULL
,DB1_Station_Code int DEFAULT NULL
,Viewing_platform int DEFAULT NULL
,Barb_date_of_recording date
,Actual_Date_of_Recording date --- change datatype from Barb
,Start_time_of_recording_text varchar(6) --- change datatype from Barb to make it easier to convert to timestamp later. Working field
,Start_time_of_recording timestamp --- new field
,Person_1_viewing int DEFAULT NULL
,Person_2_viewing int DEFAULT NULL
,Person_3_viewing int DEFAULT NULL
,Person_4_viewing int DEFAULT NULL
,Person_5_viewing int DEFAULT NULL
,Person_6_viewing int DEFAULT NULL
,Person_7_viewing int DEFAULT NULL
,Person_8_viewing int DEFAULT NULL
,Person_9_viewing int DEFAULT NULL
,Person_10_viewing int DEFAULT NULL
,Person_11_viewing int DEFAULT NULL
,Person_12_viewing int DEFAULT NULL
,Person_13_viewing int DEFAULT NULL
,Person_14_viewing int DEFAULT NULL
,Person_15_viewing int DEFAULT NULL
,Person_16_viewing int DEFAULT NULL
,Interactive_Bar_Code_Identifier int DEFAULT NULL
,VOD_Indicator int DEFAULT NULL
,VOD_Provider int DEFAULT NULL
,VOD_Service int DEFAULT NULL
,VOD_Type int DEFAULT NULL
,Device_in_use int DEFAULT NULL
,cb_row_id bigint DEFAULT NULL
)
GO
IF OBJECT_ID('ripolile.BARB_PVF07_PV207_Viewing_Record_Guests') IS NOT NULL
DROP TABLE ripolile.BARB_PVF07_PV207_Viewing_Record_Guests
GO
CREATE TABLE ripolile.BARB_PVF07_PV207_Viewing_Record_Guests (
Sky_STB_viewing varchar(1) DEFAULT NULL
,Sky_STB_holder_hh varchar(1) DEFAULT NULL
,PVF_PV2 varchar(4) DEFAULT NULL
,filename varchar(100)
,Record_type int DEFAULT NULL
,Household_number int DEFAULT NULL
,Barb_date_of_activity date -- New field
,Actual_date_of_session date  --- change datatype from Barb. If Barb start time > 24:00 then add 1 to this date
,Set_number int DEFAULT NULL
,Start_time_of_session_text varchar(6)--- change datatype from Barb to make it easier to convert to timestamp later. Working field
,Start_time_of_session timestamp  --- New field
,End_time_of_session timestamp --- new field
,Duration_of_session int DEFAULT NULL
,Session_activity_type int DEFAULT NULL
,Playback_type int DEFAULT NULL
,DB1_Station_Code int DEFAULT NULL
,Viewing_platform int DEFAULT NULL
,Barb_date_of_recording date
,Actual_Date_of_Recording date --- change datatype from Barb
,Start_time_of_recording_text varchar(6) --- change datatype from Barb to make it easier to convert to timestamp later. Working field
,Start_time_of_recording timestamp --- new field
,Male_4_9 int DEFAULT NULL
,Male_10_15 int DEFAULT NULL
,Male_16_19 int DEFAULT NULL
,Male_20_24 int DEFAULT NULL
,Male_25_34 int DEFAULT NULL
,Male_35_44 int DEFAULT NULL
,Male_45_64 int DEFAULT NULL
,Male_65 int DEFAULT NULL
,Female_4_9 int DEFAULT NULL
,Female_10_15 int DEFAULT NULL
,Female_16_19 int DEFAULT NULL
,Female_20_24 int DEFAULT NULL
,Female_25_34 int DEFAULT NULL
,Female_35_44 int DEFAULT NULL
,Female_45_64 int DEFAULT NULL
,Female_65 int DEFAULT NULL
,Interactive_Bar_Code_Identifier int DEFAULT NULL
,VOD_Indicator int DEFAULT NULL
,VOD_Provider int DEFAULT NULL
,VOD_Service int DEFAULT NULL
,VOD_Type int DEFAULT NULL
,Device_in_use int DEFAULT NULL
,cb_row_id bigint DEFAULT NULL
)

GO

IF OBJECT_ID('ripolile.Sky_STB_holder_hh_tmp_table') IS NOT NULL
DROP TABLE ripolile.Sky_STB_holder_hh_tmp_table
GO
CREATE TABLE ripolile.Sky_STB_holder_hh_tmp_table
(
  household_number INT DEFAULT NULL
)
GO
IF OBJECT_ID('ripolile.local_time_range') IS not NULL
DROP TABLE ripolile.local_time_range
GO

CREATE TABLE ripolile.local_time_range
(
  min_local_session_timestamp timestamp DEFAULT NULL
  ,min_local_recording_timestamp timestamp DEFAULT NULL
  ,min_local_timestamp timestamp DEFAULT NULL
  ,max_local_timestamp timestamp DEFAULT NULL
)
GO
IF OBJECT_ID('ripolile.local_UTC_conversion_table') IS not NULL
DROP TABLE ripolile.local_UTC_conversion_table
GO

CREATE TABLE ripolile.local_UTC_conversion_table
(
  utc_day_date date DEFAULT NULL
  ,utc_time_hours int DEFAULT NULL
  ,local_day_date date DEFAULT NULL
  ,local_time_hours int DEFAULT NULL
  ,daylight_savings_flag int DEFAULT NULL
)
GO

IF OBJECT_ID('ripolile.BARB_table_output_1') IS not NULL
DROP TABLE ripolile.BARB_table_output_1

GO

create table ripolile.BARB_table_output_1
(
filename varchar(100)
,Sky_STB_viewing varchar(1) DEFAULT NULL
,Sky_STB_holder_hh varchar(1) DEFAULT NULL
,PVF_PV2  varchar(4) DEFAULT NULL
,Household_number int DEFAULT NULL
,Barb_date_of_activity_DB1 date
-- ,Actual_date
,Set_number int DEFAULT NULL
,BARB_Start_Time_of_Session  int default NULL
,Local_Start_Time_of_Session timestamp default NULL
,Local_End_Time_of_Session timestamp default NULL
,Panel_or_guest_flag varchar(8)
,Duration_of_Session int DEFAULT NULL -- minutes
,Session_activity_type int DEFAULT NULL
,Playback_type int DEFAULT NULL
,DB1_Station_Code int DEFAULT NULL
,DB1_Station_Name varchar(100)
,Viewing_platform int DEFAULT NULL
,Barb_date_of_recording_DB1 date
-- ,Actual_Date_of_Recording date --- change datatype from Barb
,BARB_Start_time_of_recording int default NULL
,Local_Start_time_of_recording timestamp default NULL
,Local_End_time_of_recording timestamp default NULL
,Person_1_viewing int DEFAULT NULL
,Person_2_viewing int DEFAULT NULL
,Person_3_viewing int DEFAULT NULL
,Person_4_viewing int DEFAULT NULL
,Person_5_viewing int DEFAULT NULL
,Person_6_viewing int DEFAULT NULL
,Person_7_viewing int DEFAULT NULL
,Person_8_viewing int DEFAULT NULL
,Person_9_viewing int DEFAULT NULL
,Person_10_viewing int DEFAULT NULL
,Person_11_viewing int DEFAULT NULL
,Person_12_viewing int DEFAULT NULL
,Person_13_viewing int DEFAULT NULL
,Person_14_viewing int DEFAULT NULL
,Person_15_viewing int DEFAULT NULL
,Person_16_viewing int DEFAULT NULL
,Male_4_9 int DEFAULT NULL
,Male_10_15 int DEFAULT NULL
,Male_16_19 int DEFAULT NULL
,Male_20_24 int DEFAULT NULL
,Male_25_34 int DEFAULT NULL
,Male_35_44 int DEFAULT NULL
,Male_45_64 int DEFAULT NULL
,Male_65 int DEFAULT NULL
,Female_4_9 int DEFAULT NULL
,Female_10_15 int DEFAULT NULL
,Female_16_19 int DEFAULT NULL
,Female_20_24 int DEFAULT NULL
,Female_25_34 int DEFAULT NULL
,Female_35_44 int DEFAULT NULL
,Female_45_64 int DEFAULT NULL
,Female_65 int DEFAULT NULL
,Weigthed_Male_4_9 double DEFAULT NULL
,Weigthed_Male_10_15 double DEFAULT NULL
,Weigthed_Male_16_19 double DEFAULT NULL
,Weigthed_Male_20_24 double DEFAULT NULL
,Weigthed_Male_25_34 double DEFAULT NULL
,Weigthed_Male_35_44 double DEFAULT NULL
,Weigthed_Male_45_64 double DEFAULT NULL
,Weigthed_Male_65 double DEFAULT NULL
,Weigthed_Female_4_9 double DEFAULT NULL
,Weigthed_Female_10_15 double DEFAULT NULL
,Weigthed_Female_16_19 double DEFAULT NULL
,Weigthed_Female_20_24 double DEFAULT NULL
,Weigthed_Female_25_34 double DEFAULT NULL
,Weigthed_Female_35_44 double DEFAULT NULL
,Weigthed_Female_45_64 double DEFAULT NULL
,Weigthed_Female_65 double DEFAULT NULL
,total_people_viewing int DEFAULT NULL
,weighted_total_people_viewing double DEFAULT NULL
,VOD_Indicator int DEFAULT NULL
,VOD_Provider int DEFAULT NULL
,VOD_Service int DEFAULT NULL
,VOD_Type int DEFAULT NULL
,Device_in_use int DEFAULT NULL
-- ,TV_Instance_Start_Date_Time timestamp
-- ,TV_Instance_End_Date_Time timestamp
--,TV_Event_Start_Date_Time timestamp default NULL
--,TV_Event_End_Date_Time timestamp default NULL
,Household_Weight double DEFAULT NULL
,Service_Key  int DEFAULT NULL
,Channel_Name varchar(100)
,cb_row_id bigint DEFAULT NULL
--,row_id bigint primary key identity
/*
1647
*/

)
GO
-- END

-- index the table
create hg index ind_household_number_BARB_table_output_1 on ripolile.BARB_table_output_1(Household_number)
GO
create lf index ind_person_1_viewing_BARB_table_output_1 on ripolile.BARB_table_output_1(person_1_viewing)
GO
create lf index ind_person_2_viewing_BARB_table_output_1 on ripolile.BARB_table_output_1(person_2_viewing)
GO
create lf index ind_person_3_viewing_BARB_table_output_1 on ripolile.BARB_table_output_1(person_3_viewing)
GO
create lf index ind_person_4_viewing_BARB_table_output_1 on ripolile.BARB_table_output_1(person_4_viewing)
GO
create lf index ind_person_5_viewing_BARB_table_output_1 on ripolile.BARB_table_output_1(person_5_viewing)
GO
create lf index ind_person_6_viewing_BARB_table_output_1 on ripolile.BARB_table_output_1(person_6_viewing)
GO
create lf index ind_person_7_viewing_BARB_table_output_1 on ripolile.BARB_table_output_1(person_7_viewing)
GO
create lf index ind_person_8_viewing_BARB_table_output_1 on ripolile.BARB_table_output_1(person_8_viewing)
GO
create lf index ind_person_9_viewing_BARB_table_output_1 on ripolile.BARB_table_output_1(person_9_viewing)
GO
create lf index ind_person_10_viewing_BARB_table_output_1 on ripolile.BARB_table_output_1(person_10_viewing)
GO
create lf index ind_person_11_viewing_BARB_table_output_1 on ripolile.BARB_table_output_1(person_11_viewing)
GO
create lf index ind_person_12_viewing_BARB_table_output_1 on ripolile.BARB_table_output_1(person_12_viewing)
GO
create lf index ind_person_13_viewing_BARB_table_output_1 on ripolile.BARB_table_output_1(person_13_viewing)
GO
create lf index ind_person_14_viewing_BARB_table_output_1 on ripolile.BARB_table_output_1(person_14_viewing)
GO
create lf index ind_person_15_viewing_BARB_table_output_1 on ripolile.BARB_table_output_1(person_15_viewing)
GO
create lf index ind_person_16_viewing_BARB_table_output_1 on ripolile.BARB_table_output_1(person_16_viewing)
GO


IF OBJECT_ID('ripolile.BARB_table_output_1_ordered') IS not NULL
DROP TABLE ripolile.BARB_table_output_1_ordered

GO

create table ripolile.BARB_table_output_1_ordered
(
filename varchar(100)
,Sky_STB_viewing varchar(1) DEFAULT NULL
,Sky_STB_holder_hh varchar(1) DEFAULT NULL
,PVF_PV2 varchar(4) DEFAULT NULL
,Household_number int DEFAULT NULL
,Barb_date_of_activity_DB1 date
-- ,Actual_date
,Set_number int DEFAULT NULL
,BARB_Start_Time_of_Session  int default NULL
,Local_Start_Time_of_Session timestamp default NULL
,Local_End_Time_of_Session timestamp default NULL
,Panel_or_guest_flag varchar(8)
,Duration_of_Session int DEFAULT NULL -- minutes
,Session_activity_type int DEFAULT NULL
,Playback_type int DEFAULT NULL
,DB1_Station_Code int DEFAULT NULL
,DB1_Station_Name varchar(100)
,Viewing_platform int DEFAULT NULL
,Barb_date_of_recording_DB1 date
-- ,Actual_Date_of_Recording date --- change datatype from Barb
,BARB_Start_time_of_recording int default NULL
,Local_Start_time_of_recording timestamp default NULL
,Local_End_time_of_recording timestamp default NULL
,Person_1_viewing int DEFAULT NULL
,Person_2_viewing int DEFAULT NULL
,Person_3_viewing int DEFAULT NULL
,Person_4_viewing int DEFAULT NULL
,Person_5_viewing int DEFAULT NULL
,Person_6_viewing int DEFAULT NULL
,Person_7_viewing int DEFAULT NULL
,Person_8_viewing int DEFAULT NULL
,Person_9_viewing int DEFAULT NULL
,Person_10_viewing int DEFAULT NULL
,Person_11_viewing int DEFAULT NULL
,Person_12_viewing int DEFAULT NULL
,Person_13_viewing int DEFAULT NULL
,Person_14_viewing int DEFAULT NULL
,Person_15_viewing int DEFAULT NULL
,Person_16_viewing int DEFAULT NULL
,Male_4_9 int DEFAULT NULL
,Male_10_15 int DEFAULT NULL
,Male_16_19 int DEFAULT NULL
,Male_20_24 int DEFAULT NULL
,Male_25_34 int DEFAULT NULL
,Male_35_44 int DEFAULT NULL
,Male_45_64 int DEFAULT NULL
,Male_65 int DEFAULT NULL
,Female_4_9 int DEFAULT NULL
,Female_10_15 int DEFAULT NULL
,Female_16_19 int DEFAULT NULL
,Female_20_24 int DEFAULT NULL
,Female_25_34 int DEFAULT NULL
,Female_35_44 int DEFAULT NULL
,Female_45_64 int DEFAULT NULL
,Female_65 int DEFAULT NULL
,Weigthed_Male_4_9 double DEFAULT NULL
,Weigthed_Male_10_15 double DEFAULT NULL
,Weigthed_Male_16_19 double DEFAULT NULL
,Weigthed_Male_20_24 double DEFAULT NULL
,Weigthed_Male_25_34 double DEFAULT NULL
,Weigthed_Male_35_44 double DEFAULT NULL
,Weigthed_Male_45_64 double DEFAULT NULL
,Weigthed_Male_65 double DEFAULT NULL
,Weigthed_Female_4_9 double DEFAULT NULL
,Weigthed_Female_10_15 double DEFAULT NULL
,Weigthed_Female_16_19 double DEFAULT NULL
,Weigthed_Female_20_24 double DEFAULT NULL
,Weigthed_Female_25_34 double DEFAULT NULL
,Weigthed_Female_35_44 double DEFAULT NULL
,Weigthed_Female_45_64 double DEFAULT NULL
,Weigthed_Female_65 double DEFAULT NULL
,total_people_viewing int DEFAULT NULL
,weighted_total_people_viewing double DEFAULT NULL
,VOD_Indicator int DEFAULT NULL
,VOD_Provider int DEFAULT NULL
,VOD_Service int DEFAULT NULL
,VOD_Type int DEFAULT NULL
,Device_in_use int DEFAULT NULL
-- ,TV_Instance_Start_Date_Time timestamp
-- ,TV_Instance_End_Date_Time timestamp
,TV_Event_Start_Date_Time timestamp default NULL
,TV_Event_End_Date_Time timestamp default NULL
,Household_Weight double DEFAULT NULL
,Service_Key  int DEFAULT NULL
,Channel_Name varchar(100)
,cb_row_id bigint DEFAULT NULL
,row_id bigint primary key identity
/*
1647
*/

)

GO

create hg index ind_household_number_BARB_table_output_1_ordered_tab on ripolile.BARB_table_output_1_ordered(Household_number)
GO
create lf index ind_Panel_or_guest_flag_BARB_table_output_1_ordered_tab on ripolile.BARB_table_output_1_ordered(Panel_or_guest_flag)
GO
create lf index ind_set_number_BARB_table_output_1_ordered_tab on ripolile.BARB_table_output_1_ordered(set_number)
GO
create hg index ind_db1_station_code_BARB_table_output_1_ordered_tab on ripolile.BARB_table_output_1_ordered(db1_station_code)
GO
create lf index ind_session_activity_type_BARB_table_output_1_ordered_tab on ripolile.BARB_table_output_1_ordered(session_activity_type)
GO
create lf index ind_viewing_platform_BARB_table_output_1_ordered_tab on ripolile.BARB_table_output_1_ordered(viewing_platform)
GO
create dttm index ind_Local_Start_Time_of_Session_BARB_table_output_1_ordered_tab on ripolile.BARB_table_output_1_ordered(Local_Start_Time_of_Session)
GO
create dttm index ind_Local_End_Time_of_Session_BARB_table_output_1_ordered_tab on ripolile.BARB_table_output_1_ordered(Local_End_Time_of_Session)
GO
create dttm index ind_Local_Start_Time_of_Recording_BARB_table_output_1_ordered_tab on ripolile.BARB_table_output_1_ordered(Local_Start_Time_of_Recording)
GO
create dttm index ind_Local_End_Time_of_Recording_BARB_table_output_1_ordered_tab on ripolile.BARB_table_output_1_ordered(Local_End_Time_of_Recording)
GO


IF OBJECT_ID('ripolile.BARB_temp_channel_event_start') IS not NULL
DROP TABLE BARB_temp_channel_event_start

GO

create table ripolile.BARB_temp_channel_event_start
(
Household_number int DEFAULT NULL
,Panel_or_guest_flag varchar(8)
,Set_number int DEFAULT NULL
,DB1_Station_Code int DEFAULT NULL
,Session_activity_type int DEFAULT NULL
,Viewing_platform int DEFAULT NULL
,channel_event_start_time timestamp default NULL
)
GO

create hg index ind_hhd_start on ripolile.BARB_temp_channel_event_start(household_number)
GO
create lf index ind_set_start on ripolile.BARB_temp_channel_event_start(set_number)
GO
create lf index ind_db1_start on ripolile.BARB_temp_channel_event_start(db1_station_code)
GO
create lf index ind_act_start on ripolile.BARB_temp_channel_event_start(session_activity_type)
GO
create lf index ind_plat_start on ripolile.BARB_temp_channel_event_start(viewing_platform)
GO
create hg index ind_start_start on ripolile.BARB_temp_channel_event_start(channel_event_start_time)
GO

IF OBJECT_ID('ripolile.BARB_temp_channel_event_end') IS not NULL
DROP TABLE ripolile.BARB_temp_channel_event_end

GO

create table ripolile.BARB_temp_channel_event_end
(
Panel_or_guest_flag varchar(8)
,Household_number int DEFAULT NULL
,Set_number int DEFAULT NULL
,DB1_Station_Code int DEFAULT NULL
,Session_activity_type int DEFAULT NULL
,Viewing_platform int DEFAULT NULL
,channel_event_end_time timestamp default NULL
)
GO
create hg index ind_hhd on ripolile.BARB_temp_channel_event_end(household_number)
GO
create lf index ind_set on ripolile.BARB_temp_channel_event_end(set_number)
GO
create lf index ind_db1 on ripolile.BARB_temp_channel_event_end(db1_station_code)
GO
create lf index ind_act on ripolile.BARB_temp_channel_event_end(session_activity_type)
GO
create lf index ind_plat on ripolile.BARB_temp_channel_event_end(viewing_platform)
GO
create hg index ind_end on ripolile.BARB_temp_channel_event_end(channel_event_end_time)
GO

IF OBJECT_ID('ripolile.BARB_viewing_table') IS NOT NULL
drop table ripolile.BARB_viewing_table
GO
create table ripolile.BARB_viewing_table
(
filename varchar(100)
,Sky_STB_viewing varchar(1) DEFAULT NULL
,Sky_STB_holder_hh varchar(1) DEFAULT NULL
,PVF_PV2 varchar(4) DEFAULT NULL
,Household_number int DEFAULT NULL
,Barb_date_of_activity_DB1 date
-- ,Actual_date
,Set_number int DEFAULT NULL
,Panel_or_guest_flag varchar(8)
,Duration_of_Session int DEFAULT NULL -- minutes
,Session_activity_type int DEFAULT NULL
,Playback_type int DEFAULT NULL
,DB1_Station_Code int DEFAULT NULL
,DB1_Station_Name varchar(100)
,Viewing_platform int DEFAULT NULL
,Barb_date_of_recording_DB1 date
-- ,Actual_Date_of_Recording date --- change datatype from Barb
,BARB_Start_time_of_recording int default NULL
,Local_Start_time_of_recording timestamp default NULL
,Local_End_time_of_recording timestamp default NULL
,UTC_Start_time_of_recording timestamp default NULL
,UTC_End_time_of_recording timestamp default NULL
,Person_1_viewing int DEFAULT NULL
,Person_2_viewing int DEFAULT NULL
,Person_3_viewing int DEFAULT NULL
,Person_4_viewing int DEFAULT NULL
,Person_5_viewing int DEFAULT NULL
,Person_6_viewing int DEFAULT NULL
,Person_7_viewing int DEFAULT NULL
,Person_8_viewing int DEFAULT NULL
,Person_9_viewing int DEFAULT NULL
,Person_10_viewing int DEFAULT NULL
,Person_11_viewing int DEFAULT NULL
,Person_12_viewing int DEFAULT NULL
,Person_13_viewing int DEFAULT NULL
,Person_14_viewing int DEFAULT NULL
,Person_15_viewing int DEFAULT NULL
,Person_16_viewing int DEFAULT NULL
,Male_4_9 int DEFAULT NULL
,Male_10_15 int DEFAULT NULL
,Male_16_19 int DEFAULT NULL
,Male_20_24 int DEFAULT NULL
,Male_25_34 int DEFAULT NULL
,Male_35_44 int DEFAULT NULL
,Male_45_64 int DEFAULT NULL
,Male_65 int DEFAULT NULL
,Female_4_9 int DEFAULT NULL
,Female_10_15 int DEFAULT NULL
,Female_16_19 int DEFAULT NULL
,Female_20_24 int DEFAULT NULL
,Female_25_34 int DEFAULT NULL
,Female_35_44 int DEFAULT NULL
,Female_45_64 int DEFAULT NULL
,Female_65 int DEFAULT NULL
,Weigthed_Male_4_9 double DEFAULT NULL
,Weigthed_Male_10_15 double DEFAULT NULL
,Weigthed_Male_16_19 double DEFAULT NULL
,Weigthed_Male_20_24 double DEFAULT NULL
,Weigthed_Male_25_34 double DEFAULT NULL
,Weigthed_Male_35_44 double DEFAULT NULL
,Weigthed_Male_45_64 double DEFAULT NULL
,Weigthed_Male_65 double DEFAULT NULL
,Weigthed_Female_4_9 double DEFAULT NULL
,Weigthed_Female_10_15 double DEFAULT NULL
,Weigthed_Female_16_19 double DEFAULT NULL
,Weigthed_Female_20_24 double DEFAULT NULL
,Weigthed_Female_25_34 double DEFAULT NULL
,Weigthed_Female_35_44 double DEFAULT NULL
,Weigthed_Female_45_64 double DEFAULT NULL
,Weigthed_Female_65 double DEFAULT NULL
,total_people_viewing int DEFAULT NULL
,weighted_total_people_viewing double DEFAULT NULL
,VOD_Indicator int DEFAULT NULL
,VOD_Provider int DEFAULT NULL
,VOD_Service int DEFAULT NULL
,VOD_Type int DEFAULT NULL
,Device_in_use int DEFAULT NULL
,broadcast_start_date_time_local timestamp default NULL
,broadcast_end_date_time_local timestamp default NULL
,broadcast_start_date_time_UTC timestamp default NULL
,broadcast_end_date_time_UTC timestamp default NULL
,BARB_Start_Time_of_Session  int default NULL
,UTC_Start_Time_of_Session timestamp default NULL
,UTC_End_Time_of_Session timestamp default NULL
,Local_Start_Time_of_Session timestamp default NULL
,Local_End_Time_of_Session timestamp default NULL
,Local_TV_Event_Start_Date_Time timestamp default NULL
,Local_TV_Event_End_Date_Time timestamp default NULL
,Local_TV_Instance_Start_Date_Time timestamp default NULL
,Local_TV_Instance_End_Date_Time timestamp default NULL
,Local_BARB_Instance_Start_Date_Time timestamp default NULL
,Local_BARB_Instance_End_Date_Time timestamp default NULL
,UTC_TV_Event_Start_Date_Time timestamp default NULL
,UTC_TV_Event_End_Date_Time timestamp default NULL
,UTC_TV_Instance_Start_Date_Time timestamp default NULL
,UTC_TV_Instance_End_Date_Time timestamp default NULL
,UTC_BARB_Instance_Start_Date_Time timestamp default NULL
,UTC_BARB_Instance_End_Date_Time timestamp default NULL
,TV_Instance_sequence_id int default NULL
,BARB_Instance_duration int default NULL
,TV_event_duration int default NULL
,TV_instance_duration int default NULL
,Household_Weight double DEFAULT NULL
,Service_Key  int DEFAULT NULL
,Channel_Name varchar(100)
,cb_row_id bigint DEFAULT NULL
--,row_id bigint primary key identity
,programme_name varchar(255) default NULL -- from here, fields from sk_prod.VESPA_PROGRAMME_SCHEDULE
,genre_description varchar(20) default NULL
,sub_genre_description varchar(20) default NULL
,broadcast_daypart varchar(20) default NULL
,episode_number smallint DEFAULT NULL
,episodes_in_series smallint DEFAULT NULL
,three_d_flag int default NULL
,true_hd_flag int default NULL
,wide_screen_flag int default NULL -- to here, fields from sk_prod.VESPA_PROGRAMME_SCHEDULE
,channel_pack varchar(200) default NULL
)

GO

IF OBJECT_ID('ripolile.temp_sequenced_viewing_table') IS not NULL
DROP TABLE ripolile.temp_sequenced_viewing_table
GO
create table ripolile.temp_sequenced_viewing_table
(
Panel_or_guest_flag varchar(8)
,Household_number int DEFAULT NULL
,Set_number int DEFAULT NULL
,DB1_Station_Code int DEFAULT NULL
,Session_activity_type int DEFAULT NULL
,Viewing_platform int DEFAULT NULL
, Local_Start_Time_of_Session timestamp default NULL
,Local_TV_Event_Start_Date_Time timestamp default NULL
, Local_TV_Event_End_Date_Time timestamp default NULL
, Local_TV_Instance_Start_Date_Time timestamp default NULL
, Local_TV_Instance_End_Date_Time timestamp default NULL
,  instance_sequence int DEFAULT NULL
)
GO

IF OBJECT_ID('ripolile.barb_daily_monitoring') IS not NULL
DROP TABLE ripolile.barb_daily_monitoring
GO
CREATE TABLE ripolile.barb_daily_monitoring (
id_row bigint primary key identity  --- so we can check if any viewing records are not matched to the schedule
,date_of_sql_run timestamp default NULL
,date_of_interest date default NULL
,nr_of_PVF_rec_tot int default NULL
,nr_of_PVF_rec_panel_mem int default NULL
,nr_of_PVF_rec_guests int default NULL
,nr_of_PV2_rec_tot int default NULL
,nr_of_PV2_rec_panel_mem int default NULL
,nr_of_PV2_rec_guests int default NULL
,nr_of_PVF_rec_not_matching_VESPA int default NULL
,nr_of_PV2_rec_not_matching_VESPA int default NULL
,nr_of_TOT_rec_not_matching_VESPA int default NULL
,nr_of_indiv_member_details_rec int default NULL
,nr_of_TV_char_details_rec int default NULL
,nr_of_individual_weights_rec int default NULL
,nr_of_records_in_viewing_table int default NULL
,run_comment varchar(150) default NULL
)
GO

create DTTM index ind_date_of_sql_run_monitoring on ripolile.barb_daily_monitoring(date_of_sql_run)
create Date index ind_date_of_interest_monitoring on ripolile.barb_daily_monitoring(date_of_interest)

IF OBJECT_ID('ripolile.barb_daily_ind_prog_viewed') IS not NULL
DROP TABLE ripolile.barb_daily_ind_prog_viewed
GO
create table ripolile.barb_daily_ind_prog_viewed
(
filename varchar(100)
,Sky_STB_viewing varchar(1) DEFAULT NULL
,Sky_STB_holder_hh varchar(1) DEFAULT NULL
,PVF_PV2 varchar(4) DEFAULT NULL
,Household_number int DEFAULT NULL
,Barb_date_of_activity_DB1 date
-- ,Actual_date
,Set_number int DEFAULT NULL
,Panel_or_guest_flag varchar(8)
,Duration_of_Session int DEFAULT NULL -- minutes
,Session_activity_type int DEFAULT NULL
,Playback_type int DEFAULT NULL
,DB1_Station_Code int DEFAULT NULL
,DB1_Station_Name varchar(100)
,Viewing_platform int DEFAULT NULL
,Barb_date_of_recording_DB1 date
-- ,Actual_Date_of_Recording date --- change datatype from Barb
,BARB_Start_time_of_recording int default NULL
,Local_Start_time_of_recording timestamp default NULL
,Local_End_time_of_recording timestamp default NULL
,UTC_Start_time_of_recording timestamp default NULL
,UTC_End_time_of_recording timestamp default NULL
,Person_1_viewing int DEFAULT NULL
,Person_2_viewing int DEFAULT NULL
,Person_3_viewing int DEFAULT NULL
,Person_4_viewing int DEFAULT NULL
,Person_5_viewing int DEFAULT NULL
,Person_6_viewing int DEFAULT NULL
,Person_7_viewing int DEFAULT NULL
,Person_8_viewing int DEFAULT NULL
,Person_9_viewing int DEFAULT NULL
,Person_10_viewing int DEFAULT NULL
,Person_11_viewing int DEFAULT NULL
,Person_12_viewing int DEFAULT NULL
,Person_13_viewing int DEFAULT NULL
,Person_14_viewing int DEFAULT NULL
,Person_15_viewing int DEFAULT NULL
,Person_16_viewing int DEFAULT NULL
,Male_4_9 int DEFAULT NULL
,Male_10_15 int DEFAULT NULL
,Male_16_19 int DEFAULT NULL
,Male_20_24 int DEFAULT NULL
,Male_25_34 int DEFAULT NULL
,Male_35_44 int DEFAULT NULL
,Male_45_64 int DEFAULT NULL
,Male_65 int DEFAULT NULL
,Female_4_9 int DEFAULT NULL
,Female_10_15 int DEFAULT NULL
,Female_16_19 int DEFAULT NULL
,Female_20_24 int DEFAULT NULL
,Female_25_34 int DEFAULT NULL
,Female_35_44 int DEFAULT NULL
,Female_45_64 int DEFAULT NULL
,Female_65 int DEFAULT NULL
,Weigthed_Male_4_9 double DEFAULT NULL
,Weigthed_Male_10_15 double DEFAULT NULL
,Weigthed_Male_16_19 double DEFAULT NULL
,Weigthed_Male_20_24 double DEFAULT NULL
,Weigthed_Male_25_34 double DEFAULT NULL
,Weigthed_Male_35_44 double DEFAULT NULL
,Weigthed_Male_45_64 double DEFAULT NULL
,Weigthed_Male_65 double DEFAULT NULL
,Weigthed_Female_4_9 double DEFAULT NULL
,Weigthed_Female_10_15 double DEFAULT NULL
,Weigthed_Female_16_19 double DEFAULT NULL
,Weigthed_Female_20_24 double DEFAULT NULL
,Weigthed_Female_25_34 double DEFAULT NULL
,Weigthed_Female_35_44 double DEFAULT NULL
,Weigthed_Female_45_64 double DEFAULT NULL
,Weigthed_Female_65 double DEFAULT NULL
,total_people_viewing int DEFAULT NULL
,weighted_total_people_viewing double DEFAULT NULL
,VOD_Indicator int DEFAULT NULL
,VOD_Provider int DEFAULT NULL
,VOD_Service int DEFAULT NULL
,VOD_Type int DEFAULT NULL
,Device_in_use int DEFAULT NULL
,broadcast_start_date_time_local timestamp default NULL
,broadcast_end_date_time_local timestamp default NULL
,broadcast_start_date_time_UTC timestamp default NULL
,broadcast_end_date_time_UTC timestamp default NULL
,BARB_Start_Time_of_Session  int default NULL
,UTC_Start_Time_of_Session timestamp default NULL
,UTC_End_Time_of_Session timestamp default NULL
,Local_Start_Time_of_Session timestamp default NULL
,Local_End_Time_of_Session timestamp default NULL
,Local_TV_Event_Start_Date_Time timestamp default NULL
,Local_TV_Event_End_Date_Time timestamp default NULL
,Local_TV_Instance_Start_Date_Time timestamp default NULL
,Local_TV_Instance_End_Date_Time timestamp default NULL
,Local_BARB_Instance_Start_Date_Time timestamp default NULL
,Local_BARB_Instance_End_Date_Time timestamp default NULL
,UTC_TV_Event_Start_Date_Time timestamp default NULL
,UTC_TV_Event_End_Date_Time timestamp default NULL
,UTC_TV_Instance_Start_Date_Time timestamp default NULL
,UTC_TV_Instance_End_Date_Time timestamp default NULL
,UTC_BARB_Instance_Start_Date_Time timestamp default NULL
,UTC_BARB_Instance_End_Date_Time timestamp default NULL
,TV_Instance_sequence_id int default NULL
,BARB_Instance_duration int default NULL
,TV_event_duration int default NULL
,TV_instance_duration int default NULL
,Household_Weight double DEFAULT NULL
,Service_Key  int DEFAULT NULL
,Channel_Name varchar(100)
,cb_row_id bigint DEFAULT NULL
,row_id bigint primary key identity
,programme_name varchar(255) default NULL -- from here, fields from sk_prod.VESPA_PROGRAMME_SCHEDULE
,genre_description varchar(20) default NULL
,sub_genre_description varchar(20) default NULL
,broadcast_daypart varchar(20) default NULL
,episode_number smallint DEFAULT NULL
,episodes_in_series smallint DEFAULT NULL
,three_d_flag int default NULL
,true_hd_flag int default NULL
,wide_screen_flag int default NULL -- to here, fields from sk_prod.VESPA_PROGRAMME_SCHEDULE
,channel_pack varchar(200) default NULL
)
GO

create hg index ind_household_number_barb_daily_ind_prog_viewed_tab on ripolile.barb_daily_ind_prog_viewed(Household_number)
GO
create lf index ind_Panel_or_guest_flag_barb_daily_ind_prog_viewed_tab on ripolile.barb_daily_ind_prog_viewed(Panel_or_guest_flag)
GO
create lf index ind_set_number_barb_daily_ind_prog_viewed_tab on ripolile.barb_daily_ind_prog_viewed(set_number)
GO
create hg index ind_db1_station_code_barb_daily_ind_prog_viewed_tab on ripolile.barb_daily_ind_prog_viewed(db1_station_code)
GO
create lf index ind_session_activity_type_barb_daily_ind_prog_viewed_tab on ripolile.barb_daily_ind_prog_viewed(session_activity_type)
GO
create lf index ind_viewing_platform_barb_daily_ind_prog_viewed_tab on ripolile.barb_daily_ind_prog_viewed(viewing_platform)
GO
create DTTM index ind_Local_Start_Time_of_Session_barb_daily_ind_prog_viewed_tab on ripolile.barb_daily_ind_prog_viewed(Local_Start_Time_of_Session)
GO
create DTTM index ind_Local_End_Time_of_Session_barb_daily_ind_prog_viewed_tab on ripolile.barb_daily_ind_prog_viewed(Local_End_Time_of_Session)
GO
create Date index ind_Barb_date_of_activity_DB1_ind_prog_viewed_tab on ripolile.barb_daily_ind_prog_viewed(Barb_date_of_activity_DB1)

go
