
-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
-- Project Name: Oneview - BARB DB1
-- Authors: Jason Thompson (jason.thompson@skyiq.co.uk), Leonardo Ripoli (leonardo.ripoli@bskyb.com)
-- Insight Collation: V153
-- Date: 9 December 2014

-- script version 1.1

-- Business Brief:
--      To put raw BARB data coming from daily feeds into a VESPA-like format, data will be used in the OneView project.

-- Code Summary:
--      Transform the raw Barb DB viewing data into a format more useable for analysis. Match the Barb viewing events aganist Vespa programme schedule to understand genre etc

-- Modules:
-- A: Load Barb viewing data from UAT tables and process (see document BARB2010DSP01 Version 3.7C - Panel Viewing File.pdf for details)
--      A1: Create variables relevant to the execution of the script: date of interest
--      A1.1: Check if data for the date of interest is already available in the BARB_viewing_table (data belonging to either PVF or PV2 files or both might already be in place)
--      A2: Load relevant Barb data from UAT tables into temporary tables
--      A3: Create tables to hold processed Barb data - more useful datatypes e.g. timestamp instead of text
--      A4: Process and load Barb data into above tables
--      A5: Combine the PVF and PV2 viewing data into the same tables

-- B: Match Barb viewing data to Vespa EPG and transform for analysis
--      B1: Get age/gender groups for panel member viewing
--      B2: Get Event start/end times
--      B3: Match viewing to Vespa programme schedule
--      B4: Final processing of data (update TV instance fields, calculate TV instance id, calculate BARB_Instance_Start/end_Date_Time)
--      B5: Transfer all data from the temp table to the output viewing table

-- Issues/bugs
--      3. In A4, I have not dealt with the 2 days of the year when clocks change



-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
-- A1:  Create variables relevant to the execution of the script: date, whether it is pv2 or pvf file (see BARB specification)
-- In particular, use the definition: set @date_of_interest=date('2013-09-16') to set the date relevant to the data feed from BARB we want to consider


create or replace variable @date_of_interest date
GO
set @date_of_interest=date('2013-09-16') -- use format date('yyyy-mm-dd')
GO

create or replace variable @date_string varchar(12)
GO
set @date_string=CAST(@date_of_interest as varchar(12))
GO
create or replace variable @datelike_string varchar(20)
GO
set @datelike_string='%_'||substring(@date_string,1,4) || substring(@date_string,6,2) || substring(@date_string,9,2) ||'.dat'
GO
create or replace variable @process_PVF_data bit
GO
set @process_PVF_data=1 -- set to 0 later in the script if we don't need to process PVF
GO
create or replace variable @process_PV2_data bit
GO
set @process_PV2_data=1 -- set to 0 later in the script if we don't need to process PV2
GO
create or replace variable @run_comment varchar(100)
GO
set @run_comment=''
GO
create or replace variable @current_id_row int
GO


insert into ripolile.barb_daily_monitoring(date_of_sql_run, date_of_interest)
values(now(), @date_of_interest)
GO
select @current_id_row=(select max(id_row) from ripolile.barb_daily_monitoring)
GO


-- while 1=1 -- an infinite loop is used to enclose the script so as to be able to terminate the execution with a break statement if errors come out -- not used for now
-- begin


-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
-- A1.1: Check if data for the date of interest is already available in the BARB_viewing_table (data belonging to either PVF or PV2 files or both might already be in place)
-- IF PVF file is already in, then if PV2 files too is in then exit (nothing to do
-- IF PVF file not in and PV2 file data in then raise error

-- check if we already have data for that date

create or replace variable @minimum_PVF_rows_for_a_day int -- variable that will contain the minimum amount of rows we need to say if PVF data for that day is already in the viewing table
GO
create or replace variable @minimum_PV2_rows_for_a_day int -- variable that will contain the minimum amount of rows we need to say if PV2 data for that day is already in the viewing table
GO
set @minimum_PVF_rows_for_a_day=8000
GO
set @minimum_PV2_rows_for_a_day=800
GO
create or replace variable @audit_row_count_PVF bigint
GO
create or replace variable @audit_row_count_PV2 bigint
GO
SET @audit_row_count_PVF = (Select count(1) from ripolile.barb_daily_ind_prog_viewed where filename like @datelike_string and PVF_PV2='PVF')
GO
SET @audit_row_count_PV2 = (Select count(1) from ripolile.barb_daily_ind_prog_viewed where filename like @datelike_string and PVF_PV2='PV2')
GO



if @audit_row_count_PVF > @minimum_PVF_rows_for_a_day
begin

set @process_PVF_data=0 -- no need to process PVF data because it is already in
set @run_comment=@run_comment || '-PVF found:' || CAST(@audit_row_count_PVF as varchar(14))


update ripolile.barb_daily_monitoring
set run_comment = @run_comment
where id_row=@current_id_row


if @audit_row_count_PV2 > @minimum_PV2_rows_for_a_day
begin
set @process_PV2_data=0 -- no need to process PV2 data because it is already in

set @run_comment=@run_comment || '-PV2 found:' || CAST(@audit_row_count_PV2 as varchar(14))
set @run_comment=@run_comment || '-Data already in, No processing'
update ripolile.barb_daily_monitoring
set run_comment = @run_comment
where id_row=@current_id_row

return
end
else -- of if @audit_row_count_PV2 > @minimum_PV2_rows_for_a_day
begin
-- so we have PVF data but no PV2 data: then just process PV2 data
set @process_PVF_data=0
set @process_PV2_data=1

set @run_comment=@run_comment || '-Processing PV2 only'

update ripolile.barb_daily_monitoring
set run_comment = @run_comment
where id_row=@current_id_row


end

end
else -- of if @audit_row_count_PVF > @minimum_PVF_rows_for_a_day
begin
-- if we are here: no PVF file data for that day is already in place
if @audit_row_count_PV2 > @minimum_PV2_rows_for_a_day
begin
set @process_PVF_data=0
set @process_PV2_data=0


set @run_comment=@run_comment || '-PV2 found:' || CAST(@audit_row_count_PV2 as varchar(14))

set @run_comment=@run_comment || '!!!!Error: PV2 in but no PVF in!!!'


update ripolile.barb_daily_monitoring
set run_comment = @run_comment
where id_row=@current_id_row

return

end

end

GO
create or replace variable @nr_of_PVF_members int
GO
create or replace variable @nr_of_PVF_guests int
GO
create or replace variable @nr_of_PV2_members int
GO
create or replace variable @nr_of_PV2_guests int
GO
create or replace variable @nr_of_PVF_rec_not_matching_VESPA int
GO
create or replace variable @nr_of_PV2_rec_not_matching_VESPA int
GO
create or replace variable @dummy int
GO


-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
-- A2: Load relevant Barb data from UAT tables into temporary tables
-- Currently we load data from the following tables:
-- BARB_INDV_PANELMEM_DET: individual members details
-- BARB_PANEL_MEM_RESP_WGHT: weight of each person in panels
-- BARB_PVF_VWREC_PANEL_MEM: PVF files viewing data coming from panel members (see BARB specification)
-- BARB_PVF_VWREC_GUEST: PVF files viewing data coming from guests (see BARB specification)
-- BARB_PV2_VWREC_PANEL_MEM: PV2 files viewing data coming from panel members (see BARB specification)
-- BARB_PV2_VWREC_GUESTS: PV2 files viewing data coming from guests (see BARB specification)


-- BARB_INDV_PANELMEM_DET: individual members details
-- load the information we strictly need for further processing in a temporary table
truncate table ripolile.BARB_PVF04_Individual_Member_Details

GO

insert into ripolile.BARB_PVF04_Individual_Member_Details
select
        cast(cb_source_file as varchar(100)) as filename
        ,record_type
        ,date_of_birth
        ,household_number
        ,person_membership_status
        ,person_number
        ,Sex_code
        ,date_valid_for as Date_valid_for_DB1
        ,marital_status
        ,household_status
        ,working_status
from
        BARB_INDV_PANELMEM_DET
where date_valid_from <= @date_of_interest
and date_valid_to >= @date_of_interest

GO
set @dummy = @@rowcount

update ripolile.barb_daily_monitoring
set nr_of_indiv_member_details_rec = @dummy
where id_row=@current_id_row


-- BARB_PANEL_MEM_RESP_WGHT: weight of each person in panels
-- load the information we strictly need for further processing in a temporary table

GO

truncate table ripolile.BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories
GO
insert into ripolile.BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories
select
        cast(cb_source_file as varchar(100)) as filename
        ,Record_Type
        ,Household_Number
        ,Person_Number
        ,Reporting_Panel_Code
        ,cast(Processing_Weight as double)/10.0 as Processing_Weight
from sk_prod_vespa_restricted.BARB_PANEL_MEM_RESP_WGHT
where filename like @datelike_string
GO


set @dummy = @@rowcount
GO
update ripolile.barb_daily_monitoring
set nr_of_individual_weights_rec=@dummy
where id_row=@current_id_row

GO
truncate table ripolile.BARB_Panel_Demographic_Data_TV_Sets_Characteristics

-- BARB_PANEL_MEM_RESP_WGHT: weight of each person in panels
-- load the information we strictly need for further processing in a temporary table
GO
-- we only need fields household_number, set_number and reception_capability_code1 (for now)
insert into ripolile.BARB_Panel_Demographic_Data_TV_Sets_Characteristics
select household_number
,set_number
,max(reception_capability_code_1) as reception_capability_code1
,max(reception_capability_code_2) as reception_capability_code2
,max(reception_capability_code_3) as reception_capability_code3
,max(reception_capability_code_4) as reception_capability_code4
,max(reception_capability_code_5) as reception_capability_code5
,max(reception_capability_code_6) as reception_capability_code6
,max(reception_capability_code_7) as reception_capability_code7
,max(reception_capability_code_8) as reception_capability_code8
,max(reception_capability_code_9) as reception_capability_code9
,max(reception_capability_code_10) as reception_capability_code10
from BARB_PANEL_DEMOGR_TV_CHAR
where date_valid_from <= @date_of_interest
and date_valid_to >= @date_of_interest
group by household_number, set_number

GO

set @dummy = @@rowcount

update ripolile.barb_daily_monitoring
set nr_of_TV_char_details_rec=@dummy
where id_row=@current_id_row

GO
-- BARB_PVF_VWREC_PANEL_MEM: PVF files viewing data coming from panel members (see BARB specification)
-- load the information we strictly need for further processing in a temporary table


truncate table ripolile.BARB_PVF_Viewing_Record_Panel_Members
GO
if @process_PVF_data = 1
begin
-- only populate table if we need PVF file for that day
insert into ripolile.BARB_PVF_Viewing_Record_Panel_Members
SELECT cb_source_file as filename
,Record_type -- this identifies what table is being refered to see BARB2010DSP01 Version 3.7C - Panel Viewing File.pdf
,Household_number
,Date_of_Activity_DB1 --
,Set_number
,session_start_time as Start_time_of_session
,session_duration as Duration_of_session
,session_activity_type
,playback_type
,DB1_Station_Code
,Viewing_platform
,date_of_recording as Date_of_Recording_DB1
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
,Interactive_barcode_identifier as Interactive_Bar_Code_Identifier
,VOD_Indicator
,VOD_Provider
,VOD_Service
,VOD_Type
,Device_in_use
,cb_row_id
FROM sk_prod_vespa_restricted.BARB_PVF_VWREC_PANEL_MEM
where filename like @datelike_string

end
GO

-- BARB_PVF_VWREC_GUEST: PVF files viewing data coming from guests (see BARB specification)
-- load the information we strictly need for further processing in a temporary table
GO
truncate table ripolile.BARB_PVF_Viewing_Record_Guests
GO
if @process_PVF_data = 1
begin
-- only populate table if we need PVF file for that day
insert into ripolile.BARB_PVF_Viewing_Record_Guests
SELECT cb_source_file as filename
,Record_type
,Household_number
,Date_of_Activity_DB1
,Set_number
,session_start_time as Start_time_of_session
,session_duration as Duration_of_session
,session_activity_type
,Playback_type
,DB1_Station_Code
,Viewing_platform
,date_of_recording as Date_of_Recording_DB1
,Start_time_of_recording
,Male_4_9
,Male_10_15
,Male_16_19
,Male_20_24
,Male_25_34
,Male_35_44
,Male_45_64
,Male_65_plus as Male_65
,Female_4_9
,Female_10_15
,Female_16_19
,Female_20_24
,Female_25_34
,Female_35_44
,Female_45_64
,Female_65_plus as Female_65
,interactive_barcode_identifier as Interactive_Bar_Code_Identifier
,VOD_Indicator
,VOD_Provider
,VOD_Service
,VOD_Type
,Device_in_use
,cb_row_id
--INTO BARB_PVF_Viewing_Record_Guests
FROM BARB_PVF_VWREC_GUEST
where filename like @datelike_string

end

GO
-- BARB_PV2_VWREC_PANEL_MEM: PV2 files viewing data coming from panel members (see BARB specification)
-- load the information we strictly need for further processing in a temporary table
truncate table ripolile.BARB_PV2_Viewing_Record_Panel_Members

GO
if @process_PV2_data = 1
begin

insert into ripolile.BARB_PV2_Viewing_Record_Panel_Members
SELECT
cb_source_file as filename
,Record_type
,Household_number
,Date_of_Activity_DB1
,Set_number
,session_start_time as Start_time_of_session
,session_duration as Duration_of_session
,session_activity_type
,Playback_type
,DB1_Station_Code
,Viewing_platform
,date_of_recording as Date_of_Recording_DB1
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
,interactive_barcode_identifier as Interactive_Bar_Code_Identifier
,VOD_Indicator
,VOD_Provider
,VOD_Service
,VOD_Type
,Device_in_use
,cb_row_id
FROM BARB_PV2_VWREC_PANEL_MEM
where filename like @datelike_string

end
GO
-- BARB_PV2_VWREC_GUESTS: PV2 files viewing data coming from guests (see BARB specification)
-- load the information we strictly need for further processing in a temporary table
truncate table ripolile.BARB_PV2_Viewing_Record_Guests
GO

if @process_PV2_data = 1
begin

insert INTO ripolile.BARB_PV2_Viewing_Record_Guests
SELECT
cb_source_file as filename
,Record_type
,Household_number
,Date_of_Activity_DB1
,Set_number
,session_start_time as Start_time_of_session
,session_duration as Duration_of_session
,session_activity_type
,Playback_type
,DB1_Station_Code
,Viewing_platform
,date_of_recording as Date_of_Recording_DB1
,Start_time_of_recording
,Male_4_9
,Male_10_15
,Male_16_19
,Male_20_24
,Male_25_34
,Male_35_44
,Male_45_64
,Male_65_plus as Male_65
,Female_4_9
,Female_10_15
,Female_16_19
,Female_20_24
,Female_25_34
,Female_35_44
,Female_45_64
,Female_65_plus as Female_65
,interactive_barcode_identifier as Interactive_Bar_Code_Identifier
,VOD_Indicator
,VOD_Provider
,VOD_Service
,VOD_Type
,Device_in_use
,cb_row_id
FROM sk_prod_vespa_restricted.BARB_PV2_VWREC_GUESTS
where filename like @datelike_string

end

GO

-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
-- A3: Create tables to hold processed Barb data - more useful datatypes e.g. timestamp instead of text
-- These tables will be easier to manipulate.
-- Ideally this should be combined with the step above
-- This is left for future development
-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------

-- The following tables are similar to the original tables set up for Barb
-- Most of the work done here is around dates/times to make these easier to use
-- Also tables have better naming conventions consistent with Barb documentation

-- Important notes about Barb data:
-- Reporting day goes from 02:00 to 25:59 for PVF files (i.e. Barb day starts at 2am and finishes at 2am the following day)
-- All Barb times are local i.e. will be in British Summer Time when relevant
-- Need to be careful when clocks go forward/back - THIS HAS NOT BEEN DEALT WITH IN THIS CODE!!!!!
truncate table ripolile.BARB_PVF06_Viewing_Record_Panel_Members
GO


truncate table ripolile.BARB_PVF07_Viewing_Record_Guests

GO
truncate table ripolile.BARB_PV206_Viewing_Record_Panel_Members
GO
truncate table ripolile.BARB_PV207_Viewing_Record_Guests

GO

-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
-- A4: Process and load Barb data into above tables


-- Important notes about Barb data:
-- Reporting day goes from 02:00 to 25:59 for PVF files (i.e. Barb day starts at 2am and finishes at 2am the following day)
-- All Barb times are local i.e. will be in British Summer Time when relevant
-- Need to be careful when clocks go forward/back - THIS HAS NOT BEEN DEALT WITH IN THIS CODE!!!!!


----------------------------------------
-- Insert into BARB_PVF06_Viewing_Record_Panel_Members
----------------------------------------
insert into ripolile.BARB_PVF06_Viewing_Record_Panel_Members
(PVF_PV2,filename
,Record_type, Household_number, Barb_date_of_activity, Actual_date_of_session, Set_number
,Start_time_of_session_text, Start_time_of_session, End_time_of_session, Duration_of_session, Session_activity_type
,Playback_type, DB1_Station_Code, Viewing_platform, Barb_date_of_recording, Actual_Date_of_Recording
,Start_time_of_recording_text, Start_time_of_recording, Person_1_viewing, Person_2_viewing, Person_3_viewing
,Person_4_viewing, Person_5_viewing, Person_6_viewing, Person_7_viewing
,Person_8_viewing, Person_9_viewing, Person_10_viewing, Person_11_viewing
,Person_12_viewing, Person_13_viewing, Person_14_viewing, Person_15_viewing, Person_16_viewing
,Interactive_Bar_Code_Identifier, VOD_Indicator, VOD_Provider, VOD_Service
,VOD_Type, Device_in_use, cb_row_id)
select
        'PVF',filename
        ,Record_type
        ,Household_number
        -- Keep the original Date_of_Activity_DB1 as the Barb_date_of_activity
        ,Date_of_Activity_DB1
        -- Actual_date_of_session: A barb day can go over 24:00. In this case we need to increase the date by 1
        ,dateadd(dd, case when Start_time_of_session >= 2400 then 1 else 0 end,
                Date_of_Activity_DB1
                )

        ,Set_number
        ,cast(Start_time_of_session as varchar(6)) -- Start_time_of_session_text
        ,datetime('1900-01-01 00:00:00') -- Start_time_of_session. Will update this later in an update query. Easier that way
        ,datetime('1900-01-01 00:00:00') -- End_time_of_session. Will update this later in an update query. Easier that way
        ,Duration_of_session
        ,Session_activity_type
        ,Playback_type
        ,DB1_Station_Code
        ,Viewing_platform
         -- Keep the original Date_of_Recording_DB1 as the Barb_date_of_recording
        ,Date_of_Recording_DB1
        -- Date_of_Recording_DB1: A barb day can go over 24:00. In this case we need to increase the date by 1
        ,dateadd(dd, case when Start_time_of_recording >= 2400 then 1 else 0 end,
                Date_of_Recording_DB1
                )
        ,cast(Start_time_of_recording as varchar(6)) -- Start_time_of_recording_text
        ,datetime('1900-01-01 00:00:00') -- Start_time_of_recording. Will update this later in an update query. Easier that way
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
        ,Interactive_Bar_Code_Identifier
        ,VOD_Indicator
        ,VOD_Provider
        ,VOD_Service
        ,VOD_Type
        ,Device_in_use
        ,cb_row_id
from
        BARB_PVF_Viewing_Record_Panel_Members

GO

--- Update the Start and end session and recording timestamps. Its easier to deal with the barb time conversion as an update rather then as in the insert statement
update ripolile.BARB_PVF06_Viewing_Record_Panel_Members
        set Start_time_of_session = datetime(year(Actual_date_of_session) || '-' || month(Actual_date_of_session) || '-' || day(Actual_date_of_session) || ' '
                                                || case when cast(Start_time_of_session_text as int) >= 2400 then
                                                        -- As start time >= 24:00 then take off 24hours to convert barb time to GMT time. Actual_date_of_session has already been converted
                                                        case when len(Start_time_of_session_text) = 3 then
                                                                -- As only 3 chars then must be missing leading zero
                                                                cast(substring(Start_time_of_session_text, 1,1) as int) - 24 || ':' || substring(Start_time_of_session_text, 2,2) || ':00'
                                                        else
                                                                cast(substring(Start_time_of_session_text, 1,2) as int) - 24 || ':' || substring(Start_time_of_session_text, 3,2) || ':00'
                                                        end
                                                   else
                                                        -- Start time < 24:00 then barb time OK
                                                        case when len(Start_time_of_session_text) = 3 then
                                                                -- As only 3 chars then must be missing leading zero
                                                                substring(Start_time_of_session_text, 1,1) || ':' || substring(Start_time_of_session_text, 2,2) || ':00'
                                                        else
                                                                substring(Start_time_of_session_text, 1,2) || ':' || substring(Start_time_of_session_text, 3,2) || ':00'
                                                        end
                                                   end
                                                )

-- commit
--GO

GO

update ripolile.BARB_PVF06_Viewing_Record_Panel_Members
        set End_time_of_session = dateadd(mi, Duration_of_session-1, Start_time_of_session)-- the -1 is for BARB policy that the same minute cannot be assigned to different events, so if ev start 19:40 and viewed for 21 minutes, end will be at 20:00, and following event will start at 20:01

GO

update ripolile.BARB_PVF06_Viewing_Record_Panel_Members
        set Start_time_of_recording = datetime(year(Actual_Date_of_Recording) || '-' || month(Actual_Date_of_Recording) || '-' || day(Actual_Date_of_Recording) || ' '
                                                || case when cast(Start_time_of_recording_text as int) >= 2400 then
                                                        -- As start time >= 24:00 then take off 24hours to convert barb time to GMT time. Actual_Date_of_Recording has already been converted
                                                        case when len(Start_time_of_recording_text) = 3 then
                                                                -- As only 3 chars then must be missing leading zero
                                                                cast(substring(Start_time_of_recording_text, 1,1) as int) - 24 || ':' || substring(Start_time_of_recording_text, 2,2) || ':00'
                                                        else
                                                                cast(substring(Start_time_of_recording_text, 1,2) as int) - 24 || ':' || substring(Start_time_of_recording_text, 3,2) || ':00'
                                                        end
                                                   else
                                                        -- Start time < 24:00 then barb time OK
                                                        case when len(Start_time_of_recording_text) = 3 then
                                                                -- As only 3 chars then must be missing leading zero
                                                                substring(Start_time_of_recording_text, 1,1) || ':' || substring(Start_time_of_recording_text, 2,2) || ':00'
                                                        else
                                                                substring(Start_time_of_recording_text, 1,2) || ':' || substring(Start_time_of_recording_text, 3,2) || ':00'
                                                        end
                                                   end
                                                )

GO
----------------------------------------
-- Insert into BARB_PVF07_Viewing_Record_Guests
----------------------------------------
insert into ripolile.BARB_PVF07_Viewing_Record_Guests
(PVF_PV2,filename
,Record_type, Household_number, Barb_date_of_activity, Actual_date_of_session, Set_number
,Start_time_of_session_text, Start_time_of_session, End_time_of_session, Duration_of_session, Session_activity_type
,Playback_type, DB1_Station_Code, Viewing_platform, Barb_date_of_recording, Actual_Date_of_Recording
,Start_time_of_recording_text, Start_time_of_recording
,Male_4_9, Male_10_15, Male_16_19, Male_20_24, Male_25_34, Male_35_44, Male_45_64, Male_65
,Female_4_9, Female_10_15, Female_16_19, Female_20_24, Female_25_34, Female_35_44, Female_45_64, Female_65
,Interactive_Bar_Code_Identifier, VOD_Indicator, VOD_Provider, VOD_Service
,VOD_Type, Device_in_use,cb_row_id)
select
        'PVF',filename
        ,Record_type
        ,Household_number
        -- Keep the original Date_of_Activity_DB1 as the Barb_date_of_activity
        ,Date_of_Activity_DB1
        -- Actual_date_of_session: A barb day can go over 24:00. In this case we need to increase the date by 1
        ,dateadd(dd, case when Start_time_of_session >= 2400 then 1 else 0 end,
                Date_of_Activity_DB1
                )
        ,Set_number
        ,cast(Start_time_of_session as varchar(6)) -- Start_time_of_session_text
        ,datetime('1900-01-01 00:00:00') -- Start_time_of_session. Will update this later in an update query. Easier that way
        ,datetime('1900-01-01 00:00:00') -- End_time_of_session. Will update this later in an update query. Easier that way
        ,Duration_of_session
        ,Session_activity_type
        ,Playback_type
        ,DB1_Station_Code
        ,Viewing_platform
         -- Keep the original Date_of_Recording_DB1 as the Barb_date_of_recording
        ,Date_of_Recording_DB1
        -- Date_of_Recording_DB1: A barb day can go over 24:00. In this case we need to increase the date by 1
        ,dateadd(dd, case when Start_time_of_recording >= 2400 then 1 else 0 end,
                Date_of_Recording_DB1
                )
        ,cast(Start_time_of_recording as varchar(6)) -- Start_time_of_recording_text
        ,datetime('1900-01-01 00:00:00') -- Start_time_of_recording. Will update this later in an update query. Easier that way
        ,Male_4_9, Male_10_15, Male_16_19, Male_20_24, Male_25_34, Male_35_44, Male_45_64, Male_65
        ,Female_4_9, Female_10_15, Female_16_19, Female_20_24, Female_25_34, Female_35_44, Female_45_64, Female_65
        ,Interactive_Bar_Code_Identifier
        ,VOD_Indicator
        ,VOD_Provider
        ,VOD_Service
        ,VOD_Type
        ,Device_in_use
        ,cb_row_id
from
        ripolile.BARB_PVF_Viewing_Record_Guests

GO


--- Update the Start and end session and recording timestamps. Its easier to deal with the barb time conversion as an update rather then as in the insert statement
update ripolile.BARB_PVF07_Viewing_Record_Guests
        set Start_time_of_session = datetime(year(Actual_date_of_session) || '-' || month(Actual_date_of_session) || '-' || day(Actual_date_of_session) || ' '
                                                || case when cast(Start_time_of_session_text as int) >= 2400 then
                                                        -- As start time >= 24:00 then take off 24hours to convert barb time to GMT time. Actual_date_of_session has already been converted
                                                        case when len(Start_time_of_session_text) = 3 then
                                                                -- As only 3 chars then must be missing leading zero
                                                                cast(substring(Start_time_of_session_text, 1,1) as int) - 24 || ':' || substring(Start_time_of_session_text, 2,2) || ':00'
                                                        else
                                                                cast(substring(Start_time_of_session_text, 1,2) as int) - 24 || ':' || substring(Start_time_of_session_text, 3,2) || ':00'
                                                        end
                                                   else
                                                        -- Start time < 24:00 then barb time OK
                                                        case when len(Start_time_of_session_text) = 3 then
                                                                -- As only 3 chars then must be missing leading zero
                                                                substring(Start_time_of_session_text, 1,1) || ':' || substring(Start_time_of_session_text, 2,2) || ':00'
                                                        else
                                                                substring(Start_time_of_session_text, 1,2) || ':' || substring(Start_time_of_session_text, 3,2) || ':00'
                                                        end
                                                   end
                                                )


GO

update ripolile.BARB_PVF07_Viewing_Record_Guests
        set End_time_of_session = dateadd(mi, Duration_of_session-1, Start_time_of_session)-- the -1 is for BARB policy that the same minute cannot be assigned to different events, so if ev start 19:40 and viewed for 21 minutes, end will be at 20:00, and following event will start at 20:01



GO

update ripolile.BARB_PVF07_Viewing_Record_Guests
        set Start_time_of_recording = datetime(year(Actual_Date_of_Recording) || '-' || month(Actual_Date_of_Recording) || '-' || day(Actual_Date_of_Recording) || ' '
                                                || case when cast(Start_time_of_recording_text as int) >= 2400 then
                                                        -- As start time >= 24:00 then take off 24hours to convert barb time to GMT time. Actual_Date_of_Recording has already been converted
                                                        case when len(Start_time_of_recording_text) = 3 then
                                                                -- As only 3 chars then must be missing leading zero
                                                                cast(substring(Start_time_of_recording_text, 1,1) as int) - 24 || ':' || substring(Start_time_of_recording_text, 2,2) || ':00'
                                                        else
                                                                cast(substring(Start_time_of_recording_text, 1,2) as int) - 24 || ':' || substring(Start_time_of_recording_text, 3,2) || ':00'
                                                        end
                                                   else
                                                        -- Start time < 24:00 then barb time OK
                                                        case when len(Start_time_of_recording_text) = 3 then
                                                                -- As only 3 chars then must be missing leading zero
                                                                substring(Start_time_of_recording_text, 1,1) || ':' || substring(Start_time_of_recording_text, 2,2) || ':00'
                                                        else
                                                                substring(Start_time_of_recording_text, 1,2) || ':' || substring(Start_time_of_recording_text, 3,2) || ':00'
                                                        end
                                                   end
                                                )

GO

----------------------------------------
-- Insert data into BARB_PV206_Viewing_Record_Panel_Members
----------------------------------------
insert into ripolile.BARB_PV206_Viewing_Record_Panel_Members
(PVF_PV2,filename
,Record_type, Household_number, Barb_date_of_activity, Actual_date_of_session, Set_number
,Start_time_of_session_text, Start_time_of_session, End_time_of_session, Duration_of_session, Session_activity_type
,Playback_type, DB1_Station_Code, Viewing_platform, Barb_date_of_recording, Actual_Date_of_Recording
,Start_time_of_recording_text, Start_time_of_recording, Person_1_viewing, Person_2_viewing, Person_3_viewing
,Person_4_viewing, Person_5_viewing, Person_6_viewing, Person_7_viewing
,Person_8_viewing, Person_9_viewing, Person_10_viewing, Person_11_viewing
,Person_12_viewing, Person_13_viewing, Person_14_viewing, Person_15_viewing, Person_16_viewing
,Interactive_Bar_Code_Identifier, VOD_Indicator, VOD_Provider, VOD_Service
,VOD_Type, Device_in_use,cb_row_id)
select
        'PV2',filename
        ,Record_type
        ,Household_number
        -- Keep the original Date_of_Activity_DB1 as the Barb_date_of_activity
        ,Date_of_Activity_DB1
        -- Actual_date_of_session: A barb day can go over 24:00. In this case we need to increase the date by 1
        ,Date_of_Activity_DB1
        ,Set_number
        ,cast(Start_time_of_session as varchar(6)) -- Start_time_of_session_text
        ,datetime('1900-01-01 00:00:00') -- Start_time_of_session. Will update this later in an update query. Easier that way
        ,datetime('1900-01-01 00:00:00') -- End_time_of_session. Will update this later in an update query. Easier that way
        ,Duration_of_session
        ,Session_activity_type
        ,Playback_type
        ,DB1_Station_Code
        ,Viewing_platform
         -- Keep the original Date_of_Recording_DB1 as the Barb_date_of_recording
        ,Date_of_Recording_DB1
        -- Date_of_Recording_DB1: A barb day can go over 24:00. In this case we need to increase the date by 1
        ,dateadd(dd, case when Start_time_of_recording >= 2400 then 1 else 0 end,
                Date_of_Recording_DB1
                )
        ,cast(Start_time_of_recording as varchar(6)) -- Start_time_of_recording_text
        ,datetime('1900-01-01 00:00:00') -- Start_time_of_recording. Will update this later in an update query. Easier that way
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
        ,Interactive_Bar_Code_Identifier
        ,VOD_Indicator
        ,VOD_Provider
        ,VOD_Service
        ,VOD_Type
        ,Device_in_use
        ,cb_row_id
from
        ripolile.BARB_PV2_Viewing_Record_Panel_Members

GO

--- Update the Start and end session and recording timestamps. Its easier to deal with the barb time conversion as an update rather then as in the insert statement
update ripolile.BARB_PV206_Viewing_Record_Panel_Members
        set Start_time_of_session = datetime(year(Actual_date_of_session) || '-' || month(Actual_date_of_session) || '-' || day(Actual_date_of_session) || ' '
                                                || case when cast(Start_time_of_session_text as int) >= 2400 then
                                                        -- As start time >= 24:00 then take off 24hours to convert barb time to GMT time. Actual_date_of_session has already been converted
                                                        case when len(Start_time_of_session_text) = 3 then
                                                                -- As only 3 chars then must be missing leading zero
                                                                cast(substring(Start_time_of_session_text, 1,1) as int) - 24 || ':' || substring(Start_time_of_session_text, 2,2) || ':00'
                                                        else
                                                                cast(substring(Start_time_of_session_text, 1,2) as int) - 24 || ':' || substring(Start_time_of_session_text, 3,2) || ':00'
                                                        end
                                                   else
                                                        -- Start time < 24:00 then barb time OK
                                                        case when len(Start_time_of_session_text) = 3 then
                                                                -- As only 3 chars then must be missing leading zero
                                                                substring(Start_time_of_session_text, 1,1) || ':' || substring(Start_time_of_session_text, 2,2) || ':00'
                                                        else
                                                                substring(Start_time_of_session_text, 1,2) || ':' || substring(Start_time_of_session_text, 3,2) || ':00'
                                                        end
                                                   end
                                                )


GO

update ripolile.BARB_PV206_Viewing_Record_Panel_Members
        set End_time_of_session = dateadd(mi, Duration_of_session-1, Start_time_of_session)-- the -1 is for BARB policy that the same minute cannot be assigned to different events, so if ev start 19:40 and viewed for 21 minutes, end will be at 20:00, and following event will start at 20:01


GO

update ripolile.BARB_PV206_Viewing_Record_Panel_Members
        set Start_time_of_recording = datetime(year(Actual_Date_of_Recording) || '-' || month(Actual_Date_of_Recording) || '-' || day(Actual_Date_of_Recording) || ' '
                                                || case when cast(Start_time_of_recording_text as int) >= 2400 then
                                                        -- As start time >= 24:00 then take off 24hours to convert barb time to GMT time. Actual_Date_of_Recording has already been converted
                                                        case when len(Start_time_of_recording_text) = 3 then
                                                                -- As only 3 chars then must be missing leading zero
                                                                cast(substring(Start_time_of_recording_text, 1,1) as int) - 24 || ':' || substring(Start_time_of_recording_text, 2,2) || ':00'
                                                        else
                                                                cast(substring(Start_time_of_recording_text, 1,2) as int) - 24 || ':' || substring(Start_time_of_recording_text, 3,2) || ':00'
                                                        end
                                                   else
                                                        -- Start time < 24:00 then barb time OK
                                                        case when len(Start_time_of_recording_text) = 3 then
                                                                -- As only 3 chars then must be missing leading zero
                                                                substring(Start_time_of_recording_text, 1,1) || ':' || substring(Start_time_of_recording_text, 2,2) || ':00'
                                                        else
                                                                substring(Start_time_of_recording_text, 1,2) || ':' || substring(Start_time_of_recording_text, 3,2) || ':00'
                                                        end
                                                   end
                                                )


GO
----------------------------------------
-- Insert into BARB_PV207_Viewing_Record_Guests
----------------------------------------
insert into ripolile.BARB_PV207_Viewing_Record_Guests
(PVF_PV2,filename
,Record_type, Household_number, Barb_date_of_activity, Actual_date_of_session, Set_number
,Start_time_of_session_text, Start_time_of_session, End_time_of_session, Duration_of_session, Session_activity_type
,Playback_type, DB1_Station_Code, Viewing_platform, Barb_date_of_recording, Actual_Date_of_Recording
,Start_time_of_recording_text, Start_time_of_recording
,Male_4_9, Male_10_15, Male_16_19, Male_20_24, Male_25_34, Male_35_44, Male_45_64, Male_65
,Female_4_9, Female_10_15, Female_16_19, Female_20_24, Female_25_34, Female_35_44, Female_45_64, Female_65
,Interactive_Bar_Code_Identifier, VOD_Indicator, VOD_Provider, VOD_Service
,VOD_Type, Device_in_use,cb_row_id)
select
        'PV2',filename
        ,Record_type
        ,Household_number
        -- Keep the original Date_of_Activity_DB1 as the Barb_date_of_activity
        ,Date_of_Activity_DB1
        -- Actual_date_of_session: A barb day can go over 24:00. In this case we need to increase the date by 1
        ,Date_of_Activity_DB1
        ,Set_number
        ,cast(Start_time_of_session as varchar(6)) -- Start_time_of_session_text
        ,datetime('1900-01-01 00:00:00') -- Start_time_of_session. Will update this later in an update query. Easier that way
        ,datetime('1900-01-01 00:00:00') -- End_time_of_session. Will update this later in an update query. Easier that way
        ,Duration_of_session
        ,Session_activity_type
        ,Playback_type
        ,DB1_Station_Code
        ,Viewing_platform
         -- Keep the original Date_of_Recording_DB1 as the Barb_date_of_recording
        ,Date_of_Recording_DB1
        -- Date_of_Recording_DB1: A barb day can go over 24:00. In this case we need to increase the date by 1
        ,dateadd(dd, case when Start_time_of_recording >= 2400 then 1 else 0 end,
                Date_of_Recording_DB1
                )
        ,cast(Start_time_of_recording as varchar(6)) -- Start_time_of_recording_text
        ,datetime('1900-01-01 00:00:00') -- Start_time_of_recording. Will update this later in an update query. Easier that way
        ,Male_4_9, Male_10_15, Male_16_19, Male_20_24, Male_25_34, Male_35_44, Male_45_64, Male_65
        ,Female_4_9, Female_10_15, Female_16_19, Female_20_24, Female_25_34, Female_35_44, Female_45_64, Female_65
        ,Interactive_Bar_Code_Identifier
        ,VOD_Indicator
        ,VOD_Provider
        ,VOD_Service
        ,VOD_Type
        ,Device_in_use
        ,cb_row_id
from
        ripolile.BARB_PV2_Viewing_Record_Guests

GO


--- Update the Start and end session and recording timestamps. Its easier to deal with the barb time conversion as an update rather then as in the insert statement
update ripolile.BARB_PV207_Viewing_Record_Guests
        set Start_time_of_session = datetime(year(Actual_date_of_session) || '-' || month(Actual_date_of_session) || '-' || day(Actual_date_of_session) || ' '
                                                || case when cast(Start_time_of_session_text as int) >= 2400 then
                                                        -- As start time >= 24:00 then take off 24hours to convert barb time to GMT time. Actual_date_of_session has already been converted
                                                        case when len(Start_time_of_session_text) = 3 then
                                                                -- As only 3 chars then must be missing leading zero
                                                                cast(substring(Start_time_of_session_text, 1,1) as int) - 24 || ':' || substring(Start_time_of_session_text, 2,2) || ':00'
                                                        else
                                                                cast(substring(Start_time_of_session_text, 1,2) as int) - 24 || ':' || substring(Start_time_of_session_text, 3,2) || ':00'
                                                        end
                                                   else
                                                        -- Start time < 24:00 then barb time OK
                                                        case when len(Start_time_of_session_text) = 3 then
                                                                -- As only 3 chars then must be missing leading zero
                                                                substring(Start_time_of_session_text, 1,1) || ':' || substring(Start_time_of_session_text, 2,2) || ':00'
                                                        else
                                                                substring(Start_time_of_session_text, 1,2) || ':' || substring(Start_time_of_session_text, 3,2) || ':00'
                                                        end
                                                   end
                                                )


GO

update ripolile.BARB_PV207_Viewing_Record_Guests
        set End_time_of_session = dateadd(mi, Duration_of_session-1, Start_time_of_session)-- the -1 is for BARB policy that the same minute cannot be assigned to different events, so if ev start 19:40 and viewed for 21 minutes, end will be at 20:00, and following event will start at 20:01


GO

update ripolile.BARB_PV207_Viewing_Record_Guests
        set Start_time_of_recording = datetime(year(Actual_Date_of_Recording) || '-' || month(Actual_Date_of_Recording) || '-' || day(Actual_Date_of_Recording) || ' '
                                                || case when cast(Start_time_of_recording_text as int) >= 2400 then
                                                        -- As start time >= 24:00 then take off 24hours to convert barb time to GMT time. Actual_Date_of_Recording has already been converted
                                                        case when len(Start_time_of_recording_text) = 3 then
                                                                -- As only 3 chars then must be missing leading zero
                                                                cast(substring(Start_time_of_recording_text, 1,1) as int) - 24 || ':' || substring(Start_time_of_recording_text, 2,2) || ':00'
                                                        else
                                                                cast(substring(Start_time_of_recording_text, 1,2) as int) - 24 || ':' || substring(Start_time_of_recording_text, 3,2) || ':00'
                                                        end
                                                   else
                                                        -- Start time < 24:00 then barb time OK
                                                        case when len(Start_time_of_recording_text) = 3 then
                                                                -- As only 3 chars then must be missing leading zero
                                                                substring(Start_time_of_recording_text, 1,1) || ':' || substring(Start_time_of_recording_text, 2,2) || ':00'
                                                        else
                                                                substring(Start_time_of_recording_text, 1,2) || ':' || substring(Start_time_of_recording_text, 3,2) || ':00'
                                                        end
                                                   end
                                                )

----------------------------------------
-- now check if the viewing has been made through a Sky STB (reception_capability_codex=2 from BARB specification), and update the field Sky_STB_viewing accordingly
----------------------------------------


update ripolile.BARB_PVF06_Viewing_Record_Panel_Members
set Sky_STB_viewing=
(case when TV_char.reception_capability_code1=2
or TV_char.reception_capability_code2=2
or TV_char.reception_capability_code3=2
or TV_char.reception_capability_code4=2
or TV_char.reception_capability_code5=2
or TV_char.reception_capability_code6=2
or TV_char.reception_capability_code7=2
or TV_char.reception_capability_code8=2
or TV_char.reception_capability_code9=2
or TV_char.reception_capability_code10=2
then 'Y' else 'N' end)
from
ripolile.BARB_PVF06_Viewing_Record_Panel_Members view_t
left join
ripolile.BARB_Panel_Demographic_Data_TV_Sets_Characteristics as TV_char
on view_t.household_number=TV_char.household_number
and view_t.set_number=TV_char.set_number

GO

update ripolile.BARB_PVF07_Viewing_Record_Guests
set Sky_STB_viewing=
(case when TV_char.reception_capability_code1=2
or TV_char.reception_capability_code2=2
or TV_char.reception_capability_code3=2
or TV_char.reception_capability_code4=2
or TV_char.reception_capability_code5=2
or TV_char.reception_capability_code6=2
or TV_char.reception_capability_code7=2
or TV_char.reception_capability_code8=2
or TV_char.reception_capability_code9=2
or TV_char.reception_capability_code10=2
then 'Y' else 'N' end)
from
ripolile.BARB_PVF07_Viewing_Record_Guests view_t
left join
ripolile.BARB_Panel_Demographic_Data_TV_Sets_Characteristics as TV_char
on view_t.household_number=TV_char.household_number
and view_t.set_number=TV_char.set_number

GO

update ripolile.BARB_PV206_Viewing_Record_Panel_Members
set Sky_STB_viewing=
(case when TV_char.reception_capability_code1=2
or TV_char.reception_capability_code2=2
or TV_char.reception_capability_code3=2
or TV_char.reception_capability_code4=2
or TV_char.reception_capability_code5=2
or TV_char.reception_capability_code6=2
or TV_char.reception_capability_code7=2
or TV_char.reception_capability_code8=2
or TV_char.reception_capability_code9=2
or TV_char.reception_capability_code10=2
then 'Y' else 'N' end)
from
ripolile.BARB_PV206_Viewing_Record_Panel_Members view_t
left join
ripolile.BARB_Panel_Demographic_Data_TV_Sets_Characteristics as TV_char
on view_t.household_number=TV_char.household_number
and view_t.set_number=TV_char.set_number

GO

update ripolile.BARB_PV207_Viewing_Record_Guests
set Sky_STB_viewing=
(case when TV_char.reception_capability_code1=2
or TV_char.reception_capability_code2=2
or TV_char.reception_capability_code3=2
or TV_char.reception_capability_code4=2
or TV_char.reception_capability_code5=2
or TV_char.reception_capability_code6=2
or TV_char.reception_capability_code7=2
or TV_char.reception_capability_code8=2
or TV_char.reception_capability_code9=2
or TV_char.reception_capability_code10=2
then 'Y' else 'N' end)
from
ripolile.BARB_PV207_Viewing_Record_Guests view_t
left join
ripolile.BARB_Panel_Demographic_Data_TV_Sets_Characteristics as TV_char
on view_t.household_number=TV_char.household_number
and view_t.set_number=TV_char.set_number

GO



-- check if in the household there is at least one set with Sky STB and update the field Sky_STB_holder_hh accordingly
GO
truncate TABLE ripolile.Sky_STB_holder_hh_tmp_table
GO

insert into ripolile.Sky_STB_holder_hh_tmp_table
select distinct household_number
from ripolile.BARB_Panel_Demographic_Data_TV_Sets_Characteristics
where reception_capability_code1=2
or reception_capability_code2=2
or reception_capability_code3=2
or reception_capability_code4=2
or reception_capability_code5=2
or reception_capability_code6=2
or reception_capability_code7=2
or reception_capability_code8=2
or reception_capability_code9=2
or reception_capability_code10=2
GO

update ripolile.BARB_PVF06_Viewing_Record_Panel_Members
set Sky_STB_holder_hh=(case when TV_char.household_number is NULL then 'N' else 'Y' end)
from
ripolile.BARB_PVF06_Viewing_Record_Panel_Members view_t
left join
ripolile.Sky_STB_holder_hh_tmp_table as TV_char
on view_t.household_number=TV_char.household_number

GO

update ripolile.BARB_PVF07_Viewing_Record_Guests
set Sky_STB_holder_hh=(case when TV_char.household_number is NULL then 'N' else 'Y' end)
from
ripolile.BARB_PVF07_Viewing_Record_Guests view_t
left join
ripolile.Sky_STB_holder_hh_tmp_table as TV_char
on view_t.household_number=TV_char.household_number

GO

update ripolile.BARB_PV206_Viewing_Record_Panel_Members
set Sky_STB_holder_hh=(case when TV_char.household_number is NULL then 'N' else 'Y' end)
from
ripolile.BARB_PV206_Viewing_Record_Panel_Members view_t
left join
ripolile.Sky_STB_holder_hh_tmp_table as TV_char
on view_t.household_number=TV_char.household_number

GO

update ripolile.BARB_PV207_Viewing_Record_Guests
set Sky_STB_holder_hh=(case when TV_char.household_number is NULL then 'N' else 'Y' end)
from
ripolile.BARB_PV207_Viewing_Record_Guests view_t
left join
ripolile.Sky_STB_holder_hh_tmp_table as TV_char
on view_t.household_number=TV_char.household_number

GO

-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
--      A5: Combine the PVF and PV2 viewing data into the same tables
-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------


GO

-- Combine Panel viewing
truncate TABLE ripolile.BARB_PVF06_PV206_Viewing_Record_Panel_Members

GO

insert into ripolile.BARB_PVF06_PV206_Viewing_Record_Panel_Members
select * from ripolile.BARB_PVF06_Viewing_Record_Panel_Members
GO

set @nr_of_PVF_members = @@rowcount
GO

insert into ripolile.BARB_PVF06_PV206_Viewing_Record_Panel_Members
select * from ripolile.BARB_PV206_Viewing_Record_Panel_Members

GO
set @nr_of_PV2_members  = @@rowcount
GO

-- Combine Guest viewing
truncate TABLE ripolile.BARB_PVF07_PV207_Viewing_Record_Guests

GO

insert into ripolile.BARB_PVF07_PV207_Viewing_Record_Guests
select * from ripolile.BARB_PVF07_Viewing_Record_Guests
GO
set @nr_of_PVF_guests = @@rowcount

GO


insert into ripolile.BARB_PVF07_PV207_Viewing_Record_Guests
select * from ripolile.BARB_PV207_Viewing_Record_Guests

GO
set @nr_of_PV2_guests = @@rowcount
GO


update ripolile.barb_daily_monitoring
set nr_of_PVF_rec_tot=@nr_of_PVF_members+@nr_of_PVF_guests
,nr_of_PVF_rec_panel_mem=@nr_of_PVF_members
,nr_of_PVF_rec_guests=@nr_of_PVF_guests
,nr_of_PV2_rec_tot=@nr_of_PV2_members+@nr_of_PV2_guests
,nr_of_PV2_rec_panel_mem=@nr_of_PV2_members
,nr_of_PV2_rec_guests=@nr_of_PV2_guests
where id_row=@current_id_row

GO

-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
-- B1: Get age/gender groups for panel member viewing
-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------

-- There are up to 16 people that can be on the Barb panel in a single household
-- The viewing data has a 0/1 column for each individual - so we know which individuals were watching each viewing session
-- Code goes through each person in turn to find age/gender group and append sessions where they are viewing
-- The final step is then to summarie over all these sessions to get number of people in each age/gender group for each session


-- Barb viewing session:
-- Defined as being when any of the following change (in a household on a given tv): channel, activity, platform and individuals


-- BARB_table_output_1 is the temporary table that will store combined panel/guest
-- and information on age

truncate TABLE ripolile.BARB_table_output_1

GO

-- insert guest viewing info first (age/gender info already available)
insert into ripolile.BARB_table_output_1(PVF_PV2,Sky_STB_viewing,Sky_STB_holder_hh,Household_number,filename,Viewing_platform,Barb_date_of_recording_DB1,BARB_Start_time_of_recording,Local_Start_time_of_recording, Barb_date_of_activity_DB1,Set_number, BARB_Start_Time_of_Session, Local_Start_Time_of_Session, Local_End_Time_of_Session, Panel_or_guest_flag, Duration_of_Session, Session_activity_type, Playback_type,DB1_Station_Code
,Male_4_9,Male_10_15,Male_16_19,Male_20_24,Male_25_34,Male_35_44,Male_45_64,Male_65,Female_4_9,Female_10_15,Female_16_19,Female_20_24,Female_25_34,Female_35_44,Female_45_64,Female_65
,total_people_viewing
--,TV_Event_Start_Date_Time
--,TV_Event_End_Date_Time
,VOD_Indicator,VOD_Provider,VOD_Service,VOD_Type,Device_in_use,cb_row_id
)
select PVF_PV2,Sky_STB_viewing,Sky_STB_holder_hh,Household_number,filename,Viewing_platform,Barb_date_of_recording, cast(Start_time_of_recording_text as int), Start_time_of_recording, Barb_date_of_activity,Set_number,cast(Start_time_of_session_text as int) , Start_Time_of_Session, End_Time_of_Session, 'Guest', Duration_of_Session, Session_activity_type, Playback_type,DB1_Station_Code
,Male_4_9,Male_10_15,Male_16_19,Male_20_24,Male_25_34,Male_35_44,Male_45_64,Male_65,Female_4_9,Female_10_15,Female_16_19,Female_20_24,Female_25_34,Female_35_44,Female_45_64,Female_65
,(Male_4_9+Male_10_15+Male_16_19+Male_20_24+Male_25_34+Male_35_44+Male_45_64+Male_65+Female_4_9+Female_10_15+Female_16_19+Female_20_24+Female_25_34+Female_35_44+Female_45_64+Female_65)
--,Start_Time_of_Session
--,End_time_of_session
,VOD_Indicator,VOD_Provider,VOD_Service,VOD_Type,Device_in_use,cb_row_id
from ripolile.BARB_PVF07_PV207_Viewing_Record_Guests

GO


-- insert panel viewing, info on age/gender groups will be updated further down with function proc_BARB_update_fields

insert into ripolile.BARB_table_output_1(PVF_PV2,Sky_STB_viewing,Sky_STB_holder_hh,Household_number,filename,Viewing_platform,Barb_date_of_recording_DB1,BARB_Start_time_of_recording,Local_Start_time_of_recording, Barb_date_of_activity_DB1,Set_number, BARB_Start_Time_of_Session, Local_Start_Time_of_Session, Local_End_Time_of_Session, Panel_or_guest_flag, Duration_of_Session, Session_activity_type, Playback_type,DB1_Station_Code
,Person_1_viewing,Person_2_viewing,Person_3_viewing,Person_4_viewing,Person_5_viewing,Person_6_viewing,Person_7_viewing,Person_8_viewing,Person_9_viewing,Person_10_viewing,Person_11_viewing,Person_12_viewing,Person_13_viewing,Person_14_viewing,Person_15_viewing,Person_16_viewing
,total_people_viewing
--,TV_Event_Start_Date_Time
--,TV_Event_End_Date_Time
,VOD_Indicator,VOD_Provider,VOD_Service,VOD_Type,Device_in_use,cb_row_id
)
select PVF_PV2,Sky_STB_viewing,Sky_STB_holder_hh,Household_number,filename,Viewing_platform,Barb_date_of_recording, cast(Start_time_of_recording_text as int),Start_time_of_recording, Barb_date_of_activity,Set_number,cast(Start_time_of_session_text as int), Start_Time_of_Session, End_Time_of_Session, 'Panel', Duration_of_Session, Session_activity_type, Playback_type,DB1_Station_Code
,Person_1_viewing,Person_2_viewing,Person_3_viewing,Person_4_viewing,Person_5_viewing,Person_6_viewing,Person_7_viewing,Person_8_viewing,Person_9_viewing,Person_10_viewing,Person_11_viewing,Person_12_viewing,Person_13_viewing,Person_14_viewing,Person_15_viewing,Person_16_viewing
,(Person_1_viewing+Person_2_viewing+Person_3_viewing+Person_4_viewing+Person_5_viewing+Person_6_viewing+Person_7_viewing+Person_8_viewing+Person_9_viewing+Person_10_viewing+Person_11_viewing+Person_12_viewing+Person_13_viewing+Person_14_viewing+Person_15_viewing+Person_16_viewing)
--,Start_Time_of_Session
--,End_time_of_session
,VOD_Indicator,VOD_Provider,VOD_Service,VOD_Type,Device_in_use,cb_row_id
from ripolile.BARB_PVF06_PV206_Viewing_Record_Panel_Members

GO

-- here we save in a temp table the date range of viewing-----------------------------------------------------------------------------
-- since we will use the information on minimum/maximum timestamps often, better to save it in a small temp table
-- than calculating it every time we need it using max/min(fields)
truncate TABLE ripolile.local_time_range
GO
insert into ripolile.local_time_range
select min(Local_Start_Time_of_Session) as min_local_session_timestamp
,CAST(NULL as datetime) as min_local_recording_timestamp -- to be updated below
,CAST(NULL as datetime) as min_local_timestamp -- to be updated below: this contains the minimum between min_local_recording_timestamp and min_local_session_timestamp
,max(Local_End_Time_of_Session) as max_local_timestamp -- it comes of course by the max session timestamp
from ripolile.BARB_table_output_1

GO

-- here update minimum recording timestamp: there might be recording data with timestamp that is prior to the minumum live viewing timestamp
update ripolile.local_time_range
set min_local_recording_timestamp = (select min(Local_Start_time_of_recording) from BARB_table_output_1 where Local_Start_time_of_recording is not NULL)
GO
update ripolile.local_time_range
set min_local_recording_timestamp = min_local_session_timestamp
where min_local_recording_timestamp is NULL

GO

update ripolile.local_time_range
set min_local_timestamp = (case when min_local_recording_timestamp <= min_local_session_timestamp then min_local_recording_timestamp else min_local_session_timestamp end)

GO

-- here we save in a temp table the date range of viewing-----------------------------------------------------------------------------
-- the temp table will contain information on UTC and local times in the range that we need (since we will use the table for joins later,
-- better to use a small temp table than the original full sk_prod.VESPA_CALENDAR)
truncate TABLE ripolile.local_UTC_conversion_table
GO
insert into ripolile.local_UTC_conversion_table
select utc_day_date,utc_time_hours,local_day_date,local_time_hours,daylight_savings_flag
from sk_prod.VESPA_CALENDAR
where local_day_date between (select date(min_local_timestamp) from local_time_range) and (select date(max_local_timestamp) from ripolile.local_time_range)

GO

exec proc_BARB_update_fields

GO


-- update household_weight with the housewife weight (household_status 2 means 'housewife not head of household', whereas household_status 4 means the person is 'housewife and head of household')
update ripolile.BARB_table_output_1 pvf1
set pvf1.Household_Weight=
(
select max(processing_weight) as processing_weight --, mem.person_number, mem.Household_status
from
ripolile.BARB_PVF04_Individual_Member_Details mem
left join
ripolile.BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories wei
on mem.household_number=wei.household_number
and mem.person_number=wei.person_number
where mem.household_number=pvf1.household_number
and wei.Reporting_Panel_Code=50
and (mem.Household_status=2 or mem.Household_status=4)
group by pvf1.household_number
)

GO

-- update the total people watching
update ripolile.BARB_table_output_1
set weighted_total_people_viewing=
coalesce(Weigthed_Male_4_9,0)+coalesce(Weigthed_Male_10_15,0)+coalesce(Weigthed_Male_16_19,0)+coalesce(Weigthed_Male_20_24,0)+coalesce(Weigthed_Male_25_34,0)+coalesce(Weigthed_Male_35_44,0)+coalesce(Weigthed_Male_45_64,0)+coalesce(Weigthed_Male_65,0)+
coalesce(Weigthed_Female_4_9,0)+coalesce(Weigthed_Female_10_15,0)+coalesce(Weigthed_Female_16_19,0)+coalesce(Weigthed_Female_20_24,0)+coalesce(Weigthed_Female_25_34,0)+coalesce(Weigthed_Female_35_44,0)+coalesce(Weigthed_Female_45_64,0)+coalesce(Weigthed_Female_65,0)

GO


-- update service key, channel name and DB1_Station_Name with information from the table BARB_Channel_Map

update ripolile.BARB_table_output_1 pvf1
set pvf1.service_key=cm.service_key, pvf1.Channel_Name=cm.sk_name, pvf1.DB1_Station_Name=cm.db1_name
from
ripolile.BARB_table_output_1 pvf1
left join
(
select db1_station_code, service_key, sk_name, db1_name
from
vespa_analysts.BARB_Channel_Map
where
main_sk = 'Y'
) cm
on pvf1.DB1_Station_Code=cm.db1_station_code

GO

-- update end time of recording: start time of recording plus duration of session.
update ripolile.BARB_table_output_1
set Local_End_time_of_recording = dateadd(mi, barb_t.Duration_of_Session-1, barb_t.Local_Start_time_of_recording)-- the -1 is for BARB policy that the same minute cannot be assigned to different events, so if ev start 19:40 and viewed for 21 minutes, end will be at 20:00, and following event will start at 20:01
from
ripolile.BARB_table_output_1 barb_t
where Local_Start_time_of_recording is not null

GO

-- populate BARB_table_output_1_ordered,
-- which is BARB_table_output_1 ordered by Panel_or_guest_flag, household_number, set_number, Local_start_time_of_session
-- we need it for processing, i.e. finding Channel Event Start/End Times
-- A channel event defined when the channel is changed or the TV is turned on/off (consistent with Skyview approach)


truncate TABLE ripolile.BARB_table_output_1_ordered
GO
-- insert a fake row at the beginning of the table,
-- this is auxiliary to the process of calculating correctly start/end event times of the first row
-- as will be clearer later in the next section
insert into ripolile.BARB_table_output_1_ordered (set_number) VALUES('8888')

GO

insert into ripolile.BARB_table_output_1_ordered
(
filename
,Sky_STB_viewing
,Sky_STB_holder_hh
,PVF_PV2
,Household_number
,Barb_date_of_activity_DB1
-- ,Actual_date
,Set_number
,BARB_Start_Time_of_Session
,Local_Start_Time_of_Session
,Local_End_Time_of_Session
,Panel_or_guest_flag
,Duration_of_Session
,Session_activity_type
,Playback_type
,DB1_Station_Code
,DB1_Station_Name
,Viewing_platform
,Barb_date_of_recording_DB1
-- ,Actual_Date_of_Recording date --- change datatype from Barb
,BARB_Start_time_of_recording
,Local_Start_time_of_recording
,Local_End_time_of_recording
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
,Weigthed_Male_4_9
,Weigthed_Male_10_15
,Weigthed_Male_16_19
,Weigthed_Male_20_24
,Weigthed_Male_25_34
,Weigthed_Male_35_44
,Weigthed_Male_45_64
,Weigthed_Male_65
,Weigthed_Female_4_9
,Weigthed_Female_10_15
,Weigthed_Female_16_19
,Weigthed_Female_20_24
,Weigthed_Female_25_34
,Weigthed_Female_35_44
,Weigthed_Female_45_64
,Weigthed_Female_65
,total_people_viewing
,weighted_total_people_viewing
,VOD_Indicator
,VOD_Provider
,VOD_Service
,VOD_Type
,Device_in_use
--,TV_Instance_Start_Date_Time
--,TV_Instance_End_Date_Time
--,TV_Event_Start_Date_Time
--,TV_Event_End_Date_Time
,Household_Weight
,Service_Key
,Channel_Name
,cb_row_id
)
select
filename
,Sky_STB_viewing
,Sky_STB_holder_hh
,PVF_PV2
,Household_number
,Barb_date_of_activity_DB1
-- ,Actual_date
,Set_number
,BARB_Start_Time_of_Session
,Local_Start_Time_of_Session
,Local_End_Time_of_Session
,Panel_or_guest_flag
,Duration_of_Session
,Session_activity_type
,Playback_type
,DB1_Station_Code
,DB1_Station_Name
,Viewing_platform
,Barb_date_of_recording_DB1
-- ,Actual_Date_of_Recording date --- change datatype from Barb
,BARB_Start_time_of_recording
,Local_Start_time_of_recording
,Local_End_time_of_recording
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
,Weigthed_Male_4_9
,Weigthed_Male_10_15
,Weigthed_Male_16_19
,Weigthed_Male_20_24
,Weigthed_Male_25_34
,Weigthed_Male_35_44
,Weigthed_Male_45_64
,Weigthed_Male_65
,Weigthed_Female_4_9
,Weigthed_Female_10_15
,Weigthed_Female_16_19
,Weigthed_Female_20_24
,Weigthed_Female_25_34
,Weigthed_Female_35_44
,Weigthed_Female_45_64
,Weigthed_Female_65
,total_people_viewing
,weighted_total_people_viewing
,VOD_Indicator
,VOD_Provider
,VOD_Service
,VOD_Type
,Device_in_use
-- ,TV_Instance_Start_Date_Time timestamp
-- ,TV_Instance_End_Date_Time timestamp
--,TV_Event_Start_Date_Time
--,TV_Event_End_Date_Time
,Household_Weight
,Service_Key
,Channel_Name
,cb_row_id
from ripolile.BARB_table_output_1
order by Panel_or_guest_flag, household_number, set_number, Local_start_time_of_session

GO

-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
-- B2: Get Event start/end times
-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------

-- Calculate Channel Event Start/End Times
-- A channel event defined when the channel is changed or the TV is turned on/off

-- The viewing table joined to itself, but joined to either the row above or below
-- Barb definition of a session (see doc BARB2010DSP01 Version 3.7C - Panel Viewing File.pdf) is
-- If there is a change for a given tv set in a given household in either channel/session activity/ viewing platform / individuals then must be a new session

-- Existing Skyview uses Vespa like events ie. same as Barb session but ignoring the changes in indivual watching TV
-- So if the row above/below is a different channel/tv set/household/ session activity/ viewing platform then must be either start/end of event


-- Identify rows that are Channel Event Starts
-- and save in a temp table (remember we inserted a fake row at the beginning of the table
-- to have a match for the first row


truncate TABLE BARB_temp_channel_event_start

GO

insert into
        ripolile.BARB_temp_channel_event_start
select
        v1.household_number
        ,v1.Panel_or_guest_flag
        ,v1.set_number
        ,v1.db1_station_code
        ,v1.session_activity_type
        ,v1.viewing_platform
        ,v1.Local_Start_Time_of_Session as channel_event_start_time
from
        ripolile.BARB_table_output_1_ordered v1
     inner join
        ripolile.BARB_table_output_1_ordered v2
     on v1.row_id = v2.row_id+1
where
        v1.Local_Start_Time_of_Session > dateadd(mi, 1, v2.Local_End_Time_of_Session)
        or v1.household_number <> v2.household_number
        or v1.Panel_or_guest_flag <> v2.Panel_or_guest_flag
        or v1.set_number <> v2.set_number
        or v1.db1_station_code <> v2.db1_station_code
        or v1.session_activity_type <> v2.session_activity_type
        or v1.viewing_platform <> v2.viewing_platform

GO
-- delete the fake first row, it has served its purpose for the calculation of the start time of the first row
-- now we don't need it anymore
delete from ripolile.BARB_table_output_1_ordered where set_number = 8888 -- delete the dummy record that we added


-- Update channel event start times in the temp viewing table
update ripolile.BARB_table_output_1_ordered
        set v1.TV_Event_Start_Date_Time =
                (select max(s.channel_event_start_time)
                from BARB_temp_channel_event_start s
                where v1.household_number = s.household_number
                and v1.set_number = s.set_number
                and v1.Panel_or_guest_flag = s.Panel_or_guest_flag
                and v1.db1_station_code = s.db1_station_code
                and v1.session_activity_type = s.session_activity_type
                and v1.viewing_platform = s.viewing_platform
                and v1.Local_Start_Time_of_Session >= s.channel_event_start_time
                group by s.household_number, s.set_number, s.db1_station_code, s.session_activity_type, s.viewing_platform)
        from ripolile.BARB_table_output_1_ordered v1


-- Identify rows that are Channel Event Ends
-- The following update query can't match the last row_id (if last row_id is 100 then would have to match to row_id 101)
-- i.e. the last channel event doesn't get an end time. Will deal with this later
-- By adding a dummy record at the end this match will work

GO
-- dual process to what had been done before for the calculation of the event start of the first row,
-- we insert a fake row at the bottom of the table, to calculate effectively the end time of the last row
insert into ripolile.BARB_table_output_1_ordered (set_number) VALUES('9999')

GO

-- Identify rows that are Channel Event Ends
-- and save in a temp table


truncate TABLE ripolile.BARB_temp_channel_event_end

GO

insert into
        ripolile.BARB_temp_channel_event_end
select
--        v1.row_id
        v1.Panel_or_guest_flag
        ,v1.household_number
        ,v1.set_number
        ,v1.db1_station_code
        ,v1.session_activity_type
        ,v1.viewing_platform
        ,v1.Local_End_Time_of_Session as channel_event_end_time
from
        ripolile.BARB_table_output_1_ordered v1
     inner join
        ripolile.BARB_table_output_1_ordered v2
     -- this join can't match the last row_id (if last row_id is 100 then would have to match to row_id 101)
     -- i.e. the last channel event doesn't get an end time. Will deal with this later
     on v1.row_id = v2.row_id-1
where
        v2.Local_Start_Time_of_Session > dateadd(mi, 1, v1.Local_End_Time_of_Session)
        or v1.household_number <> v2.household_number
        or v1.Panel_or_guest_flag <> v2.Panel_or_guest_flag
        or v1.set_number <> v2.set_number
        or v1.db1_station_code <> v2.db1_station_code
        or v1.session_activity_type <> v2.session_activity_type
        or v1.viewing_platform <> v2.viewing_platform


GO
-- delete the fake last row, it has served its purpose for the calculation of the end time of the last row
-- now we don't need it anymore
delete from ripolile.BARB_table_output_1_ordered where set_number = 9999 -- delete the dummy record that we added

GO

-- Update channel event end times
update ripolile.BARB_table_output_1_ordered
        set v1.TV_Event_End_Date_Time =
                (select min(s.channel_event_end_time)
                from BARB_temp_channel_event_end s
                where v1.household_number = s.household_number
                and v1.Panel_or_guest_flag = s.Panel_or_guest_flag
                and v1.set_number = s.set_number
                and v1.db1_station_code = s.db1_station_code
                and v1.session_activity_type = s.session_activity_type
                and v1.viewing_platform = s.viewing_platform
                and v1.Local_End_Time_of_Session <= s.channel_event_end_time
                group by s.household_number, s.set_number, s.db1_station_code, s.session_activity_type, s.viewing_platform)
        from ripolile.BARB_table_output_1_ordered v1

GO

-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
-- B3: Match viewing to Vespa programme schedule
-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------

-- I (Jason) have created a table called BARB_Channel_Map
-- I (Jason) have mapped DB1_station_codes to Service_Keys
-- Note that there is not a 1 to 1 relationship
-- But I (Jason) have identified the "main" service key that maps to a db1 code by the total viewing duration over the past month or so
-- The main service key identified by main_sk = 'Y'
-- Multiple DB1 codes may feed into a single service key - but this is OK
-- We will need to work out how this table gets updated regularly - out of scope for this code


-- Firstly, we create the final temp table, having the same structure of the output viewing table
-- when all processing is finished we will insert all the records in the original output viewing table
truncate table ripolile.BARB_viewing_table
GO
-- here we match viewing with VESPA programme schedule
-- for live viewing data only (recorded data will be dealt with later)
insert into ripolile.BARB_viewing_table
(
filename
,Sky_STB_viewing
,Sky_STB_holder_hh
,PVF_PV2
,Household_number
,Barb_date_of_activity_DB1
-- ,Actual_date
,Set_number
,BARB_Start_Time_of_Session
,Local_Start_Time_of_Session
,Local_End_Time_of_Session
,Panel_or_guest_flag
,Duration_of_Session
,Session_activity_type
,Playback_type
,DB1_Station_Code
,DB1_Station_Name
,Viewing_platform
,Barb_date_of_recording_DB1
-- ,Actual_Date_of_Recording date --- change datatype from Barb
,BARB_Start_time_of_recording
,Local_Start_time_of_recording
,Local_End_time_of_recording
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
,Weigthed_Male_4_9
,Weigthed_Male_10_15
,Weigthed_Male_16_19
,Weigthed_Male_20_24
,Weigthed_Male_25_34
,Weigthed_Male_35_44
,Weigthed_Male_45_64
,Weigthed_Male_65
,Weigthed_Female_4_9
,Weigthed_Female_10_15
,Weigthed_Female_16_19
,Weigthed_Female_20_24
,Weigthed_Female_25_34
,Weigthed_Female_35_44
,Weigthed_Female_45_64
,Weigthed_Female_65
,total_people_viewing
,weighted_total_people_viewing
,VOD_Indicator
,VOD_Provider
,VOD_Service
,VOD_Type
,Device_in_use
,broadcast_start_date_time_local
,broadcast_end_date_time_local
,broadcast_start_date_time_UTC
,broadcast_end_date_time_UTC
,Local_TV_Instance_Start_Date_Time
,Local_TV_Instance_End_Date_Time
,Local_TV_Event_Start_Date_Time
,Local_TV_Event_End_Date_Time
,Household_Weight
,Service_Key
,Channel_Name
,cb_row_id
,programme_name
,genre_description
,sub_genre_description
,broadcast_daypart
,episode_number
,episodes_in_series
,three_d_flag
,true_hd_flag
,wide_screen_flag
)
select
filename
,Sky_STB_viewing
,Sky_STB_holder_hh
,PVF_PV2
,Household_number
,Barb_date_of_activity_DB1
-- ,Actual_date
,Set_number
,BARB_Start_Time_of_Session
,Local_Start_Time_of_Session
,Local_End_Time_of_Session
,Panel_or_guest_flag
,Duration_of_Session
,Session_activity_type
,Playback_type
,DB1_Station_Code
,DB1_Station_Name
,Viewing_platform
,Barb_date_of_recording_DB1
-- ,Actual_Date_of_Recording date --- change datatype from Barb
,BARB_Start_time_of_recording
,Local_Start_time_of_recording
,Local_End_time_of_recording
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
,Weigthed_Male_4_9
,Weigthed_Male_10_15
,Weigthed_Male_16_19
,Weigthed_Male_20_24
,Weigthed_Male_25_34
,Weigthed_Male_35_44
,Weigthed_Male_45_64
,Weigthed_Male_65
,Weigthed_Female_4_9
,Weigthed_Female_10_15
,Weigthed_Female_16_19
,Weigthed_Female_20_24
,Weigthed_Female_25_34
,Weigthed_Female_35_44
,Weigthed_Female_45_64
,Weigthed_Female_65
,total_people_viewing
,weighted_total_people_viewing
,VOD_Indicator
,VOD_Provider
,VOD_Service
,VOD_Type
,Device_in_use
,broadcast_start_date_time_local
,broadcast_end_date_time_local
,broadcast_start_date_time_UTC
,broadcast_end_date_time_UTC
        ,case
                when pvf.TV_Event_Start_Date_Time >= sch.broadcast_start_date_time_local then pvf.TV_Event_Start_Date_Time
                else sch.broadcast_start_date_time_local
        end as TV_Instance_Start_Date_Time
        ,case
                when pvf.TV_Event_End_Date_Time < sch.broadcast_end_date_time_local then pvf.TV_Event_End_Date_Time
                else dateadd(mi, -1, sch.broadcast_end_date_time_local) -- when the end time of instance comes from VESPA, we decrement by 1 to make it consistent with BARB policy
        end as TV_Instance_End_Date_Time
,pvf.TV_Event_Start_Date_Time
,pvf.TV_Event_End_Date_Time
,Household_Weight
,pvf.Service_Key
,pvf.Channel_Name
,pvf.cb_row_id
,sch.programme_name
,sch.genre_description
,sch.sub_genre_description
,sch.broadcast_daypart
,sch.episode_number
,sch.episodes_in_series
,cast(sch.three_d_flag as int)
,cast(sch.true_hd_flag as int)
,cast(sch.wide_screen_flag as int)
--into BARB_viewing_table
from
     ripolile.BARB_table_output_1_ordered pvf
          left join
     sk_prod.VESPA_PROGRAMME_SCHEDULE sch
     on pvf.service_key = sch.service_key
     and pvf.Local_Start_Time_of_Session < sch.broadcast_end_date_time_local
     and pvf.Local_End_Time_of_Session >= sch.broadcast_start_date_time_local
where pvf.Local_Start_time_of_recording is null -- probably we can use this to limit the number of fields (check if it gives the same number of records

GO


-- here we match viewing with VESPA programme schedule
-- for recorded viewing data only
insert into ripolile.BARB_viewing_table
(
filename
,Sky_STB_viewing
,Sky_STB_holder_hh
,PVF_PV2
,Household_number
,Barb_date_of_activity_DB1
-- ,Actual_date
,Set_number
,BARB_Start_Time_of_Session
,Local_Start_Time_of_Session
,Local_End_Time_of_Session
,Panel_or_guest_flag
,Duration_of_Session
,Session_activity_type
,Playback_type
,DB1_Station_Code
,DB1_Station_Name
,Viewing_platform
,Barb_date_of_recording_DB1
-- ,Actual_Date_of_Recording date --- change datatype from Barb
,BARB_Start_time_of_recording
,Local_Start_time_of_recording
,Local_End_time_of_recording
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
,Weigthed_Male_4_9
,Weigthed_Male_10_15
,Weigthed_Male_16_19
,Weigthed_Male_20_24
,Weigthed_Male_25_34
,Weigthed_Male_35_44
,Weigthed_Male_45_64
,Weigthed_Male_65
,Weigthed_Female_4_9
,Weigthed_Female_10_15
,Weigthed_Female_16_19
,Weigthed_Female_20_24
,Weigthed_Female_25_34
,Weigthed_Female_35_44
,Weigthed_Female_45_64
,Weigthed_Female_65
,total_people_viewing
,weighted_total_people_viewing
,VOD_Indicator
,VOD_Provider
,VOD_Service
,VOD_Type
,Device_in_use
,broadcast_start_date_time_local
,broadcast_end_date_time_local
,broadcast_start_date_time_UTC
,broadcast_end_date_time_UTC
,Local_TV_Instance_Start_Date_Time
,Local_TV_Instance_End_Date_Time
,Local_TV_Event_Start_Date_Time
,Local_TV_Event_End_Date_Time
,Household_Weight
,Service_Key
,Channel_Name
,cb_row_id
,programme_name
,genre_description
,sub_genre_description
,broadcast_daypart
,episode_number
,episodes_in_series
,three_d_flag
,true_hd_flag
,wide_screen_flag
)
select
filename
,Sky_STB_viewing
,Sky_STB_holder_hh
,PVF_PV2
,Household_number
,Barb_date_of_activity_DB1
-- ,Actual_date
,Set_number
,BARB_Start_Time_of_Session
,Local_Start_Time_of_Session
,Local_End_Time_of_Session
,Panel_or_guest_flag
,Duration_of_Session
,Session_activity_type
,Playback_type
,DB1_Station_Code
,DB1_Station_Name
,Viewing_platform
,Barb_date_of_recording_DB1
-- ,Actual_Date_of_Recording date --- change datatype from Barb
,BARB_Start_time_of_recording
,Local_Start_time_of_recording
,Local_End_time_of_recording
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
,Weigthed_Male_4_9
,Weigthed_Male_10_15
,Weigthed_Male_16_19
,Weigthed_Male_20_24
,Weigthed_Male_25_34
,Weigthed_Male_35_44
,Weigthed_Male_45_64
,Weigthed_Male_65
,Weigthed_Female_4_9
,Weigthed_Female_10_15
,Weigthed_Female_16_19
,Weigthed_Female_20_24
,Weigthed_Female_25_34
,Weigthed_Female_35_44
,Weigthed_Female_45_64
,Weigthed_Female_65
,total_people_viewing
,weighted_total_people_viewing
,VOD_Indicator
,VOD_Provider
,VOD_Service
,VOD_Type
,Device_in_use
,broadcast_start_date_time_local
,broadcast_end_date_time_local
,broadcast_start_date_time_UTC
,broadcast_end_date_time_UTC
,case when pvf.Local_Start_time_of_recording >= sch.broadcast_start_date_time_local then pvf.Local_Start_Time_of_Session
                else dateadd(mi, datediff(mi,pvf.Local_Start_time_of_recording, pvf.Local_Start_Time_of_Session),sch.broadcast_start_date_time_local) end
,case when pvf.Local_End_time_of_recording < sch.broadcast_end_date_time_local then pvf.Local_End_Time_of_Session
                else dateadd(mi, -1,dateadd(mi, datediff(mi, pvf.Local_Start_time_of_recording, pvf.Local_Start_Time_of_Session),sch.broadcast_end_date_time_local) ) end
,pvf.TV_Event_Start_Date_Time
,pvf.TV_Event_End_Date_Time
,Household_Weight
,pvf.Service_Key
,pvf.Channel_Name
,pvf.cb_row_id
,sch.programme_name
,sch.genre_description
,sch.sub_genre_description
,sch.broadcast_daypart
,sch.episode_number
,sch.episodes_in_series
,cast(sch.three_d_flag as int)
,cast(sch.true_hd_flag as int)
,cast(sch.wide_screen_flag as int)
--into BARB_viewing_table
from
     ripolile.BARB_table_output_1_ordered pvf
          left join
     sk_prod.VESPA_PROGRAMME_SCHEDULE sch
     on pvf.service_key = sch.service_key
     and pvf.Local_Start_time_of_recording < sch.broadcast_end_date_time_local
     and pvf.Local_End_time_of_recording >= sch.broadcast_start_date_time_local
where pvf.Local_Start_time_of_recording is not null -- probably we can use this to limit the number of fields (check if it gives the same number of records

GO


set @nr_of_PVF_rec_not_matching_VESPA=(select count(1) from ripolile.BARB_viewing_table where broadcast_start_date_time_local is null and PVF_PV2='PVF')

set @nr_of_PV2_rec_not_matching_VESPA=(select count(1) from ripolile.BARB_viewing_table where broadcast_start_date_time_local is null and PVF_PV2='PV2')

update ripolile.barb_daily_monitoring
set nr_of_PVF_rec_not_matching_VESPA=@nr_of_PVF_rec_not_matching_VESPA
,nr_of_PV2_rec_not_matching_VESPA=@nr_of_PV2_rec_not_matching_VESPA
,nr_of_TOT_rec_not_matching_VESPA=@nr_of_PVF_rec_not_matching_VESPA+@nr_of_PV2_rec_not_matching_VESPA
where id_row=@current_id_row

GO
-- update the channel pack
update ripolile.BARB_viewing_table
set channel_pack = map.channel_pack
from
ripolile.BARB_viewing_table view_t
left join
(
select service_key, max(channel_pack) as channel_pack
from
vespa_Analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES
group by service_key
) map
on view_t.service_key = map.service_key

GO

-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
-- B4: Final processing of data: update TV instance fields, calculate TV instance id, calculate BARB_Instance_Start/end_Date_Time
-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------

-- fill NULL TV instance values with BARB event data
update ripolile.BARB_viewing_table
set Local_TV_Instance_Start_Date_Time = view_t.Local_Start_Time_of_Session
, Local_TV_Instance_End_Date_Time = view_t.Local_End_Time_of_Session
from
ripolile.BARB_viewing_table view_t
where Local_TV_Instance_Start_Date_Time IS NULL
OR Local_TV_Instance_End_Date_Time IS NULL

GO

truncate TABLE ripolile.temp_sequenced_viewing_table
GO
-- calculate TV instance id within a TV event
-- since row_number or rank is not supported in update/delete activities, we create a temp table to make a join
insert into temp_sequenced_viewing_table
select Panel_or_guest_flag, household_number, set_number, db1_station_code, session_activity_type, viewing_platform, Local_Start_Time_of_Session
,Local_TV_Event_Start_Date_Time, Local_TV_Event_End_Date_Time, Local_TV_Instance_Start_Date_Time, Local_TV_Instance_End_Date_Time, row_number() over (partition by Panel_or_guest_flag, household_number, set_number, db1_station_code, session_activity_type, viewing_platform, /*Local_Start_Time_of_Session,*/ Local_TV_Event_Start_Date_Time order by Local_Start_Time_of_Session, Local_TV_Instance_Start_Date_Time) as instance_sequence
from ripolile.BARB_viewing_table

GO

-- update TV instance sequence id in the viewing table
update ripolile.BARB_viewing_table
set TV_Instance_sequence_id = seq.instance_sequence
from
ripolile.BARB_viewing_table view_t
left join
ripolile.temp_sequenced_viewing_table seq
on
view_t.Panel_or_guest_flag = seq.Panel_or_guest_flag
and
view_t.household_number = seq.household_number
and
view_t.set_number = seq.set_number
and
view_t.db1_station_code = seq.db1_station_code
and
view_t.session_activity_type = seq.session_activity_type
and
view_t.viewing_platform = seq.viewing_platform
and
view_t.Local_Start_Time_of_Session = seq.Local_Start_Time_of_Session
and
view_t.Local_TV_Event_Start_Date_Time = seq.Local_TV_Event_Start_Date_Time
and
view_t.Local_TV_Event_End_Date_Time = seq.Local_TV_Event_End_Date_Time
and
view_t.Local_TV_Instance_Start_Date_Time = seq.Local_TV_Instance_Start_Date_Time
and
view_t.Local_TV_Instance_End_Date_Time = seq.Local_TV_Instance_End_Date_Time

GO

-- here we update BARB_Instance_Start_Date_Time and BARB_Instance_End_Date_Time
-- BARB_Instance_Start_Date_Time which will be the maximum among Local_Start_Time_of_Session, TV_Event_Start_Date_Time, TV_Instance_Start_Date_Time
-- BARB_Instance_End_Date_Time which will be the minimum among Local_End_Time_of_Session, TV_Event_End_Date_Time, TV_Instance_End_Date_Time

update ripolile.BARB_viewing_table
set Local_BARB_Instance_Start_Date_Time = CASE WHEN coalesce(view_t.Local_Start_Time_of_Session,'') >= coalesce(view_t.Local_TV_Event_Start_Date_Time,'') THEN
    CASE WHEN coalesce(view_t.Local_Start_Time_of_Session,'') >= coalesce(view_t.Local_TV_Instance_Start_Date_Time,'') THEN coalesce(view_t.Local_Start_Time_of_Session,'') ELSE coalesce(view_t.Local_TV_Instance_Start_Date_Time,'') END
ELSE
    CASE WHEN coalesce(view_t.Local_TV_Event_Start_Date_Time,'') >= coalesce(view_t.Local_TV_Instance_Start_Date_Time,'') THEN coalesce(view_t.Local_TV_Event_Start_Date_Time,'') ELSE coalesce(view_t.Local_TV_Instance_Start_Date_Time,'') END
END
, Local_BARB_Instance_End_Date_Time = CASE WHEN coalesce(view_t.Local_End_Time_of_Session,'') <= coalesce(view_t.Local_TV_Event_End_Date_Time,'') THEN
    CASE WHEN coalesce(view_t.Local_End_Time_of_Session,'') <= coalesce(view_t.Local_TV_Instance_End_Date_Time,'') THEN coalesce(view_t.Local_End_Time_of_Session,'') ELSE coalesce(view_t.Local_TV_Instance_End_Date_Time,'') END
ELSE
    CASE WHEN coalesce(view_t.Local_TV_Event_End_Date_Time,'') <= coalesce(view_t.Local_TV_Instance_End_Date_Time,'') THEN coalesce(view_t.Local_TV_Event_End_Date_Time,'') ELSE coalesce(view_t.Local_TV_Instance_End_Date_Time,'') END
END
from
ripolile.BARB_viewing_table view_t

GO

-- here we calculate all durations: BARB instance, TV event and TV instance (simple difference: end time-start time)
---- the +1 is for BARB policy that the same minute cannot be assigned to different events, so if ev start 19:40 and viewed for 21 minutes, end will be at 20:00, and following event will start at 20:01
-- so to have the actual event duration we must add 1
update ripolile.BARB_viewing_table
set BARB_Instance_duration = datediff(mi,Local_BARB_Instance_Start_Date_Time,Local_BARB_Instance_End_Date_Time)+1
,TV_event_duration = datediff(mi,Local_TV_Event_Start_Date_Time,Local_TV_Event_End_Date_Time)+1
,TV_instance_duration = datediff(mi,Local_TV_Instance_Start_Date_Time,Local_TV_Instance_End_Date_Time)+1

GO

-- here update the UTC times: we convert local day and time to UTC using data in local_UTC_conversion_table (coming from sk_prod.VESPA_CALENDAR)
update ripolile.BARB_viewing_table
set UTC_Start_Time_of_Session = datetime(utc_day_date || ' ' || utc_time_hours || ':' || datepart(mi, view_t.Local_Start_Time_of_Session) || ':00.000000')
from
ripolile.BARB_viewing_table view_t
inner join
ripolile.local_UTC_conversion_table loc
on local_day_date = date(view_t.Local_Start_Time_of_Session)
and local_time_hours = datepart(hh, view_t.Local_Start_Time_of_Session)
where Local_Start_Time_of_Session is not null

GO

update ripolile.BARB_viewing_table
set UTC_End_Time_of_Session = datetime(utc_day_date || ' ' || utc_time_hours || ':' || datepart(mi, view_t.Local_End_Time_of_Session) || ':00.000000')
from
ripolile.BARB_viewing_table view_t
inner join
ripolile.local_UTC_conversion_table loc
on local_day_date = date(view_t.Local_End_Time_of_Session)
and local_time_hours = datepart(hh, view_t.Local_End_Time_of_Session)
where Local_End_Time_of_Session is not null

GO


update ripolile.BARB_viewing_table
set UTC_Start_time_of_recording = datetime(utc_day_date || ' ' || utc_time_hours || ':' || datepart(mi, view_t.Local_Start_time_of_recording) || ':00.000000')
from
ripolile.BARB_viewing_table view_t
inner join
ripolile.local_UTC_conversion_table loc
on local_day_date = date(view_t.Local_Start_time_of_recording)
and local_time_hours = datepart(hh, view_t.Local_Start_time_of_recording)
where Local_Start_time_of_recording is not null

GO

update ripolile.BARB_viewing_table
set UTC_End_time_of_recording = datetime(utc_day_date || ' ' || utc_time_hours || ':' || datepart(mi, view_t.Local_End_time_of_recording) || ':00.000000')
from
ripolile.BARB_viewing_table view_t
inner join
ripolile.local_UTC_conversion_table loc
on local_day_date = date(view_t.Local_End_time_of_recording)
and local_time_hours = datepart(hh, view_t.Local_End_time_of_recording)
where Local_End_time_of_recording is not null

GO

update ripolile.BARB_viewing_table
set UTC_TV_Event_Start_Date_Time = datetime(utc_day_date || ' ' || utc_time_hours || ':' || datepart(mi, view_t.Local_TV_Event_Start_Date_Time) || ':00.000000')
from
ripolile.BARB_viewing_table view_t
inner join
ripolile.local_UTC_conversion_table loc
on local_day_date = date(view_t.Local_TV_Event_Start_Date_Time)
and local_time_hours = datepart(hh, view_t.Local_TV_Event_Start_Date_Time)
where Local_TV_Event_Start_Date_Time is not null

GO

update ripolile.BARB_viewing_table
set UTC_TV_Event_End_Date_Time = datetime(utc_day_date || ' ' || utc_time_hours || ':' || datepart(mi, view_t.Local_TV_Event_End_Date_Time) || ':00.000000' )
from
ripolile.BARB_viewing_table view_t
inner join
ripolile.local_UTC_conversion_table loc
on local_day_date = date(view_t.Local_TV_Event_End_Date_Time)
and local_time_hours = datepart(hh, view_t.Local_TV_Event_End_Date_Time)
where view_t.Local_TV_Event_End_Date_Time is not null

GO

update ripolile.BARB_viewing_table
set UTC_TV_Instance_Start_Date_Time = datetime(utc_day_date || ' ' || utc_time_hours || ':' || datepart(mi, view_t.Local_TV_Instance_Start_Date_Time) || ':00.000000' )
from
ripolile.BARB_viewing_table view_t
inner join
ripolile.local_UTC_conversion_table loc
on local_day_date = date(view_t.Local_TV_Instance_Start_Date_Time)
and local_time_hours = datepart(hh, view_t.Local_TV_Instance_Start_Date_Time)
where view_t.Local_TV_Instance_Start_Date_Time is not null

GO


update ripolile.BARB_viewing_table
set UTC_TV_Instance_End_Date_Time = datetime(utc_day_date || ' ' || utc_time_hours || ':' || datepart(mi, view_t.Local_TV_Instance_End_Date_Time) || ':00.000000' )
from
ripolile.BARB_viewing_table view_t
inner join
ripolile.local_UTC_conversion_table loc
on local_day_date = date(view_t.Local_TV_Instance_End_Date_Time)
and local_time_hours = datepart(hh, view_t.Local_TV_Instance_End_Date_Time)
where view_t.Local_TV_Instance_End_Date_Time is not null

GO

update ripolile.BARB_viewing_table
set UTC_BARB_Instance_Start_Date_Time = datetime(utc_day_date || ' ' || utc_time_hours || ':' || datepart(mi, view_t.Local_BARB_Instance_Start_Date_Time) || ':00.000000' )
from
ripolile.BARB_viewing_table view_t
inner join
ripolile.local_UTC_conversion_table loc
on local_day_date = date(view_t.Local_BARB_Instance_Start_Date_Time)
and local_time_hours = datepart(hh, view_t.Local_BARB_Instance_Start_Date_Time)
where view_t.Local_BARB_Instance_Start_Date_Time is not null

GO


update ripolile.BARB_viewing_table
set UTC_BARB_Instance_End_Date_Time = datetime(utc_day_date || ' ' || utc_time_hours || ':' || datepart(mi, view_t.Local_BARB_Instance_End_Date_Time) || ':00.000000' )
from
ripolile.BARB_viewing_table view_t
inner join
ripolile.local_UTC_conversion_table loc
on local_day_date = date(view_t.Local_BARB_Instance_End_Date_Time)
and local_time_hours = datepart(hh, view_t.Local_BARB_Instance_End_Date_Time)
where view_t.Local_BARB_Instance_End_Date_Time is not null

GO


-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
-- B5: Transfer all data from the temp table to the output viewing table
-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------

-- finally: transfer all data from the temp table to the output viewing table

if @process_PVF_data=1 or @process_PV2_data=1
begin

insert into ripolile.barb_daily_ind_prog_viewed
(
filename
,Sky_STB_viewing
,Sky_STB_holder_hh
,PVF_PV2
,Household_number
,Barb_date_of_activity_DB1
-- ,Actual_date
,Set_number
,Panel_or_guest_flag
,Duration_of_Session
,Session_activity_type
,Playback_type
,DB1_Station_Code
,DB1_Station_Name
,Viewing_platform
,Barb_date_of_recording_DB1
-- ,Actual_Date_of_Recording date ---
,BARB_Start_time_of_recording
,Local_Start_time_of_recording
,Local_End_time_of_recording
,UTC_Start_time_of_recording
,UTC_End_time_of_recording
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
,Weigthed_Male_4_9
,Weigthed_Male_10_15
,Weigthed_Male_16_19
,Weigthed_Male_20_24
,Weigthed_Male_25_34
,Weigthed_Male_35_44
,Weigthed_Male_45_64
,Weigthed_Male_65
,Weigthed_Female_4_9
,Weigthed_Female_10_15
,Weigthed_Female_16_19
,Weigthed_Female_20_24
,Weigthed_Female_25_34
,Weigthed_Female_35_44
,Weigthed_Female_45_64
,Weigthed_Female_65
,total_people_viewing
,weighted_total_people_viewing
,VOD_Indicator
,VOD_Provider
,VOD_Service
,VOD_Type
,Device_in_use
,broadcast_start_date_time_local
,broadcast_end_date_time_local
,broadcast_start_date_time_UTC
,broadcast_end_date_time_UTC
,BARB_Start_Time_of_Session
,UTC_Start_Time_of_Session
,UTC_End_Time_of_Session
,Local_Start_Time_of_Session
,Local_End_Time_of_Session
,Local_TV_Event_Start_Date_Time
,Local_TV_Event_End_Date_Time
,Local_TV_Instance_Start_Date_Time
,Local_TV_Instance_End_Date_Time
,Local_BARB_Instance_Start_Date_Time
,Local_BARB_Instance_End_Date_Time
,UTC_TV_Event_Start_Date_Time
,UTC_TV_Event_End_Date_Time
,UTC_TV_Instance_Start_Date_Time
,UTC_TV_Instance_End_Date_Time
,UTC_BARB_Instance_Start_Date_Time
,UTC_BARB_Instance_End_Date_Time
,TV_Instance_sequence_id
,BARB_Instance_duration
,TV_event_duration
,TV_instance_duration
,Household_Weight
,Service_Key 
,Channel_Name
,cb_row_id
--,row_id bigint primary key identity
,programme_name -- from here, fields from sk_prod.VESPA_PROGRAMME_SCHEDULE
,genre_description
,sub_genre_description
,broadcast_daypart
,episode_number
,episodes_in_series
,three_d_flag
,true_hd_flag
,wide_screen_flag -- to here, fields from sk_prod.VESPA_PROGRAMME_SCHEDULE
,channel_pack
)
select 
filename
,Sky_STB_viewing
,Sky_STB_holder_hh
,PVF_PV2
,Household_number
,Barb_date_of_activity_DB1
-- ,Actual_date
,Set_number
,Panel_or_guest_flag
,Duration_of_Session
,Session_activity_type
,Playback_type
,DB1_Station_Code
,DB1_Station_Name
,Viewing_platform
,Barb_date_of_recording_DB1
-- ,Actual_Date_of_Recording date --- change datatype from Barb
,BARB_Start_time_of_recording
,Local_Start_time_of_recording
,Local_End_time_of_recording
,UTC_Start_time_of_recording
,UTC_End_time_of_recording
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
,Weigthed_Male_4_9
,Weigthed_Male_10_15
,Weigthed_Male_16_19
,Weigthed_Male_20_24
,Weigthed_Male_25_34
,Weigthed_Male_35_44
,Weigthed_Male_45_64
,Weigthed_Male_65
,Weigthed_Female_4_9
,Weigthed_Female_10_15
,Weigthed_Female_16_19
,Weigthed_Female_20_24
,Weigthed_Female_25_34
,Weigthed_Female_35_44
,Weigthed_Female_45_64
,Weigthed_Female_65
,total_people_viewing
,weighted_total_people_viewing
,VOD_Indicator
,VOD_Provider
,VOD_Service
,VOD_Type
,Device_in_use
,broadcast_start_date_time_local
,broadcast_end_date_time_local
,broadcast_start_date_time_UTC
,broadcast_end_date_time_UTC
,BARB_Start_Time_of_Session 
,UTC_Start_Time_of_Session
,UTC_End_Time_of_Session
,Local_Start_Time_of_Session
,Local_End_Time_of_Session
,Local_TV_Event_Start_Date_Time
,Local_TV_Event_End_Date_Time
,Local_TV_Instance_Start_Date_Time
,Local_TV_Instance_End_Date_Time
,Local_BARB_Instance_Start_Date_Time
,Local_BARB_Instance_End_Date_Time
,UTC_TV_Event_Start_Date_Time
,UTC_TV_Event_End_Date_Time
,UTC_TV_Instance_Start_Date_Time
,UTC_TV_Instance_End_Date_Time
,UTC_BARB_Instance_Start_Date_Time
,UTC_BARB_Instance_End_Date_Time
,TV_Instance_sequence_id
,BARB_Instance_duration
,TV_event_duration
,TV_instance_duration
,Household_Weight
,Service_Key 
,Channel_Name
,cb_row_id
--,row_id bigint primary key identity
,programme_name -- from here, fields from sk_prod.VESPA_PROGRAMME_SCHEDULE
,genre_description
,sub_genre_description
,broadcast_daypart
,episode_number
,episodes_in_series
,three_d_flag
,true_hd_flag
,wide_screen_flag -- to here, fields from sk_prod.VESPA_PROGRAMME_SCHEDULE
,channel_pack
from ripolile.BARB_viewing_table

end

GO
set @dummy = @@rowcount
GO
update ripolile.barb_daily_monitoring
set nr_of_records_in_viewing_table = @dummy
where id_row=@current_id_row

GO


-- truncate all temp tables:

truncate table ripolile.BARB_PVF04_Individual_Member_Details

GO

truncate table ripolile.BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories
GO
truncate table ripolile.BARB_Panel_Demographic_Data_TV_Sets_Characteristics

-- BARB_PANEL_MEM_RESP_WGHT: weight of each person in panels
-- load the information we strictly need for further processing in a temporary table
GO
truncate table ripolile.BARB_PVF_Viewing_Record_Panel_Members
GO
truncate table ripolile.BARB_PVF_Viewing_Record_Guests
GO
truncate table ripolile.BARB_PV2_Viewing_Record_Panel_Members

GO
truncate table ripolile.BARB_PV2_Viewing_Record_Guests
GO
truncate table ripolile.BARB_PVF06_Viewing_Record_Panel_Members
GO


truncate table ripolile.BARB_PVF07_Viewing_Record_Guests

GO
truncate table ripolile.BARB_PV206_Viewing_Record_Panel_Members
GO
truncate table ripolile.BARB_PV207_Viewing_Record_Guests

GO
truncate TABLE ripolile.Sky_STB_holder_hh_tmp_table
GO
truncate TABLE ripolile.BARB_PVF06_PV206_Viewing_Record_Panel_Members

GO
truncate TABLE ripolile.BARB_PVF07_PV207_Viewing_Record_Guests
GO
truncate TABLE ripolile.BARB_table_output_1

GO
truncate TABLE ripolile.local_time_range
GO
truncate TABLE ripolile.local_UTC_conversion_table
GO
truncate TABLE ripolile.BARB_table_output_1_ordered
GO
truncate TABLE BARB_temp_channel_event_start

GO
truncate TABLE ripolile.BARB_temp_channel_event_end

GO
truncate table ripolile.BARB_viewing_table
GO
truncate TABLE ripolile.temp_sequenced_viewing_table
GO


GO

set @run_comment=@run_comment || '-Procedure completed at ' || cast(now() as varchar(25))

update ripolile.barb_daily_monitoring
set run_comment = @run_comment
where id_row=@current_id_row

-- return -- exit from the while loop

-- end -- of the while 1=1 that encloses the whole script


