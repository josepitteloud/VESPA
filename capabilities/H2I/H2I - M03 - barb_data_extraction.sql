 /*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES

--------------------------------------------------------------------------------------------------------------
**Project Name:							OPS 2.0
**Analysts:                             Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jason Thompson
**Stakeholder:                          SkyIQ
**Due Date:                             11/07/2014
**Project Code (Insight Collation):     
**Sharepoint Folder:                    
                                                                        
**Business Brief:

	This Module is set to extract the data from the raw barb file into a readable data structure prior
	manipulation

**Module:
	
	M03: Barb Data Extraction
			M03.0 - Initialising Environment
			M03.1 - Extracting Data
			M03.2 - Returning Results
	
--------------------------------------------------------------------------------------------------------------
*/

----------------------------------
--M03.0 - Initialising Environment
----------------------------------

create or replace procedure v289_m03_barb_data_extraction
as begin

	MESSAGE cast(now() as timestamp)||' | Begining M03.0 - Initialising Environment' TO CLIENT
	
	-- Variables
	DECLARE @file_creation_date date
	DECLARE @file_creation_time time
	DECLARE @file_type  varchar(12)
	DECLARE @File_Version Int
	DECLARE @filename varchar(13)
	
	SET @file_creation_date = (SELECT min(CAST(substr(imported_text,7,8) AS Date))
                                FROM PI_BARB_import
                                WHERE substr(imported_text,1,2) = '01')

	SET @file_creation_time = (SELECT min(CAST(substr(imported_text,15,2) || ':' || substr(imported_text,17,2) || ':' || substr(imported_text,19,2)  AS Time))
									FROM PI_BARB_import
									WHERE substr(imported_text,1,2) = '01')

	SET @file_type = (SELECT min(substr(imported_text,21,12))
									FROM PI_BARB_import
									WHERE substr(imported_text,1,2) = '01')

	SET @File_Version = (SELECT min(CAST(substr(imported_text,33,3) AS Int))
									FROM PI_BARB_import
									WHERE substr(imported_text,1,2) = '01')

	SET @Filename = (SELECT min(substr(imported_text,36,13))
									FROM PI_BARB_import
									WHERE substr(imported_text,1,2) = '01')

	MESSAGE cast(now() as timestamp)||' | @ M03.0: Initialising Environment DONE' TO CLIENT

--------------------------
-- M03.1 - Extracting Data
--------------------------

	MESSAGE cast(now() as timestamp)||' | Begining M03.1 - Extracting Data' TO CLIENT
	
-- BARB_Individual_Panel_Member_Details

	if object_id('BARB_Individual_Panel_Member_Details') is not null
		truncate table BARB_Individual_Panel_Member_Details
		
	commit

	INSERT	INTO BARB_Individual_Panel_Member_Details
	SELECT 	@file_creation_date
			,@file_creation_time
			,@file_type
			,@file_version
			,@filename
			,CAST(substr(imported_text,1,2) AS Int)
			,CAST(substr(imported_text,3,7) AS Int)
			,CAST(substr(imported_text,10,8) AS Int)
			,CAST(substr(imported_text,18,1) AS Int)
			,CAST(substr(imported_text,19,2) AS Int)
			,CAST(substr(imported_text,21,1) AS Int)
			,CAST(substr(imported_text,22,8) AS Int)
			,CAST(substr(imported_text,30,1) AS Int)
			,CAST(substr(imported_text,31,1) AS Int)
			,CAST(substr(imported_text,32,1) AS Int)
			,CAST(substr(imported_text,33,1) AS Int)
			,CAST(substr(imported_text,34,1) AS Int)
			,CAST(substr(imported_text,35,1) AS Int)
			,CAST(substr(imported_text,36,1) AS Int)
			,CAST(substr(imported_text,37,2) AS Int)
			,CAST(substr(imported_text,39,2) AS Int)
	FROM 	PI_BARB_IMPORT
	WHERE 	substr(imported_text,1,2) = '04' -- this identifies what table is being referred to see BARB2010DSP01 Version 3.7C - Panel Viewing File.pdf
	
	commit
	MESSAGE cast(now() as timestamp)||' | @ M03.1: Table BARB_Individual_Panel_Member_Details LOADED' TO CLIENT
	
	
-- BARB_PVF04_Individual_Member_Details

	if object_id('BARB_PVF04_Individual_Member_Details') is not null
		truncate table BARB_PVF04_Individual_Member_Details
		
	commit

	insert	into BARB_PVF04_Individual_Member_Details
	select	file_creation_date
			,file_creation_time
			,file_type
			,file_version
			,filename
			,Record_type
			,Household_number
			,date(
					cast(cast(Date_valid_for_DB1/10000 as int) as char(4)) || '-' ||
					cast(cast(Date_valid_for_DB1/100 as int) - cast(Date_valid_for_DB1/10000 as int) * 100 as varchar(2)) || '-' ||
					cast(Date_valid_for_DB1 - cast(Date_valid_for_DB1/100 as int) * 100 as varchar(2))
					)
			,Person_membership_status
			,Person_number
			,Sex_code
			,date(
					cast(cast(Date_of_birth/10000 as int) as char(4)) || '-' ||
					cast(cast(Date_of_birth/100 as int) - cast(Date_of_birth/10000 as int) * 100 as varchar(2)) || '-' ||
					cast(Date_of_birth - cast(Date_of_birth/100 as int) * 100 as varchar(2))
					)
			,Marital_status
			,Household_status
			,Working_status
			,Terminal_age_of_education
			,Welsh_Language_code
			,Gaelic_language_code
			,Dependency_of_Children
			,Life_stage_12_classifications
			,Ethnic_Origin
	from	BARB_Individual_Panel_Member_Details
	
	commit
	MESSAGE cast(now() as timestamp)||' | @ M03.1: Table BARB_PVF04_Individual_Member_Details LOADED' TO CLIENT
	
	
-- BARB_Panel_Member_Responses_Weights_and_Viewing_Categories

	if object_id('BARB_Panel_Member_Responses_Weights_and_Viewing_Categories') is not null
		truncate table BARB_Panel_Member_Responses_Weights_and_Viewing_Categories
		
	commit

	INSERT	INTO BARB_Panel_Member_Responses_Weights_and_Viewing_Categories
	SELECT 	@file_creation_date
			,@file_creation_time
			,@file_type
			,@file_version
			,@filename
			,CAST(substr(imported_text,1,2) AS Int)
			,CAST(substr(imported_text,3,7) AS Int)
			,CAST(substr(imported_text,10,2) AS Int)
			,CAST(substr(imported_text,12,5) AS Int)
			,CAST(substr(imported_text,17,8) AS Int)
			,CAST(substr(imported_text,25,1) AS Int)
			,CAST(substr(imported_text,26,7) AS Int)
			,CAST(substr(imported_text,33,1) AS Int)
			,CAST(substr(imported_text,34,1) AS Int)
			,CAST(substr(imported_text,35,1) AS Int)
			,CAST(substr(imported_text,36,1) AS Int)
			,CAST(substr(imported_text,37,1) AS Int)
			,CAST(substr(imported_text,38,1) AS Int)
	FROM 	PI_BARB_IMPORT
	WHERE 	substr(imported_text,1,2) = '05' -- this identifies what table is being refered to see BARB2010DSP01 Version 3.7C - Panel Viewing File.pdf
	
	commit
	MESSAGE cast(now() as timestamp)||' | @ M03.1: Table BARB_Panel_Member_Responses_Weights_and_Viewing_Categories LOADED' TO CLIENT
	
	
-- BARB_PVF_Viewing_Record_Panel_Members

	if object_id('BARB_PVF_Viewing_Record_Panel_Members') is not null
		truncate table BARB_PVF_Viewing_Record_Panel_Members
		
	commit

	INSERT	INTO BARB_PVF_Viewing_Record_Panel_Members
	SELECT 	@file_creation_date
			,@file_creation_time
			,@file_type
			,@file_version
			,@filename
			,CAST(substr(imported_text,1,2) AS Int)
			,CAST(substr(imported_text,3,7) AS Int)
			,CAST(substr(imported_text,10,8) AS Int)
			,CAST(substr(imported_text,18,2) AS Int)
			,CAST(substr(imported_text,20,4) AS Int)
			,CAST(substr(imported_text,24,4) AS Int)
			,CAST(substr(imported_text,28,2) AS Int)
			,substr(imported_text,30,1)
			,substr(imported_text,31,5)
			,CAST(substr(imported_text,36,1) AS Int)
			,CAST(substr(imported_text,37,8) AS Int)
			,CAST(substr(imported_text,45,4) AS Int)
			,CAST(substr(imported_text,49,1) AS Int)
			,CAST(substr(imported_text,50,1) AS Int)
			,CAST(substr(imported_text,51,1) AS Int)
			,CAST(substr(imported_text,52,1) AS Int)
			,CAST(substr(imported_text,53,1) AS Int)
			,CAST(substr(imported_text,54,1) AS Int)
			,CAST(substr(imported_text,55,1) AS Int)
			,CAST(substr(imported_text,56,1) AS Int)
			,CAST(substr(imported_text,57,1) AS Int)
			,CAST(substr(imported_text,58,1) AS Int)
			,CAST(substr(imported_text,59,1) AS Int)
			,CAST(substr(imported_text,60,1) AS Int)
			,CAST(substr(imported_text,61,1) AS Int)
			,CAST(substr(imported_text,62,1) AS Int)
			,CAST(substr(imported_text,63,1) AS Int)
			,CAST(substr(imported_text,64,1) AS Int)
			,CAST(substr(imported_text,65,9) AS Int)
			,CAST(substr(imported_text,74,1) AS Int)
			,CAST(substr(imported_text,75,5) AS Int)
			,CAST(substr(imported_text,80,5) AS Int)
			,CAST(substr(imported_text,85,5) AS Int)
			,CAST(substr(imported_text,90,4) AS Int)
	FROM 	PI_BARB_IMPORT
	WHERE 	substr(imported_text,1,2) = '06' -- this identifies what table is being refered to see BARB2010DSP01 Version 3.7C - Panel Viewing File.pdf

	commit
	MESSAGE cast(now() as timestamp)||' | @ M03.1: Table BARB_PVF_Viewing_Record_Panel_Members LOADED' TO CLIENT
	
	
-- BARB_PVF06_Viewing_Record_Panel_Members

	if object_id('BARB_PVF06_Viewing_Record_Panel_Members') is not null
		truncate table BARB_PVF06_Viewing_Record_Panel_Members
		
	commit
	
	insert	into BARB_PVF06_Viewing_Record_Panel_Members(
		file_creation_date
		,file_creation_time
		,file_type
		,file_version
		,filename
		,Record_type
		,Household_number
		,Barb_date_of_activity
		,Actual_date_of_session
		,Set_number
		,Start_time_of_session_text
		,Start_time_of_session
		,End_time_of_session
		,Duration_of_session
		,Session_activity_type
		,Playback_type
		,DB1_Station_Code
		,Viewing_platform
		,Barb_date_of_recording
		,Actual_Date_of_Recording
		,Start_time_of_recording_text
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
		,Interactive_Bar_Code_Identifier
		,VOD_Indicator
		,VOD_Provider
		,VOD_Service
		,VOD_Type
		,Device_in_use
	)
	select	file_creation_date
			,file_creation_time
			,file_type
			,file_version
			,filename
			,Record_type
			,Household_number
			-- Keep the original Date_of_Activity_DB1 as the Barb_date_of_activity
			,date(
					cast(cast(Date_of_Activity_DB1/10000 as int) as char(4)) || '-' ||
					cast(cast(Date_of_Activity_DB1/100 as int) - cast(Date_of_Activity_DB1/10000 as int) * 100 as varchar(2)) || '-' ||
					cast(Date_of_Activity_DB1 - cast(Date_of_Activity_DB1/100 as int) * 100 as varchar(2))
					)
			-- Actual_date_of_session: A barb day can go over 24:00. In this case we need to increase the date by 1
			,dateadd(dd, case when Start_time_of_session >= 2400 then 1 else 0 end,
					date(
							cast(cast(Date_of_Activity_DB1/10000 as int) as char(4)) || '-' ||
							cast(cast(Date_of_Activity_DB1/100 as int) - cast(Date_of_Activity_DB1/10000 as int) * 100 as varchar(2)) || '-' ||
							cast(Date_of_Activity_DB1 - cast(Date_of_Activity_DB1/100 as int) * 100 as varchar(2))
							)
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
			,date(
					cast(cast(Date_of_Recording_DB1/10000 as int) as char(4)) || '-' ||
					cast(cast(Date_of_Recording_DB1/100 as int) - cast(Date_of_Recording_DB1/10000 as int) * 100 as varchar(2)) || '-' ||
					cast(Date_of_Recording_DB1 - cast(Date_of_Recording_DB1/100 as int) * 100 as varchar(2))
					)
			-- Date_of_Recording_DB1: A barb day can go over 24:00. In this case we need to increase the date by 1
			,dateadd(dd, case when Start_time_of_recording >= 2400 then 1 else 0 end,
					date(
							cast(cast(Date_of_Recording_DB1/10000 as int) as char(4)) || '-' ||
							cast(cast(Date_of_Recording_DB1/100 as int) - cast(Date_of_Recording_DB1/10000 as int) * 100 as varchar(2)) || '-' ||
							cast(Date_of_Recording_DB1 - cast(Date_of_Recording_DB1/100 as int) * 100 as varchar(2))
							)
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
	from	BARB_PVF_Viewing_Record_Panel_Members
	
	--- Update the Start and end session and recording timestamps. Its easier to deal with the barb time conversion as an update rather then as in the insert statement
update BARB_PVF06_Viewing_Record_Panel_Members
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

update BARB_PVF06_Viewing_Record_Panel_Members
        set End_time_of_session = dateadd(mi, Duration_of_session, Start_time_of_session) -- ERROR should be dateadd(mi, Duration_of_session -1, Start_time_of_session) data rectified later so that code will need to change if fixed here


update BARB_PVF06_Viewing_Record_Panel_Members
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
	
	
	
	commit
	MESSAGE cast(now() as timestamp)||' | @ M03.1: Table BARB_PVF_Viewing_Record_Panel_Members LOADED' TO CLIENT
	
	
-- BARB_Panel_Demographic_Data_TV_Sets_Characteristics	
	
	if object_id('BARB_Panel_Demographic_Data_TV_Sets_Characteristics') is not null
		truncate table BARB_Panel_Demographic_Data_TV_Sets_Characteristics
		
	commit
	
	INSERT	INTO BARB_Panel_Demographic_Data_TV_Sets_Characteristics
	SELECT 	@file_creation_date
			,@file_creation_time
			,@file_type
			,@file_version
			,@filename
			,CAST(substr(imported_text,1,2) AS Int)
			,CAST(substr(imported_text,3,7) AS Int)
			,CAST(substr(imported_text,10,8) AS Int)
			,CAST(substr(imported_text,18,1) AS Int)
			,CAST(substr(imported_text,19,2) AS Int)
			,CAST(substr(imported_text,21,1) AS Int)
			,CAST(substr(imported_text,22,1) AS Int)
			,CAST(substr(imported_text,23,1) AS Int)
			,CAST(substr(imported_text,24,1) AS Int)
			,CAST(substr(imported_text,25,1) AS Int)
			,CAST(substr(imported_text,26,1) AS Int)
			,CAST(substr(imported_text,27,1) AS Int)
			,CAST(substr(imported_text,28,1) AS Int)
			,CAST(substr(imported_text,35,1) AS Int)
			,CAST(substr(imported_text,36,1) AS Int)
			,CAST(substr(imported_text,37,1) AS Int)
			,CAST(substr(imported_text,38,1) AS Int)
			,CAST(substr(imported_text,39,1) AS Int)
			,CAST(substr(imported_text,40,1) AS Int)
			,CAST(substr(imported_text,41,3) AS Int)
			,CAST(substr(imported_text,44,3) AS Int)
			,CAST(substr(imported_text,47,3) AS Int)
			,CAST(substr(imported_text,50,3) AS Int)
			,CAST(substr(imported_text,53,3) AS Int)
			,CAST(substr(imported_text,56,3) AS Int)
			,CAST(substr(imported_text,59,3) AS Int)
			,CAST(substr(imported_text,62,3) AS Int)
			,CAST(substr(imported_text,65,3) AS Int)
			,CAST(substr(imported_text,68,3) AS Int)
	FROM 	PI_BARB_IMPORT
	WHERE 	substr(imported_text,1,2) = '03' -- this identifies what table is being refered to see BARB2010DSP01 Version 3.7C - Panel Viewing File.pdf

	commit
	MESSAGE cast(now() as timestamp)||' | @ M03.1: Table BARB_Panel_Demographic_Data_TV_Sets_Characteristics LOADED' TO CLIENT
	

-- BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories

	if object_id('BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories') is not null
		truncate table BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories
		
	commit

	insert	into BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories
	select  file_creation_date 
			,file_creation_time 
			,file_type
			,file_version
			,filename
			,Record_Type
			,Household_Number
			,Person_Number
			,Reporting_Panel_Code
			,date(
					cast(cast(Date_of_Activity_DB1/10000 as int) as char(4)) || '-' ||
					cast(cast(Date_of_Activity_DB1/100 as int) - cast(Date_of_Activity_DB1/10000 as int) * 100 as varchar(2)) || '-' ||
					cast(Date_of_Activity_DB1 - cast(Date_of_Activity_DB1/100 as int) * 100 as varchar(2))
				)
			,Response_Code
			,Processing_Weight / 10
			,Adults_Commercial_TV_Viewing_Sextile
			,ABC1_Adults_Commercial_TV_Viewing_Sextile
			,Adults_Total_Viewing_Sextile
			,ABC1_Adults_Total_Viewing_Sextile
			,Adults_16_34_Commercial_TV_Viewing_Sextile
			,Adults_16_34_Total_Viewing_Sextile 
	from    BARB_Panel_Member_Responses_Weights_and_Viewing_Categories
	
	commit
	MESSAGE cast(now() as timestamp)||' | @ M03.1: Table BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories LOADED' TO CLIENT


	
	
	MESSAGE cast(now() as timestamp)||' | @ M03.1: Extracting Data DONE' TO CLIENT
	
----------------------------
-- M03.2 - Returning Results	
----------------------------

	MESSAGE cast(now() as timestamp)||' | M03 Finished' TO CLIENT
	
end;

commit;
grant execute on v289_m03_barb_data_extraction to vespa_group_low_security;
commit;

