create or replace procedure ${SQLFILE_ARG001}.v289_m00_initialisation_sv (@processing_date DATE = NULL)
AS
BEGIN
	message convert(TIMESTAMP, now()) || ' | Begining M00.0 - Initialising Environment' TO client message convert(TIMESTAMP, now()) || ' | @ M00.0: Initialising Environment DONE' TO client message convert(TIMESTAMP, now()) || ' | Begining M00.1 - Initialising Tables' TO client

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('PI_BARB_IMPORT_SV')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.1: Creating Table PI_BARB_import_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.PI_BARB_import_sv (
			imported_text VARCHAR(200) NULL DEFAULT NULL
			,
			)

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.PI_BARB_import_sv
			TO vespa_group_low_security

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.1: Creating Table PI_BARB_import_sv DONE' TO client
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('BARB_Individual_Panel_Member_Details_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.1: Creating Table BARB_Individual_Panel_Member_Details_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.BARB_Individual_Panel_Member_Details_sv (
			file_creation_date DATE NULL DEFAULT NULL
			,file_creation_time TIME NULL DEFAULT NULL
			,file_type VARCHAR(12) NULL DEFAULT NULL
			,file_version INTEGER NULL DEFAULT NULL
			,filename VARCHAR(13) NULL DEFAULT NULL
			,Record_type INTEGER NULL DEFAULT NULL
			,Household_number INTEGER NULL DEFAULT NULL
			,Date_valid_for_DB1 INTEGER NULL DEFAULT NULL
			,Person_membership_status INTEGER NULL DEFAULT NULL
			,Person_number INTEGER NULL DEFAULT NULL
			,Sex_code INTEGER NULL DEFAULT NULL
			,Date_of_birth INTEGER NULL DEFAULT NULL
			,Marital_status INTEGER NULL DEFAULT NULL
			,Household_status INTEGER NULL DEFAULT NULL
			,Working_status INTEGER NULL DEFAULT NULL
			,Terminal_age_of_education INTEGER NULL DEFAULT NULL
			,Welsh_Language_code INTEGER NULL DEFAULT NULL
			,Gaelic_language_code INTEGER NULL DEFAULT NULL
			,Dependency_of_Children INTEGER NULL DEFAULT NULL
			,Life_stage_12_classifications INTEGER NULL DEFAULT NULL
			,Ethnic_Origin INTEGER NULL DEFAULT NULL
			,
			) message convert (
			TIMESTAMP
			,now()
			) || ' | @ M00.1: Creating Table BARB_Individual_Panel_Member_Details_sv DONE' TO client

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.BARB_Individual_Panel_Member_Details_sv
			TO vespa_group_low_security

		COMMIT WORK
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('BARB_Panel_Member_Responses_Weights_and_Viewing_Categories_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.1: Creating Table BARB_Panel_Member_Responses_Weights_and_Viewing_Categories_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.BARB_Panel_Member_Responses_Weights_and_Viewing_Categories_sv (
			file_creation_date DATE NULL DEFAULT NULL
			,file_creation_time TIME NULL DEFAULT NULL
			,file_type VARCHAR(12) NULL DEFAULT NULL
			,file_version INTEGER NULL DEFAULT NULL
			,filename VARCHAR(13) NULL DEFAULT NULL
			,Record_Type INTEGER NULL DEFAULT NULL
			,Household_Number INTEGER NULL DEFAULT NULL
			,Person_Number INTEGER NULL DEFAULT NULL
			,Reporting_Panel_Code INTEGER NULL DEFAULT NULL
			,Date_of_Activity_DB1 INTEGER NULL DEFAULT NULL
			,Response_Code INTEGER NULL DEFAULT NULL
			,Processing_Weight INTEGER NULL DEFAULT NULL
			,Adults_Commercial_TV_Viewing_Sextile INTEGER NULL DEFAULT NULL
			,ABC1_Adults_Commercial_TV_Viewing_Sextile INTEGER NULL DEFAULT NULL
			,Adults_Total_Viewing_Sextile INTEGER NULL DEFAULT NULL
			,ABC1_Adults_Total_Viewing_Sextile INTEGER NULL DEFAULT NULL
			,Adults_16_34_Commercial_TV_Viewing_Sextile INTEGER NULL DEFAULT NULL
			,Adults_16_34_Total_Viewing_Sextile INTEGER NULL DEFAULT NULL
			,
			) message convert (
			TIMESTAMP
			,now()
			) || ' | @ M00.1: Creating Table BARB_Panel_Member_Responses_Weights_and_Viewing_Categories_sv DONE' TO client

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.BARB_Panel_Member_Responses_Weights_and_Viewing_Categories_sv
			TO vespa_group_low_security

		COMMIT WORK
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('BARB_PVF_Viewing_Record_Panel_Members_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.1: Creating Table BARB_PVF_Viewing_Record_Panel_Members_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.BARB_PVF_Viewing_Record_Panel_Members_sv (
			file_creation_date DATE NULL DEFAULT NULL
			,file_creation_time TIME NULL DEFAULT NULL
			,file_type VARCHAR(12) NULL DEFAULT NULL
			,file_version INTEGER NULL DEFAULT NULL
			,filename VARCHAR(13) NULL DEFAULT NULL
			,Record_type INTEGER NULL DEFAULT NULL
			,Household_number INTEGER NULL DEFAULT NULL
			,Date_of_Activity_DB1 INTEGER NULL DEFAULT NULL
			,Set_number INTEGER NULL DEFAULT NULL
			,Start_time_of_session INTEGER NULL DEFAULT NULL
			,Duration_of_session INTEGER NULL DEFAULT NULL
			,Session_activity_type INTEGER NULL DEFAULT NULL
			,Playback_type VARCHAR(1) NULL DEFAULT NULL
			,DB1_Station_Code INTEGER NULL DEFAULT NULL
			,Viewing_platform INTEGER NULL DEFAULT NULL
			,Date_of_Recording_DB1 INTEGER NULL DEFAULT NULL
			,Start_time_of_recording INTEGER NULL DEFAULT NULL
			,Person_1_viewing INTEGER NULL DEFAULT NULL
			,Person_2_viewing INTEGER NULL DEFAULT NULL
			,Person_3_viewing INTEGER NULL DEFAULT NULL
			,Person_4_viewing INTEGER NULL DEFAULT NULL
			,Person_5_viewing INTEGER NULL DEFAULT NULL
			,Person_6_viewing INTEGER NULL DEFAULT NULL
			,Person_7_viewing INTEGER NULL DEFAULT NULL
			,Person_8_viewing INTEGER NULL DEFAULT NULL
			,Person_9_viewing INTEGER NULL DEFAULT NULL
			,Person_10_viewing INTEGER NULL DEFAULT NULL
			,Person_11_viewing INTEGER NULL DEFAULT NULL
			,Person_12_viewing INTEGER NULL DEFAULT NULL
			,Person_13_viewing INTEGER NULL DEFAULT NULL
			,Person_14_viewing INTEGER NULL DEFAULT NULL
			,Person_15_viewing INTEGER NULL DEFAULT NULL
			,Person_16_viewing INTEGER NULL DEFAULT NULL
			,Interactive_Bar_Code_Identifier INTEGER NULL DEFAULT NULL
			,VOD_Indicator INTEGER NULL DEFAULT NULL
			,VOD_Provider INTEGER NULL DEFAULT NULL
			,VOD_Service INTEGER NULL DEFAULT NULL
			,VOD_Type INTEGER NULL DEFAULT NULL
			,Device_in_use INTEGER NULL DEFAULT NULL
			,
			) message convert (
			TIMESTAMP
			,now()
			) || ' | @ M00.1: Creating Table BARB_PVF_Viewing_Record_Panel_Members_sv DONE' TO client

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.BARB_PVF_Viewing_Record_Panel_Members_sv
			TO vespa_group_low_security

		COMMIT WORK
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('BARB_PVF06_Viewing_Record_Panel_Members_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.1: Creating Table BARB_PVF06_Viewing_Record_Panel_Members_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.BARB_PVF06_Viewing_Record_Panel_Members_sv (
			id_row BIGINT NOT NULL DEFAULT autoincrement
			,file_creation_date DATE NULL DEFAULT NULL
			,file_creation_time TIME NULL DEFAULT NULL
			,file_type VARCHAR(12) NULL DEFAULT NULL
			,file_version INTEGER NULL DEFAULT NULL
			,filename VARCHAR(13) NULL DEFAULT NULL
			,Record_type INTEGER NULL DEFAULT NULL
			,Household_number INTEGER NULL DEFAULT NULL
			,Barb_date_of_activity DATE NULL DEFAULT NULL
			,Actual_date_of_session DATE NULL DEFAULT NULL
			,Set_number INTEGER NULL DEFAULT NULL
			,Start_time_of_session_text VARCHAR(6) NULL DEFAULT NULL
			,Start_time_of_session TIMESTAMP NULL DEFAULT NULL
			,End_time_of_session TIMESTAMP NULL DEFAULT NULL
			,Duration_of_session INTEGER NULL DEFAULT NULL
			,Session_activity_type INTEGER NULL DEFAULT NULL
			,Playback_type VARCHAR(1) NULL DEFAULT NULL
			,DB1_Station_Code INTEGER NULL DEFAULT NULL
			,Viewing_platform INTEGER NULL DEFAULT NULL
			,Barb_date_of_recording DATE NULL DEFAULT NULL
			,Actual_Date_of_Recording DATE NULL DEFAULT NULL
			,Start_time_of_recording_text VARCHAR(6) NULL DEFAULT NULL
			,Start_time_of_recording TIMESTAMP NULL DEFAULT NULL
			,Person_1_viewing INTEGER NULL DEFAULT NULL
			,Person_2_viewing INTEGER NULL DEFAULT NULL
			,Person_3_viewing INTEGER NULL DEFAULT NULL
			,Person_4_viewing INTEGER NULL DEFAULT NULL
			,Person_5_viewing INTEGER NULL DEFAULT NULL
			,Person_6_viewing INTEGER NULL DEFAULT NULL
			,Person_7_viewing INTEGER NULL DEFAULT NULL
			,Person_8_viewing INTEGER NULL DEFAULT NULL
			,Person_9_viewing INTEGER NULL DEFAULT NULL
			,Person_10_viewing INTEGER NULL DEFAULT NULL
			,Person_11_viewing INTEGER NULL DEFAULT NULL
			,Person_12_viewing INTEGER NULL DEFAULT NULL
			,Person_13_viewing INTEGER NULL DEFAULT NULL
			,Person_14_viewing INTEGER NULL DEFAULT NULL
			,Person_15_viewing INTEGER NULL DEFAULT NULL
			,Person_16_viewing INTEGER NULL DEFAULT NULL
			,Interactive_Bar_Code_Identifier INTEGER NULL DEFAULT NULL
			,VOD_Indicator INTEGER NULL DEFAULT NULL
			,VOD_Provider INTEGER NULL DEFAULT NULL
			,VOD_Service INTEGER NULL DEFAULT NULL
			,VOD_Type INTEGER NULL DEFAULT NULL
			,Device_in_use INTEGER NULL DEFAULT NULL
			,PRIMARY KEY (id_row)
			,
			)

		COMMIT WORK

		CREATE hg INDEX ind_household_number ON ${SQLFILE_ARG001}.BARB_PVF06_Viewing_Record_Panel_Members_sv (household_number)

		CREATE hg INDEX ind_db1 ON ${SQLFILE_ARG001}.BARB_PVF06_Viewing_Record_Panel_Members_sv (db1_station_code)

		CREATE hg INDEX ind_start ON ${SQLFILE_ARG001}.BARB_PVF06_Viewing_Record_Panel_Members_sv (Start_time_of_session)

		CREATE hg INDEX ind_end ON ${SQLFILE_ARG001}.BARB_PVF06_Viewing_Record_Panel_Members_sv (End_time_of_session)

		CREATE hg INDEX ind_date ON ${SQLFILE_ARG001}.BARB_PVF06_Viewing_Record_Panel_Members_sv (Barb_date_of_activity) message convert (
			TIMESTAMP
			,now()
			) || ' | @ M00.1: Creating Table BARB_PVF06_Viewing_Record_Panel_Members_sv DONE' TO client

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.BARB_PVF06_Viewing_Record_Panel_Members_sv
			TO vespa_group_low_security

		COMMIT WORK
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('BARB_Panel_Demographic_Data_TV_Sets_Characteristics_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.1: Creating Table BARB_Panel_Demographic_Data_TV_Sets_Characteristics_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.BARB_Panel_Demographic_Data_TV_Sets_Characteristics_sv (
			file_creation_date DATE NULL DEFAULT NULL
			,file_creation_time TIME NULL DEFAULT NULL
			,file_type VARCHAR(12) NULL DEFAULT NULL
			,file_version INTEGER NULL DEFAULT NULL
			,filename VARCHAR(13) NULL DEFAULT NULL
			,Record_Type INTEGER NULL DEFAULT NULL
			,Household_number INTEGER NULL DEFAULT NULL
			,Date_Valid_for_DB1 INTEGER NULL DEFAULT NULL
			,Set_Membership_Status INTEGER NULL DEFAULT NULL
			,Set_number INTEGER NULL DEFAULT NULL
			,Teletext INTEGER NULL DEFAULT NULL
			,Main_Location INTEGER NULL DEFAULT NULL
			,Analogue_Terrestrial INTEGER NULL DEFAULT NULL
			,Digital_Terrestrial INTEGER NULL DEFAULT NULL
			,Analogue_Satellite INTEGER NULL DEFAULT NULL
			,Digital_Satellite INTEGER NULL DEFAULT NULL
			,Analogue_Cable INTEGER NULL DEFAULT NULL
			,Digital_Cable INTEGER NULL DEFAULT NULL
			,VCR_present INTEGER NULL DEFAULT NULL
			,Sky_PVR_present INTEGER NULL DEFAULT NULL
			,Other_PVR_present INTEGER NULL DEFAULT NULL
			,DVD_Player_only_present INTEGER NULL DEFAULT NULL
			,DVD_Recorder_present INTEGER NULL DEFAULT NULL
			,HD_reception INTEGER NULL DEFAULT NULL
			,Reception_Capability_Code1 INTEGER NULL DEFAULT NULL
			,Reception_Capability_Code2 INTEGER NULL DEFAULT NULL
			,Reception_Capability_Code3 INTEGER NULL DEFAULT NULL
			,Reception_Capability_Code4 INTEGER NULL DEFAULT NULL
			,Reception_Capability_Code5 INTEGER NULL DEFAULT NULL
			,Reception_Capability_Code6 INTEGER NULL DEFAULT NULL
			,Reception_Capability_Code7 INTEGER NULL DEFAULT NULL
			,Reception_Capability_Code8 INTEGER NULL DEFAULT NULL
			,Reception_Capability_Code9 INTEGER NULL DEFAULT NULL
			,Reception_Capability_Code10 INTEGER NULL DEFAULT NULL
			,
			) message convert (
			TIMESTAMP
			,now()
			) || ' | @ M00.1: Creating Table BARB_Panel_Demographic_Data_TV_Sets_Characteristics_sv DONE' TO client

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.BARB_Panel_Demographic_Data_TV_Sets_Characteristics_sv
			TO vespa_group_low_security

		COMMIT WORK
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('BARB_PVF04_Individual_Member_Details_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.1: Creating Table BARB_PVF04_Individual_Member_Details_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.BARB_PVF04_Individual_Member_Details_sv (
			file_creation_date DATE NULL DEFAULT NULL
			,file_creation_time TIME NULL DEFAULT NULL
			,file_type VARCHAR(12) NULL DEFAULT NULL
			,file_version INTEGER NULL DEFAULT NULL
			,filename VARCHAR(13) NULL DEFAULT NULL
			,Record_type INTEGER NULL DEFAULT NULL
			,Household_number INTEGER NULL DEFAULT NULL
			,Date_valid_for_DB1 DATE NULL DEFAULT NULL
			,Person_membership_status INTEGER NULL DEFAULT NULL
			,Person_number INTEGER NULL DEFAULT NULL
			,Sex_code INTEGER NULL DEFAULT NULL
			,Date_of_birth DATE NULL DEFAULT NULL
			,Marital_status INTEGER NULL DEFAULT NULL
			,Household_status INTEGER NULL DEFAULT NULL
			,Working_status INTEGER NULL DEFAULT NULL
			,Terminal_age_of_education INTEGER NULL DEFAULT NULL
			,Welsh_Language_code INTEGER NULL DEFAULT NULL
			,Gaelic_language_code INTEGER NULL DEFAULT NULL
			,Dependency_of_Children INTEGER NULL DEFAULT NULL
			,Life_stage_12_classifications INTEGER NULL DEFAULT NULL
			,Ethnic_Origin INTEGER NULL DEFAULT NULL
			,
			)

		COMMIT WORK

		CREATE hg INDEX ind_hhd ON ${SQLFILE_ARG001}.BARB_PVF04_Individual_Member_Details_sv (Household_number)

		CREATE lf INDEX ind_person ON ${SQLFILE_ARG001}.BARB_PVF04_Individual_Member_Details_sv (person_number)

		CREATE lf INDEX ind_create ON ${SQLFILE_ARG001}.BARB_PVF04_Individual_Member_Details_sv (file_creation_date) message convert (
			TIMESTAMP
			,now()
			) || ' | @ M00.1: Creating Table BARB_PVF04_Individual_Member_Details_sv DONE' TO client

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.BARB_PVF04_Individual_Member_Details_sv
			TO vespa_group_low_security

		COMMIT WORK
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('V289_M04_CHANNEL_GENRE_LOOKUP')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.1: Creating Table V289_M04_Channel_Genre_Lookup' TO client

		CREATE TABLE ${SQLFILE_ARG001}.V289_M04_Channel_Genre_Lookup (
			channel_genre VARCHAR(20) NOT NULL DEFAULT NULL
			,programme_genre VARCHAR(20) NOT NULL DEFAULT NULL
			,
			)

		COMMIT WORK

		INSERT INTO V289_M04_Channel_Genre_Lookup (
			channel_genre
			,programme_genre
			)
		VALUES (
			'Kids'
			,'Children'
			)
			,(
			'Entertainment'
			,'Entertainment'
			)
			,(
			'Lifestyle & Culture'
			,'Entertainment'
			)
			,(
			'Movies'
			,'Movies'
			)
			,(
			'Music'
			,'Music & Radio'
			)
			,(
			'Radio'
			,'Music & Radio'
			)
			,(
			'Documentaries'
			,'News & Documentaries'
			)
			,(
			'News'
			,'News & Documentaries'
			)
			,(
			'Specialist'
			,'Specialist'
			)
			,(
			'Sport'
			,'Sports'
			)
			,(
			'N/a'
			,'na'
			)

		COMMIT WORK

		CREATE UNIQUE lf INDEX ind_channel ON ${SQLFILE_ARG001}.V289_M04_Channel_Genre_Lookup (channel_genre)

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.1: Creating Table V289_M04_Channel_Genre_Lookup DONE' TO client

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.V289_M04_Channel_Genre_Lookup
			TO vespa_group_low_security

		COMMIT WORK
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('V289_PIV_Grouped_Segments_desc_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.1: Creating Table V289_PIV_Grouped_Segments_desc_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.V289_PIV_Grouped_Segments_desc_sv (
			row_id INTEGER NOT NULL DEFAULT autoincrement
			,channel_pack VARCHAR(40) NOT NULL DEFAULT NULL
			,daypart VARCHAR(30) NOT NULL DEFAULT NULL
			,Genre VARCHAR(20) NOT NULL DEFAULT NULL
			,segment_id INTEGER NOT NULL DEFAULT NULL
			,active_flag BIT NOT NULL DEFAULT 0
			,Updated_On DATETIME NOT NULL DEFAULT TIMESTAMP
			,Updated_By VARCHAR(40) NOT NULL DEFAULT '${SQLFILE_ARG001}'
			,segment_name INTEGER NULL DEFAULT NULL
			,
			)

		INSERT INTO V289_PIV_Grouped_Segments_desc_sv (
			segment_id
			,segment_name
			,channel_pack
			,daypart
			,Genre
			)
		VALUES (
			2
			,2
			,'Unknown'
			,'breakfast'
			,'Children'
			)
			,(
			2
			,2
			,'Unknown'
			,'early prime'
			,'Children'
			)
			,(
			2
			,2
			,'Unknown'
			,'late night'
			,'Children'
			)
			,(
			2
			,2
			,'Unknown'
			,'lunch'
			,'Children'
			)
			,(
			2
			,2
			,'Unknown'
			,'morning'
			,'Children'
			)
			,(
			2
			,2
			,'Unknown'
			,'night'
			,'Children'
			)
			,(
			2
			,2
			,'Unknown'
			,'prime'
			,'Children'
			)
			,(
			2
			,2
			,'Unknown'
			,'breakfast'
			,'Entertainment'
			)
			,(
			2
			,2
			,'Unknown'
			,'early prime'
			,'Entertainment'
			)
			,(
			2
			,2
			,'Unknown'
			,'late night'
			,'Entertainment'
			)
			,(
			2
			,2
			,'Unknown'
			,'lunch'
			,'Entertainment'
			)
			,(
			2
			,2
			,'Unknown'
			,'morning'
			,'Entertainment'
			)
			,(
			2
			,2
			,'Unknown'
			,'night'
			,'Entertainment'
			)
			,(
			2
			,2
			,'Unknown'
			,'prime'
			,'Entertainment'
			)
			,(
			2
			,2
			,'Unknown'
			,'breakfast'
			,'Movies'
			)
			,(
			2
			,2
			,'Unknown'
			,'early prime'
			,'Movies'
			)
			,(
			2
			,2
			,'Unknown'
			,'late night'
			,'Movies'
			)
			,(
			2
			,2
			,'Unknown'
			,'lunch'
			,'Movies'
			)
			,(
			2
			,2
			,'Unknown'
			,'morning'
			,'Movies'
			)
			,(
			2
			,2
			,'Unknown'
			,'night'
			,'Movies'
			)
			,(
			2
			,2
			,'Unknown'
			,'prime'
			,'Movies'
			)
			,(
			2
			,2
			,'Unknown'
			,'breakfast'
			,'Music & Radio'
			)
			,(
			2
			,2
			,'Unknown'
			,'early prime'
			,'Music & Radio'
			)
			,(
			2
			,2
			,'Unknown'
			,'late night'
			,'Music & Radio'
			)
			,(
			2
			,2
			,'Unknown'
			,'lunch'
			,'Music & Radio'
			)
			,(
			2
			,2
			,'Unknown'
			,'morning'
			,'Music & Radio'
			)
			,(
			2
			,2
			,'Unknown'
			,'night'
			,'Music & Radio'
			)
			,(
			2
			,2
			,'Unknown'
			,'prime'
			,'Music & Radio'
			)
			,(
			2
			,2
			,'Unknown'
			,'breakfast'
			,'News & Documentaries'
			)
			,(
			2
			,2
			,'Unknown'
			,'early prime'
			,'News & Documentaries'
			)
			,(
			2
			,2
			,'Unknown'
			,'late night'
			,'News & Documentaries'
			)
			,(
			2
			,2
			,'Unknown'
			,'lunch'
			,'News & Documentaries'
			)
			,(
			2
			,2
			,'Unknown'
			,'morning'
			,'News & Documentaries'
			)
			,(
			2
			,2
			,'Unknown'
			,'night'
			,'News & Documentaries'
			)
			,(
			2
			,2
			,'Unknown'
			,'prime'
			,'News & Documentaries'
			)
			,(
			2
			,2
			,'Unknown'
			,'breakfast'
			,'Specialist'
			)
			,(
			2
			,2
			,'Unknown'
			,'early prime'
			,'Specialist'
			)
			,(
			2
			,2
			,'Unknown'
			,'late night'
			,'Specialist'
			)
			,(
			2
			,2
			,'Unknown'
			,'lunch'
			,'Specialist'
			)
			,(
			2
			,2
			,'Unknown'
			,'morning'
			,'Specialist'
			)
			,(
			2
			,2
			,'Unknown'
			,'night'
			,'Specialist'
			)
			,(
			2
			,2
			,'Unknown'
			,'prime'
			,'Specialist'
			)
			,(
			2
			,2
			,'Unknown'
			,'breakfast'
			,'Sports'
			)
			,(
			2
			,2
			,'Unknown'
			,'early prime'
			,'Sports'
			)
			,(
			2
			,2
			,'Unknown'
			,'late night'
			,'Sports'
			)
			,(
			2
			,2
			,'Unknown'
			,'lunch'
			,'Sports'
			)
			,(
			2
			,2
			,'Unknown'
			,'morning'
			,'Sports'
			)
			,(
			2
			,2
			,'Unknown'
			,'night'
			,'Sports'
			)
			,(
			2
			,2
			,'Unknown'
			,'prime'
			,'Sports'
			)
			,(
			2
			,2
			,'Unknown'
			,'breakfast'
			,'Unknown'
			)
			,(
			2
			,2
			,'Unknown'
			,'early prime'
			,'Unknown'
			)
			,(
			2
			,2
			,'Unknown'
			,'late night'
			,'Unknown'
			)
			,(
			2
			,2
			,'Unknown'
			,'lunch'
			,'Unknown'
			)
			,(
			2
			,2
			,'Unknown'
			,'morning'
			,'Unknown'
			)
			,(
			2
			,2
			,'Unknown'
			,'night'
			,'Unknown'
			)
			,(
			2
			,2
			,'Unknown'
			,'prime'
			,'Unknown'
			)
			,(
			1
			,1
			,'Diginets'
			,'breakfast'
			,'Children'
			)
			,(
			1
			,1
			,'Diginets non-commercial'
			,'breakfast'
			,'Children'
			)
			,(
			1
			,1
			,'Other'
			,'breakfast'
			,'Children'
			)
			,(
			1
			,1
			,'Other non-commercial'
			,'breakfast'
			,'Children'
			)
			,(
			1
			,1
			,'Terrestrial'
			,'breakfast'
			,'Children'
			)
			,(
			1
			,1
			,'Terrestrial non-commercial'
			,'breakfast'
			,'Children'
			)
			,(
			9
			,9
			,'Diginets'
			,'early prime'
			,'Children'
			)
			,(
			9
			,9
			,'Diginets non-commercial'
			,'early prime'
			,'Children'
			)
			,(
			9
			,9
			,'Other'
			,'early prime'
			,'Children'
			)
			,(
			9
			,9
			,'Other non-commercial'
			,'early prime'
			,'Children'
			)
			,(
			9
			,9
			,'Terrestrial'
			,'early prime'
			,'Children'
			)
			,(
			9
			,9
			,'Terrestrial non-commercial'
			,'early prime'
			,'Children'
			)
			,(
			17
			,17
			,'Diginets'
			,'late night'
			,'Children'
			)
			,(
			17
			,17
			,'Diginets non-commercial'
			,'late night'
			,'Children'
			)
			,(
			17
			,17
			,'Other'
			,'late night'
			,'Children'
			)
			,(
			17
			,17
			,'Other non-commercial'
			,'late night'
			,'Children'
			)
			,(
			17
			,17
			,'Terrestrial'
			,'late night'
			,'Children'
			)
			,(
			17
			,17
			,'Terrestrial non-commercial'
			,'late night'
			,'Children'
			)
			,(
			25
			,25
			,'Diginets'
			,'lunch'
			,'Children'
			)
			,(
			25
			,25
			,'Diginets non-commercial'
			,'lunch'
			,'Children'
			)
			,(
			25
			,25
			,'Other'
			,'lunch'
			,'Children'
			)
			,(
			25
			,25
			,'Other non-commercial'
			,'lunch'
			,'Children'
			)
			,(
			25
			,25
			,'Terrestrial'
			,'lunch'
			,'Children'
			)
			,(
			25
			,25
			,'Terrestrial non-commercial'
			,'lunch'
			,'Children'
			)
			,(
			33
			,33
			,'Diginets'
			,'morning'
			,'Children'
			)
			,(
			33
			,33
			,'Diginets non-commercial'
			,'morning'
			,'Children'
			)
			,(
			33
			,33
			,'Other'
			,'morning'
			,'Children'
			)
			,(
			33
			,33
			,'Other non-commercial'
			,'morning'
			,'Children'
			)
			,(
			33
			,33
			,'Terrestrial'
			,'morning'
			,'Children'
			)
			,(
			33
			,33
			,'Terrestrial non-commercial'
			,'morning'
			,'Children'
			)
			,(
			0
			,0
			,'Diginets'
			,'na'
			,'Children'
			)
			,(
			0
			,0
			,'Diginets non-commercial'
			,'na'
			,'Children'
			)
			,(
			0
			,0
			,'Other'
			,'na'
			,'Children'
			)
			,(
			0
			,0
			,'Other non-commercial'
			,'na'
			,'Children'
			)
			,(
			0
			,0
			,'Terrestrial'
			,'na'
			,'Children'
			)
			,(
			0
			,0
			,'Terrestrial non-commercial'
			,'na'
			,'Children'
			)
			,(
			41
			,41
			,'Diginets'
			,'night'
			,'Children'
			)
			,(
			41
			,41
			,'Diginets non-commercial'
			,'night'
			,'Children'
			)
			,(
			41
			,41
			,'Other'
			,'night'
			,'Children'
			)
			,(
			41
			,41
			,'Other non-commercial'
			,'night'
			,'Children'
			)
			,(
			41
			,41
			,'Terrestrial'
			,'night'
			,'Children'
			)
			,(
			41
			,41
			,'Terrestrial non-commercial'
			,'night'
			,'Children'
			)
			,(
			49
			,49
			,'Diginets'
			,'prime'
			,'Children'
			)
			,(
			49
			,49
			,'Diginets non-commercial'
			,'prime'
			,'Children'
			)
			,(
			49
			,49
			,'Other'
			,'prime'
			,'Children'
			)
			,(
			49
			,49
			,'Other non-commercial'
			,'prime'
			,'Children'
			)
			,(
			49
			,49
			,'Terrestrial'
			,'prime'
			,'Children'
			)
			,(
			49
			,49
			,'Terrestrial non-commercial'
			,'prime'
			,'Children'
			)
			,(
			56
			,56
			,'Diginets'
			,'breakfast'
			,'Entertainment'
			)
			,(
			56
			,56
			,'Diginets non-commercial'
			,'breakfast'
			,'Entertainment'
			)
			,(
			56
			,56
			,'Other'
			,'breakfast'
			,'Entertainment'
			)
			,(
			56
			,56
			,'Other non-commercial'
			,'breakfast'
			,'Entertainment'
			)
			,(
			56
			,56
			,'Terrestrial'
			,'breakfast'
			,'Entertainment'
			)
			,(
			56
			,56
			,'Terrestrial non-commercial'
			,'breakfast'
			,'Entertainment'
			)
			,(
			10
			,10
			,'Diginets'
			,'early prime'
			,'Entertainment'
			)
			,(
			10
			,10
			,'Diginets non-commercial'
			,'early prime'
			,'Entertainment'
			)
			,(
			10
			,10
			,'Other'
			,'early prime'
			,'Entertainment'
			)
			,(
			10
			,10
			,'Other non-commercial'
			,'early prime'
			,'Entertainment'
			)
			,(
			10
			,10
			,'Terrestrial'
			,'early prime'
			,'Entertainment'
			)
			,(
			10
			,10
			,'Terrestrial non-commercial'
			,'early prime'
			,'Entertainment'
			)
			,(
			18
			,18
			,'Diginets'
			,'late night'
			,'Entertainment'
			)
			,(
			18
			,18
			,'Diginets non-commercial'
			,'late night'
			,'Entertainment'
			)
			,(
			18
			,18
			,'Other'
			,'late night'
			,'Entertainment'
			)
			,(
			18
			,18
			,'Other non-commercial'
			,'late night'
			,'Entertainment'
			)
			,(
			18
			,18
			,'Terrestrial'
			,'late night'
			,'Entertainment'
			)
			,(
			18
			,18
			,'Terrestrial non-commercial'
			,'late night'
			,'Entertainment'
			)
			,(
			26
			,26
			,'Diginets'
			,'lunch'
			,'Entertainment'
			)
			,(
			26
			,26
			,'Diginets non-commercial'
			,'lunch'
			,'Entertainment'
			)
			,(
			26
			,26
			,'Other'
			,'lunch'
			,'Entertainment'
			)
			,(
			26
			,26
			,'Other non-commercial'
			,'lunch'
			,'Entertainment'
			)
			,(
			26
			,26
			,'Terrestrial'
			,'lunch'
			,'Entertainment'
			)
			,(
			26
			,26
			,'Terrestrial non-commercial'
			,'lunch'
			,'Entertainment'
			)
			,(
			34
			,34
			,'Diginets'
			,'morning'
			,'Entertainment'
			)
			,(
			34
			,34
			,'Diginets non-commercial'
			,'morning'
			,'Entertainment'
			)
			,(
			34
			,34
			,'Other'
			,'morning'
			,'Entertainment'
			)
			,(
			34
			,34
			,'Other non-commercial'
			,'morning'
			,'Entertainment'
			)
			,(
			34
			,34
			,'Terrestrial'
			,'morning'
			,'Entertainment'
			)
			,(
			34
			,34
			,'Terrestrial non-commercial'
			,'morning'
			,'Entertainment'
			)
			,(
			0
			,0
			,'Diginets'
			,'na'
			,'Entertainment'
			)
			,(
			0
			,0
			,'Diginets non-commercial'
			,'na'
			,'Entertainment'
			)
			,(
			0
			,0
			,'Other'
			,'na'
			,'Entertainment'
			)
			,(
			0
			,0
			,'Other non-commercial'
			,'na'
			,'Entertainment'
			)
			,(
			0
			,0
			,'Terrestrial'
			,'na'
			,'Entertainment'
			)
			,(
			0
			,0
			,'Terrestrial non-commercial'
			,'na'
			,'Entertainment'
			)
			,(
			42
			,42
			,'Diginets'
			,'night'
			,'Entertainment'
			)
			,(
			42
			,42
			,'Diginets non-commercial'
			,'night'
			,'Entertainment'
			)
			,(
			42
			,42
			,'Other'
			,'night'
			,'Entertainment'
			)
			,(
			42
			,42
			,'Other non-commercial'
			,'night'
			,'Entertainment'
			)
			,(
			42
			,42
			,'Terrestrial'
			,'night'
			,'Entertainment'
			)
			,(
			42
			,42
			,'Terrestrial non-commercial'
			,'night'
			,'Entertainment'
			)
			,(
			50
			,50
			,'Diginets'
			,'prime'
			,'Entertainment'
			)
			,(
			50
			,50
			,'Diginets non-commercial'
			,'prime'
			,'Entertainment'
			)
			,(
			50
			,50
			,'Other'
			,'prime'
			,'Entertainment'
			)
			,(
			50
			,50
			,'Other non-commercial'
			,'prime'
			,'Entertainment'
			)
			,(
			50
			,50
			,'Terrestrial'
			,'prime'
			,'Entertainment'
			)
			,(
			50
			,50
			,'Terrestrial non-commercial'
			,'prime'
			,'Entertainment'
			)
			,(
			3
			,3
			,'Diginets'
			,'breakfast'
			,'Movies'
			)
			,(
			3
			,3
			,'Diginets non-commercial'
			,'breakfast'
			,'Movies'
			)
			,(
			3
			,3
			,'Other'
			,'breakfast'
			,'Movies'
			)
			,(
			3
			,3
			,'Other non-commercial'
			,'breakfast'
			,'Movies'
			)
			,(
			3
			,3
			,'Terrestrial'
			,'breakfast'
			,'Movies'
			)
			,(
			3
			,3
			,'Terrestrial non-commercial'
			,'breakfast'
			,'Movies'
			)
			,(
			11
			,11
			,'Diginets'
			,'early prime'
			,'Movies'
			)
			,(
			11
			,11
			,'Diginets non-commercial'
			,'early prime'
			,'Movies'
			)
			,(
			11
			,11
			,'Other'
			,'early prime'
			,'Movies'
			)
			,(
			11
			,11
			,'Other non-commercial'
			,'early prime'
			,'Movies'
			)
			,(
			11
			,11
			,'Terrestrial'
			,'early prime'
			,'Movies'
			)
			,(
			11
			,11
			,'Terrestrial non-commercial'
			,'early prime'
			,'Movies'
			)
			,(
			19
			,19
			,'Diginets'
			,'late night'
			,'Movies'
			)
			,(
			19
			,19
			,'Diginets non-commercial'
			,'late night'
			,'Movies'
			)
			,(
			19
			,19
			,'Other'
			,'late night'
			,'Movies'
			)
			,(
			19
			,19
			,'Other non-commercial'
			,'late night'
			,'Movies'
			)
			,(
			19
			,19
			,'Terrestrial'
			,'late night'
			,'Movies'
			)
			,(
			19
			,19
			,'Terrestrial non-commercial'
			,'late night'
			,'Movies'
			)
			,(
			27
			,27
			,'Diginets'
			,'lunch'
			,'Movies'
			)
			,(
			27
			,27
			,'Diginets non-commercial'
			,'lunch'
			,'Movies'
			)
			,(
			27
			,27
			,'Other'
			,'lunch'
			,'Movies'
			)
			,(
			27
			,27
			,'Other non-commercial'
			,'lunch'
			,'Movies'
			)
			,(
			27
			,27
			,'Terrestrial'
			,'lunch'
			,'Movies'
			)
			,(
			27
			,27
			,'Terrestrial non-commercial'
			,'lunch'
			,'Movies'
			)
			,(
			3
			,3
			,'Diginets'
			,'morning'
			,'Movies'
			)
			,(
			3
			,3
			,'Diginets non-commercial'
			,'morning'
			,'Movies'
			)
			,(
			3
			,3
			,'Other'
			,'morning'
			,'Movies'
			)
			,(
			3
			,3
			,'Other non-commercial'
			,'morning'
			,'Movies'
			)
			,(
			3
			,3
			,'Terrestrial'
			,'morning'
			,'Movies'
			)
			,(
			3
			,3
			,'Terrestrial non-commercial'
			,'morning'
			,'Movies'
			)
			,(
			0
			,0
			,'Diginets'
			,'na'
			,'Movies'
			)
			,(
			0
			,0
			,'Diginets non-commercial'
			,'na'
			,'Movies'
			)
			,(
			0
			,0
			,'Other'
			,'na'
			,'Movies'
			)
			,(
			0
			,0
			,'Other non-commercial'
			,'na'
			,'Movies'
			)
			,(
			0
			,0
			,'Terrestrial'
			,'na'
			,'Movies'
			)
			,(
			0
			,0
			,'Terrestrial non-commercial'
			,'na'
			,'Movies'
			)
			,(
			43
			,43
			,'Diginets'
			,'night'
			,'Movies'
			)
			,(
			43
			,43
			,'Diginets non-commercial'
			,'night'
			,'Movies'
			)
			,(
			43
			,43
			,'Other'
			,'night'
			,'Movies'
			)
			,(
			43
			,43
			,'Other non-commercial'
			,'night'
			,'Movies'
			)
			,(
			43
			,43
			,'Terrestrial'
			,'night'
			,'Movies'
			)
			,(
			43
			,43
			,'Terrestrial non-commercial'
			,'night'
			,'Movies'
			)
			,(
			51
			,51
			,'Diginets'
			,'prime'
			,'Movies'
			)
			,(
			51
			,51
			,'Diginets non-commercial'
			,'prime'
			,'Movies'
			)
			,(
			51
			,51
			,'Other'
			,'prime'
			,'Movies'
			)
			,(
			51
			,51
			,'Other non-commercial'
			,'prime'
			,'Movies'
			)
			,(
			51
			,51
			,'Terrestrial'
			,'prime'
			,'Movies'
			)
			,(
			51
			,51
			,'Terrestrial non-commercial'
			,'prime'
			,'Movies'
			)
			,(
			4
			,4
			,'Diginets'
			,'breakfast'
			,'Music & Radio'
			)
			,(
			4
			,4
			,'Diginets non-commercial'
			,'breakfast'
			,'Music & Radio'
			)
			,(
			4
			,4
			,'Other'
			,'breakfast'
			,'Music & Radio'
			)
			,(
			4
			,4
			,'Other non-commercial'
			,'breakfast'
			,'Music & Radio'
			)
			,(
			4
			,4
			,'Terrestrial'
			,'breakfast'
			,'Music & Radio'
			)
			,(
			4
			,4
			,'Terrestrial non-commercial'
			,'breakfast'
			,'Music & Radio'
			)
			,(
			12
			,12
			,'Diginets'
			,'early prime'
			,'Music & Radio'
			)
			,(
			12
			,12
			,'Diginets non-commercial'
			,'early prime'
			,'Music & Radio'
			)
			,(
			12
			,12
			,'Other'
			,'early prime'
			,'Music & Radio'
			)
			,(
			12
			,12
			,'Other non-commercial'
			,'early prime'
			,'Music & Radio'
			)
			,(
			12
			,12
			,'Terrestrial'
			,'early prime'
			,'Music & Radio'
			)
			,(
			12
			,12
			,'Terrestrial non-commercial'
			,'early prime'
			,'Music & Radio'
			)
			,(
			20
			,20
			,'Diginets'
			,'late night'
			,'Music & Radio'
			)
			,(
			20
			,20
			,'Diginets non-commercial'
			,'late night'
			,'Music & Radio'
			)
			,(
			20
			,20
			,'Other'
			,'late night'
			,'Music & Radio'
			)
			,(
			20
			,20
			,'Other non-commercial'
			,'late night'
			,'Music & Radio'
			)
			,(
			20
			,20
			,'Terrestrial'
			,'late night'
			,'Music & Radio'
			)
			,(
			20
			,20
			,'Terrestrial non-commercial'
			,'late night'
			,'Music & Radio'
			)
			,(
			28
			,28
			,'Diginets'
			,'lunch'
			,'Music & Radio'
			)
			,(
			28
			,28
			,'Diginets non-commercial'
			,'lunch'
			,'Music & Radio'
			)
			,(
			28
			,28
			,'Other'
			,'lunch'
			,'Music & Radio'
			)
			,(
			28
			,28
			,'Other non-commercial'
			,'lunch'
			,'Music & Radio'
			)
			,(
			28
			,28
			,'Terrestrial'
			,'lunch'
			,'Music & Radio'
			)
			,(
			28
			,28
			,'Terrestrial non-commercial'
			,'lunch'
			,'Music & Radio'
			)
			,(
			4
			,4
			,'Diginets'
			,'morning'
			,'Music & Radio'
			)
			,(
			4
			,4
			,'Diginets non-commercial'
			,'morning'
			,'Music & Radio'
			)
			,(
			4
			,4
			,'Other'
			,'morning'
			,'Music & Radio'
			)
			,(
			4
			,4
			,'Other non-commercial'
			,'morning'
			,'Music & Radio'
			)
			,(
			4
			,4
			,'Terrestrial'
			,'morning'
			,'Music & Radio'
			)
			,(
			4
			,4
			,'Terrestrial non-commercial'
			,'morning'
			,'Music & Radio'
			)
			,(
			0
			,0
			,'Diginets'
			,'na'
			,'Music & Radio'
			)
			,(
			0
			,0
			,'Diginets non-commercial'
			,'na'
			,'Music & Radio'
			)
			,(
			0
			,0
			,'Other'
			,'na'
			,'Music & Radio'
			)
			,(
			0
			,0
			,'Other non-commercial'
			,'na'
			,'Music & Radio'
			)
			,(
			0
			,0
			,'Terrestrial'
			,'na'
			,'Music & Radio'
			)
			,(
			0
			,0
			,'Terrestrial non-commercial'
			,'na'
			,'Music & Radio'
			)
			,(
			44
			,44
			,'Diginets'
			,'night'
			,'Music & Radio'
			)
			,(
			44
			,44
			,'Diginets non-commercial'
			,'night'
			,'Music & Radio'
			)
			,(
			44
			,44
			,'Other'
			,'night'
			,'Music & Radio'
			)
			,(
			44
			,44
			,'Other non-commercial'
			,'night'
			,'Music & Radio'
			)
			,(
			44
			,44
			,'Terrestrial'
			,'night'
			,'Music & Radio'
			)
			,(
			44
			,44
			,'Terrestrial non-commercial'
			,'night'
			,'Music & Radio'
			)
			,(
			52
			,52
			,'Diginets'
			,'prime'
			,'Music & Radio'
			)
			,(
			52
			,52
			,'Diginets non-commercial'
			,'prime'
			,'Music & Radio'
			)
			,(
			52
			,52
			,'Other'
			,'prime'
			,'Music & Radio'
			)
			,(
			52
			,52
			,'Other non-commercial'
			,'prime'
			,'Music & Radio'
			)
			,(
			52
			,52
			,'Terrestrial'
			,'prime'
			,'Music & Radio'
			)
			,(
			52
			,52
			,'Terrestrial non-commercial'
			,'prime'
			,'Music & Radio'
			)
			,(
			0
			,0
			,'Diginets'
			,'breakfast'
			,'na'
			)
			,(
			0
			,0
			,'Diginets non-commercial'
			,'breakfast'
			,'na'
			)
			,(
			0
			,0
			,'Other'
			,'breakfast'
			,'na'
			)
			,(
			0
			,0
			,'Other non-commercial'
			,'breakfast'
			,'na'
			)
			,(
			0
			,0
			,'Terrestrial'
			,'breakfast'
			,'na'
			)
			,(
			0
			,0
			,'Terrestrial non-commercial'
			,'breakfast'
			,'na'
			)
			,(
			0
			,0
			,'Diginets'
			,'early prime'
			,'na'
			)
			,(
			0
			,0
			,'Diginets non-commercial'
			,'early prime'
			,'na'
			)
			,(
			0
			,0
			,'Other'
			,'early prime'
			,'na'
			)
			,(
			0
			,0
			,'Other non-commercial'
			,'early prime'
			,'na'
			)
			,(
			0
			,0
			,'Terrestrial'
			,'early prime'
			,'na'
			)
			,(
			0
			,0
			,'Terrestrial non-commercial'
			,'early prime'
			,'na'
			)
			,(
			0
			,0
			,'Diginets'
			,'late night'
			,'na'
			)
			,(
			0
			,0
			,'Diginets non-commercial'
			,'late night'
			,'na'
			)
			,(
			0
			,0
			,'Other'
			,'late night'
			,'na'
			)
			,(
			0
			,0
			,'Other non-commercial'
			,'late night'
			,'na'
			)
			,(
			0
			,0
			,'Terrestrial'
			,'late night'
			,'na'
			)
			,(
			0
			,0
			,'Terrestrial non-commercial'
			,'late night'
			,'na'
			)
			,(
			0
			,0
			,'Diginets'
			,'lunch'
			,'na'
			)
			,(
			0
			,0
			,'Diginets non-commercial'
			,'lunch'
			,'na'
			)
			,(
			0
			,0
			,'Other'
			,'lunch'
			,'na'
			)
			,(
			0
			,0
			,'Other non-commercial'
			,'lunch'
			,'na'
			)
			,(
			0
			,0
			,'Terrestrial'
			,'lunch'
			,'na'
			)
			,(
			0
			,0
			,'Terrestrial non-commercial'
			,'lunch'
			,'na'
			)
			,(
			0
			,0
			,'Diginets'
			,'morning'
			,'na'
			)
			,(
			0
			,0
			,'Diginets non-commercial'
			,'morning'
			,'na'
			)
			,(
			0
			,0
			,'Other'
			,'morning'
			,'na'
			)
			,(
			0
			,0
			,'Other non-commercial'
			,'morning'
			,'na'
			)
			,(
			0
			,0
			,'Terrestrial'
			,'morning'
			,'na'
			)
			,(
			0
			,0
			,'Terrestrial non-commercial'
			,'morning'
			,'na'
			)
			,(
			0
			,0
			,'Diginets'
			,'na'
			,'na'
			)
			,(
			0
			,0
			,'Diginets non-commercial'
			,'na'
			,'na'
			)
			,(
			0
			,0
			,'Other'
			,'na'
			,'na'
			)
			,(
			0
			,0
			,'Other non-commercial'
			,'na'
			,'na'
			)
			,(
			0
			,0
			,'Terrestrial'
			,'na'
			,'na'
			)
			,(
			0
			,0
			,'Terrestrial non-commercial'
			,'na'
			,'na'
			)
			,(
			0
			,0
			,'Diginets'
			,'night'
			,'na'
			)
			,(
			0
			,0
			,'Diginets non-commercial'
			,'night'
			,'na'
			)
			,(
			0
			,0
			,'Other'
			,'night'
			,'na'
			)
			,(
			0
			,0
			,'Other non-commercial'
			,'night'
			,'na'
			)
			,(
			0
			,0
			,'Terrestrial'
			,'night'
			,'na'
			)
			,(
			0
			,0
			,'Terrestrial non-commercial'
			,'night'
			,'na'
			)
			,(
			0
			,0
			,'Diginets'
			,'prime'
			,'na'
			)
			,(
			0
			,0
			,'Diginets non-commercial'
			,'prime'
			,'na'
			)
			,(
			0
			,0
			,'Other'
			,'prime'
			,'na'
			)
			,(
			0
			,0
			,'Other non-commercial'
			,'prime'
			,'na'
			)
			,(
			0
			,0
			,'Terrestrial'
			,'prime'
			,'na'
			)
			,(
			0
			,0
			,'Terrestrial non-commercial'
			,'prime'
			,'na'
			)
			,(
			5
			,5
			,'Diginets'
			,'breakfast'
			,'News & Documentaries'
			)
			,(
			5
			,5
			,'Diginets non-commercial'
			,'breakfast'
			,'News & Documentaries'
			)
			,(
			5
			,5
			,'Other'
			,'breakfast'
			,'News & Documentaries'
			)
			,(
			5
			,5
			,'Other non-commercial'
			,'breakfast'
			,'News & Documentaries'
			)
			,(
			5
			,5
			,'Terrestrial'
			,'breakfast'
			,'News & Documentaries'
			)
			,(
			5
			,5
			,'Terrestrial non-commercial'
			,'breakfast'
			,'News & Documentaries'
			)
			,(
			13
			,13
			,'Diginets'
			,'early prime'
			,'News & Documentaries'
			)
			,(
			13
			,13
			,'Diginets non-commercial'
			,'early prime'
			,'News & Documentaries'
			)
			,(
			13
			,13
			,'Other'
			,'early prime'
			,'News & Documentaries'
			)
			,(
			13
			,13
			,'Other non-commercial'
			,'early prime'
			,'News & Documentaries'
			)
			,(
			13
			,13
			,'Terrestrial'
			,'early prime'
			,'News & Documentaries'
			)
			,(
			13
			,13
			,'Terrestrial non-commercial'
			,'early prime'
			,'News & Documentaries'
			)
			,(
			21
			,21
			,'Diginets'
			,'late night'
			,'News & Documentaries'
			)
			,(
			21
			,21
			,'Diginets non-commercial'
			,'late night'
			,'News & Documentaries'
			)
			,(
			21
			,21
			,'Other'
			,'late night'
			,'News & Documentaries'
			)
			,(
			21
			,21
			,'Other non-commercial'
			,'late night'
			,'News & Documentaries'
			)
			,(
			21
			,21
			,'Terrestrial'
			,'late night'
			,'News & Documentaries'
			)
			,(
			21
			,21
			,'Terrestrial non-commercial'
			,'late night'
			,'News & Documentaries'
			)
			,(
			29
			,29
			,'Diginets'
			,'lunch'
			,'News & Documentaries'
			)
			,(
			29
			,29
			,'Diginets non-commercial'
			,'lunch'
			,'News & Documentaries'
			)
			,(
			29
			,29
			,'Other'
			,'lunch'
			,'News & Documentaries'
			)
			,(
			29
			,29
			,'Other non-commercial'
			,'lunch'
			,'News & Documentaries'
			)
			,(
			29
			,29
			,'Terrestrial'
			,'lunch'
			,'News & Documentaries'
			)
			,(
			29
			,29
			,'Terrestrial non-commercial'
			,'lunch'
			,'News & Documentaries'
			)
			,(
			37
			,37
			,'Diginets'
			,'morning'
			,'News & Documentaries'
			)
			,(
			37
			,37
			,'Diginets non-commercial'
			,'morning'
			,'News & Documentaries'
			)
			,(
			37
			,37
			,'Other'
			,'morning'
			,'News & Documentaries'
			)
			,(
			37
			,37
			,'Other non-commercial'
			,'morning'
			,'News & Documentaries'
			)
			,(
			37
			,37
			,'Terrestrial'
			,'morning'
			,'News & Documentaries'
			)
			,(
			37
			,37
			,'Terrestrial non-commercial'
			,'morning'
			,'News & Documentaries'
			)
			,(
			0
			,0
			,'Diginets'
			,'na'
			,'News & Documentaries'
			)
			,(
			0
			,0
			,'Diginets non-commercial'
			,'na'
			,'News & Documentaries'
			)
			,(
			0
			,0
			,'Other'
			,'na'
			,'News & Documentaries'
			)
			,(
			0
			,0
			,'Other non-commercial'
			,'na'
			,'News & Documentaries'
			)
			,(
			0
			,0
			,'Terrestrial'
			,'na'
			,'News & Documentaries'
			)
			,(
			0
			,0
			,'Terrestrial non-commercial'
			,'na'
			,'News & Documentaries'
			)
			,(
			45
			,45
			,'Diginets'
			,'night'
			,'News & Documentaries'
			)
			,(
			45
			,45
			,'Diginets non-commercial'
			,'night'
			,'News & Documentaries'
			)
			,(
			45
			,45
			,'Other'
			,'night'
			,'News & Documentaries'
			)
			,(
			45
			,45
			,'Other non-commercial'
			,'night'
			,'News & Documentaries'
			)
			,(
			45
			,45
			,'Terrestrial'
			,'night'
			,'News & Documentaries'
			)
			,(
			45
			,45
			,'Terrestrial non-commercial'
			,'night'
			,'News & Documentaries'
			)
			,(
			53
			,53
			,'Diginets'
			,'prime'
			,'News & Documentaries'
			)
			,(
			53
			,53
			,'Diginets non-commercial'
			,'prime'
			,'News & Documentaries'
			)
			,(
			53
			,53
			,'Other'
			,'prime'
			,'News & Documentaries'
			)
			,(
			53
			,53
			,'Other non-commercial'
			,'prime'
			,'News & Documentaries'
			)
			,(
			53
			,53
			,'Terrestrial'
			,'prime'
			,'News & Documentaries'
			)
			,(
			53
			,53
			,'Terrestrial non-commercial'
			,'prime'
			,'News & Documentaries'
			)
			,(
			6
			,6
			,'Diginets'
			,'breakfast'
			,'Specialist'
			)
			,(
			6
			,6
			,'Diginets non-commercial'
			,'breakfast'
			,'Specialist'
			)
			,(
			6
			,6
			,'Other'
			,'breakfast'
			,'Specialist'
			)
			,(
			6
			,6
			,'Other non-commercial'
			,'breakfast'
			,'Specialist'
			)
			,(
			6
			,6
			,'Terrestrial'
			,'breakfast'
			,'Specialist'
			)
			,(
			6
			,6
			,'Terrestrial non-commercial'
			,'breakfast'
			,'Specialist'
			)
			,(
			6
			,6
			,'Diginets'
			,'early prime'
			,'Specialist'
			)
			,(
			6
			,6
			,'Diginets non-commercial'
			,'early prime'
			,'Specialist'
			)
			,(
			6
			,6
			,'Other'
			,'early prime'
			,'Specialist'
			)
			,(
			6
			,6
			,'Other non-commercial'
			,'early prime'
			,'Specialist'
			)
			,(
			6
			,6
			,'Terrestrial'
			,'early prime'
			,'Specialist'
			)
			,(
			6
			,6
			,'Terrestrial non-commercial'
			,'early prime'
			,'Specialist'
			)
			,(
			6
			,6
			,'Diginets'
			,'late night'
			,'Specialist'
			)
			,(
			6
			,6
			,'Diginets non-commercial'
			,'late night'
			,'Specialist'
			)
			,(
			6
			,6
			,'Other'
			,'late night'
			,'Specialist'
			)
			,(
			6
			,6
			,'Other non-commercial'
			,'late night'
			,'Specialist'
			)
			,(
			6
			,6
			,'Terrestrial'
			,'late night'
			,'Specialist'
			)
			,(
			6
			,6
			,'Terrestrial non-commercial'
			,'late night'
			,'Specialist'
			)
			,(
			6
			,6
			,'Diginets'
			,'lunch'
			,'Specialist'
			)
			,(
			6
			,6
			,'Diginets non-commercial'
			,'lunch'
			,'Specialist'
			)
			,(
			6
			,6
			,'Other'
			,'lunch'
			,'Specialist'
			)
			,(
			6
			,6
			,'Other non-commercial'
			,'lunch'
			,'Specialist'
			)
			,(
			6
			,6
			,'Terrestrial'
			,'lunch'
			,'Specialist'
			)
			,(
			6
			,6
			,'Terrestrial non-commercial'
			,'lunch'
			,'Specialist'
			)
			,(
			6
			,6
			,'Diginets'
			,'morning'
			,'Specialist'
			)
			,(
			6
			,6
			,'Diginets non-commercial'
			,'morning'
			,'Specialist'
			)
			,(
			6
			,6
			,'Other'
			,'morning'
			,'Specialist'
			)
			,(
			6
			,6
			,'Other non-commercial'
			,'morning'
			,'Specialist'
			)
			,(
			6
			,6
			,'Terrestrial'
			,'morning'
			,'Specialist'
			)
			,(
			6
			,6
			,'Terrestrial non-commercial'
			,'morning'
			,'Specialist'
			)
			,(
			0
			,0
			,'Diginets'
			,'na'
			,'Specialist'
			)
			,(
			0
			,0
			,'Diginets non-commercial'
			,'na'
			,'Specialist'
			)
			,(
			0
			,0
			,'Other'
			,'na'
			,'Specialist'
			)
			,(
			0
			,0
			,'Other non-commercial'
			,'na'
			,'Specialist'
			)
			,(
			0
			,0
			,'Terrestrial'
			,'na'
			,'Specialist'
			)
			,(
			0
			,0
			,'Terrestrial non-commercial'
			,'na'
			,'Specialist'
			)
			,(
			6
			,6
			,'Diginets'
			,'night'
			,'Specialist'
			)
			,(
			6
			,6
			,'Diginets non-commercial'
			,'night'
			,'Specialist'
			)
			,(
			6
			,6
			,'Other'
			,'night'
			,'Specialist'
			)
			,(
			6
			,6
			,'Other non-commercial'
			,'night'
			,'Specialist'
			)
			,(
			6
			,6
			,'Terrestrial'
			,'night'
			,'Specialist'
			)
			,(
			6
			,6
			,'Terrestrial non-commercial'
			,'night'
			,'Specialist'
			)
			,(
			6
			,6
			,'Diginets'
			,'prime'
			,'Specialist'
			)
			,(
			6
			,6
			,'Diginets non-commercial'
			,'prime'
			,'Specialist'
			)
			,(
			6
			,6
			,'Other'
			,'prime'
			,'Specialist'
			)
			,(
			6
			,6
			,'Other non-commercial'
			,'prime'
			,'Specialist'
			)
			,(
			6
			,6
			,'Terrestrial'
			,'prime'
			,'Specialist'
			)
			,(
			6
			,6
			,'Terrestrial non-commercial'
			,'prime'
			,'Specialist'
			)
			,(
			7
			,7
			,'Diginets'
			,'breakfast'
			,'Sports'
			)
			,(
			7
			,7
			,'Diginets non-commercial'
			,'breakfast'
			,'Sports'
			)
			,(
			7
			,7
			,'Other'
			,'breakfast'
			,'Sports'
			)
			,(
			7
			,7
			,'Other non-commercial'
			,'breakfast'
			,'Sports'
			)
			,(
			7
			,7
			,'Terrestrial'
			,'breakfast'
			,'Sports'
			)
			,(
			7
			,7
			,'Terrestrial non-commercial'
			,'breakfast'
			,'Sports'
			)
			,(
			15
			,15
			,'Diginets'
			,'early prime'
			,'Sports'
			)
			,(
			15
			,15
			,'Diginets non-commercial'
			,'early prime'
			,'Sports'
			)
			,(
			15
			,15
			,'Other'
			,'early prime'
			,'Sports'
			)
			,(
			15
			,15
			,'Other non-commercial'
			,'early prime'
			,'Sports'
			)
			,(
			15
			,15
			,'Terrestrial'
			,'early prime'
			,'Sports'
			)
			,(
			15
			,15
			,'Terrestrial non-commercial'
			,'early prime'
			,'Sports'
			)
			,(
			23
			,23
			,'Diginets'
			,'late night'
			,'Sports'
			)
			,(
			23
			,23
			,'Diginets non-commercial'
			,'late night'
			,'Sports'
			)
			,(
			23
			,23
			,'Other'
			,'late night'
			,'Sports'
			)
			,(
			23
			,23
			,'Other non-commercial'
			,'late night'
			,'Sports'
			)
			,(
			23
			,23
			,'Terrestrial'
			,'late night'
			,'Sports'
			)
			,(
			23
			,23
			,'Terrestrial non-commercial'
			,'late night'
			,'Sports'
			)
			,(
			7
			,7
			,'Diginets'
			,'lunch'
			,'Sports'
			)
			,(
			7
			,7
			,'Diginets non-commercial'
			,'lunch'
			,'Sports'
			)
			,(
			7
			,7
			,'Other'
			,'lunch'
			,'Sports'
			)
			,(
			7
			,7
			,'Other non-commercial'
			,'lunch'
			,'Sports'
			)
			,(
			7
			,7
			,'Terrestrial'
			,'lunch'
			,'Sports'
			)
			,(
			7
			,7
			,'Terrestrial non-commercial'
			,'lunch'
			,'Sports'
			)
			,(
			7
			,7
			,'Diginets'
			,'morning'
			,'Sports'
			)
			,(
			7
			,7
			,'Diginets non-commercial'
			,'morning'
			,'Sports'
			)
			,(
			7
			,7
			,'Other'
			,'morning'
			,'Sports'
			)
			,(
			7
			,7
			,'Other non-commercial'
			,'morning'
			,'Sports'
			)
			,(
			7
			,7
			,'Terrestrial'
			,'morning'
			,'Sports'
			)
			,(
			7
			,7
			,'Terrestrial non-commercial'
			,'morning'
			,'Sports'
			)
			,(
			0
			,0
			,'Diginets'
			,'na'
			,'Sports'
			)
			,(
			0
			,0
			,'Diginets non-commercial'
			,'na'
			,'Sports'
			)
			,(
			0
			,0
			,'Other'
			,'na'
			,'Sports'
			)
			,(
			0
			,0
			,'Other non-commercial'
			,'na'
			,'Sports'
			)
			,(
			0
			,0
			,'Terrestrial'
			,'na'
			,'Sports'
			)
			,(
			0
			,0
			,'Terrestrial non-commercial'
			,'na'
			,'Sports'
			)
			,(
			23
			,23
			,'Diginets'
			,'night'
			,'Sports'
			)
			,(
			23
			,23
			,'Diginets non-commercial'
			,'night'
			,'Sports'
			)
			,(
			23
			,23
			,'Other'
			,'night'
			,'Sports'
			)
			,(
			23
			,23
			,'Other non-commercial'
			,'night'
			,'Sports'
			)
			,(
			23
			,23
			,'Terrestrial'
			,'night'
			,'Sports'
			)
			,(
			23
			,23
			,'Terrestrial non-commercial'
			,'night'
			,'Sports'
			)
			,(
			55
			,55
			,'Diginets'
			,'prime'
			,'Sports'
			)
			,(
			55
			,55
			,'Diginets non-commercial'
			,'prime'
			,'Sports'
			)
			,(
			55
			,55
			,'Other'
			,'prime'
			,'Sports'
			)
			,(
			55
			,55
			,'Other non-commercial'
			,'prime'
			,'Sports'
			)
			,(
			55
			,55
			,'Terrestrial'
			,'prime'
			,'Sports'
			)
			,(
			55
			,55
			,'Terrestrial non-commercial'
			,'prime'
			,'Sports'
			)
			,(
			8
			,8
			,'Diginets'
			,'breakfast'
			,'Unknown'
			)
			,(
			8
			,8
			,'Diginets non-commercial'
			,'breakfast'
			,'Unknown'
			)
			,(
			8
			,8
			,'Other'
			,'breakfast'
			,'Unknown'
			)
			,(
			8
			,8
			,'Other non-commercial'
			,'breakfast'
			,'Unknown'
			)
			,(
			8
			,8
			,'Terrestrial'
			,'breakfast'
			,'Unknown'
			)
			,(
			8
			,8
			,'Terrestrial non-commercial'
			,'breakfast'
			,'Unknown'
			)
			,(
			8
			,8
			,'Diginets'
			,'early prime'
			,'Unknown'
			)
			,(
			8
			,8
			,'Diginets non-commercial'
			,'early prime'
			,'Unknown'
			)
			,(
			8
			,8
			,'Other'
			,'early prime'
			,'Unknown'
			)
			,(
			8
			,8
			,'Other non-commercial'
			,'early prime'
			,'Unknown'
			)
			,(
			8
			,8
			,'Terrestrial'
			,'early prime'
			,'Unknown'
			)
			,(
			8
			,8
			,'Terrestrial non-commercial'
			,'early prime'
			,'Unknown'
			)
			,(
			8
			,8
			,'Diginets'
			,'late night'
			,'Unknown'
			)
			,(
			8
			,8
			,'Diginets non-commercial'
			,'late night'
			,'Unknown'
			)
			,(
			8
			,8
			,'Other'
			,'late night'
			,'Unknown'
			)
			,(
			8
			,8
			,'Other non-commercial'
			,'late night'
			,'Unknown'
			)
			,(
			8
			,8
			,'Terrestrial'
			,'late night'
			,'Unknown'
			)
			,(
			8
			,8
			,'Terrestrial non-commercial'
			,'late night'
			,'Unknown'
			)
			,(
			8
			,8
			,'Diginets'
			,'lunch'
			,'Unknown'
			)
			,(
			8
			,8
			,'Diginets non-commercial'
			,'lunch'
			,'Unknown'
			)
			,(
			8
			,8
			,'Other'
			,'lunch'
			,'Unknown'
			)
			,(
			8
			,8
			,'Other non-commercial'
			,'lunch'
			,'Unknown'
			)
			,(
			8
			,8
			,'Terrestrial'
			,'lunch'
			,'Unknown'
			)
			,(
			8
			,8
			,'Terrestrial non-commercial'
			,'lunch'
			,'Unknown'
			)
			,(
			8
			,8
			,'Diginets'
			,'morning'
			,'Unknown'
			)
			,(
			8
			,8
			,'Diginets non-commercial'
			,'morning'
			,'Unknown'
			)
			,(
			8
			,8
			,'Other'
			,'morning'
			,'Unknown'
			)
			,(
			8
			,8
			,'Other non-commercial'
			,'morning'
			,'Unknown'
			)
			,(
			8
			,8
			,'Terrestrial'
			,'morning'
			,'Unknown'
			)
			,(
			8
			,8
			,'Terrestrial non-commercial'
			,'morning'
			,'Unknown'
			)
			,(
			8
			,8
			,'Diginets'
			,'na'
			,'Unknown'
			)
			,(
			8
			,8
			,'Diginets non-commercial'
			,'na'
			,'Unknown'
			)
			,(
			8
			,8
			,'Other'
			,'na'
			,'Unknown'
			)
			,(
			8
			,8
			,'Other non-commercial'
			,'na'
			,'Unknown'
			)
			,(
			8
			,8
			,'Terrestrial'
			,'na'
			,'Unknown'
			)
			,(
			8
			,8
			,'Terrestrial non-commercial'
			,'na'
			,'Unknown'
			)
			,(
			8
			,8
			,'Diginets'
			,'night'
			,'Unknown'
			)
			,(
			8
			,8
			,'Diginets non-commercial'
			,'night'
			,'Unknown'
			)
			,(
			8
			,8
			,'Other'
			,'night'
			,'Unknown'
			)
			,(
			8
			,8
			,'Other non-commercial'
			,'night'
			,'Unknown'
			)
			,(
			8
			,8
			,'Terrestrial'
			,'night'
			,'Unknown'
			)
			,(
			8
			,8
			,'Terrestrial non-commercial'
			,'night'
			,'Unknown'
			)
			,(
			8
			,8
			,'Diginets'
			,'prime'
			,'Unknown'
			)
			,(
			8
			,8
			,'Diginets non-commercial'
			,'prime'
			,'Unknown'
			)
			,(
			8
			,8
			,'Other'
			,'prime'
			,'Unknown'
			)
			,(
			8
			,8
			,'Other non-commercial'
			,'prime'
			,'Unknown'
			)
			,(
			8
			,8
			,'Terrestrial'
			,'prime'
			,'Unknown'
			)
			,(
			8
			,8
			,'Terrestrial non-commercial'
			,'prime'
			,'Unknown'
			)

		COMMIT WORK

		CREATE lf INDEX id1 ON ${SQLFILE_ARG001}.V289_PIV_Grouped_Segments_desc_sv (segment_id)

		CREATE lf INDEX id2 ON ${SQLFILE_ARG001}.V289_PIV_Grouped_Segments_desc_sv (channel_pack)

		CREATE lf INDEX id3 ON ${SQLFILE_ARG001}.V289_PIV_Grouped_Segments_desc_sv (daypart)

		CREATE lf INDEX id4 ON ${SQLFILE_ARG001}.V289_PIV_Grouped_Segments_desc_sv (Genre)

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.1: Creating Table V289_PIV_Grouped_Segments_desc_sv DONE' TO client

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.V289_PIV_Grouped_Segments_desc_sv
			TO vespa_group_low_security
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('V289_M08_SKY_HH_composition_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.1: Creating Table V289_M08_SKY_HH_composition_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.V289_M08_SKY_HH_composition_sv (
			row_id INTEGER NOT NULL DEFAULT autoincrement
			,account_number VARCHAR(20) NOT NULL
			,cb_key_household BIGINT NOT NULL
			,exp_cb_key_db_person BIGINT NULL
			,cb_key_individual BIGINT NULL
			,cb_key_db_person BIGINT NULL
			,cb_address_line_1 VARCHAR(200) NULL
			,HH_person_number TINYINT NULL
			,person_gender CHAR(1) NULL
			,person_age TINYINT NULL
			,person_ageband VARCHAR(10) NULL
			,exp_person_head TINYINT NULL
			,person_income NUMERIC NULL
			,person_head CHAR(1) NULL DEFAULT '0'
			,household_size TINYINT NULL
			,demographic_ID TINYINT NULL
			,non_viewer TINYINT NULL DEFAULT 0
			,viewer_hhsize TINYINT NULL
			,nonviewer_household TINYINT NULL DEFAULT 0
			,panel_flag BIT NOT NULL DEFAULT 0
			,randd DECIMAL(15, 14) NULL
			,Updated_On DATETIME NOT NULL DEFAULT TIMESTAMP
			,Updated_By VARCHAR(30) NOT NULL DEFAULT '${SQLFILE_ARG001}'
			,
			)

		COMMIT WORK

		CREATE hg INDEX hg1 ON ${SQLFILE_ARG001}.V289_M08_SKY_HH_composition_sv (account_number)

		CREATE hg INDEX hg2 ON ${SQLFILE_ARG001}.V289_M08_SKY_HH_composition_sv (cb_key_household)

		CREATE hg INDEX hg3 ON ${SQLFILE_ARG001}.V289_M08_SKY_HH_composition_sv (exp_cb_key_db_person)

		CREATE hg INDEX hg4 ON ${SQLFILE_ARG001}.V289_M08_SKY_HH_composition_sv (cb_address_line_1)

		CREATE hg INDEX hg5 ON ${SQLFILE_ARG001}.V289_M08_SKY_HH_composition_sv (row_id)

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.1: Creating Table V289_M08_SKY_HH_composition_sv DONE' TO client

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.V289_M08_SKY_HH_composition_sv
			TO vespa_group_low_security
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('V289_M08_SKY_HH_view_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.1: Creating Table V289_M08_SKY_HH_view_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.V289_M08_SKY_HH_view_sv (
			account_number VARCHAR(20) NOT NULL
			,cb_key_household BIGINT NOT NULL
			,cb_address_line_1 VARCHAR(200) NULL
			,HH_composition TINYINT NULL
			,Children_count TINYINT NULL DEFAULT 0
			,non_matching_flag BIT NULL DEFAULT 0
			,edited_add_flag BIT NOT NULL DEFAULT 0
			,panel_flag BIT NOT NULL DEFAULT 0
			,h_0_4_flag BIT NULL DEFAULT 0
			,h_5_11_flag BIT NULL DEFAULT 0
			,h_12_17_flag BIT NULL DEFAULT 0
			,Updated_On DATETIME NOT NULL DEFAULT TIMESTAMP
			,Updated_By VARCHAR(30) NOT NULL DEFAULT '${SQLFILE_ARG001}'
			,
			)

		COMMIT WORK

		CREATE hg INDEX idac ON ${SQLFILE_ARG001}.V289_M08_SKY_HH_view_sv (account_number)

		CREATE hg INDEX idal ON ${SQLFILE_ARG001}.V289_M08_SKY_HH_view_sv (cb_address_line_1)

		CREATE hg INDEX idhh ON ${SQLFILE_ARG001}.V289_M08_SKY_HH_view_sv (cb_key_household)

		CREATE hg INDEX idcc ON ${SQLFILE_ARG001}.V289_M08_SKY_HH_view_sv (Children_count)

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.1: Creating Table V289_M08_SKY_HH_view_sv DONE' TO client

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.V289_M08_SKY_HH_view_sv
			TO vespa_group_low_security
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('V289_M12_Skyview_weighted_duration_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.1: Creating Table V289_M12_Skyview_weighted_duration_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.V289_M12_Skyview_weighted_duration_sv (
			row_id INTEGER NOT NULL DEFAULT autoincrement
			,source VARCHAR(6) NULL
			,the_day DATE NULL
			,service_key INTEGER NULL
			,person_ageband INTEGER NULL
			,person_gender INTEGER NULL
			,session_daypart INTEGER NULL
			,channel_name VARCHAR(200) NULL
			,weighted_duration_mins DOUBLE NULL
			,Updated_On DATETIME NOT NULL DEFAULT TIMESTAMP
			,Updated_By VARCHAR(30) NOT NULL DEFAULT '${SQLFILE_ARG001}'
			,
			)

		COMMIT WORK message convert(TIMESTAMP, now()) || ' |  M00.1: Creating Table V289_M12_Skyview_weighted_duration_sv DONE' TO client

		GRANT SELECT
			ON ${SQLFILE_ARG001}.V289_M12_Skyview_weighted_duration_sv
			TO vespa_group_low_security

		COMMIT WORK
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('v289_genderage_lookup_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating View v289_genderage_lookup_sv' TO client

		SELECT *
			,'the_id' = identity(1)
		INTO v289_genderage_lookup_sv
		FROM (
			SELECT 'sex' = 'Male'
			
			UNION
			
			SELECT 'sex' = 'Female'
			) AS thesex
		CROSS JOIN (
			SELECT 'ageband' = '01-17'
			
			UNION
			
			SELECT 'ageband' = '18-19'
			
			UNION
			
			SELECT 'ageband' = '20-24'
			
			UNION
			
			SELECT 'ageband' = '25-34'
			
			UNION
			
			SELECT 'ageband' = '35-44'
			
			UNION
			
			SELECT 'ageband' = '45-64'
			
			UNION
			
			SELECT 'ageband' = '65+'
			) AS theage

		COMMIT WORK

		CREATE UNIQUE INDEX key1 ON ${SQLFILE_ARG001}.v289_genderage_lookup_sv (the_id)

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.v289_genderage_lookup_sv
			TO vespa_group_low_security

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating View v289_genderage_lookup_sv DONE' TO client
	END message convert(TIMESTAMP, now()) || ' | Begining M00.2 - Initialising Views DONE' TO client

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('v289_M06_dp_raw_data_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table v289_M06_dp_raw_data_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.v289_M06_dp_raw_data_sv (
			pk_viewing_prog_instance_fact BIGINT NOT NULL
			,dth_event_id BIGINT NOT NULL DEFAULT - 1
			,dk_event_start_datehour_dim BIGINT NOT NULL
			,dk_event_end_datehour_dim BIGINT NOT NULL
			,dk_broadcast_start_Datehour_dim BIGINT NOT NULL
			,dk_instance_start_datehour_dim BIGINT NOT NULL
			,duration INTEGER NULL
			,genre_description VARCHAR(20) NULL
			,service_key INTEGER NULL
			,cb_key_household BIGINT NULL
			,event_start_date_time_utc TIMESTAMP NULL
			,event_end_date_time_utc TIMESTAMP NULL
			,account_number VARCHAR(20) NULL
			,subscriber_id INTEGER NULL
			,service_instance_id VARCHAR(1) NULL
			,programme_name VARCHAR(100) NULL
			,capping_end_Date_time_utc TIMESTAMP NULL
			,broadcast_start_date_time_utc TIMESTAMP NULL
			,broadcast_end_date_time_utc TIMESTAMP NULL
			,instance_start_date_time_utc TIMESTAMP NULL
			,instance_end_date_time_utc TIMESTAMP NULL
			,dk_barb_min_start_datehour_dim BIGINT NOT NULL
			,dk_barb_min_start_time_dim BIGINT NOT NULL
			,dk_barb_min_end_datehour_dim BIGINT NOT NULL
			,dk_barb_min_end_time_dim BIGINT NOT NULL
			,barb_min_start_date_time_utc TIMESTAMP NULL
			,barb_min_end_date_time_utc TIMESTAMP NULL
			,live_recorded VARCHAR(8) NOT NULL
			,
			)

		COMMIT WORK

		CREATE UNIQUE INDEX key1 ON ${SQLFILE_ARG001}.v289_M06_dp_raw_data_sv (pk_viewing_prog_instance_fact)

		CREATE hg INDEX hg0 ON ${SQLFILE_ARG001}.v289_M06_dp_raw_data_sv (dth_event_id)

		CREATE hg INDEX hg1 ON ${SQLFILE_ARG001}.v289_M06_dp_raw_data_sv (dk_event_start_datehour_dim)

		CREATE hg INDEX hg2 ON ${SQLFILE_ARG001}.v289_M06_dp_raw_data_sv (dk_broadcast_start_datehour_dim)

		CREATE hg INDEX hg3 ON ${SQLFILE_ARG001}.v289_M06_dp_raw_data_sv (dk_instance_start_datehour_dim)

		CREATE hg INDEX hg5 ON ${SQLFILE_ARG001}.v289_M06_dp_raw_data_sv (service_key)

		CREATE hg INDEX hg6 ON ${SQLFILE_ARG001}.v289_M06_dp_raw_data_sv (account_number)

		CREATE hg INDEX hg7 ON ${SQLFILE_ARG001}.v289_M06_dp_raw_data_sv (subscriber_id)

		CREATE hg INDEX hg8 ON ${SQLFILE_ARG001}.v289_M06_dp_raw_data_sv (programme_name)

		CREATE hg INDEX hg9 ON ${SQLFILE_ARG001}.v289_M06_dp_raw_data_sv (dk_barb_min_start_datehour_dim)

		CREATE hg INDEX hg10 ON ${SQLFILE_ARG001}.v289_M06_dp_raw_data_sv (dk_barb_min_start_time_dim)

		CREATE hg INDEX hg11 ON ${SQLFILE_ARG001}.v289_M06_dp_raw_data_sv (dk_barb_min_end_datehour_dim)

		CREATE hg INDEX hg12 ON ${SQLFILE_ARG001}.v289_M06_dp_raw_data_sv (dk_barb_min_end_time_dim)

		CREATE hg INDEX hg13 ON ${SQLFILE_ARG001}.v289_M06_dp_raw_data_sv (event_end_date_time_utc)

		CREATE hg INDEX hg14 ON ${SQLFILE_ARG001}.v289_M06_dp_raw_data_sv (event_start_date_time_utc)

		CREATE lf INDEX lf1 ON ${SQLFILE_ARG001}.v289_M06_dp_raw_data_sv (genre_description)

		CREATE lf INDEX lf2 ON ${SQLFILE_ARG001}.v289_M06_dp_raw_data_sv (live_recorded)

		CREATE dttm INDEX dttm1 ON ${SQLFILE_ARG001}.v289_M06_dp_raw_data_sv (barb_min_start_date_time_utc)

		CREATE dttm INDEX dttm2 ON ${SQLFILE_ARG001}.v289_M06_dp_raw_data_sv (barb_min_end_date_time_utc)

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.v289_M06_dp_raw_data_sv
			TO vespa_group_low_security

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table v289_M06_dp_raw_data_sv DONE' TO client
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('v289_M17_vod_raw_data_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table v289_M17_vod_raw_data_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.v289_M17_vod_raw_data_sv (
			pk_viewing_prog_instance_fact BIGINT NOT NULL
			,dth_event_id BIGINT NOT NULL DEFAULT - 1
			,dk_event_start_datehour_dim BIGINT NOT NULL
			,dk_event_end_datehour_dim BIGINT NOT NULL
			,dk_broadcast_start_Datehour_dim BIGINT NOT NULL
			,dk_instance_start_datehour_dim BIGINT NOT NULL
			,duration INTEGER NULL
			,genre_description VARCHAR(20) NULL
			,service_key INTEGER NULL
			,cb_key_household BIGINT NULL
			,event_start_date_time_utc TIMESTAMP NULL
			,event_end_date_time_utc TIMESTAMP NULL
			,account_number VARCHAR(20) NULL
			,subscriber_id INTEGER NULL
			,service_instance_id VARCHAR(1) NULL
			,programme_name VARCHAR(100) NULL
			,capping_end_Date_time_utc TIMESTAMP NULL
			,broadcast_start_date_time_utc TIMESTAMP NULL
			,broadcast_end_date_time_utc TIMESTAMP NULL
			,instance_start_date_time_utc TIMESTAMP NULL
			,instance_end_date_time_utc TIMESTAMP NULL
			,provider_id VARCHAR(40) NULL DEFAULT NULL
			,provider_id_number INTEGER NULL DEFAULT - 1
			,barb_min_start_date_time_utc TIMESTAMP NULL DEFAULT NULL
			,barb_min_end_date_time_utc TIMESTAMP NULL DEFAULT NULL
			,
			)

		COMMIT WORK

		CREATE UNIQUE INDEX key1 ON ${SQLFILE_ARG001}.v289_M17_vod_raw_data_sv (pk_viewing_prog_instance_fact)

		CREATE hg INDEX hg0 ON ${SQLFILE_ARG001}.v289_M17_vod_raw_data_sv (dth_event_id)

		CREATE hg INDEX hg1 ON ${SQLFILE_ARG001}.v289_M17_vod_raw_data_sv (dk_event_start_datehour_dim)

		CREATE hg INDEX hg2 ON ${SQLFILE_ARG001}.v289_M17_vod_raw_data_sv (dk_broadcast_start_datehour_dim)

		CREATE hg INDEX hg3 ON ${SQLFILE_ARG001}.v289_M17_vod_raw_data_sv (dk_instance_start_datehour_dim)

		CREATE hg INDEX hg5 ON ${SQLFILE_ARG001}.v289_M17_vod_raw_data_sv (service_key)

		CREATE hg INDEX hg6 ON ${SQLFILE_ARG001}.v289_M17_vod_raw_data_sv (account_number)

		CREATE hg INDEX hg7 ON ${SQLFILE_ARG001}.v289_M17_vod_raw_data_sv (subscriber_id)

		CREATE hg INDEX hg8 ON ${SQLFILE_ARG001}.v289_M17_vod_raw_data_sv (programme_name)

		CREATE lf INDEX lf1 ON ${SQLFILE_ARG001}.v289_M17_vod_raw_data_sv (genre_description)

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.v289_M17_vod_raw_data_sv
			TO vespa_group_low_security

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table v289_M17_vod_raw_data_sv DONE' TO client
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('V289_M07_dp_data_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table V289_M07_dp_data_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.V289_M07_dp_data_sv (
			account_number VARCHAR(20) NOT NULL
			,subscriber_id DECIMAL(10) NOT NULL
			,event_id BIGINT NULL DEFAULT NULL
			,event_Start_utc TIMESTAMP NOT NULL
			,event_end_utc TIMESTAMP NOT NULL
			,chunk_start TIMESTAMP NULL DEFAULT NULL
			,chunk_end TIMESTAMP NULL DEFAULT NULL
			,event_duration_seg INTEGER NOT NULL
			,chunk_duration_seg INTEGER NULL DEFAULT NULL
			,programme_genre VARCHAR(20) NULL DEFAULT NULL
			,session_daypart VARCHAR(11) NULL DEFAULT NULL
			,hhsize TINYINT NULL DEFAULT 0
			,viewer_hhsize TINYINT NULL DEFAULT 0
			,channel_pack VARCHAR(40) NULL DEFAULT NULL
			,segment_id INTEGER NULL DEFAULT NULL
			,Overlap_batch INTEGER NULL DEFAULT NULL
			,session_size TINYINT NULL DEFAULT 0
			,event_start_dim BIGINT NOT NULL
			,event_end_dim BIGINT NOT NULL
			,service_key INTEGER NULL
			,provider_id VARCHAR(40) NULL DEFAULT NULL
			,provider_id_number INTEGER NULL DEFAULT - 1
			,viewing_type_flag TINYINT NULL DEFAULT 0
			,barb_min_start_date_time_utc TIMESTAMP NULL
			,barb_min_end_date_time_utc TIMESTAMP NULL
			,
			)

		COMMIT WORK

		CREATE hg INDEX hg1 ON ${SQLFILE_ARG001}.V289_M07_dp_data_sv (account_number)

		CREATE hg INDEX hg2 ON ${SQLFILE_ARG001}.V289_M07_dp_data_sv (subscriber_id)

		CREATE hg INDEX hg3 ON ${SQLFILE_ARG001}.V289_M07_dp_data_sv (event_start_dim)

		CREATE hg INDEX hg4 ON ${SQLFILE_ARG001}.V289_M07_dp_data_sv (event_end_dim)

		CREATE hg INDEX hg5 ON ${SQLFILE_ARG001}.V289_M07_dp_data_sv (segment_id)

		CREATE hg INDEX hg6 ON ${SQLFILE_ARG001}.V289_M07_dp_data_sv (overlap_batch)

		CREATE hg INDEX hg7 ON ${SQLFILE_ARG001}.V289_M07_dp_data_sv (event_id)

		CREATE dttm INDEX dttim1 ON ${SQLFILE_ARG001}.V289_M07_dp_data_sv (event_start_utc)

		CREATE dttm INDEX dttim2 ON ${SQLFILE_ARG001}.V289_M07_dp_data_sv (event_end_utc)

		CREATE dttm INDEX dttim3 ON ${SQLFILE_ARG001}.V289_M07_dp_data_sv (chunk_start)

		CREATE dttm INDEX dttim4 ON ${SQLFILE_ARG001}.V289_M07_dp_data_sv (chunk_end)

		COMMIT WORK

		GRANT ALL PRIVILEGES
			ON ${SQLFILE_ARG001}.V289_M07_dp_data_sv
			TO vespa_group_low_security

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table V289_M07_dp_data_sv DONE' TO client
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('v289_m01_t_process_manager_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table v289_m01_t_process_manager_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.v289_m01_t_process_manager_sv (
			sequencer INTEGER NOT NULL DEFAULT autoincrement
			,task VARCHAR(50) NOT NULL
			,STATUS BIT NOT NULL DEFAULT 0
			,exe_date DATE NULL
			,audit_date DATE NOT NULL
			,
			)

		COMMIT WORK

		INSERT INTO v289_m01_t_process_manager_sv (
			task
			,audit_date
			)
		VALUES (
			'v289_m04_barb_data_preparation_sv'
			,convert(DATE, now())
			)

		INSERT INTO v289_m01_t_process_manager_sv (
			task
			,audit_date
			)
		VALUES (
			'v289_m08_Experian_data_preparation_sv'
			,convert(DATE, now())
			)

		INSERT INTO v289_m01_t_process_manager_sv (
			task
			,audit_date
			)
		VALUES (
			'v289_m05_barb_Matrices_generation_sv'
			,convert(DATE, now())
			)

		INSERT INTO v289_m01_t_process_manager_sv (
			task
			,audit_date
			)
		VALUES (
			'v289_m06_DP_data_extraction_sv'
			,convert(DATE, now())
			)

		INSERT INTO v289_m01_t_process_manager_sv (
			task
			,audit_date
			)
		VALUES (
			'v289_m17_PullVOD_data_extraction_sv'
			,convert(DATE, now())
			)

		INSERT INTO v289_m01_t_process_manager_sv (
			task
			,audit_date
			)
		VALUES (
			'V289_M07_dp_data_sv_preparation_sv'
			,convert(DATE, now())
			)

		INSERT INTO v289_m01_t_process_manager_sv (
			task
			,audit_date
			)
		VALUES (
			'v289_m15_non_viewers_assignment'
			,convert(DATE, now())
			)

		INSERT INTO v289_m01_t_process_manager_sv (
			task
			,audit_date
			)
		VALUES (
			'v289_m09_Session_size_definition_sv'
			,convert(DATE, now())
			)

		INSERT INTO v289_m01_t_process_manager_sv (
			task
			,audit_date
			)
		VALUES (
			'v289_M10_individuals_selection_sv'
			,convert(DATE, now())
			)

		INSERT INTO v289_m01_t_process_manager_sv (
			task
			,audit_date
			)
		VALUES (
			'v289_M19_Non_Viewing_Households'
			,convert(DATE, now())
			)

		INSERT INTO v289_m01_t_process_manager_sv (
			task
			,audit_date
			)
		VALUES (
			'V289_M11_01_SC3_v1_1__do_weekly_segmentation_sv'
			,convert(DATE, now())
			)

		INSERT INTO v289_m01_t_process_manager_sv (
			task
			,audit_date
			)
		VALUES (
			'V289_M11_02_SC3_v1_1__prepare_panel_members_sv'
			,convert(DATE, now())
			)

		INSERT INTO v289_m01_t_process_manager_sv (
			task
			,audit_date
			)
		VALUES (
			'V289_M11_03_SC3I_v1_1__add_individual_data_sv'
			,convert(DATE, now())
			)

		INSERT INTO v289_m01_t_process_manager_sv (
			task
			,audit_date
			)
		VALUES (
			'V289_M11_04_SC3I_v1_1__make_weights_sv_BARB_sv'
			,convert(DATE, now())
			)

		INSERT INTO v289_m01_t_process_manager_sv (
			task
			,audit_date
			)
		VALUES (
			'v289_m12_validation_sv'
			,convert(DATE, now())
			)

		INSERT INTO v289_m01_t_process_manager_sv (
			task
			,audit_date
			)
		VALUES (
			'v289_M13_Create_Final_TechEdge_Output_Tables_sv'
			,convert(DATE, now())
			)

		INSERT INTO v289_m01_t_process_manager_sv (
			task
			,audit_date
			)
		VALUES (
			'v289_M14_Create_Final_Olive_Output_Tables'
			,convert(DATE, now())
			)

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.v289_m01_t_process_manager_sv
			TO vespa_group_low_security

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table v289_m01_t_process_manager_sv DONE' TO client
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('SC3I_Variables_lookup_v1_1_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3I_Variables_lookup_v1_1_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.SC3I_Variables_lookup_v1_1_sv (
			id INTEGER NOT NULL
			,scaling_variable VARCHAR(20) NOT NULL
			,
			)

		COMMIT WORK

		CREATE lf INDEX ind1 ON ${SQLFILE_ARG001}.SC3I_Variables_lookup_v1_1_sv (id)

		CREATE lf INDEX ind2 ON ${SQLFILE_ARG001}.SC3I_Variables_lookup_v1_1_sv (scaling_variable)

		COMMIT WORK

		INSERT INTO SC3I_Variables_lookup_v1_1_sv
		VALUES (
			1
			,'hhcomposition'
			)

		INSERT INTO SC3I_Variables_lookup_v1_1_sv
		VALUES (
			2
			,'package'
			)

		INSERT INTO SC3I_Variables_lookup_v1_1_sv
		VALUES (
			3
			,'isba_tv_region'
			)

		INSERT INTO SC3I_Variables_lookup_v1_1_sv
		VALUES (
			4
			,'age_band'
			)

		INSERT INTO SC3I_Variables_lookup_v1_1_sv
		VALUES (
			5
			,'gender'
			)

		INSERT INTO SC3I_Variables_lookup_v1_1_sv
		VALUES (
			6
			,'head_of_hhd'
			)

		INSERT INTO SC3I_Variables_lookup_v1_1_sv
		VALUES (
			7
			,'hh_size'
			)

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.SC3I_Variables_lookup_v1_1_sv
			TO vespa_group_low_security

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3I_Variables_lookup_v1_1_sv DONE' TO client
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('SC3I_Sky_base_segment_snapshots_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3I_Sky_base_segment_snapshots_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.SC3I_Sky_base_segment_snapshots_sv (
			account_number VARCHAR(20) NOT NULL
			,profiling_date DATE NOT NULL
			,HH_person_number TINYINT NOT NULL
			,population_scaling_segment_id INTEGER NOT NULL
			,vespa_scaling_segment_id INTEGER NOT NULL
			,expected_boxes INTEGER NOT NULL
			,
			)

		COMMIT WORK

		CREATE hg INDEX ind1 ON ${SQLFILE_ARG001}.SC3I_Sky_base_segment_snapshots_sv (account_number)

		CREATE lf INDEX ind2 ON ${SQLFILE_ARG001}.SC3I_Sky_base_segment_snapshots_sv (profiling_date)

		CREATE lf INDEX ind3 ON ${SQLFILE_ARG001}.SC3I_Sky_base_segment_snapshots_sv (HH_person_number)

		CREATE hg INDEX ind4 ON ${SQLFILE_ARG001}.SC3I_Sky_base_segment_snapshots_sv (population_scaling_segment_id)

		CREATE hg INDEX ind5 ON ${SQLFILE_ARG001}.SC3I_Sky_base_segment_snapshots_sv (vespa_scaling_segment_id)

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.SC3I_Sky_base_segment_snapshots_sv
			TO vespa_group_low_security

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3I_Sky_base_segment_snapshots_sv DONE' TO client
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('SC3I_Todays_panel_members_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3I_Todays_panel_members_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.SC3I_Todays_panel_members_sv (
			account_number VARCHAR(20) NOT NULL
			,HH_person_number TINYINT NOT NULL
			,scaling_segment_id INTEGER NOT NULL
			,
			)

		COMMIT WORK

		CREATE hg INDEX ind1 ON ${SQLFILE_ARG001}.SC3I_Todays_panel_members_sv (account_number)

		CREATE lf INDEX ind2 ON ${SQLFILE_ARG001}.SC3I_Todays_panel_members_sv (HH_person_number)

		CREATE hg INDEX ind3 ON ${SQLFILE_ARG001}.SC3I_Todays_panel_members_sv (scaling_segment_id)

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.SC3I_Todays_panel_members_sv
			TO vespa_group_low_security message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3I_Todays_panel_members_sv DONE' TO client
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('SC3I_weighting_working_table_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3I_weighting_working_table_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.SC3I_weighting_working_table_sv (
			scaling_segment_id INTEGER NOT NULL
			,sky_base_universe VARCHAR(50) NULL
			,sky_base_accounts DOUBLE NOT NULL
			,vespa_panel DOUBLE NULL DEFAULT 0
			,category_weight DOUBLE NULL
			,sum_of_weights DOUBLE NULL
			,segment_weight DOUBLE NULL
			,indices_actual DOUBLE NULL
			,indices_weighted DOUBLE NULL
			,PRIMARY KEY (scaling_segment_id)
			,
			)

		COMMIT WORK

		CREATE hg INDEX indx_un ON ${SQLFILE_ARG001}.SC3I_weighting_working_table_sv (sky_base_universe)

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3I_weighting_working_table_sv DONE' TO client
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('SC3I_category_working_table_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3I_category_working_table_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.SC3I_category_working_table_sv (
			sky_base_universe VARCHAR(50) NULL
			,PROFILE VARCHAR(50) NULL
			,value VARCHAR(70) NULL
			,sky_base_accounts DOUBLE NULL
			,vespa_panel DOUBLE NULL
			,category_weight DOUBLE NULL
			,sum_of_weights DOUBLE NULL
			,convergence_flag TINYINT NULL DEFAULT 1
			,
			)

		COMMIT WORK

		CREATE hg INDEX indx_universe ON ${SQLFILE_ARG001}.SC3I_category_working_table_sv (sky_base_universe)

		CREATE hg INDEX indx_profile ON ${SQLFILE_ARG001}.SC3I_category_working_table_sv (PROFILE)

		CREATE hg INDEX indx_value ON ${SQLFILE_ARG001}.SC3I_category_working_table_sv (value)

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3I_category_working_table_sv DONE' TO client
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('SC3I_category_subtotals_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3I_category_subtotals_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.SC3I_category_subtotals_sv (
			scaling_date DATE NULL
			,sky_base_universe VARCHAR(50) NULL
			,PROFILE VARCHAR(50) NULL
			,value VARCHAR(70) NULL
			,sky_base_accounts DOUBLE NULL
			,vespa_panel DOUBLE NULL
			,category_weight DOUBLE NULL
			,sum_of_weights DOUBLE NULL
			,convergence TINYINT NULL
			,
			)

		COMMIT WORK

		CREATE INDEX indx_date ON ${SQLFILE_ARG001}.SC3I_category_subtotals_sv (scaling_date)

		CREATE hg INDEX indx_universe ON ${SQLFILE_ARG001}.SC3I_category_subtotals_sv (sky_base_universe)

		CREATE hg INDEX indx_profile ON ${SQLFILE_ARG001}.SC3I_category_subtotals_sv (PROFILE)

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3I_category_subtotals_sv DONE' TO client
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('SC3I_metrics_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3I_metrics_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.SC3I_metrics_sv (
			scaling_date DATE NULL
			,iterations INTEGER NULL
			,convergence TINYINT NULL
			,max_weight REAL NULL
			,av_weight REAL NULL
			,sum_of_weights REAL NULL
			,sky_base BIGINT NULL
			,vespa_panel BIGINT NULL
			,non_scalable_accounts BIGINT NULL
			,sum_of_convergence REAL NULL
			,
			)

		COMMIT WORK

		CREATE INDEX indx_date ON ${SQLFILE_ARG001}.SC3I_metrics_sv (scaling_date)

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3I_metrics_sv DONE' TO client
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('SC3I_non_convergences_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3I_non_convergences_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.SC3I_non_convergences_sv (
			scaling_date DATE NULL
			,scaling_segment_id INTEGER NULL
			,difference REAL NULL
			,
			)

		COMMIT WORK

		CREATE INDEX indx_date ON ${SQLFILE_ARG001}.SC3I_non_convergences_sv (scaling_date)

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3I_non_convergences_sv DONE' TO client
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('SC3I_Weightings_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3I_Weightings_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.SC3I_Weightings_sv (
			scaling_day DATE NOT NULL
			,scaling_segment_ID INTEGER NOT NULL
			,vespa_accounts BIGINT NOT NULL DEFAULT 0
			,sky_base_accounts BIGINT NOT NULL
			,weighting DOUBLE NULL DEFAULT NULL
			,sum_of_weights DOUBLE NULL DEFAULT NULL
			,indices_actual DOUBLE NULL
			,indices_weighted DOUBLE NULL
			,convergence TINYINT NULL
			,PRIMARY KEY (
				scaling_day
				,scaling_segment_ID
				)
			,
			)

		COMMIT WORK

		CREATE DATE INDEX idx1 ON ${SQLFILE_ARG001}.SC3I_Weightings_sv (scaling_day)

		CREATE hg INDEX idx2 ON ${SQLFILE_ARG001}.SC3I_Weightings_sv (scaling_segment_ID)

		GRANT SELECT
			ON ${SQLFILE_ARG001}.SC3I_Weightings_sv
			TO vespa_group_low_security

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3I_Weightings_sv DONE' TO client
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('SC3I_Intervals_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3I_Intervals_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.SC3I_Intervals_sv (
			account_number VARCHAR(20) NOT NULL
			,HH_person_number TINYINT NOT NULL
			,reporting_starts DATE NOT NULL
			,reporting_ends DATE NOT NULL
			,scaling_segment_ID INTEGER NOT NULL
			,
			)

		COMMIT WORK

		CREATE INDEX for_joining ON ${SQLFILE_ARG001}.SC3I_Intervals_sv (
			scaling_segment_ID
			,reporting_starts
			)

		CREATE hg INDEX idx1 ON ${SQLFILE_ARG001}.SC3I_Intervals_sv (account_number)

		CREATE hg INDEX idx2 ON ${SQLFILE_ARG001}.SC3I_Intervals_sv (HH_person_number)

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.SC3I_Intervals_sv
			TO vespa_group_low_security

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3I_Intervals_sv DONE' TO client
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING_sv (
			account_number VARCHAR(20) NOT NULL
			,HH_person_number TINYINT NOT NULL
			,scaling_date DATE NOT NULL
			,scaling_weighting REAL NOT NULL
			,build_date DATETIME NOT NULL
			,
			)

		COMMIT WORK

		CREATE dttm INDEX for_loading ON ${SQLFILE_ARG001}.V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING_sv (build_date)

		CREATE hg INDEX idx1 ON ${SQLFILE_ARG001}.V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING_sv (account_number)

		CREATE DATE INDEX idx3 ON ${SQLFILE_ARG001}.V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING_sv (scaling_date)

		CREATE hg INDEX idx4 ON ${SQLFILE_ARG001}.V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING_sv (HH_person_number)

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING_sv
			TO vespa_group_low_security

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING_sv DONE' TO client
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('V289_M11_04_Barb_weighted_population_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table V289_M11_04_Barb_weighted_population_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.V289_M11_04_Barb_weighted_population_sv (
			ageband VARCHAR(10) NULL
			,gender CHAR(1) NULL
			,viewed_tv VARCHAR(20) NULL
			,head_of_hhd CHAR(1) NULL
			,hh_size VARCHAR(2) NULL
			,barb_weight DOUBLE NULL
			,
			)

		COMMIT WORK

		CREATE lf INDEX ind1 ON ${SQLFILE_ARG001}.V289_M11_04_Barb_weighted_population_sv (ageband)

		CREATE lf INDEX ind2 ON ${SQLFILE_ARG001}.V289_M11_04_Barb_weighted_population_sv (gender)

		CREATE lf INDEX ind3 ON ${SQLFILE_ARG001}.V289_M11_04_Barb_weighted_population_sv (viewed_tv)

		CREATE lf INDEX ind4 ON ${SQLFILE_ARG001}.V289_M11_04_Barb_weighted_population_sv (head_of_hhd)

		CREATE lf INDEX ind5 ON ${SQLFILE_ARG001}.V289_M11_04_Barb_weighted_population_sv (hh_size)

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.V289_M11_04_Barb_weighted_population_sv
			TO vespa_group_low_security

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table V289_M11_04_Barb_weighted_population_sv DONE' TO client
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('SC3_Weightings_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3_Weightings_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.SC3_Weightings_sv (
			scaling_day DATE NOT NULL
			,scaling_segment_ID INTEGER NOT NULL
			,vespa_accounts BIGINT NOT NULL DEFAULT 0
			,sky_base_accounts BIGINT NOT NULL
			,weighting DOUBLE NULL DEFAULT NULL
			,sum_of_weights DOUBLE NULL DEFAULT NULL
			,indices_actual DOUBLE NULL
			,indices_weighted DOUBLE NULL
			,convergence TINYINT NULL
			,PRIMARY KEY (
				scaling_day
				,scaling_segment_ID
				)
			,
			)

		COMMIT WORK

		CREATE DATE INDEX idx1 ON ${SQLFILE_ARG001}.SC3_Weightings_sv (scaling_day)

		CREATE hg INDEX idx2 ON ${SQLFILE_ARG001}.SC3_Weightings_sv (scaling_segment_ID)

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.SC3_Weightings_sv
			TO vespa_group_low_security

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3_Weightings_sv DONE' TO client
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('SC3_Intervals_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3_Intervals_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.SC3_Intervals_sv (
			account_number VARCHAR(20) NOT NULL
			,reporting_starts DATE NOT NULL
			,reporting_ends DATE NOT NULL
			,scaling_segment_ID INTEGER NOT NULL
			,PRIMARY KEY (
				account_number
				,reporting_starts
				)
			,
			)

		COMMIT WORK

		CREATE INDEX for_joining ON ${SQLFILE_ARG001}.SC3_Intervals_sv (
			scaling_segment_ID
			,reporting_starts
			)

		CREATE hg INDEX idx1 ON ${SQLFILE_ARG001}.SC3_Intervals_sv (account_number)

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.SC3_Intervals_sv
			TO vespa_group_low_security

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3_Intervals_sv DONE' TO client
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('VESPA_HOUSEHOLD_WEIGHTING_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table VESPA_HOUSEHOLD_WEIGHTING_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.VESPA_HOUSEHOLD_WEIGHTING_sv (
			account_number VARCHAR(20) NOT NULL
			,cb_key_household BIGINT NOT NULL
			,scaling_date DATE NOT NULL
			,scaling_weighting REAL NOT NULL
			,build_date DATETIME NOT NULL
			,PRIMARY KEY (
				account_number
				,scaling_date
				)
			,
			)

		COMMIT WORK

		CREATE dttm INDEX for_loading ON ${SQLFILE_ARG001}.VESPA_HOUSEHOLD_WEIGHTING_sv (build_date)

		CREATE hg INDEX idx1 ON ${SQLFILE_ARG001}.VESPA_HOUSEHOLD_WEIGHTING_sv (account_number)

		CREATE hg INDEX idx2 ON ${SQLFILE_ARG001}.VESPA_HOUSEHOLD_WEIGHTING_sv (cb_key_household)

		CREATE DATE INDEX idx3 ON ${SQLFILE_ARG001}.VESPA_HOUSEHOLD_WEIGHTING_sv (scaling_date)

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.VESPA_HOUSEHOLD_WEIGHTING_sv
			TO vespa_group_low_security

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table VESPA_HOUSEHOLD_WEIGHTING_sv DONE' TO client
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('SC3_Sky_base_segment_snapshots_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3_Sky_base_segment_snapshots_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.SC3_Sky_base_segment_snapshots_sv (
			account_number VARCHAR(20) NOT NULL
			,profiling_date DATE NOT NULL
			,cb_key_household BIGINT NOT NULL
			,population_scaling_segment_id BIGINT NULL
			,vespa_scaling_segment_id BIGINT NULL
			,expected_boxes TINYINT NULL
			,PRIMARY KEY (
				account_number
				,profiling_date
				)
			,
			)

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.SC3_Sky_base_segment_snapshots_sv
			TO vespa_group_low_security

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3_Sky_base_segment_snapshots_sv DONE' TO client
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('SC3_Todays_panel_members_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3_Todays_panel_members_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.SC3_Todays_panel_members_sv (
			account_number VARCHAR(20) NOT NULL
			,scaling_segment_id BIGINT NOT NULL
			,PRIMARY KEY (account_number)
			,
			)

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.SC3_Todays_panel_members_sv
			TO vespa_group_low_security

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3_Todays_panel_members_sv DONE' TO client
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('SC3_Todays_segment_weights_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3_Todays_segment_weights_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.SC3_Todays_segment_weights_sv (
			scaling_segment_id BIGINT NOT NULL
			,scaling_weighting REAL NOT NULL
			,PRIMARY KEY (scaling_segment_id)
			,
			)

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.SC3_Todays_segment_weights_sv
			TO vespa_group_low_security

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3_Todays_segment_weights_sv DONE' TO client
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('SC3_scaling_weekly_sample_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3_scaling_weekly_sample_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.SC3_scaling_weekly_sample_sv (
			account_number VARCHAR(20) NOT NULL
			,cb_key_household BIGINT NOT NULL
			,cb_key_individual BIGINT NOT NULL
			,consumerview_cb_row_id BIGINT NULL
			,universe VARCHAR(30) NULL
			,sky_base_universe VARCHAR(30) NULL
			,vespa_universe VARCHAR(30) NULL
			,weighting_universe VARCHAR(30) NULL
			,isba_tv_region VARCHAR(20) NULL
			,hhcomposition VARCHAR(2) NOT NULL DEFAULT 'U'
			,tenure VARCHAR(15) NOT NULL DEFAULT 'E) Unknown'
			,num_mix INTEGER NULL
			,mix_pack VARCHAR(20) NULL
			,package VARCHAR(20) NULL
			,boxtype VARCHAR(35) NULL
			,no_of_stbs VARCHAR(15) NULL
			,hd_subscription VARCHAR(5) NULL
			,pvr VARCHAR(5) NULL
			,population_scaling_segment_id INTEGER NULL DEFAULT NULL
			,vespa_scaling_segment_id INTEGER NULL DEFAULT NULL
			,mr_boxes INTEGER NULL
			,PRIMARY KEY (account_number)
			,
			)

		COMMIT WORK

		CREATE INDEX for_segment_identification_raw ON ${SQLFILE_ARG001}.SC3_scaling_weekly_sample_sv (
			isba_tv_region
			,hhcomposition
			,tenure
			,package
			,no_of_stbs
			,hd_subscription
			,pvr
			)

		CREATE INDEX experian_joining ON ${SQLFILE_ARG001}.SC3_scaling_weekly_sample_sv (consumerview_cb_row_id)

		CREATE INDEX for_grouping1 ON ${SQLFILE_ARG001}.SC3_scaling_weekly_sample_sv (population_scaling_segment_id)

		CREATE INDEX for_grouping2 ON ${SQLFILE_ARG001}.SC3_scaling_weekly_sample_sv (vespa_scaling_segment_id)

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.SC3_scaling_weekly_sample_sv
			TO vespa_group_low_security

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3_scaling_weekly_sample_sv DONE' TO client
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('SC3_weighting_working_table_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3_weighting_working_table_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.SC3_weighting_working_table_sv (
			scaling_segment_id INTEGER NOT NULL
			,sky_base_universe VARCHAR(50) NULL
			,sky_base_accounts DOUBLE NOT NULL
			,vespa_panel DOUBLE NOT NULL DEFAULT 0
			,category_weight DOUBLE NULL
			,sum_of_weights DOUBLE NULL
			,segment_weight DOUBLE NULL
			,indices_actual DOUBLE NULL
			,indices_weighted DOUBLE NULL
			,PRIMARY KEY (scaling_segment_id)
			,
			)

		COMMIT WORK

		CREATE hg INDEX indx_un ON ${SQLFILE_ARG001}.SC3_weighting_working_table_sv (sky_base_universe)

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.SC3_weighting_working_table_sv
			TO vespa_group_low_security

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3_weighting_working_table_sv DONE' TO client
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('SC3_category_working_table_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3_category_working_table_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.SC3_category_working_table_sv (
			sky_base_universe VARCHAR(50) NULL
			,PROFILE VARCHAR(50) NULL
			,value VARCHAR(70) NULL
			,sky_base_accounts DOUBLE NULL
			,vespa_panel DOUBLE NULL
			,category_weight DOUBLE NULL
			,sum_of_weights DOUBLE NULL
			,convergence_flag TINYINT NOT NULL DEFAULT 1
			,
			)

		COMMIT WORK

		CREATE hg INDEX indx_universe ON ${SQLFILE_ARG001}.SC3_category_working_table_sv (sky_base_universe)

		CREATE hg INDEX indx_profile ON ${SQLFILE_ARG001}.SC3_category_working_table_sv (PROFILE)

		CREATE hg INDEX indx_value ON ${SQLFILE_ARG001}.SC3_category_working_table_sv (value)

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.SC3_category_working_table_sv
			TO vespa_group_low_security

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3_category_working_table_sv DONE' TO client
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('SC3_category_subtotals_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3_category_subtotals_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.SC3_category_subtotals_sv (
			scaling_date DATE NULL
			,sky_base_universe VARCHAR(50) NULL
			,PROFILE VARCHAR(50) NULL
			,value VARCHAR(70) NULL
			,sky_base_accounts DOUBLE NULL
			,vespa_panel DOUBLE NULL
			,category_weight DOUBLE NULL
			,sum_of_weights DOUBLE NULL
			,convergence TINYINT NULL
			,
			)

		COMMIT WORK

		CREATE INDEX indx_date ON ${SQLFILE_ARG001}.SC3_category_subtotals_sv (scaling_date)

		CREATE hg INDEX indx_universe ON ${SQLFILE_ARG001}.SC3_category_subtotals_sv (sky_base_universe)

		CREATE hg INDEX indx_profile ON ${SQLFILE_ARG001}.SC3_category_subtotals_sv (PROFILE)

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.SC3_category_subtotals_sv
			TO vespa_group_low_security

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3_category_subtotals_sv DONE' TO client
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('SC3_metrics_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3_metrics_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.SC3_metrics_sv (
			scaling_date DATE NULL
			,iterations INTEGER NULL
			,convergence TINYINT NULL
			,max_weight REAL NULL
			,av_weight REAL NULL
			,sum_of_weights REAL NULL
			,sky_base BIGINT NULL
			,vespa_panel BIGINT NULL
			,non_scalable_accounts BIGINT NULL
			,sum_of_convergence REAL NULL
			,
			)

		COMMIT WORK

		CREATE INDEX indx_date ON ${SQLFILE_ARG001}.SC3_metrics_sv (scaling_date)

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.SC3_metrics_sv
			TO vespa_group_low_security

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3_metrics_sv DONE' TO client
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('SC3_non_convergences_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3_non_convergences_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.SC3_non_convergences_sv (
			scaling_date DATE NULL
			,scaling_segment_id INTEGER NULL
			,difference REAL NULL
			,
			)

		COMMIT WORK

		CREATE INDEX indx_date ON ${SQLFILE_ARG001}.SC3_non_convergences_sv (scaling_date)

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.SC3_non_convergences_sv
			TO vespa_group_low_security

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table SC3_non_convergences_sv DONE' TO client
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories_sv (
			file_creation_date DATE NULL
			,file_creation_time TIME NULL
			,file_type VARCHAR(12) NULL
			,file_version INTEGER NULL
			,filename VARCHAR(13) NULL
			,Record_Type INTEGER NULL DEFAULT NULL
			,Household_Number INTEGER NULL DEFAULT NULL
			,Person_Number INTEGER NULL DEFAULT NULL
			,Reporting_Panel_Code INTEGER NULL DEFAULT NULL
			,Date_of_Activity_DB1 DATE NULL
			,Response_Code INTEGER NULL DEFAULT NULL
			,Processing_Weight INTEGER NULL DEFAULT NULL
			,Adults_Commercial_TV_Viewing_Sextile INTEGER NULL DEFAULT NULL
			,ABC1_Adults_Commercial_TV_Viewing_Sextile INTEGER NULL DEFAULT NULL
			,Adults_Total_Viewing_Sextile INTEGER NULL DEFAULT NULL
			,ABC1_Adults_Total_Viewing_Sextile INTEGER NULL DEFAULT NULL
			,Adults_16_34_Commercial_TV_Viewing_Sextile INTEGER NULL DEFAULT NULL
			,Adults_16_34_Total_Viewing_Sextile INTEGER NULL DEFAULT NULL
			,
			)

		COMMIT WORK

		CREATE hg INDEX ind_hhd ON ${SQLFILE_ARG001}.BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories_sv (Household_Number)

		CREATE lf INDEX ind_person ON ${SQLFILE_ARG001}.BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories_sv (Person_Number)

		CREATE lf INDEX ind_panel ON ${SQLFILE_ARG001}.BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories_sv (Reporting_Panel_Code)

		CREATE lf INDEX ind_date ON ${SQLFILE_ARG001}.BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories_sv (Date_of_Activity_DB1)

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories_sv
			TO vespa_group_low_security

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating Table BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories_sv DONE' TO client
	END message convert(TIMESTAMP, now()) || ' | @ M00.1: Creating Table barb_weights_sv' TO client

	DECLARE @sql_ VARCHAR(5000)

	IF EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('barb_weights_sv')
			)
		DROP TABLE ${SQLFILE_ARG001}.barb_weights_sv

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.2: Creating View barb_weights_sv' TO client

	SET @sql_ = 'select  distinct ' || 'date_of_activity_db1 ' || ',reporting_panel_code ' || ',household_number ' || ',person_number ' || ',processing_weight/10 as processing_weight ' || 'into   barb_weights_sv ' || 'from    BARB_PANEL_MEM_RESP_WGHT ' || 'where   date_of_activity =''' || @processing_date || ''' ' || 'and     reporting_panel_code = 50'

	EXECUTE (@sql_)

	COMMIT WORK

	CREATE hg INDEX hg1 ON ${SQLFILE_ARG001}.barb_weights_sv (household_number)

	CREATE lf INDEX lf1 ON ${SQLFILE_ARG001}.barb_weights_sv (person_number)

	GRANT SELECT
		ON ${SQLFILE_ARG001}.barb_weights_sv
		TO vespa_group_low_security

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.1: Creating Table barb_weights_sv DONE' TO client

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('V289_M13_individual_viewing_live_vosdal_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.1: Creating Table V289_M13_individual_viewing_live_vosdal_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.V289_M13_individual_viewing_live_vosdal_sv (
			SUBSCRIBER_ID DECIMAL(10) NULL
			,ACCOUNT_NUMBER VARCHAR(20) NOT NULL
			,STB_BROADCAST_START_TIME TIMESTAMP NOT NULL
			,STB_BROADCAST_END_TIME TIMESTAMP NOT NULL
			,STB_EVENT_START_TIME TIMESTAMP NOT NULL
			,TIMESHIFT INTEGER NOT NULL
			,service_key INTEGER NOT NULL
			,Platform_flag INTEGER NOT NULL
			,Original_Service_key INTEGER NOT NULL
			,AdSmart_flag INTEGER NOT NULL DEFAULT 0
			,DTH_VIEWING_EVENT_ID BIGINT NOT NULL
			,person_1 SMALLINT NOT NULL DEFAULT 0
			,person_2 SMALLINT NOT NULL DEFAULT 0
			,person_3 SMALLINT NOT NULL DEFAULT 0
			,person_4 SMALLINT NOT NULL DEFAULT 0
			,person_5 SMALLINT NOT NULL DEFAULT 0
			,person_6 SMALLINT NOT NULL DEFAULT 0
			,person_7 SMALLINT NOT NULL DEFAULT 0
			,person_8 SMALLINT NOT NULL DEFAULT 0
			,person_9 SMALLINT NOT NULL DEFAULT 0
			,person_10 SMALLINT NOT NULL DEFAULT 0
			,person_11 SMALLINT NOT NULL DEFAULT 0
			,person_12 SMALLINT NOT NULL DEFAULT 0
			,person_13 SMALLINT NOT NULL DEFAULT 0
			,person_14 SMALLINT NOT NULL DEFAULT 0
			,person_15 SMALLINT NOT NULL DEFAULT 0
			,person_16 SMALLINT NOT NULL DEFAULT 0
			,
			)

		COMMIT WORK

		CREATE hg INDEX hg_idx_1 ON ${SQLFILE_ARG001}.V289_M13_individual_viewing_live_vosdal_sv (SUBSCRIBER_ID)

		CREATE hg INDEX hg_idx_2 ON ${SQLFILE_ARG001}.V289_M13_individual_viewing_live_vosdal_sv (ACCOUNT_NUMBER)

		CREATE hg INDEX hg_idx_3 ON ${SQLFILE_ARG001}.V289_M13_individual_viewing_live_vosdal_sv (DTH_VIEWING_EVENT_ID)

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.V289_M13_individual_viewing_live_vosdal_sv
			TO vespa_group_low_security

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.1: Creating Table V289_M13_individual_viewing_live_vosdal_sv DONE' TO client
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('V289_M13_individual_viewing_timeshift_pullvod_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.1: Creating Table V289_M13_individual_viewing_timeshift_pullvod_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.V289_M13_individual_viewing_timeshift_pullvod_sv (
			SUBSCRIBER_ID DECIMAL(10) NULL
			,ACCOUNT_NUMBER VARCHAR(20) NOT NULL
			,STB_BROADCAST_START_TIME TIMESTAMP NOT NULL
			,STB_BROADCAST_END_TIME TIMESTAMP NOT NULL
			,STB_EVENT_START_TIME TIMESTAMP NOT NULL
			,TIMESHIFT INTEGER NOT NULL
			,service_key INTEGER NOT NULL
			,Platform_flag INTEGER NOT NULL
			,Original_Service_key INTEGER NOT NULL
			,AdSmart_flag INTEGER NOT NULL DEFAULT 0
			,DTH_VIEWING_EVENT_ID BIGINT NOT NULL
			,person_1 SMALLINT NOT NULL DEFAULT 0
			,person_2 SMALLINT NOT NULL DEFAULT 0
			,person_3 SMALLINT NOT NULL DEFAULT 0
			,person_4 SMALLINT NOT NULL DEFAULT 0
			,person_5 SMALLINT NOT NULL DEFAULT 0
			,person_6 SMALLINT NOT NULL DEFAULT 0
			,person_7 SMALLINT NOT NULL DEFAULT 0
			,person_8 SMALLINT NOT NULL DEFAULT 0
			,person_9 SMALLINT NOT NULL DEFAULT 0
			,person_10 SMALLINT NOT NULL DEFAULT 0
			,person_11 SMALLINT NOT NULL DEFAULT 0
			,person_12 SMALLINT NOT NULL DEFAULT 0
			,person_13 SMALLINT NOT NULL DEFAULT 0
			,person_14 SMALLINT NOT NULL DEFAULT 0
			,person_15 SMALLINT NOT NULL DEFAULT 0
			,person_16 SMALLINT NOT NULL DEFAULT 0
			,
			)

		COMMIT WORK

		CREATE hg INDEX hg_idx_1 ON ${SQLFILE_ARG001}.V289_M13_individual_viewing_timeshift_pullvod_sv (SUBSCRIBER_ID)

		CREATE hg INDEX hg_idx_2 ON ${SQLFILE_ARG001}.V289_M13_individual_viewing_timeshift_pullvod_sv (ACCOUNT_NUMBER)

		CREATE hg INDEX hg_idx_3 ON ${SQLFILE_ARG001}.V289_M13_individual_viewing_timeshift_pullvod_sv (DTH_VIEWING_EVENT_ID)

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.V289_M13_individual_viewing_timeshift_pullvod_sv
			TO vespa_group_low_security

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.1: Creating Table V289_M13_individual_viewing_timeshift_pullvod_sv DONE' TO client
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'TABLE'
				AND upper(tname) = UPPER('V289_M13_individual_details_sv')
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.1: Creating Table V289_M13_individual_details_sv' TO client

		CREATE TABLE ${SQLFILE_ARG001}.V289_M13_individual_details_sv (
			account_number VARCHAR(20) NOT NULL
			,person_number INTEGER NOT NULL
			,ind_scaling_weight DOUBLE NOT NULL
			,gender INTEGER NOT NULL
			,age_band INTEGER NOT NULL
			,head_of_hhd INTEGER NOT NULL
			,hhsize INTEGER NOT NULL
			,
			)

		COMMIT WORK

		CREATE hg INDEX hg_idx_1 ON ${SQLFILE_ARG001}.V289_M13_individual_details_sv (account_number)

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.V289_M13_individual_details_sv
			TO vespa_group_low_security

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.1: Creating Table V289_M13_individual_details_sv DONE' TO client
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND lower(tname) = LOWER('v289_m16_dq_mct_checks')
				AND tabletype = 'TABLE'
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.1: Creating Table v289_m16_dq_mct_checks' TO client

		CREATE TABLE ${SQLFILE_ARG001}.v289_m16_dq_mct_checks (
			sequencer INTEGER NULL DEFAULT autoincrement
			,target_table VARCHAR(100) NOT NULL
			,target_field VARCHAR(100) NOT NULL
			,test_context VARCHAR(100) NOT NULL
			,processing_date DATE NULL
			,actual_value DECIMAL(18, 3) NULL DEFAULT 0
			,tolerance DECIMAL(18, 3) NOT NULL
			,test_result VARCHAR(15) NULL DEFAULT 'Pending'
			,
			)

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.v289_m16_dq_mct_checks
			TO vespa_group_low_security

		COMMIT WORK

		INSERT INTO v289_m16_dq_mct_checks (
			target_table
			,target_field
			,test_context
			,tolerance
			)
		SELECT *
		FROM (
			SELECT 'a' = 'v289_m16_barb_Check1'
				,'b' = 'n_sky_viewing'
				,'c' = 'Volume of Sky Viewers Households'
				,'d' = .75
			
			UNION
			
			SELECT 'v289_m16_barb_Check1'
				,'n_digisat_viewing'
				,'Volume of Digital Satelite Viewers Households'
				,.75
			
			UNION
			
			SELECT 'v289_m16_barb_Check1'
				,'n_viewerhouseholds'
				,'Volume of Viewers Households'
				,.8
			
			UNION
			
			SELECT 'v289_m16_barb_Check1'
				,'tot_min_watch_non_scaled'
				,'Total Minutes Watched Non-Scaled'
				,.75
			
			UNION
			
			SELECT 'v289_m16_barb_Check1'
				,'tot_min_watch_scaled'
				,'Total Minutes Watched Scaled'
				,.75
			
			UNION
			
			SELECT 'v289_m16_barb_Check2'
				,'nhouseholds'
				,'Volume of Households on Panel_member_detail Table'
				,.95
			
			UNION
			
			SELECT 'v289_m16_barb_Check2'
				,'people'
				,'Volume of Individuals on Panel_member_detail Table'
				,.95
			
			UNION
			
			SELECT 'v289_m16_barb_Check3'
				,'n_hh'
				,'Volume of Households in Barb With Sky boxes'
				,.95
			
			UNION
			
			SELECT 'v289_m16_barb_Check3'
				,'n_digital'
				,'Volume of Households in Barb With DigitSat boxes'
				,.95
			
			UNION
			
			SELECT 'v289_m16_barb_Check4'
				,'sample'
				,'Volume of Individuals Scaled'
				,.95
			
			UNION
			
			SELECT 'v289_m16_barb_Check4'
				,'sow'
				,'Sum of Scaling Weights'
				,.98
			
			UNION
			
			SELECT 'v289_m16_h2i_check1'
				,'nrows'
				,'Volume of records on the Viewing Table'
				,.9
			
			UNION
			
			SELECT 'v289_m16_h2i_check1'
				,'ncapped'
				,'Volume of capped records on the Viewing Table'
				,.85
			
			UNION
			
			SELECT 'v289_m16_h2i_check1'
				,'null_genres'
				,'Volume of null Genres in records on the Viewing Table'
				,.75
			
			UNION
			
			SELECT 'v289_m16_h2i_check2'
				,'sample'
				,'Volume of accounts scaled for the processing date'
				,.85
			) AS base
		ORDER BY a ASC

		COMMIT WORK

		CREATE UNIQUE INDEX key1 ON ${SQLFILE_ARG001}.v289_m16_dq_mct_checks (sequencer)

		CREATE lf INDEX lf1 ON ${SQLFILE_ARG001}.v289_m16_dq_mct_checks (target_table)

		CREATE lf INDEX lf2 ON ${SQLFILE_ARG001}.v289_m16_dq_mct_checks (target_field)

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.1: Creating Table v289_m16_dq_mct_checks DONE' TO client
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND lower(tname) = LOWER('v289_m16_dq_fact_checks')
				AND tabletype = 'TABLE'
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.1: Creating Table v289_m16_dq_fact_checks' TO client

		CREATE TABLE ${SQLFILE_ARG001}.v289_m16_dq_fact_checks (
			sequencer INTEGER NULL DEFAULT autoincrement
			,source VARCHAR(10) NOT NULL
			,test_context VARCHAR(100) NOT NULL
			,processing_date DATE NULL
			,actual_value DECIMAL(18, 3) NULL DEFAULT 0
			,test_result VARCHAR(15) NULL DEFAULT 'Pending'
			,
			)

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.v289_m16_dq_fact_checks
			TO vespa_group_low_security

		COMMIT WORK

		CREATE UNIQUE INDEX key1 ON ${SQLFILE_ARG001}.v289_m16_dq_fact_checks (sequencer)

		CREATE lf INDEX lf1 ON ${SQLFILE_ARG001}.v289_m16_dq_fact_checks (source)

		CREATE lf INDEX lf2 ON ${SQLFILE_ARG001}.v289_m16_dq_fact_checks (test_context)

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.1: Creating Table v289_m16_dq_fact_checks DONE' TO client
	END

	IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND lower(tname) = LOWER('v289_m16_dq_fact_checks_post')
				AND tabletype = 'TABLE'
			)
	BEGIN
		message convert(TIMESTAMP, now()) || ' | @ M00.1: Creating Table v289_m16_dq_fact_checks_post' TO client

		CREATE TABLE ${SQLFILE_ARG001}.v289_m16_dq_fact_checks_post (
			sequencer INTEGER NULL DEFAULT autoincrement
			,source VARCHAR(10) NOT NULL
			,module_ VARCHAR(10) NOT NULL
			,test_context VARCHAR(150) NOT NULL
			,processing_date DATE NULL
			,actual_value DECIMAL(18, 3) NULL DEFAULT 0
			,test_result VARCHAR(15) NULL DEFAULT 'Pending'
			,Updated_On DATETIME NOT NULL DEFAULT TIMESTAMP
			,
			)

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.v289_m16_dq_fact_checks_post
			TO vespa_group_low_security

		COMMIT WORK

		CREATE UNIQUE INDEX key1 ON ${SQLFILE_ARG001}.v289_m16_dq_fact_checks_post (sequencer)

		CREATE lf INDEX lf1 ON ${SQLFILE_ARG001}.v289_m16_dq_fact_checks_post (source)

		CREATE lf INDEX lf2 ON ${SQLFILE_ARG001}.v289_m16_dq_fact_checks_post (test_context)

		COMMIT WORK message convert(TIMESTAMP, now()) || ' | @ M00.1: Creating Table v289_m16_dq_fact_checks_post DONE' TO client
	END message convert(TIMESTAMP, now()) || ' | Begining M00.1 - Initialising Tables DONE' TO client message convert(TIMESTAMP, now()) || ' | Begining M00.2 - Initialising Views' TO client

	COMMIT WORK

			
		IF NOT EXISTS (
				SELECT tname
				FROM syscatalog
				WHERE creator = '${SQLFILE_ARG001}'
					AND tabletype = 'VIEW'
					AND upper(tname) = UPPER('barb_rawview_sv')
				)
		BEGIN
			CREATE VIEW ${SQLFILE_ARG001}.barb_rawview_sv
			AS
			SELECT *
			FROM BARB_PVF06_Viewing_Record_Panel_Members_sv

			COMMIT WORK

			GRANT SELECT
				ON ${SQLFILE_ARG001}.barb_rawview_sv
				TO vespa_group_low_security

			COMMIT WORK 
		END

--WARNING! ERRORS ENCOUNTERED DURING SQL PARSING!
		IF NOT EXISTS (
			SELECT tname
			FROM syscatalog
			WHERE creator = '${SQLFILE_ARG001}'
				AND tabletype = 'VIEW'
				AND upper(tname) = UPPER('Barb_skytvs_sv')
			)
		BEGIN
		CREATE VIEW ${SQLFILE_ARG001}.Barb_skytvs_sv
		AS
		SELECT *
		FROM BARB_Panel_Demographic_Data_TV_Sets_Characteristics_sv
		WHERE file_creation_date = (
				SELECT max(file_creation_date)
				FROM BARB_Panel_Demographic_Data_TV_Sets_Characteristics_sv
				)

		COMMIT WORK

		GRANT SELECT
			ON ${SQLFILE_ARG001}.Barb_skytvs_sv
			TO vespa_group_low_security

		COMMIT WORK 
		END
	END ;
GO 
commit;