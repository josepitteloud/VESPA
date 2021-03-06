

CREATE TABLE "BARB_PANEL_VIEWING_FILE_" (
        "file_creation_date" date DEFAULT NULL,
        "file_creation_time" time DEFAULT NULL,
        "file_type"          varchar(12) DEFAULT NULL,
        "file_version"       int DEFAULT NULL,
        "filename"           varchar(13) DEFAULT NULL,
        "Household_number"   int DEFAULT NULL,
        "Date_of_Activity_DB1" date DEFAULT NULL,
        "Set_number"         int DEFAULT NULL,
        "Start_time_of_session" int DEFAULT NULL,
        "Duration_of_session" int DEFAULT NULL,
        "Session_activity_type" int DEFAULT NULL,
        "Playback_type"      varchar(1) DEFAULT NULL,
        "DB1_Station_Code"   varchar(5) DEFAULT NULL,
        "Viewing_platform"   int DEFAULT NULL,
        "Date_of_Recording_DB1" date DEFAULT NULL,
        "Start_time_of_recording" int DEFAULT NULL,
        "Person_1_viewing"   int DEFAULT NULL,
        "Person_2_viewing"   int DEFAULT NULL,
        "Person_3_viewing"   int DEFAULT NULL,
        "Person_4_viewing"   int DEFAULT NULL,
        "Person_5_viewing"   int DEFAULT NULL,
        "Person_6_viewing"   int DEFAULT NULL,
        "Person_7_viewing"   int DEFAULT NULL,
        "Person_8_viewing"   int DEFAULT NULL,
        "Person_9_viewing"   int DEFAULT NULL,
        "Person_10_viewing"  int DEFAULT NULL,
        "Person_11_viewing"  int DEFAULT NULL,
        "Person_12_viewing"  int DEFAULT NULL,
        "Person_13_viewing"  int DEFAULT NULL,
        "Person_14_viewing"  int DEFAULT NULL,
        "Person_15_viewing"  int DEFAULT NULL,
        "Person_16_viewing"  int DEFAULT NULL,
        "Interactive_Bar_Code_Identifier" int DEFAULT NULL
)
;

CREATE TABLE "BARB_PANEL_VIEWING_FILE_HOME_CHARACTERISTICS" (
        "file_creation_date" date DEFAULT NULL,
        "file_creation_time" time DEFAULT NULL,
        "file_type"          varchar(12) DEFAULT NULL,
        "file_version"       int DEFAULT NULL,
        "filename"           varchar(13) DEFAULT NULL,
        "Household_number"   int DEFAULT NULL,
        "Date_Valid_For"     int DEFAULT NULL,
        "Panel_membership_status" int DEFAULT NULL,
        "No_of_TV_Sets"      int DEFAULT NULL,
        "No_of_VCRs"         int DEFAULT NULL,
        "No_of_PVRs"         int DEFAULT NULL,
        "No_of_DVDs"         int DEFAULT NULL,
        "No_of_People"       int DEFAULT NULL,
        "Social_Class"       varchar(2) DEFAULT NULL,
        "Presence_of_Children" int DEFAULT NULL,
        "Demographic_cell_1" int DEFAULT NULL,
        "BBC_Region_code"    int DEFAULT NULL,
        "BBC_ITV_Area_Segment" int DEFAULT NULL,
        "S4C_Segment"        int DEFAULT NULL,
        "Language_Spoken_at_Home" int DEFAULT NULL,
        "Welsh_Speaking_Home" int DEFAULT NULL,
        "Number_of_DVD_Recorders" int DEFAULT NULL,
        "Number_of_DVD_Players_not_recorders" int DEFAULT NULL,
        "Number_of_Sky_plus_PVRs" int DEFAULT NULL,
        "Number_of_other_PVRs" int DEFAULT NULL,
        "Broadband"          int DEFAULT NULL,
        "BBC_Sub_Reporting_Region" int DEFAULT NULL
)
;

CREATE TABLE "BARB_PANEL_VIEWING_FILE_INDIVIDAL_PANEL_MEMBER_DETAILS" (
        "file_creation_date" date DEFAULT NULL,
        "file_creation_time" time DEFAULT NULL,
        "file_type"          varchar(12) DEFAULT NULL,
        "file_version"       int DEFAULT NULL,
        "filename"           varchar(13) DEFAULT NULL,
        "Household_number"   int DEFAULT NULL,
        "Date_valid_for_DB1" date DEFAULT NULL,
        "Person_membership_status" int DEFAULT NULL,
        "Person_number"      int DEFAULT NULL,
        "Sex_code"           int DEFAULT NULL,
        "Date_of_birth"      date DEFAULT NULL,
        "Marital_status"     int DEFAULT NULL,
        "Household_status"   int DEFAULT NULL,
        "Working_status"     int DEFAULT NULL,
        "Terminal_age_of_education" int DEFAULT NULL,
        "Welsh_Language_code" int DEFAULT NULL,
        "Gaelic_language_code" int DEFAULT NULL,
        "Dependency_of_Children" int DEFAULT NULL,
        "Life_stage_12_classifications" int DEFAULT NULL,
        "Ethnic_Origin"      int DEFAULT NULL
)
;

CREATE TABLE "BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY" (
        "file_creation_date" date DEFAULT NULL,
        "file_creation_time" time DEFAULT NULL,
        "file_type"          varchar(12) DEFAULT NULL,
        "file_version"       int DEFAULT NULL,
        "filename"           varchar(13) DEFAULT NULL,
        "Household_Number"   int DEFAULT NULL,
        "Person_Number"      int DEFAULT NULL,
        "Reporting_Panel_Code" int DEFAULT NULL,
        "Date_of_Activity_DB1" date DEFAULT NULL,
        "Response_Code"      int DEFAULT NULL,
        "Processing_Weight"  numeric(7, 4) DEFAULT NULL,
        "Adults_Commercial_TV_Viewing_Sextile" int DEFAULT NULL,
        "ABC1_Adults_Commercial_TV_Viewing_Sextile" int DEFAULT NULL,
        "Adults_Total_Viewing_Sextile" int DEFAULT NULL,
        "ABC1_Adults_Total_Viewing_Sextile" int DEFAULT NULL,
        "Adults_16_34_Commercial_TV_Viewing_Sextile" int DEFAULT NULL,
        "Adults_16_34_Total_Viewing_Sextile" int DEFAULT NULL
)
;

CREATE TABLE "BARB_PANEL_VIEWING_FILE_TV_SET_CHARACTERISTICS" (
        "file_creation_date" date DEFAULT NULL,
        "file_creation_time" time DEFAULT NULL,
        "file_type"          varchar(12) DEFAULT NULL,
        "file_version"       int DEFAULT NULL,
        "filename"           varchar(13) DEFAULT NULL,
        "Household_number"   int DEFAULT NULL,
        "Date_Valid_for_DB1" date DEFAULT NULL,
        "Set_Membership_Status" int DEFAULT NULL,
        "Set_number"         int DEFAULT NULL,
        "Teletext"           int DEFAULT NULL,
        "Main_Location"      int DEFAULT NULL,
        "Analogue_Terrestrial" int DEFAULT NULL,
        "Digital_Terrestrial" int DEFAULT NULL,
        "Analogue_Satellite" int DEFAULT NULL,
        "Digital_Satellite"  int DEFAULT NULL,
        "Analogue_Cable"     int DEFAULT NULL,
        "Digital_Cable"      int DEFAULT NULL,
        "Blank_for_future_platforms" varchar(6) DEFAULT NULL,
        "VCR_present"        int DEFAULT NULL,
        "Sky_plus_PVR_present" int DEFAULT NULL,
        "Other_PVR_present"  int DEFAULT NULL,
        "DVD_Player_only_present" int DEFAULT NULL,
        "DVD_Recorder_present" int DEFAULT NULL,
        "HD_reception"       int DEFAULT NULL,
        "Reception_Capability_Code_1" int DEFAULT NULL,
        "Reception_Capability_Code_2" int DEFAULT NULL,
        "Reception_Capability_Code_3" int DEFAULT NULL,
        "Reception_Capability_Code_4" int DEFAULT NULL,
        "Reception_Capability_Code_5" int DEFAULT NULL,
        "Reception_Capability_Code_6" int DEFAULT NULL,
        "Reception_Capability_Code_7" int DEFAULT NULL,
        "Reception_Capability_Code_8" int DEFAULT NULL,
        "Reception_Capability_Code_9" int DEFAULT NULL,
        "Reception_Capability_Code_10" int DEFAULT NULL
)
;

CREATE TABLE "BARB_PANEL_VIEWING_FILE_VIEWING_RECORD_GUESTS" (
        "file_creation_date" date DEFAULT NULL,
        "file_creation_time" time DEFAULT NULL,
        "file_type"          varchar(12) DEFAULT NULL,
        "file_version"       int DEFAULT NULL,
        "filename"           varchar(13) DEFAULT NULL,
        "Household_number"   int DEFAULT NULL,
        "Date_of_Activity_DB1" date DEFAULT NULL,
        "Set_number"         int DEFAULT NULL,
        "Start_time_of_session" int DEFAULT NULL,
        "Duration_of_session" int DEFAULT NULL,
        "Session_activity_type" int DEFAULT NULL,
        "Playback_type"      varchar(1) DEFAULT NULL,
        "DB1_Station_Code"   varchar(5) DEFAULT NULL,
        "Viewing_platform"   int DEFAULT NULL,
        "Date_of_Recording_DB1" date DEFAULT NULL,
        "Start_time_of_recording" int DEFAULT NULL,
        "Male_4_9"           int DEFAULT NULL,
        "Male_10_15"         int DEFAULT NULL,
        "Male_16_19"         int DEFAULT NULL,
        "Male_20_24"         int DEFAULT NULL,
        "Male_25_34"         int DEFAULT NULL,
        "Male_35_44"         int DEFAULT NULL,
        "Male_45_64"         int DEFAULT NULL,
        "Male_65_plus"       int DEFAULT NULL,
        "Female_4_9"         int DEFAULT NULL,
        "Female_10_15"       int DEFAULT NULL,
        "Female_16_19"       int DEFAULT NULL,
        "Female_20_24"       int DEFAULT NULL,
        "Female_25_34"       int DEFAULT NULL,
        "Female_35_44"       int DEFAULT NULL,
        "Female_45_64"       int DEFAULT NULL,
        "Female_65_plus"     int DEFAULT NULL,
        "Interactive_Bar_Code_Identifier" int DEFAULT NULL
)
;

CREATE TABLE "BARB_PANEL_VIEWING_FILE_VIEWING_RECORD_PANEL_MEMBERS" (
        "file_creation_date" date DEFAULT NULL,
        "file_creation_time" time DEFAULT NULL,
        "file_type"          varchar(12) DEFAULT NULL,
        "file_version"       int DEFAULT NULL,
        "filename"           varchar(13) DEFAULT NULL,
        "Household_number"   int DEFAULT NULL,
        "Date_of_Activity_DB1" date DEFAULT NULL,
        "Set_number"         int DEFAULT NULL,
        "Start_time_of_session" int DEFAULT NULL,
        "Duration_of_session" int DEFAULT NULL,
        "Session_activity_type" int DEFAULT NULL,
        "Playback_type"      varchar(1) DEFAULT NULL,
        "DB1_Station_Code"   varchar(5) DEFAULT NULL,
        "Viewing_platform"   int DEFAULT NULL,
        "Date_of_Recording_DB1" date DEFAULT NULL,
        "Start_time_of_recording" int DEFAULT NULL,
        "Person_1_viewing"   int DEFAULT NULL,
        "Person_2_viewing"   int DEFAULT NULL,
        "Person_3_viewing"   int DEFAULT NULL,
        "Person_4_viewing"   int DEFAULT NULL,
        "Person_5_viewing"   int DEFAULT NULL,
        "Person_6_viewing"   int DEFAULT NULL,
        "Person_7_viewing"   int DEFAULT NULL,
        "Person_8_viewing"   int DEFAULT NULL,
        "Person_9_viewing"   int DEFAULT NULL,
        "Person_10_viewing"  int DEFAULT NULL,
        "Person_11_viewing"  int DEFAULT NULL,
        "Person_12_viewing"  int DEFAULT NULL,
        "Person_13_viewing"  int DEFAULT NULL,
        "Person_14_viewing"  int DEFAULT NULL,
        "Person_15_viewing"  int DEFAULT NULL,
        "Person_16_viewing"  int DEFAULT NULL,
        "Interactive_Bar_Code_Identifier" int DEFAULT NULL
)
;

