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
**Project Name:                                                 Skyview H2I
**Analysts:                             Angel Donnarumma        (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jason Thompson          (Jason.Thompson@skyiq.co.uk)
                                                                                ,Hoi Yu Tang            (HoiYu.Tang@skyiq.co.uk)
                                                                                ,Jose Pitteloud         (jose.pitteloud@skyiq.co.uk)
**Stakeholder:                          SkyIQ
                                                                                ,Jose Loureda           (Jose.Loureda@skyiq.co.uk)
**Due Date:                             11/07/2014
**Project Code (Insight Collation):     v289
**Sharepoint Folder:

        http://sp-department.bskyb.com/sites/SIGEvolved/Shared%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FSIGEvolved%2FShared%20Documents%2F01%20Analysis%20Requests%2FV289%20-%20Skyview%20Futures%2F01%20Plans%20Briefs%20and%20Project%20Admin

**Business Brief:

        This Module is meant to verify all bases for the H2I project are set on the schema that is
        triggering the business logic...

**Module:

        M00: Initialisation
                        M00.0 - Initialising Environment
                        M00.1 - Initialising Tables
                        M00.2 - Initialising Views
                        M00.3 - Returning Results

--------------------------------------------------------------------------------------------------------------
*/

----------------------------------
--M00.0 - Initialising Environment
----------------------------------

create or replace procedure v289_m00_initialisation
        @processing_date date = null
as begin

        MESSAGE cast(now() as timestamp)||' | Begining M00.0 - Initialising Environment' TO CLIENT
        MESSAGE cast(now() as timestamp)||' | @ M00.0: Initialising Environment DONE' TO CLIENT

------------------------------
-- M00.1 - Initialising Tables
------------------------------

        MESSAGE cast(now() as timestamp)||' | Begining M00.1 - Initialising Tables' TO CLIENT

-- PI_BARB_import

        if object_id('PI_BARB_import') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table PI_BARB_import' TO CLIENT

                create table PI_BARB_import(
                        imported_text varchar(200) default null
                )

                commit
                grant select on PI_BARB_import to vespa_group_low_security
                commit

                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table PI_BARB_import DONE' TO CLIENT
        end

-- BARB_Individual_Panel_Member_Details

        if object_id('BARB_Individual_Panel_Member_Details') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table BARB_Individual_Panel_Member_Details' TO CLIENT

                CREATE TABLE BARB_Individual_Panel_Member_Details       (
                        file_creation_date                              date
                        ,file_creation_time                     time
                        ,file_type                                              varchar(12)
                        ,file_version                                   int
                        ,filename                                               varchar(13)
                        ,Record_type                                    int             DEFAULT NULL
                        ,Household_number                               int             DEFAULT NULL
                        ,Date_valid_for_DB1                     int                     DEFAULT NULL
                        ,Person_membership_status               int             DEFAULT NULL
                        ,Person_number                                  int             DEFAULT NULL
                        ,Sex_code                                               int             DEFAULT NULL
                        ,Date_of_birth                                  int             DEFAULT NULL
                        ,Marital_status                                 int             DEFAULT NULL
                        ,Household_status                               int             DEFAULT NULL
                        ,Working_status                                 int             DEFAULT NULL
                        ,Terminal_age_of_education              int             DEFAULT NULL
                        ,Welsh_Language_code                    int             DEFAULT NULL
                        ,Gaelic_language_code                   int             DEFAULT NULL
                        ,Dependency_of_Children                 int             DEFAULT NULL
                        ,Life_stage_12_classifications  int             DEFAULT NULL
                        ,Ethnic_Origin                                  int             DEFAULT NULL
                )

                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table BARB_Individual_Panel_Member_Details DONE' TO CLIENT

                commit
                grant select on BARB_Individual_Panel_Member_Details to vespa_group_low_security
                commit
        end

-- BARB_Panel_Member_Responses_Weights_and_Viewing_Categories

        if object_id('BARB_Panel_Member_Responses_Weights_and_Viewing_Categories') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table BARB_Panel_Member_Responses_Weights_and_Viewing_Categories' TO CLIENT

                CREATE TABLE BARB_Panel_Member_Responses_Weights_and_Viewing_Categories (
                        file_creation_date                                                      date
                        ,file_creation_time                                             time
                        ,file_type                                                                      varchar(12)
                        ,file_version                                                           int
                        ,filename                                                                       varchar(13)
                        ,Record_Type                                                            int             DEFAULT NULL
                        ,Household_Number                                                       int             DEFAULT NULL
                        ,Person_Number                                                          int             DEFAULT NULL
                        ,Reporting_Panel_Code                                           int             DEFAULT NULL
                        ,Date_of_Activity_DB1                                           int             DEFAULT NULL
                        ,Response_Code                                                          int             DEFAULT NULL
                        ,Processing_Weight                                                      int             DEFAULT NULL
                        ,Adults_Commercial_TV_Viewing_Sextile           int             DEFAULT NULL
                        ,ABC1_Adults_Commercial_TV_Viewing_Sextile      int             DEFAULT NULL
                        ,Adults_Total_Viewing_Sextile                           int             DEFAULT NULL
                        ,ABC1_Adults_Total_Viewing_Sextile                      int             DEFAULT NULL
                        ,Adults_16_34_Commercial_TV_Viewing_Sextile     int             DEFAULT NULL
                        ,Adults_16_34_Total_Viewing_Sextile             int             DEFAULT NULL
                )

                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table BARB_Panel_Member_Responses_Weights_and_Viewing_Categories DONE' TO CLIENT

                commit
                grant select on BARB_Panel_Member_Responses_Weights_and_Viewing_Categories to vespa_group_low_security
                commit

        end



-- BARB_PVF_Viewing_Record_Panel_Members

        if object_id('BARB_PVF_Viewing_Record_Panel_Members') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table BARB_PVF_Viewing_Record_Panel_Members' TO CLIENT

                CREATE TABLE BARB_PVF_Viewing_Record_Panel_Members (
                        file_creation_date                                      date
                        ,file_creation_time                             time
                        ,file_type                                                      varchar(12)
                        ,file_version                                           int
                        ,filename                                                       varchar(13)
                        ,Record_type                                            int             DEFAULT NULL
                        ,Household_number                                       int             DEFAULT NULL
                        ,Date_of_Activity_DB1                           int             DEFAULT NULL
                        ,Set_number                                             int             DEFAULT NULL
                        ,Start_time_of_session                          int             DEFAULT NULL
                        ,Duration_of_session                            int             DEFAULT NULL
                        ,Session_activity_type                          int             DEFAULT NULL
                        ,Playback_type                                          varchar(1)      DEFAULT NULL
                        ,DB1_Station_Code                                       int             DEFAULT NULL
                        ,Viewing_platform                                       int             DEFAULT NULL
                        ,Date_of_Recording_DB1                          int             DEFAULT NULL
                        ,Start_time_of_recording                        int             DEFAULT NULL
                        ,Person_1_viewing                                       int             DEFAULT NULL
                        ,Person_2_viewing                                       int             DEFAULT NULL
                        ,Person_3_viewing                                       int             DEFAULT NULL
                        ,Person_4_viewing                                       int             DEFAULT NULL
                        ,Person_5_viewing                                       int             DEFAULT NULL
                        ,Person_6_viewing                                       int             DEFAULT NULL
                        ,Person_7_viewing                                       int             DEFAULT NULL
                        ,Person_8_viewing                                       int             DEFAULT NULL
                        ,Person_9_viewing                                       int             DEFAULT NULL
                        ,Person_10_viewing                                      int             DEFAULT NULL
                        ,Person_11_viewing                                      int             DEFAULT NULL
                        ,Person_12_viewing                                      int             DEFAULT NULL
                        ,Person_13_viewing                                      int             DEFAULT NULL
                        ,Person_14_viewing                                      int             DEFAULT NULL
                        ,Person_15_viewing                                      int             DEFAULT NULL
                        ,Person_16_viewing                                      int             DEFAULT NULL
                        ,Interactive_Bar_Code_Identifier        int             DEFAULT NULL
                        ,VOD_Indicator                                          int             DEFAULT NULL
                        ,VOD_Provider                                           int             DEFAULT NULL
                        ,VOD_Service                                            int             DEFAULT NULL
                        ,VOD_Type                                                       int             DEFAULT NULL
                        ,Device_in_use                                          int             DEFAULT NULL
                )

                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table BARB_PVF_Viewing_Record_Panel_Members DONE' TO CLIENT

                commit
                grant select on BARB_PVF_Viewing_Record_Panel_Members to vespa_group_low_security
                commit

        end


-- BARB_PVF06_Viewing_Record_Panel_Members

        if object_id('BARB_PVF06_Viewing_Record_Panel_Members') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table BARB_PVF06_Viewing_Record_Panel_Members' TO CLIENT

                CREATE TABLE BARB_PVF06_Viewing_Record_Panel_Members    (
                        id_row                                                          bigint          primary key identity  --- so we can check if any viewing records are not matched to the schedule
                        ,file_creation_date                             date
                        ,file_creation_time                             time
                        ,file_type                                                      varchar(12)
                        ,file_version                                           int
                        ,filename                                                       varchar(13)
                        ,Record_type                                            int             DEFAULT NULL
                        ,Household_number                                       int             DEFAULT NULL
                        ,Barb_date_of_activity                          date            -- New field
                        ,Actual_date_of_session                         date            -- change datatype from Barb. If Barb start time > 24:00 then add 1 to this date
                        ,Set_number                                             int             DEFAULT NULL
                        ,Start_time_of_session_text             varchar(6)      -- change datatype from Barb to make it easier to convert to timestamp later. Working field
                        ,Start_time_of_session                          timestamp       -- New field
                        ,End_time_of_session                            timestamp       -- new field
                        ,Duration_of_session                            int             DEFAULT NULL
                        ,Session_activity_type                          int             DEFAULT NULL
                        ,Playback_type                                          varchar(1)      DEFAULT NULL
                        ,DB1_Station_Code                                       int             DEFAULT NULL
                        ,Viewing_platform                                       int             DEFAULT NULL
                        ,Barb_date_of_recording                         date
                        ,Actual_Date_of_Recording                       date --- change datatype from Barb
                        ,Start_time_of_recording_text           varchar(6) --- change datatype from Barb to make it easier to convert to timestamp later. Working field
                        ,Start_time_of_recording                        timestamp --- new field
                        ,Person_1_viewing                                       int             DEFAULT NULL
                        ,Person_2_viewing                                       int             DEFAULT NULL
                        ,Person_3_viewing                                       int             DEFAULT NULL
                        ,Person_4_viewing                                       int             DEFAULT NULL
                        ,Person_5_viewing                                       int             DEFAULT NULL
                        ,Person_6_viewing                                       int             DEFAULT NULL
                        ,Person_7_viewing                                       int             DEFAULT NULL
                        ,Person_8_viewing                                       int             DEFAULT NULL
                        ,Person_9_viewing                                       int             DEFAULT NULL
                        ,Person_10_viewing                                      int             DEFAULT NULL
                        ,Person_11_viewing                                      int             DEFAULT NULL
                        ,Person_12_viewing                                      int             DEFAULT NULL
                        ,Person_13_viewing                                      int             DEFAULT NULL
                        ,Person_14_viewing                                      int             DEFAULT NULL
                        ,Person_15_viewing                                      int             DEFAULT NULL
                        ,Person_16_viewing                                      int             DEFAULT NULL
                        ,Interactive_Bar_Code_Identifier        int                     DEFAULT NULL
                        ,VOD_Indicator                                          int             DEFAULT NULL
                        ,VOD_Provider                                           int             DEFAULT NULL
                        ,VOD_Service                                            int             DEFAULT NULL
                        ,VOD_Type                                                       int             DEFAULT NULL
                        ,Device_in_use                                          int             DEFAULT NULL
                )
                commit

                create hg index ind_household_number on BARB_PVF06_Viewing_Record_Panel_Members(household_number)
                create hg index ind_db1 on BARB_PVF06_Viewing_Record_Panel_Members(db1_station_code)
                create hg index ind_start on BARB_PVF06_Viewing_Record_Panel_Members(Start_time_of_session)
                create hg index ind_end on BARB_PVF06_Viewing_Record_Panel_Members(End_time_of_session)
                create hg index ind_date on BARB_PVF06_Viewing_Record_Panel_Members(Barb_date_of_activity)

                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table BARB_PVF06_Viewing_Record_Panel_Members DONE' TO CLIENT

                commit
                grant select on BARB_PVF06_Viewing_Record_Panel_Members to vespa_group_low_security
                commit
        end



-- BARB_Panel_Demographic_Data_TV_Sets_Characteristics

        if object_id('BARB_Panel_Demographic_Data_TV_Sets_Characteristics') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table BARB_Panel_Demographic_Data_TV_Sets_Characteristics' TO CLIENT

                CREATE TABLE BARB_Panel_Demographic_Data_TV_Sets_Characteristics (
                        file_creation_date                              date
                        ,file_creation_time                     time
                        ,file_type                                              varchar(12)
                        ,file_version                                   int
                        ,filename                                               varchar(13)
                        ,Record_Type                                    int             DEFAULT NULL
                        ,Household_number                               int             DEFAULT NULL
                        ,Date_Valid_for_DB1                     int             DEFAULT NULL
                        ,Set_Membership_Status                  int             DEFAULT NULL
                        ,Set_number                                     int             DEFAULT NULL
                        ,Teletext                                               int             DEFAULT NULL
                        ,Main_Location                                  int             DEFAULT NULL
                        ,Analogue_Terrestrial                   int             DEFAULT NULL
                        ,Digital_Terrestrial                    int             DEFAULT NULL
                        ,Analogue_Satellite                     int             DEFAULT NULL
                        ,Digital_Satellite                              int             DEFAULT NULL
                        ,Analogue_Cable                                 int             DEFAULT NULL
                        ,Digital_Cable                                  int             DEFAULT NULL
                        ,VCR_present                                    int             DEFAULT NULL
                        ,Sky_PVR_present                                int             DEFAULT NULL
                        ,Other_PVR_present                              int             DEFAULT NULL
                        ,DVD_Player_only_present                int             DEFAULT NULL
                        ,DVD_Recorder_present                   int             DEFAULT NULL
                        ,HD_reception                                   int             DEFAULT NULL
                        ,Reception_Capability_Code1     int             DEFAULT NULL
                        ,Reception_Capability_Code2     int             DEFAULT NULL
                        ,Reception_Capability_Code3     int             DEFAULT NULL
                        ,Reception_Capability_Code4     int             DEFAULT NULL
                        ,Reception_Capability_Code5     int             DEFAULT NULL
                        ,Reception_Capability_Code6     int             DEFAULT NULL
                        ,Reception_Capability_Code7     int             DEFAULT NULL
                        ,Reception_Capability_Code8     int             DEFAULT NULL
                        ,Reception_Capability_Code9     int             DEFAULT NULL
                        ,Reception_Capability_Code10    int             DEFAULT NULL
                )

                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table BARB_Panel_Demographic_Data_TV_Sets_Characteristics DONE' TO CLIENT

                commit
                grant select on BARB_Panel_Demographic_Data_TV_Sets_Characteristics to vespa_group_low_security
                commit
        end



-- BARB_PVF04_Individual_Member_Details

        if object_id('BARB_PVF04_Individual_Member_Details') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table BARB_PVF04_Individual_Member_Details' TO CLIENT

                CREATE TABLE BARB_PVF04_Individual_Member_Details (
                        file_creation_date                              date
                        ,file_creation_time                     time
                        ,file_type                                              varchar(12)
                        ,file_version                                   int
                        ,filename                                               varchar(13)
                        ,Record_type                                    int             DEFAULT NULL
                        ,Household_number                               int             DEFAULT NULL
                        ,Date_valid_for_DB1                     date
                        ,Person_membership_status               int             DEFAULT NULL
                        ,Person_number                                  int             DEFAULT NULL
                        ,Sex_code                                               int             DEFAULT NULL
                        ,Date_of_birth                                  date
                        ,Marital_status                                 int             DEFAULT NULL
                        ,Household_status                               int             DEFAULT NULL
                        ,Working_status                                 int             DEFAULT NULL
                        ,Terminal_age_of_education              int             DEFAULT NULL
                        ,Welsh_Language_code                    int             DEFAULT NULL
                        ,Gaelic_language_code                   int             DEFAULT NULL
                        ,Dependency_of_Children                 int             DEFAULT NULL
                        ,Life_stage_12_classifications  int             DEFAULT NULL
                        ,Ethnic_Origin                                  int             DEFAULT NULL
                )

                commit
                create hg index ind_hhd         on BARB_PVF04_Individual_Member_Details(Household_number)
                create lf index ind_person      on BARB_PVF04_Individual_Member_Details(person_number)
                create lf index ind_create      on BARB_PVF04_Individual_Member_Details(file_creation_date)

                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table BARB_PVF04_Individual_Member_Details DONE' TO CLIENT

                commit
                grant select on BARB_PVF04_Individual_Member_Details to vespa_group_low_security
                commit
        end




---- V289_PIV_Grouped_Segments_desc

IF object_id('V289_PIV_Grouped_Segments_desc') IS  NULL
        BEGIN

                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table V289_PIV_Grouped_Segments_desc' TO CLIENT

                CREATE TABLE      V289_PIV_Grouped_Segments_desc(
                         row_id                                 INT             IDENTITY        not null
                        , channel_pack                  VARCHAR (40)DEFAULT NULL                not null
                        , daypart                               VARCHAR (30) DEFAULT NULL       not null
                        , Genre                                 VARCHAR(20) DEFAULT NULL        not null
                        , segment_id                    INT             DEFAULT NULL            not null
                        , active_flag                   BIT             DEFAULT 0               not null
                        , Updated_On            DATETIME        DEFAULT TIMESTAMP               not null
                        , Updated_By            VARCHAR(40)     DEFAULT user_name()             not null
            , segment_name          INT                 DEFAULT NULL null)


                INSERT INTO V289_PIV_Grouped_Segments_desc (segment_id, segment_name,   channel_pack,   daypart, Genre)

                   VALUES
		
				
				(1,1,'Diginets','breakfast','Children'),
			(1,1,'Diginets non-commercial','breakfast','Children'),
			(1,1,'Other','breakfast','Children'),
			(1,1,'Other non-commercial','breakfast','Children'),
			(1,1,'Terrestrial','breakfast','Children'),
			(1,1,'Terrestrial non-commercial','breakfast','Children'),
			(9,9,'Diginets','early prime','Children'),
			(9,9,'Diginets non-commercial','early prime','Children'),
			(9,9,'Other','early prime','Children'),
			(9,9,'Other non-commercial','early prime','Children'),
			(9,9,'Terrestrial','early prime','Children'),
			(9,9,'Terrestrial non-commercial','early prime','Children'),
			(17,17,'Diginets','late night','Children'),
			(17,17,'Diginets non-commercial','late night','Children'),
			(17,17,'Other','late night','Children'),
			(17,17,'Other non-commercial','late night','Children'),
			(17,17,'Terrestrial','late night','Children'),
			(17,17,'Terrestrial non-commercial','late night','Children'),
			(25,25,'Diginets','lunch','Children'),
			(25,25,'Diginets non-commercial','lunch','Children'),
			(25,25,'Other','lunch','Children'),
			(25,25,'Other non-commercial','lunch','Children'),
			(25,25,'Terrestrial','lunch','Children'),
			(25,25,'Terrestrial non-commercial','lunch','Children'),
			(33,33,'Diginets','morning','Children'),
			(33,33,'Diginets non-commercial','morning','Children'),
			(33,33,'Other','morning','Children'),
			(33,33,'Other non-commercial','morning','Children'),
			(33,33,'Terrestrial','morning','Children'),
			(33,33,'Terrestrial non-commercial','morning','Children'),
			(0,0,'Diginets','na','Children'),
			(0,0,'Diginets non-commercial','na','Children'),
			(0,0,'Other','na','Children'),
			(0,0,'Other non-commercial','na','Children'),
			(0,0,'Terrestrial','na','Children'),
			(0,0,'Terrestrial non-commercial','na','Children'),
			(41,41,'Diginets','night','Children'),
			(41,41,'Diginets non-commercial','night','Children'),
			(41,41,'Other','night','Children'),
			(41,41,'Other non-commercial','night','Children'),
			(41,41,'Terrestrial','night','Children'),
			(41,41,'Terrestrial non-commercial','night','Children'),
			(49,49,'Diginets','prime','Children'),
			(49,49,'Diginets non-commercial','prime','Children'),
			(49,49,'Other','prime','Children'),
			(49,49,'Other non-commercial','prime','Children'),
			(49,49,'Terrestrial','prime','Children'),
			(49,49,'Terrestrial non-commercial','prime','Children'),
			(2,2,'Diginets','breakfast','Entertainment'),
			(2,2,'Diginets non-commercial','breakfast','Entertainment'),
			(2,2,'Other','breakfast','Entertainment'),
			(2,2,'Other non-commercial','breakfast','Entertainment'),
			(2,2,'Terrestrial','breakfast','Entertainment'),
			(2,2,'Terrestrial non-commercial','breakfast','Entertainment'),
			(10,10,'Diginets','early prime','Entertainment'),
			(10,10,'Diginets non-commercial','early prime','Entertainment'),
			(10,10,'Other','early prime','Entertainment'),
			(10,10,'Other non-commercial','early prime','Entertainment'),
			(10,10,'Terrestrial','early prime','Entertainment'),
			(10,10,'Terrestrial non-commercial','early prime','Entertainment'),
			(18,18,'Diginets','late night','Entertainment'),
			(18,18,'Diginets non-commercial','late night','Entertainment'),
			(18,18,'Other','late night','Entertainment'),
			(18,18,'Other non-commercial','late night','Entertainment'),
			(18,18,'Terrestrial','late night','Entertainment'),
			(18,18,'Terrestrial non-commercial','late night','Entertainment'),
			(26,26,'Diginets','lunch','Entertainment'),
			(26,26,'Diginets non-commercial','lunch','Entertainment'),
			(26,26,'Other','lunch','Entertainment'),
			(26,26,'Other non-commercial','lunch','Entertainment'),
			(26,26,'Terrestrial','lunch','Entertainment'),
			(26,26,'Terrestrial non-commercial','lunch','Entertainment'),
			(34,34,'Diginets','morning','Entertainment'),
			(34,34,'Diginets non-commercial','morning','Entertainment'),
			(34,34,'Other','morning','Entertainment'),
			(34,34,'Other non-commercial','morning','Entertainment'),
			(34,34,'Terrestrial','morning','Entertainment'),
			(34,34,'Terrestrial non-commercial','morning','Entertainment'),
			(0,0,'Diginets','na','Entertainment'),
			(0,0,'Diginets non-commercial','na','Entertainment'),
			(0,0,'Other','na','Entertainment'),
			(0,0,'Other non-commercial','na','Entertainment'),
			(0,0,'Terrestrial','na','Entertainment'),
			(0,0,'Terrestrial non-commercial','na','Entertainment'),
			(42,42,'Diginets','night','Entertainment'),
			(42,42,'Diginets non-commercial','night','Entertainment'),
			(42,42,'Other','night','Entertainment'),
			(42,42,'Other non-commercial','night','Entertainment'),
			(42,42,'Terrestrial','night','Entertainment'),
			(42,42,'Terrestrial non-commercial','night','Entertainment'),
			(50,50,'Diginets','prime','Entertainment'),
			(50,50,'Diginets non-commercial','prime','Entertainment'),
			(50,50,'Other','prime','Entertainment'),
			(50,50,'Other non-commercial','prime','Entertainment'),
			(50,50,'Terrestrial','prime','Entertainment'),
			(50,50,'Terrestrial non-commercial','prime','Entertainment'),
			(3,3,'Diginets','breakfast','Movies'),
			(3,3,'Diginets non-commercial','breakfast','Movies'),
			(3,3,'Other','breakfast','Movies'),
			(3,3,'Other non-commercial','breakfast','Movies'),
			(3,3,'Terrestrial','breakfast','Movies'),
			(3,3,'Terrestrial non-commercial','breakfast','Movies'),
			(11,11,'Diginets','early prime','Movies'),
			(11,11,'Diginets non-commercial','early prime','Movies'),
			(11,11,'Other','early prime','Movies'),
			(11,11,'Other non-commercial','early prime','Movies'),
			(11,11,'Terrestrial','early prime','Movies'),
			(11,11,'Terrestrial non-commercial','early prime','Movies'),
			(19,19,'Diginets','late night','Movies'),
			(19,19,'Diginets non-commercial','late night','Movies'),
			(19,19,'Other','late night','Movies'),
			(19,19,'Other non-commercial','late night','Movies'),
			(19,19,'Terrestrial','late night','Movies'),
			(19,19,'Terrestrial non-commercial','late night','Movies'),
			(27,27,'Diginets','lunch','Movies'),
			(27,27,'Diginets non-commercial','lunch','Movies'),
			(27,27,'Other','lunch','Movies'),
			(27,27,'Other non-commercial','lunch','Movies'),
			(27,27,'Terrestrial','lunch','Movies'),
			(27,27,'Terrestrial non-commercial','lunch','Movies'),
			(3,3,'Diginets','morning','Movies'),
			(3,3,'Diginets non-commercial','morning','Movies'),
			(3,3,'Other','morning','Movies'),
			(3,3,'Other non-commercial','morning','Movies'),
			(3,3,'Terrestrial','morning','Movies'),
			(3,3,'Terrestrial non-commercial','morning','Movies'),
			(0,0,'Diginets','na','Movies'),
			(0,0,'Diginets non-commercial','na','Movies'),
			(0,0,'Other','na','Movies'),
			(0,0,'Other non-commercial','na','Movies'),
			(0,0,'Terrestrial','na','Movies'),
			(0,0,'Terrestrial non-commercial','na','Movies'),
			(43,43,'Diginets','night','Movies'),
			(43,43,'Diginets non-commercial','night','Movies'),
			(43,43,'Other','night','Movies'),
			(43,43,'Other non-commercial','night','Movies'),
			(43,43,'Terrestrial','night','Movies'),
			(43,43,'Terrestrial non-commercial','night','Movies'),
			(51,51,'Diginets','prime','Movies'),
			(51,51,'Diginets non-commercial','prime','Movies'),
			(51,51,'Other','prime','Movies'),
			(51,51,'Other non-commercial','prime','Movies'),
			(51,51,'Terrestrial','prime','Movies'),
			(51,51,'Terrestrial non-commercial','prime','Movies'),
			(4,4,'Diginets','breakfast','Music & Radio'),
			(4,4,'Diginets non-commercial','breakfast','Music & Radio'),
			(4,4,'Other','breakfast','Music & Radio'),
			(4,4,'Other non-commercial','breakfast','Music & Radio'),
			(4,4,'Terrestrial','breakfast','Music & Radio'),
			(4,4,'Terrestrial non-commercial','breakfast','Music & Radio'),
			(12,12,'Diginets','early prime','Music & Radio'),
			(12,12,'Diginets non-commercial','early prime','Music & Radio'),
			(12,12,'Other','early prime','Music & Radio'),
			(12,12,'Other non-commercial','early prime','Music & Radio'),
			(12,12,'Terrestrial','early prime','Music & Radio'),
			(12,12,'Terrestrial non-commercial','early prime','Music & Radio'),
			(20,20,'Diginets','late night','Music & Radio'),
			(20,20,'Diginets non-commercial','late night','Music & Radio'),
			(20,20,'Other','late night','Music & Radio'),
			(20,20,'Other non-commercial','late night','Music & Radio'),
			(20,20,'Terrestrial','late night','Music & Radio'),
			(20,20,'Terrestrial non-commercial','late night','Music & Radio'),
			(28,28,'Diginets','lunch','Music & Radio'),
			(28,28,'Diginets non-commercial','lunch','Music & Radio'),
			(28,28,'Other','lunch','Music & Radio'),
			(28,28,'Other non-commercial','lunch','Music & Radio'),
			(28,28,'Terrestrial','lunch','Music & Radio'),
			(28,28,'Terrestrial non-commercial','lunch','Music & Radio'),
			(4,4,'Diginets','morning','Music & Radio'),
			(4,4,'Diginets non-commercial','morning','Music & Radio'),
			(4,4,'Other','morning','Music & Radio'),
			(4,4,'Other non-commercial','morning','Music & Radio'),
			(4,4,'Terrestrial','morning','Music & Radio'),
			(4,4,'Terrestrial non-commercial','morning','Music & Radio'),
			(0,0,'Diginets','na','Music & Radio'),
			(0,0,'Diginets non-commercial','na','Music & Radio'),
			(0,0,'Other','na','Music & Radio'),
			(0,0,'Other non-commercial','na','Music & Radio'),
			(0,0,'Terrestrial','na','Music & Radio'),
			(0,0,'Terrestrial non-commercial','na','Music & Radio'),
			(44,44,'Diginets','night','Music & Radio'),
			(44,44,'Diginets non-commercial','night','Music & Radio'),
			(44,44,'Other','night','Music & Radio'),
			(44,44,'Other non-commercial','night','Music & Radio'),
			(44,44,'Terrestrial','night','Music & Radio'),
			(44,44,'Terrestrial non-commercial','night','Music & Radio'),
			(52,52,'Diginets','prime','Music & Radio'),
			(52,52,'Diginets non-commercial','prime','Music & Radio'),
			(52,52,'Other','prime','Music & Radio'),
			(52,52,'Other non-commercial','prime','Music & Radio'),
			(52,52,'Terrestrial','prime','Music & Radio'),
			(52,52,'Terrestrial non-commercial','prime','Music & Radio'),
			(0,0,'Diginets','breakfast','na'),
			(0,0,'Diginets non-commercial','breakfast','na'),
			(0,0,'Other','breakfast','na'),
			(0,0,'Other non-commercial','breakfast','na'),
			(0,0,'Terrestrial','breakfast','na'),
			(0,0,'Terrestrial non-commercial','breakfast','na'),
			(0,0,'Diginets','early prime','na'),
			(0,0,'Diginets non-commercial','early prime','na'),
			(0,0,'Other','early prime','na'),
			(0,0,'Other non-commercial','early prime','na'),
			(0,0,'Terrestrial','early prime','na'),
			(0,0,'Terrestrial non-commercial','early prime','na'),
			(0,0,'Diginets','late night','na'),
			(0,0,'Diginets non-commercial','late night','na'),
			(0,0,'Other','late night','na'),
			(0,0,'Other non-commercial','late night','na'),
			(0,0,'Terrestrial','late night','na'),
			(0,0,'Terrestrial non-commercial','late night','na'),
			(0,0,'Diginets','lunch','na'),
			(0,0,'Diginets non-commercial','lunch','na'),
			(0,0,'Other','lunch','na'),
			(0,0,'Other non-commercial','lunch','na'),
			(0,0,'Terrestrial','lunch','na'),
			(0,0,'Terrestrial non-commercial','lunch','na'),
			(0,0,'Diginets','morning','na'),
			(0,0,'Diginets non-commercial','morning','na'),
			(0,0,'Other','morning','na'),
			(0,0,'Other non-commercial','morning','na'),
			(0,0,'Terrestrial','morning','na'),
			(0,0,'Terrestrial non-commercial','morning','na'),
			(0,0,'Diginets','na','na'),
			(0,0,'Diginets non-commercial','na','na'),
			(0,0,'Other','na','na'),
			(0,0,'Other non-commercial','na','na'),
			(0,0,'Terrestrial','na','na'),
			(0,0,'Terrestrial non-commercial','na','na'),
			(0,0,'Diginets','night','na'),
			(0,0,'Diginets non-commercial','night','na'),
			(0,0,'Other','night','na'),
			(0,0,'Other non-commercial','night','na'),
			(0,0,'Terrestrial','night','na'),
			(0,0,'Terrestrial non-commercial','night','na'),
			(0,0,'Diginets','prime','na'),
			(0,0,'Diginets non-commercial','prime','na'),
			(0,0,'Other','prime','na'),
			(0,0,'Other non-commercial','prime','na'),
			(0,0,'Terrestrial','prime','na'),
			(0,0,'Terrestrial non-commercial','prime','na'),
			(5,5,'Diginets','breakfast','News & Documentaries'),
			(5,5,'Diginets non-commercial','breakfast','News & Documentaries'),
			(5,5,'Other','breakfast','News & Documentaries'),
			(5,5,'Other non-commercial','breakfast','News & Documentaries'),
			(5,5,'Terrestrial','breakfast','News & Documentaries'),
			(5,5,'Terrestrial non-commercial','breakfast','News & Documentaries'),
			(13,13,'Diginets','early prime','News & Documentaries'),
			(13,13,'Diginets non-commercial','early prime','News & Documentaries'),
			(13,13,'Other','early prime','News & Documentaries'),
			(13,13,'Other non-commercial','early prime','News & Documentaries'),
			(13,13,'Terrestrial','early prime','News & Documentaries'),
			(13,13,'Terrestrial non-commercial','early prime','News & Documentaries'),
			(21,21,'Diginets','late night','News & Documentaries'),
			(21,21,'Diginets non-commercial','late night','News & Documentaries'),
			(21,21,'Other','late night','News & Documentaries'),
			(21,21,'Other non-commercial','late night','News & Documentaries'),
			(21,21,'Terrestrial','late night','News & Documentaries'),
			(21,21,'Terrestrial non-commercial','late night','News & Documentaries'),
			(29,29,'Diginets','lunch','News & Documentaries'),
			(29,29,'Diginets non-commercial','lunch','News & Documentaries'),
			(29,29,'Other','lunch','News & Documentaries'),
			(29,29,'Other non-commercial','lunch','News & Documentaries'),
			(29,29,'Terrestrial','lunch','News & Documentaries'),
			(29,29,'Terrestrial non-commercial','lunch','News & Documentaries'),
			(37,37,'Diginets','morning','News & Documentaries'),
			(37,37,'Diginets non-commercial','morning','News & Documentaries'),
			(37,37,'Other','morning','News & Documentaries'),
			(37,37,'Other non-commercial','morning','News & Documentaries'),
			(37,37,'Terrestrial','morning','News & Documentaries'),
			(37,37,'Terrestrial non-commercial','morning','News & Documentaries'),
			(0,0,'Diginets','na','News & Documentaries'),
			(0,0,'Diginets non-commercial','na','News & Documentaries'),
			(0,0,'Other','na','News & Documentaries'),
			(0,0,'Other non-commercial','na','News & Documentaries'),
			(0,0,'Terrestrial','na','News & Documentaries'),
			(0,0,'Terrestrial non-commercial','na','News & Documentaries'),
			(45,45,'Diginets','night','News & Documentaries'),
			(45,45,'Diginets non-commercial','night','News & Documentaries'),
			(45,45,'Other','night','News & Documentaries'),
			(45,45,'Other non-commercial','night','News & Documentaries'),
			(45,45,'Terrestrial','night','News & Documentaries'),
			(45,45,'Terrestrial non-commercial','night','News & Documentaries'),
			(53,53,'Diginets','prime','News & Documentaries'),
			(53,53,'Diginets non-commercial','prime','News & Documentaries'),
			(53,53,'Other','prime','News & Documentaries'),
			(53,53,'Other non-commercial','prime','News & Documentaries'),
			(53,53,'Terrestrial','prime','News & Documentaries'),
			(53,53,'Terrestrial non-commercial','prime','News & Documentaries'),
			(6,6,'Diginets','breakfast','Specialist'),
			(6,6,'Diginets non-commercial','breakfast','Specialist'),
			(6,6,'Other','breakfast','Specialist'),
			(6,6,'Other non-commercial','breakfast','Specialist'),
			(6,6,'Terrestrial','breakfast','Specialist'),
			(6,6,'Terrestrial non-commercial','breakfast','Specialist'),
			(6,6,'Diginets','early prime','Specialist'),
			(6,6,'Diginets non-commercial','early prime','Specialist'),
			(6,6,'Other','early prime','Specialist'),
			(6,6,'Other non-commercial','early prime','Specialist'),
			(6,6,'Terrestrial','early prime','Specialist'),
			(6,6,'Terrestrial non-commercial','early prime','Specialist'),
			(6,6,'Diginets','late night','Specialist'),
			(6,6,'Diginets non-commercial','late night','Specialist'),
			(6,6,'Other','late night','Specialist'),
			(6,6,'Other non-commercial','late night','Specialist'),
			(6,6,'Terrestrial','late night','Specialist'),
			(6,6,'Terrestrial non-commercial','late night','Specialist'),
			(6,6,'Diginets','lunch','Specialist'),
			(6,6,'Diginets non-commercial','lunch','Specialist'),
			(6,6,'Other','lunch','Specialist'),
			(6,6,'Other non-commercial','lunch','Specialist'),
			(6,6,'Terrestrial','lunch','Specialist'),
			(6,6,'Terrestrial non-commercial','lunch','Specialist'),
			(6,6,'Diginets','morning','Specialist'),
			(6,6,'Diginets non-commercial','morning','Specialist'),
			(6,6,'Other','morning','Specialist'),
			(6,6,'Other non-commercial','morning','Specialist'),
			(6,6,'Terrestrial','morning','Specialist'),
			(6,6,'Terrestrial non-commercial','morning','Specialist'),
			(0,0,'Diginets','na','Specialist'),
			(0,0,'Diginets non-commercial','na','Specialist'),
			(0,0,'Other','na','Specialist'),
			(0,0,'Other non-commercial','na','Specialist'),
			(0,0,'Terrestrial','na','Specialist'),
			(0,0,'Terrestrial non-commercial','na','Specialist'),
			(6,6,'Diginets','night','Specialist'),
			(6,6,'Diginets non-commercial','night','Specialist'),
			(6,6,'Other','night','Specialist'),
			(6,6,'Other non-commercial','night','Specialist'),
			(6,6,'Terrestrial','night','Specialist'),
			(6,6,'Terrestrial non-commercial','night','Specialist'),
			(6,6,'Diginets','prime','Specialist'),
			(6,6,'Diginets non-commercial','prime','Specialist'),
			(6,6,'Other','prime','Specialist'),
			(6,6,'Other non-commercial','prime','Specialist'),
			(6,6,'Terrestrial','prime','Specialist'),
			(6,6,'Terrestrial non-commercial','prime','Specialist'),
			(7,7,'Diginets','breakfast','Sports'),
			(7,7,'Diginets non-commercial','breakfast','Sports'),
			(7,7,'Other','breakfast','Sports'),
			(7,7,'Other non-commercial','breakfast','Sports'),
			(7,7,'Terrestrial','breakfast','Sports'),
			(7,7,'Terrestrial non-commercial','breakfast','Sports'),
			(15,15,'Diginets','early prime','Sports'),
			(15,15,'Diginets non-commercial','early prime','Sports'),
			(15,15,'Other','early prime','Sports'),
			(15,15,'Other non-commercial','early prime','Sports'),
			(15,15,'Terrestrial','early prime','Sports'),
			(15,15,'Terrestrial non-commercial','early prime','Sports'),
			(23,23,'Diginets','late night','Sports'),
			(23,23,'Diginets non-commercial','late night','Sports'),
			(23,23,'Other','late night','Sports'),
			(23,23,'Other non-commercial','late night','Sports'),
			(23,23,'Terrestrial','late night','Sports'),
			(23,23,'Terrestrial non-commercial','late night','Sports'),
			(7,7,'Diginets','lunch','Sports'),
			(7,7,'Diginets non-commercial','lunch','Sports'),
			(7,7,'Other','lunch','Sports'),
			(7,7,'Other non-commercial','lunch','Sports'),
			(7,7,'Terrestrial','lunch','Sports'),
			(7,7,'Terrestrial non-commercial','lunch','Sports'),
			(7,7,'Diginets','morning','Sports'),
			(7,7,'Diginets non-commercial','morning','Sports'),
			(7,7,'Other','morning','Sports'),
			(7,7,'Other non-commercial','morning','Sports'),
			(7,7,'Terrestrial','morning','Sports'),
			(7,7,'Terrestrial non-commercial','morning','Sports'),
			(0,0,'Diginets','na','Sports'),
			(0,0,'Diginets non-commercial','na','Sports'),
			(0,0,'Other','na','Sports'),
			(0,0,'Other non-commercial','na','Sports'),
			(0,0,'Terrestrial','na','Sports'),
			(0,0,'Terrestrial non-commercial','na','Sports'),
			(23,23,'Diginets','night','Sports'),
			(23,23,'Diginets non-commercial','night','Sports'),
			(23,23,'Other','night','Sports'),
			(23,23,'Other non-commercial','night','Sports'),
			(23,23,'Terrestrial','night','Sports'),
			(23,23,'Terrestrial non-commercial','night','Sports'),
			(55,55,'Diginets','prime','Sports'),
			(55,55,'Diginets non-commercial','prime','Sports'),
			(55,55,'Other','prime','Sports'),
			(55,55,'Other non-commercial','prime','Sports'),
			(55,55,'Terrestrial','prime','Sports'),
			(55,55,'Terrestrial non-commercial','prime','Sports'),
			(8,8,'Diginets','breakfast','Unknown'),
			(8,8,'Diginets non-commercial','breakfast','Unknown'),
			(8,8,'Other','breakfast','Unknown'),
			(8,8,'Other non-commercial','breakfast','Unknown'),
			(8,8,'Terrestrial','breakfast','Unknown'),
			(8,8,'Terrestrial non-commercial','breakfast','Unknown'),
			(8,8,'Diginets','early prime','Unknown'),
			(8,8,'Diginets non-commercial','early prime','Unknown'),
			(8,8,'Other','early prime','Unknown'),
			(8,8,'Other non-commercial','early prime','Unknown'),
			(8,8,'Terrestrial','early prime','Unknown'),
			(8,8,'Terrestrial non-commercial','early prime','Unknown'),
			(8,8,'Diginets','late night','Unknown'),
			(8,8,'Diginets non-commercial','late night','Unknown'),
			(8,8,'Other','late night','Unknown'),
			(8,8,'Other non-commercial','late night','Unknown'),
			(8,8,'Terrestrial','late night','Unknown'),
			(8,8,'Terrestrial non-commercial','late night','Unknown'),
			(8,8,'Diginets','lunch','Unknown'),
			(8,8,'Diginets non-commercial','lunch','Unknown'),
			(8,8,'Other','lunch','Unknown'),
			(8,8,'Other non-commercial','lunch','Unknown'),
			(8,8,'Terrestrial','lunch','Unknown'),
			(8,8,'Terrestrial non-commercial','lunch','Unknown'),
			(8,8,'Diginets','morning','Unknown'),
			(8,8,'Diginets non-commercial','morning','Unknown'),
			(8,8,'Other','morning','Unknown'),
			(8,8,'Other non-commercial','morning','Unknown'),
			(8,8,'Terrestrial','morning','Unknown'),
			(8,8,'Terrestrial non-commercial','morning','Unknown'),
			(8,8,'Diginets','na','Unknown'),
			(8,8,'Diginets non-commercial','na','Unknown'),
			(8,8,'Other','na','Unknown'),
			(8,8,'Other non-commercial','na','Unknown'),
			(8,8,'Terrestrial','na','Unknown'),
			(8,8,'Terrestrial non-commercial','na','Unknown'),
			(8,8,'Diginets','night','Unknown'),
			(8,8,'Diginets non-commercial','night','Unknown'),
			(8,8,'Other','night','Unknown'),
			(8,8,'Other non-commercial','night','Unknown'),
			(8,8,'Terrestrial','night','Unknown'),
			(8,8,'Terrestrial non-commercial','night','Unknown'),
			(8,8,'Diginets','prime','Unknown'),
			(8,8,'Diginets non-commercial','prime','Unknown'),
			(8,8,'Other','prime','Unknown'),
			(8,8,'Other non-commercial','prime','Unknown'),
			(8,8,'Terrestrial','prime','Unknown'),
			(8,8,'Terrestrial non-commercial','prime','Unknown')


                        COMMIT
                CREATE LF INDEX id1 ON V289_PIV_Grouped_Segments_desc(segment_id)
                CREATE LF INDEX id2 ON V289_PIV_Grouped_Segments_desc(channel_pack)
                CREATE LF INDEX id3 ON V289_PIV_Grouped_Segments_desc(daypart)
                CREATE LF INDEX id4 ON V289_PIV_Grouped_Segments_desc(Genre)

                COMMIT
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table V289_PIV_Grouped_Segments_desc DONE' TO CLIENT
                COMMIT
                GRANT SELECT ON V289_PIV_Grouped_Segments_desc TO vespa_group_low_security

        END



IF object_id('V289_M08_SKY_HH_composition') IS NULL
        BEGIN

        MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table V289_M08_SKY_HH_composition' TO CLIENT

        CREATE TABLE V289_M08_SKY_HH_composition (
      row_id                INT         IDENTITY                not null
    , account_number        VARCHAR(20)                         NOT NULL
    , cb_key_household      BIGINT                              NOT NULL
    , exp_cb_key_db_person  BIGINT                              null
    , cb_key_individual     BIGINT                              null
    , cb_key_db_person      BIGINT                              null
    , cb_address_line_1     VARCHAR (200)                       null
    , HH_person_number      TINYINT                             null
    , person_gender         CHAR (1)                            null
    , person_age            TINYINT                             null
    , person_ageband        varchar(10)                         null
    , exp_person_head       TINYINT                             null
    , person_income         NUMERIC                             null
    , person_head           char(1)     DEFAULT '0'             null
    , household_size        TINYINT                             null
    , demographic_ID        TINYINT                             null
    , non_viewer            TINYINT     DEFAULT 0               null
    , viewer_hhsize         TINYINT                             null
    , Updated_On            DATETIME    DEFAULT TIMESTAMP       not null
    , Updated_By            VARCHAR(30) DEFAULT user_name()     not null)

        COMMIT

        create hg index hg1 on V289_M08_SKY_HH_composition (account_number)
        create hg index hg2 on V289_M08_SKY_HH_composition (cb_key_household)
        create hg index hg3 on V289_M08_SKY_HH_composition (exp_cb_key_db_person)
        create hg index hg4 on V289_M08_SKY_HH_composition (cb_address_line_1)
        commit

        MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table V289_M08_SKY_HH_composition DONE' TO CLIENT
        COMMIT
        GRANT SELECT ON V289_M08_SKY_HH_composition TO vespa_group_low_security

        END

IF object_id('V289_M08_SKY_HH_view') IS NULL
        BEGIN

        MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table V289_M08_SKY_HH_view' TO CLIENT

        CREATE TABLE V289_M08_SKY_HH_view    (
    account_number          VARCHAR(20)                 NOT NULL
    , cb_key_household      BIGINT                      NOT NULL
    , cb_address_line_1     VARCHAR (200)               NULL
    , HH_composition        TINYINT                     NULL
    , Children_count        TINYINT     DEFAULT 0       NULL
    , non_matching_flag     BIT         DEFAULT 0       NULL
    , edited_add_flag       BIT         DEFAULT 0       NOT NULL
    , Updated_On            DATETIME    DEFAULT TIMESTAMP       NOT NULL
    , Updated_By            VARCHAR(30) DEFAULT user_name()     NOT NULL)

        COMMIT

        CREATE HG INDEX idac ON V289_M08_SKY_HH_view(account_number)            commit
        CREATE HG INDEX idal ON V289_M08_SKY_HH_view(cb_address_line_1)         commit
        CREATE HG INDEX idhh ON V289_M08_SKY_HH_view(cb_key_household)          commit


        MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table V289_M08_SKY_HH_view DONE' TO CLIENT
        COMMIT
        GRANT SELECT ON V289_M08_SKY_HH_view TO vespa_group_low_security

        END





IF object_id('V289_M12_Skyview_weighted_duration') IS NULL
        BEGIN

                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table V289_M12_Skyview_weighted_duration' TO CLIENT

                CREATE TABLE V289_M12_Skyview_weighted_duration (
                  row_id                                INT identity            not null
                , source                                VARCHAR (6)             null
        , the_day                               DATE                            null
        , service_key                   INT                                     null
        , person_ageband                INT                                     null
        , person_gender                 INT                                     null
        , session_daypart               INT                                     null
                , channel_name                  VARCHAR(200)                    null
        , weighted_duration_mins DOUBLE                                         null
                , Updated_On            DATETIME    DEFAULT TIMESTAMP           not null
                , Updated_By            VARCHAR(30) DEFAULT user_name()         not null
        )

                COMMIT
                MESSAGE cast(now() as timestamp)||' |  M00.1: Creating Table V289_M12_Skyview_weighted_duration DONE' TO CLIENT

                GRANT SELECT ON V289_M12_Skyview_weighted_duration TO vespa_group_low_security

                COMMIT

        END

-- v289_genderage_lookup

        if object_id('v289_genderage_lookup') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating View v289_genderage_lookup' TO CLIENT

                select  *
                                ,identity(1) as the_id
                into    v289_genderage_lookup
                from    (
                                        select  'Male'  as sex
                                        union
                                        select  'Female' as sex
                                )   as thesex
                                cross join  (
                                                                select  '01-17' as ageband
                                                                union
                                                                select  '18-19' as ageband
                                                                union
                                                                select  '20-24' as ageband
                                                                union
                                                                select  '25-34' as ageband
                                                                union
                                                                select  '35-44' as ageband
                                                                union
                                                                select  '45-64' as ageband
                                                                union
                                                                select  '65+'   as ageband
                                                        )   as theage

                commit

                create unique index key1 on v289_genderage_lookup(the_id)
                commit

                grant select on v289_genderage_lookup to vespa_group_low_security
                commit

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating View v289_genderage_lookup DONE' TO CLIENT

        end

        MESSAGE cast(now() as timestamp)||' | Begining M00.2 - Initialising Views DONE' TO CLIENT


-- v289_M06_dp_raw_data

        if object_id('v289_M06_dp_raw_data') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table v289_M06_dp_raw_data' TO CLIENT

                create table v289_M06_dp_raw_data(
                        pk_viewing_prog_instance_fact           bigint                                  not null
                        ,dth_event_id                           bigint              default -1          not null
                        ,dk_event_start_datehour_dim            integer                                 not null
                        ,dk_event_end_datehour_dim                      integer                         not null
                        ,dk_broadcast_start_Datehour_dim        integer                                 not null
                        ,dk_instance_start_datehour_dim         integer                                 not null
                        ,dk_viewing_event_dim                           integer                         not null
                        ,duration                                                       integer         null
                        ,genre_description                                      varchar(20)             null
                        ,service_key                                            integer                 null
                        ,cb_key_household                                       bigint                  null
                        ,event_start_date_time_utc                      timestamp                       null
                        ,event_end_date_time_utc                        timestamp                       null
                        ,account_number                                         varchar(20)             null
                        ,subscriber_id                                          integer                 null
                        ,service_instance_id                            varchar(1)                      null
                        ,programme_name                                         varchar(100)            null
                        ,capping_end_Date_time_utc                      timestamp                       null
                        ,broadcast_start_date_time_utc          timestamp                               null
                        ,broadcast_end_date_time_utc            timestamp                               null
                        ,instance_start_date_time_utc           timestamp                               null
                        ,instance_end_date_time_utc                     timestamp                       null
                )

                commit

                create unique index key1 on v289_M06_dp_raw_data(pk_viewing_prog_instance_fact)
                create hg index hg0 on v289_M06_dp_raw_data(dth_event_id)
                create hg index hg1 on v289_M06_dp_raw_data(dk_event_start_datehour_dim)
                create hg index hg2 on v289_M06_dp_raw_data(dk_broadcast_start_datehour_dim)
                create hg index hg3 on v289_M06_dp_raw_data(dk_instance_start_datehour_dim)
                create hg index hg4 on v289_M06_dp_raw_data(dk_viewing_event_dim)
                create hg index hg5 on v289_M06_dp_raw_data(service_key)
                create hg index hg6 on v289_M06_dp_raw_data(account_number)
                create hg index hg7 on v289_M06_dp_raw_data(subscriber_id)
                create hg index hg8 on v289_M06_dp_raw_data(programme_name)
                create lf index lf1 on v289_M06_dp_raw_data(genre_description)
                commit

                grant select on v289_M06_dp_raw_data to vespa_group_low_security
                commit

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table v289_M06_dp_raw_data DONE' TO CLIENT

        end



-- V289_M07_dp_data

        if object_id('V289_M07_dp_data') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table V289_M07_dp_data' TO CLIENT

                create table V289_M07_dp_data(

                        account_number          varchar(20)                                     not null
                        ,subscriber_id          decimal(10)                                     not null
                        ,event_id                       bigint          default null            null
                        ,event_Start_utc        timestamp                                       not null
                        ,event_end_utc          timestamp                                       not null
                        ,chunk_start            timestamp       default null                    null
                        ,chunk_end                      timestamp       default null            null
                        ,event_duration_seg     int                                             not null
                        ,chunk_duration_seg int                 default null                    null
                        ,programme_genre        varchar(20)     default null                    null
                        ,session_daypart        varchar(11)     default null                    null
                        ,hhsize                         tinyint         default 0               null
                        ,viewer_hhsize        tinyint                  default 0               null  -- hhsize minus non-viewers
                        ,channel_pack           varchar(40) default null                        null
                        ,segment_id                     int                     default null    null
                        ,Overlap_batch          int                     default null            null
                        ,session_size           tinyint         default 0                       null
                        ,event_start_dim        int                                             not null
                        ,event_end_dim          int                                             not null
                )

                commit

                create hg index hg1     on v289_m07_dp_data(account_number)
                create hg index hg2     on v289_m07_dp_data(subscriber_id)
                create hg index hg3     on v289_m07_dp_data(event_start_dim)
                create hg index hg4     on v289_m07_dp_data(event_end_dim)
                create hg index hg5 on v289_m07_dp_data(segment_id)
                create hg index hg6 on v289_m07_dp_data(overlap_batch)
                create hg index hg7 on v289_m07_dp_data(event_id)
                create dttm index dttim1 on v289_m07_dp_data(event_start_utc)
                create dttm index dttim2 on v289_m07_dp_data(event_end_utc)
                commit

                grant all privileges on v289_m07_dp_data to vespa_group_low_security
                commit

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table V289_M07_dp_data DONE' TO CLIENT

        end

-- v289_m01_t_process_manager

        if object_id('v289_m01_t_process_manager') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table v289_m01_t_process_manager' TO CLIENT

                create table v289_m01_t_process_manager(
                        sequencer       integer         default autoincrement           not null
                        ,task           varchar(50)                                     not null
                        ,status         bit         default 0                           not null
                        ,exe_date       date                                            null
                        ,audit_date     date                                            not null
                )
                commit

                insert  into v289_m01_t_process_manager(task,audit_date) Values('v289_m08_Experian_data_preparation',cast(now() as date))
                insert  into v289_m01_t_process_manager(task,audit_date) Values('v289_m04_barb_data_preparation',cast(now() as date))
                insert  into v289_m01_t_process_manager(task,audit_date) Values('v289_m05_barb_Matrices_generation',cast(now() as date))
                insert  into v289_m01_t_process_manager(task,audit_date) Values('v289_m06_DP_data_extraction',cast(now() as date))
                insert  into v289_m01_t_process_manager(task,audit_date) Values('v289_m07_dp_data_preparation',cast(now() as date))
                insert  into v289_m01_t_process_manager(task,audit_date) Values('v289_m15_non_viewers_assignment',cast(now() as date))
                insert  into v289_m01_t_process_manager(task,audit_date) Values('v289_m09_Session_size_definition',cast(now() as date))
                insert  into v289_m01_t_process_manager(task,audit_date) Values('v289_M10_individuals_selection',cast(now() as date))
                insert  into v289_m01_t_process_manager(task,audit_date) Values('V289_M11_01_SC3_v1_1__do_weekly_segmentation',cast(now() as date))
                insert  into v289_m01_t_process_manager(task,audit_date) Values('V289_M11_02_SC3_v1_1__prepare_panel_members',cast(now() as date))
                insert  into v289_m01_t_process_manager(task,audit_date) Values('V289_M11_03_SC3I_v1_1__add_individual_data',cast(now() as date))
                insert  into v289_m01_t_process_manager(task,audit_date) Values('V289_M11_04_SC3I_v1_1__make_weights_BARB',cast(now() as date))
                insert  into v289_m01_t_process_manager(task,audit_date) Values('v289_m12_validation',cast(now() as date))
                insert  into v289_m01_t_process_manager(task,audit_date) Values('v289_M13_Create_Final_TechEdge_Output_Tables',cast(now() as date))
                commit

                grant select on v289_m01_t_process_manager to vespa_group_low_security
                commit

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table v289_m01_t_process_manager DONE' TO CLIENT

        end


-- SC3I_Variables_lookup_v1_1

        if object_id('SC3I_Variables_lookup_v1_1') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3I_Variables_lookup_v1_1' TO CLIENT

                create table SC3I_Variables_lookup_v1_1 (
                                id                      int             not null
                                ,scaling_variable        varchar(20)    not null
                )

                commit

                create lf index ind1 on SC3I_Variables_lookup_v1_1(id)
                create lf index ind2 on SC3I_Variables_lookup_v1_1(scaling_variable)
                commit

                insert into SC3I_Variables_lookup_v1_1 values (1, 'hhcomposition')
                insert into SC3I_Variables_lookup_v1_1 values (2, 'package')
                insert into SC3I_Variables_lookup_v1_1 values (3, 'isba_tv_region')
                insert into SC3I_Variables_lookup_v1_1 values (4, 'age_band')
                insert into SC3I_Variables_lookup_v1_1 values (5, 'gender')
                insert into SC3I_Variables_lookup_v1_1 values (6, 'head_of_hhd')
                commit

                grant select on SC3I_Variables_lookup_v1_1 to vespa_group_low_security
                commit

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3I_Variables_lookup_v1_1 DONE' TO CLIENT

        end


-- SC3I_Segments_lookup_v1_1

        if object_id('SC3I_Segments_lookup_v1_1') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3I_Segments_lookup_v1_1' TO CLIENT

                create table SC3I_Segments_lookup_v1_1 (
                        scaling_segment_id      int                     not null
                        ,sky_base_universe      varchar(30)             not null
                        ,isba_tv_region         varchar(30)             not null
                        ,hhcomposition          varchar(30)             not null
                        ,package                        varchar(30)     not null
                        ,head_of_hhd            varchar(1)              not null
                        ,gender                         varchar(1)      not null
                        ,age_band                       varchar(10)     not null
                        ,viewed_tv              char(1)                 not null
                )
                commit

                create hg index ind_seg on SC3I_Segments_lookup_v1_1(scaling_segment_id)
                create lf index ind_uni on SC3I_Segments_lookup_v1_1(sky_base_universe)
                create lf index ind_region on SC3I_Segments_lookup_v1_1(isba_tv_region)
                create lf index ind_comp on SC3I_Segments_lookup_v1_1(hhcomposition)
                create lf index ind_package on SC3I_Segments_lookup_v1_1(package)
                create lf index ind_head on SC3I_Segments_lookup_v1_1(head_of_hhd)
                create lf index ind_gender on SC3I_Segments_lookup_v1_1(gender)
                create lf index ind_age on SC3I_Segments_lookup_v1_1(age_band)
                create lf index ind_viewed on SC3I_Segments_lookup_v1_1(viewed_tv)

                commit

                grant select on SC3I_Segments_lookup_v1_1 to vespa_group_low_security
                commit

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3I_Segments_lookup_v1_1 DONE' TO CLIENT

        end


-- SC3I_Sky_base_segment_snapshots

        if object_id('SC3I_Sky_base_segment_snapshots') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3I_Sky_base_segment_snapshots' TO CLIENT

                create table SC3I_Sky_base_segment_snapshots (
                        account_number                  varchar(20)     not null
                        ,profiling_date                 date            not null
                        ,HH_person_number               tinyint         not null
                        ,population_scaling_segment_id  int             not null
                        ,vespa_scaling_segment_id       int             not null
                        ,expected_boxes                 int             not null
                )
                commit

                create hg index ind1 on SC3I_Sky_base_segment_snapshots(account_number)
                create lf index ind2 on SC3I_Sky_base_segment_snapshots(profiling_date)
                create lf index ind3 on SC3I_Sky_base_segment_snapshots(HH_person_number)
                create hg index ind4 on SC3I_Sky_base_segment_snapshots(population_scaling_segment_id)
                create hg index ind5 on SC3I_Sky_base_segment_snapshots(vespa_scaling_segment_id)
                commit

                grant select on SC3I_Sky_base_segment_snapshots to vespa_group_low_security
                commit

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3I_Sky_base_segment_snapshots DONE' TO CLIENT

        end


-- SC3I_Todays_panel_members

        if object_id('SC3I_Todays_panel_members') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3I_Todays_panel_members' TO CLIENT

                create table SC3I_Todays_panel_members (
                        account_number          varchar(20)     not null
                        ,HH_person_number       tinyint         not null
                        ,scaling_segment_id     int             not null
                )
                commit

                create hg index ind1 on SC3I_Todays_panel_members(account_number)
                create lf index ind2 on SC3I_Todays_panel_members(HH_person_number)
                create hg index ind3 on SC3I_Todays_panel_members(scaling_segment_id)
                commit

                grant select on SC3I_Todays_panel_members to vespa_group_low_security
                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3I_Todays_panel_members DONE' TO CLIENT

        end


-- SC3I_weighting_working_table

        if object_id('SC3I_weighting_working_table') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3I_weighting_working_table' TO CLIENT

                CREATE TABLE SC3I_weighting_working_table (
                        scaling_segment_id      INT             primary key     not null
                        ,sky_base_universe      VARCHAR(50)                     null
                        ,sky_base_accounts      DOUBLE                          not null
                        ,vespa_panel            DOUBLE          default 0       null
                        ,category_weight        DOUBLE                          null
                        ,sum_of_weights         DOUBLE                          null
                        ,segment_weight         DOUBLE                          null
                        ,indices_actual         DOUBLE                          null
                        ,indices_weighted       DOUBLE                          null
                )
                commit

                CREATE HG INDEX indx_un on SC3I_weighting_working_table(sky_base_universe)
                commit

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3I_weighting_working_table DONE' TO CLIENT

        end


-- SC3I_category_working_table

        if object_id('SC3I_category_working_table') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3I_category_working_table' TO CLIENT

                CREATE TABLE SC3I_category_working_table (
                         sky_base_universe      VARCHAR(50)                     null
                        ,profile                VARCHAR(50)                     null
                        ,value                  VARCHAR(70)                     null
                        ,sky_base_accounts      DOUBLE                          null
                        ,vespa_panel            DOUBLE                          null
                        ,category_weight        DOUBLE                          null
                        ,sum_of_weights         DOUBLE                          null
                        ,convergence_flag       TINYINT     DEFAULT 1           null
                )
                commit

                create hg index indx_universe on SC3I_category_working_table(sky_base_universe)
                create hg index indx_profile on SC3I_category_working_table(profile)
                create hg index indx_value on SC3I_category_working_table(value)

                commit

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3I_category_working_table DONE' TO CLIENT

        end


-- SC3I_category_subtotals

        if object_id('SC3I_category_subtotals') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3I_category_subtotals' TO CLIENT

                CREATE TABLE SC3I_category_subtotals (
                         scaling_date           date            null
                        ,sky_base_universe      VARCHAR(50)     null
                        ,profile                VARCHAR(50)     null
                        ,value                  VARCHAR(70)     null
                        ,sky_base_accounts      DOUBLE          null
                        ,vespa_panel            DOUBLE          null
                        ,category_weight        DOUBLE          null
                        ,sum_of_weights         DOUBLE          null
                        ,convergence            TINYINT         null
                )
                commit

                create index indx_date on SC3I_category_subtotals(scaling_date)
                create hg index indx_universe on SC3I_category_subtotals(sky_base_universe)
                create hg index indx_profile on SC3I_category_subtotals(profile)
                commit

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3I_category_subtotals DONE' TO CLIENT

        end


-- SC3I_metrics

        if object_id('SC3I_metrics') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3I_metrics' TO CLIENT

                CREATE TABLE SC3I_metrics (
                        scaling_date           DATE             null
                        ,iterations            int              null
                        ,convergence           tinyint          null
                        ,max_weight            float            null
                        ,av_weight             float            null
                        ,sum_of_weights        float            null
                        ,sky_base              bigint           null
                        ,vespa_panel           bigint           null
                        ,non_scalable_accounts bigint           null
                        ,sum_of_convergence    float            null
                )
                commit

                create index indx_date on SC3I_metrics(scaling_date)
                commit

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3I_metrics DONE' TO CLIENT

        end


-- SC3I_non_convergences

        if object_id('SC3I_non_convergences') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3I_non_convergences' TO CLIENT

                CREATE TABLE SC3I_non_convergences (
                          scaling_date           DATE           null
                         ,scaling_segment_id     int            null
                         ,difference             float          null
                )
                commit

                create index indx_date on SC3I_non_convergences(scaling_date)
                commit

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3I_non_convergences DONE' TO CLIENT

        end


-- SC3I_Weightings

        if object_id('SC3I_Weightings') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3I_Weightings' TO CLIENT

                create table SC3I_Weightings (
                        scaling_day                 date            not null
                        ,scaling_segment_ID         int             not null        -- links to the segments lookup table
                        ,vespa_accounts             bigint          default 0       not null -- Vespa panel accounts in this segment reporting back for this day
                        ,sky_base_accounts          bigint          not null        -- Sky base accounts for this day by segment
                        ,weighting                  double          default null    null  -- The weight for an account in this segment
                        ,sum_of_weights             double          default null    null-- The total weight for all accounts in this segment
                        ,indices_actual             double                          null
                        ,indices_weighted           double                          null
                        ,convergence                tinyint                         null
                        ,primary key (scaling_day, scaling_segment_ID)             -- not null
                )
                commit

                create date index idx1 on SC3I_Weightings(scaling_day)
                create hg index idx2 on SC3I_Weightings(scaling_segment_ID)

                grant select on SC3I_Weightings to vespa_group_low_security
                commit

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3I_Weightings DONE' TO CLIENT

        end


-- SC3I_Intervals

        if object_id('SC3I_Intervals') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3I_Intervals' TO CLIENT

                create table SC3I_Intervals (
                        account_number              varchar(20)     not null
                        ,HH_person_number           tinyint          not null
                        ,reporting_starts           date            not null
                        ,reporting_ends             date            not null
                        ,scaling_segment_ID         int             not null        -- links to the segments lookup table
                )
                commit

                create index for_joining on SC3I_Intervals (scaling_segment_ID, reporting_starts)
                create hg index idx1 on SC3I_Intervals (account_number)
                create hg index idx2 on SC3I_Intervals (HH_person_number)
                commit

                grant select on SC3I_Intervals to vespa_group_low_security
                commit

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3I_Intervals DONE' TO CLIENT

        end


-- V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING

        if object_id('V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING' TO CLIENT

                create table V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING (
                        account_number              varchar(20)     not null
                        ,HH_person_number           tinyint         not null
                        ,scaling_date               date            not null        -- date on which scaling is applied
                        ,scaling_weighting          float           not null
                        ,build_date                 datetime        not null        -- tracking processing to assist VIQ loads
                )
                commit

                create dttm index for_loading on V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING (build_date)
                create hg index idx1 on V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING (account_number)
                create date index idx3 on V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING (scaling_date)
                create hg index idx4 on V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING (HH_person_number)
                commit

                grant select on V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING to vespa_group_low_security
                commit

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING DONE' TO CLIENT

        end


-- V289_M11_04_Barb_weighted_population

        if object_id('V289_M11_04_Barb_weighted_population') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table V289_M11_04_Barb_weighted_population' TO CLIENT

                create table V289_M11_04_Barb_weighted_population (
                        ageband                 varchar(10)     null
                        ,gender                 char(1)         null
                        ,viewed_tv              char(1)         null
                        ,head_of_hhd            char(1)         null
                        ,barb_weight    double                  null
                )
                commit

                create lf index ind1 on V289_M11_04_Barb_weighted_population(ageband)
                create lf index ind2 on V289_M11_04_Barb_weighted_population(gender)
                create lf index ind3 on V289_M11_04_Barb_weighted_population(viewed_tv)
                create lf index ind4 on V289_M11_04_Barb_weighted_population(head_of_hhd)

                commit

                grant select on V289_M11_04_Barb_weighted_population to vespa_group_low_security
                commit

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table V289_M11_04_Barb_weighted_population DONE' TO CLIENT

        end


-- SC3_Weightings

        if object_id('SC3_Weightings') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3_Weightings' TO CLIENT

                create table SC3_Weightings (
                        scaling_day                 date            not null
                        ,scaling_segment_ID         int             not null        -- links to the segments lookup table
                        ,vespa_accounts             bigint          default 0       not null  -- Vespa panel accounts in this segment reporting back for this day
                        ,sky_base_accounts          bigint          not null        -- Sky base accounts for this day by segment
                        ,weighting                  double          default null    null -- The weight for an account in this segment
                        ,sum_of_weights             double          default null    null -- The total weight for all accounts in this segment
                        ,indices_actual             double              null
                        ,indices_weighted           double              null
                        ,convergence                tinyint             null
                        ,primary key (scaling_day, scaling_segment_ID)  -- not null
                )
                commit

                create date index idx1 on SC3_Weightings(scaling_day)
                create hg index idx2 on SC3_Weightings(scaling_segment_ID)
                commit

                grant select on SC3_Weightings to vespa_group_low_security
                commit

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3_Weightings DONE' TO CLIENT

        end


-- SC3_Intervals

        if object_id('SC3_Intervals') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3_Intervals' TO CLIENT

                create table SC3_Intervals (
                        account_number              varchar(20)     not null
                        ,reporting_starts           date            not null
                        ,reporting_ends             date            not null
                        ,scaling_segment_ID         int             not null        -- links to the segments lookup table
                        ,primary key (account_number, reporting_starts)             -- Won't bother forcing the no-overlap in DB constraints, but this is a good start
                )
                commit

                create index for_joining on SC3_Intervals (scaling_segment_ID, reporting_starts)
                create hg index idx1 on SC3_Intervals (account_number)
                commit

                grant select on SC3_Intervals to vespa_group_low_security
                commit

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3_Intervals DONE' TO CLIENT

        end


-- VESPA_HOUSEHOLD_WEIGHTING

        if object_id('VESPA_HOUSEHOLD_WEIGHTING') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table VESPA_HOUSEHOLD_WEIGHTING' TO CLIENT

                create table VESPA_HOUSEHOLD_WEIGHTING (
                        account_number              varchar(20)     not null
                        ,cb_key_household           bigint          not null
                        ,scaling_date               date            not null        -- date on which scaling is applied
                        ,scaling_weighting          float           not null
                        ,build_date                 datetime        not null        -- tracking processing to assist VIQ loads
                        ,primary key (account_number, scaling_date)
                )
                commit

                create dttm index for_loading on VESPA_HOUSEHOLD_WEIGHTING (build_date)
                create hg index idx1 on VESPA_HOUSEHOLD_WEIGHTING (account_number)
                create hg index idx2 on VESPA_HOUSEHOLD_WEIGHTING (cb_key_household)
                create date index idx3 on VESPA_HOUSEHOLD_WEIGHTING (scaling_date)
                commit

                grant select on VESPA_HOUSEHOLD_WEIGHTING to vespa_group_low_security
                commit

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table VESPA_HOUSEHOLD_WEIGHTING DONE' TO CLIENT

        end


-- SC3_Sky_base_segment_snapshots

        if object_id('SC3_Sky_base_segment_snapshots') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3_Sky_base_segment_snapshots' TO CLIENT

                create table SC3_Sky_base_segment_snapshots (
                        account_number                  varchar(20)     not null
                        ,profiling_date                 date            not null
                        ,cb_key_household               bigint          not null    -- needed for VIQ interface
                        ,population_scaling_segment_id  bigint          null
                        ,vespa_scaling_segment_id       bigint          null
                        ,expected_boxes                 tinyint         null            -- number of boxes in household; need to check they're all reporting
                        ,primary key (account_number, profiling_date)   -- not null
                )
                commit

                grant select on SC3_Sky_base_segment_snapshots to vespa_group_low_security
                commit

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3_Sky_base_segment_snapshots DONE' TO CLIENT

        end


-- SC3_Todays_panel_members

        if object_id('SC3_Todays_panel_members') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3_Todays_panel_members' TO CLIENT

                create table SC3_Todays_panel_members (
                        account_number              varchar(20)     not null primary key
                        ,scaling_segment_id         bigint          not null
                )
                commit

                grant select on SC3_Todays_panel_members to vespa_group_low_security
                commit

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3_Todays_panel_members DONE' TO CLIENT

        end


-- SC3_Todays_segment_weights

        if object_id('SC3_Todays_segment_weights') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3_Todays_segment_weights' TO CLIENT

                create table SC3_Todays_segment_weights (
                        scaling_segment_id          bigint          not null primary key
                        ,scaling_weighting          float           not null
                )
                commit

                grant select on SC3_Todays_segment_weights to vespa_group_low_security
                commit

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3_Todays_segment_weights DONE' TO CLIENT

        end


-- SC3_scaling_weekly_sample

        if object_id('SC3_scaling_weekly_sample') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3_scaling_weekly_sample' TO CLIENT

                CREATE TABLE SC3_scaling_weekly_sample (
                         account_number                     VARCHAR(20)     primary key not null
                        ,cb_key_household                   BIGINT          not null            -- Needed for VIQ interim solution
                        ,cb_key_individual                  BIGINT          not null            -- For ConsumerView linkage
                        ,consumerview_cb_row_id             BIGINT          null                    -- Not really needed for consumerview linkage, but whatever
                        ,universe                           VARCHAR(30)     null                    -- Single or multiple box household. Look at trying to make obsolete
                        ,sky_base_universe                  VARCHAR(30)     null                    -- Not adsmartable, Adsmartable with consent, Adsmartable but no consent household
                        ,vespa_universe                     VARCHAR(30)     null                    -- Universe used for Vespa
                        ,weighting_universe                 VARCHAR(30)     null                    -- Universe used for weighting purposes
                        ,isba_tv_region                     VARCHAR(20)     null                    -- Scaling variable 1 : Region
                        ,hhcomposition                      VARCHAR(2)      DEFAULT 'U'          not null -- Scaling variable 2: Household composition, originally from Experian Consumerview, collated later
                        ,tenure                             VARCHAR(15)     DEFAULT 'E) Unknown' not null-- Scaling variable 3: Tenure 'Unknown' removed from vespa panel
                        ,num_mix                            INT                 null
                        ,mix_pack                           VARCHAR(20)         null
                        ,package                            VARCHAR(20)         null                -- Scaling variable 4: Package
                        ,boxtype                            VARCHAR(35)         null                -- Old scaling variable 5: Look at ways to make obsolete.
                        ,no_of_stbs                         VARCHAR(15)         null                -- Scaling variable 5: No of set top boxes
                        ,hd_subscription                    VARCHAR(5)          null                -- Scaling variable 6: HD subscription
                        ,pvr                                VARCHAR(5)          null                -- Scaling variable 6: Is the box pvr capable?
                        ,population_scaling_segment_id      INT             DEFAULT NULL        null        -- segment scaling id for identifying segments in population
                        ,vespa_scaling_segment_id       INT             DEFAULT NULL            null -- segment scaling id for identifying segments used in rim weighting
                        ,mr_boxes                           INT                 null
                --    ,complete_viewing                   TINYINT         DEFAULT 0           -- Flag for all accounts with complete viewing data - DISCONTINUED; now interfaces with scoring module via defined tables
                )
                commit

                -- Might it be this one guy? this index rebuild making everything super slow? But it should be going in as a single atomic commit... but on inserts, it still only takes 55 sec...
                CREATE INDEX for_segment_identification_raw ON SC3_scaling_weekly_sample(isba_tv_region,hhcomposition, tenure, package, no_of_stbs, hd_subscription, pvr)
                CREATE INDEX experian_joining ON SC3_scaling_weekly_sample (consumerview_cb_row_id)
                CREATE INDEX for_grouping1 ON SC3_scaling_weekly_sample (population_scaling_segment_id)
                CREATE INDEX for_grouping2 ON SC3_scaling_weekly_sample (vespa_scaling_segment_id)
                COMMIT

                grant select on SC3_scaling_weekly_sample to vespa_group_low_security
                commit

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3_scaling_weekly_sample DONE' TO CLIENT

        end


-- SC3_weighting_working_table

        if object_id('SC3_weighting_working_table') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3_weighting_working_table' TO CLIENT

                CREATE TABLE SC3_weighting_working_table (
                        scaling_segment_id      INT             primary key
                        ,sky_base_universe      VARCHAR(50)     null
                        ,sky_base_accounts      DOUBLE          not null
                        ,vespa_panel            DOUBLE          default 0 not null
                        ,category_weight        DOUBLE          null
                        ,sum_of_weights         DOUBLE          null
                        ,segment_weight         DOUBLE          null
                        ,indices_actual         DOUBLE          null
                        ,indices_weighted       DOUBLE          null
                )
                commit

                CREATE HG INDEX indx_un on SC3_weighting_working_table(sky_base_universe)
                COMMIT

                grant select on SC3_weighting_working_table to vespa_group_low_security
                commit

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3_weighting_working_table DONE' TO CLIENT

        end


-- SC3_category_working_table

        if object_id('SC3_category_working_table') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3_category_working_table' TO CLIENT

                CREATE TABLE SC3_category_working_table (
                         sky_base_universe      VARCHAR(50)     null
                        ,profile                VARCHAR(50)     null
                        ,value                  VARCHAR(70)     null
                        ,sky_base_accounts      DOUBLE          null
                        ,vespa_panel            DOUBLE          null
                        ,category_weight        DOUBLE          null
                        ,sum_of_weights         DOUBLE          null
                        ,convergence_flag       TINYINT     DEFAULT 1 not null
                )
                commit

                create hg index indx_universe on SC3_category_working_table(sky_base_universe)
                create hg index indx_profile on SC3_category_working_table(profile)
                create hg index indx_value on SC3_category_working_table(value)
                COMMIT

                grant select on SC3_category_working_table to vespa_group_low_security
                commit

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3_category_working_table DONE' TO CLIENT

        end


-- SC3_category_subtotals

        if object_id('SC3_category_subtotals') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3_category_subtotals' TO CLIENT

                CREATE TABLE SC3_category_subtotals (
                         scaling_date           date            null
                        ,sky_base_universe      VARCHAR(50)     null
                        ,profile                VARCHAR(50)     null
                        ,value                  VARCHAR(70)     null
                        ,sky_base_accounts      DOUBLE          null
                        ,vespa_panel            DOUBLE          null
                        ,category_weight        DOUBLE          null
                        ,sum_of_weights         DOUBLE          null
                        ,convergence            TINYINT         null
                )
                commit

                create index indx_date on SC3_category_subtotals(scaling_date)
                create hg index indx_universe on SC3_category_subtotals(sky_base_universe)
                create hg index indx_profile on SC3_category_subtotals(profile)
                COMMIT

                grant select on SC3_category_subtotals to vespa_group_low_security
                commit

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3_category_subtotals DONE' TO CLIENT

        end


-- SC3_metrics

        if object_id('SC3_metrics') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3_metrics' TO CLIENT

                CREATE TABLE SC3_metrics (
                         scaling_date           DATE    null
                         ,iterations            int     null
                         ,convergence           tinyint null
                         ,max_weight            float   null
                         ,av_weight             float   null
                         ,sum_of_weights        float   null
                         ,sky_base              bigint  null
                         ,vespa_panel           bigint  null
                         ,non_scalable_accounts bigint  null
                         ,sum_of_convergence    float   null
                )
                commit

                create index indx_date on SC3_metrics(scaling_date)
                commit

                grant select on SC3_metrics to vespa_group_low_security
                commit

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3_metrics DONE' TO CLIENT

        end


-- SC3_non_convergences

        if object_id('SC3_non_convergences') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3_non_convergences' TO CLIENT

                CREATE TABLE SC3_non_convergences (
                          scaling_date           DATE   null
                         ,scaling_segment_id     int    null
                         ,difference             float  null
                )
                commit

                create index indx_date on SC3_non_convergences(scaling_date)
                commit

                grant select on SC3_non_convergences to vespa_group_low_security
                commit

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table SC3_non_convergences DONE' TO CLIENT

        end


-- BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories

        if object_id('BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories' TO CLIENT

                CREATE TABLE BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories (
                        file_creation_date                                                      date
                        ,file_creation_time                                             time
                        ,file_type                                                                      varchar(12)
                        ,file_version                                                           int
                        ,filename                                                                       varchar(13)
                        ,Record_Type                                                            int DEFAULT NULL
                        ,Household_Number                                                       int DEFAULT NULL
                        ,Person_Number                                                          int DEFAULT NULL
                        ,Reporting_Panel_Code                                           int DEFAULT NULL
                        ,Date_of_Activity_DB1                                           date
                        ,Response_Code                                                          int DEFAULT NULL
                        ,Processing_Weight                                                      int DEFAULT NULL
                        ,Adults_Commercial_TV_Viewing_Sextile           int DEFAULT NULL
                        ,ABC1_Adults_Commercial_TV_Viewing_Sextile      int DEFAULT NULL
                        ,Adults_Total_Viewing_Sextile                           int DEFAULT NULL
                        ,ABC1_Adults_Total_Viewing_Sextile                      int DEFAULT NULL
                        ,Adults_16_34_Commercial_TV_Viewing_Sextile int DEFAULT NULL
                        ,Adults_16_34_Total_Viewing_Sextile             int DEFAULT NULL
                )
                commit

                create hg index ind_hhd on BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories(Household_Number)
                create lf index ind_person on BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories(Person_Number)
                create lf index ind_panel on BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories(Reporting_Panel_Code)
                create lf index ind_date on BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories(Date_of_Activity_DB1)
                commit

                grant select on BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories to vespa_group_low_security
                commit

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories DONE' TO CLIENT

        end








-- barb_weights

        MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table barb_weights' TO CLIENT

                if object_id('barb_weights') is not null
                        drop table barb_weights

                select  distinct
                                date_of_activity_db1
                                ,reporting_panel_code
                                ,household_number
                                ,person_number
                                ,processing_weight /10 as processing_weight
                into    barb_weights
                from    BARB_Panel_Member_Responses_Weights_and_Viewing_Categories
                where   file_creation_date = (select max(file_Creation_date) from BARB_Panel_Member_Responses_Weights_and_Viewing_Categories)
                and     reporting_panel_code = 50
                and             date_of_activity_db1 = cast(replace(cast(@processing_date as varchar(10)),'-','') as integer)

                commit
                create hg index hg1 on barb_weights(household_number)
                create lf index lf1 on barb_weights(person_number)
                grant select on barb_weights to vespa_group_low_security
                commit

                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table barb_weights DONE' TO CLIENT





-- V289_M13_individual_viewing

                if object_id('V289_M13_individual_viewing') is null
                begin

                        MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table V289_M13_individual_viewing' TO CLIENT

                        create table V289_M13_individual_viewing (
                                        VIEWING_DEVICE_HASH             bigint          null-- subscriber_id
                                        ,ACCOUNT_NUMBER_HASH            int             not null
                                        ,STB_BROADCAST_START_TIME       timestamp       not null
                                        ,STB_BROADCAST_END_TIME         timestamp       not null
                                        ,STB_EVENT_START_TIME           timestamp       not null
                                        ,TIMESHIFT                      int             not null
                                        ,service_key                    int             not null
                                        ,Platform_flag                  int             not null
                                        ,Original_Service_key           int             not null
                                        ,AdSmart_flag                   int             not null
                                        ,DTH_VIEWING_EVENT_ID           bigint          not null
                                        ,person_1                       smallint        default 0 not null
                                        ,person_2                       smallint        default 0 not null
                                        ,person_3                       smallint        default 0 not null
                                        ,person_4                       smallint        default 0 not null
                                        ,person_5                       smallint        default 0 not null
                                        ,person_6                       smallint        default 0 not null
                                        ,person_7                       smallint        default 0 not null
                                        ,person_8                       smallint        default 0 not null
                                        ,person_9                       smallint        default 0 not null
                                        ,person_10                      smallint        default 0 not null
                                        ,person_11                      smallint        default 0 not null
                                        ,person_12                      smallint        default 0 not null
                                        ,person_13                      smallint        default 0 not null
                                        ,person_14                      smallint        default 0 not null
                                        ,person_15                      smallint        default 0 not null
                                        ,person_16                      smallint        default 0 not null
                        )
                        commit

                        MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table V289_M13_individual_viewing DONE' TO CLIENT

                end




-- V289_M13_individual_details

                if object_id('V289_M13_individual_details') is null
                begin

                        MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table V289_M13_individual_details' TO CLIENT

                        create table V289_M13_individual_details (
                                        account_number_hash                     int     not null
                                        ,person_number                  int             not null
                                        ,ind_scaling_weight             double          not null
                                        ,gender                         int             not null
                                        ,age_band                       int             not null
                                        ,head_of_hhd                    int             not null
                        )
                        commit

                        MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table V289_M13_individual_details DONE' TO CLIENT

                end



        MESSAGE cast(now() as timestamp)||' | Begining M00.1 - Initialising Tables DONE' TO CLIENT
                COMMIT


-----------------------------
-- M00.2 - Initialising Views
-----------------------------

        MESSAGE cast(now() as timestamp)||' | Begining M00.2 - Initialising Views' TO CLIENT

-- barb_rawview

        if object_id('barb_rawview') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating View barb_rawview' TO CLIENT

                create  view barb_rawview as
                select  *
                from    BARB_PVF06_Viewing_Record_Panel_Members

                commit
                grant select on barb_rawview to vespa_group_low_security
                commit

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating View barb_rawview DONE' TO CLIENT

        end

-- Barb_skytvs

        if object_id('Barb_skytvs') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating View Barb_skytvs' TO CLIENT

                create  view Barb_skytvs as
                select  *
                from    BARB_Panel_Demographic_Data_TV_Sets_Characteristics
                where   file_creation_date = (select max(file_creation_date) from BARB_Panel_Demographic_Data_TV_Sets_Characteristics)

                commit
                grant select on Barb_skytvs to vespa_group_low_security
                commit

                MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating View Barb_skytvs DONE' TO CLIENT

        end




----------------------------
-- M00.3 - Returning Results
----------------------------

        MESSAGE cast(now() as timestamp)||' | M00 Finished' TO CLIENT

end;

commit;
grant execute on v289_m00_initialisation to vespa_group_low_security;
commit;

