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
                         row_id                                 INT             IDENTITY
                        , channel_pack                  VARCHAR (40)DEFAULT NULL
                        , daypart                               VARCHAR (30) DEFAULT NULL
                        , Genre                                 VARCHAR(20) DEFAULT NULL
                        , segment_id                    INT             DEFAULT NULL 
                        , active_flag                   BIT             DEFAULT 0 
                        , Updated_On            DATETIME        DEFAULT TIMESTAMP 
                        , Updated_By            VARCHAR(40)     DEFAULT user_name()
            , segment_name          INT                 DEFAULT NULL )
        
                
                INSERT INTO V289_PIV_Grouped_Segments_desc (segment_id, segment_name,   channel_pack,   daypart, Genre)
                
                VALUES 
                (1,1,'Diginets','breakfast','Children'),
                (5,5,'Diginets non-commercial','breakfast','Children'),
                (9,9,'Other','breakfast','Children'),
                (15,15,'Other non-commercial','breakfast','Children'),
                (21,21,'Terrestrial','breakfast','Children'),
                (21,21,'Terrestrial non-commercial','breakfast','Children'),
                (22,22,'Diginets','breakfast','Entertainment'),
                (29,29,'Diginets non-commercial','breakfast','Entertainment'),
                (34,34,'Other','breakfast','Entertainment'),
                (34,34,'Other non-commercial','breakfast','Entertainment'),
                (41,41,'Terrestrial','breakfast','Entertainment'),
                (48,48,'Terrestrial non-commercial','breakfast','Entertainment'),
                (55,55,'Diginets','breakfast','Movies'),
                (55,55,'Diginets non-commercial','breakfast','Movies'),
                (61,61,'Other','breakfast','Movies'),
                (61,61,'Other non-commercial','breakfast','Movies'),
                (55,55,'Terrestrial','breakfast','Movies'),					--	(68,68,'Terrestrial','breakfast','Movies'),
                (55,55,'Terrestrial non-commercial','breakfast','Movies'),	--	(68,68,'Terrestrial non-commercial','breakfast','Movies'),
                (77,77,'Diginets','breakfast','Music & Radio'),
                (77,77,'Diginets non-commercial','breakfast','Music & Radio'),
                (83,83,'Other','breakfast','Music & Radio'),
                (83,83,'Other non-commercial','breakfast','Music & Radio'),
                (90,90,'Terrestrial','breakfast','Music & Radio'),
                (90,90,'Terrestrial non-commercial','breakfast','Music & Radio'),
                (91,91,'Diginets','breakfast','News & Documentaries'),			
                (91,91,'Diginets non-commercial','breakfast','News & Documentaries'),		--(98,98,'Diginets non-commercial','breakfast','News & Documentaries'),
                (105,105,'Other','breakfast','News & Documentaries'),
                (105,105,'Other non-commercial','breakfast','News & Documentaries'),
                (112,112,'Terrestrial','breakfast','News & Documentaries'),
                (119,119,'Terrestrial non-commercial','breakfast','News & Documentaries'),
                (126,126,'Diginets','breakfast','Specialist'),
                (126,126,'Diginets non-commercial','breakfast','Specialist'),
                (129,129,'Other','breakfast','Specialist'),
                (129,129,'Other non-commercial','breakfast','Specialist'),
                (132,132,'Terrestrial','breakfast','Specialist'),
                (132,132,'Terrestrial non-commercial','breakfast','Specialist'),
                (134,134,'Diginets','breakfast','Sports'),
                (134,134,'Diginets non-commercial','breakfast','Sports'),
                (139,139,'Other','breakfast','Sports'),
                (139,139,'Other non-commercial','breakfast','Sports'),
                (146,146,'Terrestrial','breakfast','Sports'),
                (146,146,'Terrestrial non-commercial','breakfast','Sports'),
                (154,154,'Diginets','breakfast','Unknown'),
                (154,154,'Diginets non-commercial','breakfast','Unknown'),
                (154,154,'Other','breakfast','Unknown'),
                (154,154,'Other non-commercial','breakfast','Unknown'),
                (154,154,'Terrestrial','breakfast','Unknown'),
                (154,154,'Terrestrial non-commercial','breakfast','Unknown'),
                (0,0,'Diginets','breakfast','na'),
                (0,0,'Diginets non-commercial','breakfast','na'),
                (0,0,'Other','breakfast','na'),
                (0,0,'Other non-commercial','breakfast','na'),
                (0,0,'Terrestrial','breakfast','na'),
                (0,0,'Terrestrial non-commercial','breakfast','na'),
				--------------------------------------------------------------------------
                (1,1,'Diginets','morning','Children'),				-- (2,2,'Diginets','morning','Children'),
                (6,6,'Diginets non-commercial','morning','Children'),
                (10,10,'Other','morning','Children'),
                (16,16,'Other non-commercial','morning','Children'),
                (21,21,'Terrestrial','morning','Children'),
                (21,21,'Terrestrial non-commercial','morning','Children'),
                (23,23,'Diginets','morning','Entertainment'),
                (29,29,'Diginets non-commercial','morning','Entertainment'),
                (35,35,'Other','morning','Entertainment'),
                (35,35,'Other non-commercial','morning','Entertainment'),
                (42,42,'Terrestrial','morning','Entertainment'),
                (49,49,'Terrestrial non-commercial','morning','Entertainment'),
                (55,55,'Diginets','morning','Movies'),
                (55,55,'Diginets non-commercial','morning','Movies'),
                (62,62,'Other','morning','Movies'),
                (62,62,'Other non-commercial','morning','Movies'),
                (55,55,'Terrestrial','morning','Movies'),						--	(68,68,'Terrestrial','morning','Movies'),
                (55,55,'Terrestrial non-commercial','morning','Movies'),		--	(68,68,'Terrestrial non-commercial','morning','Movies'),
                (77,77,'Diginets','morning','Music & Radio'),
                (77,77,'Diginets non-commercial','morning','Music & Radio'),
                (84,84,'Other','morning','Music & Radio'),
                (84,84,'Other non-commercial','morning','Music & Radio'),
                (90,90,'Terrestrial','morning','Music & Radio'),
                (90,90,'Terrestrial non-commercial','morning','Music & Radio'),
                (92,92,'Diginets','morning','News & Documentaries'),
                (99,99,'Diginets non-commercial','morning','News & Documentaries'),
                (106,106,'Other','morning','News & Documentaries'),
                (106,106,'Other non-commercial','morning','News & Documentaries'),
                (113,113,'Terrestrial','morning','News & Documentaries'),
                (120,120,'Terrestrial non-commercial','morning','News & Documentaries'),
                (126,126,'Diginets','morning','Specialist'), 						--  (127,127,'Diginets'					,'morning','Specialist'),
                (126,126,'Diginets non-commercial','morning','Specialist'),			--	(127,127,'Diginets non-commercial'	,'morning','Specialist'),
                (130,130,'Other','morning','Specialist'),
                (130,130,'Other non-commercial','morning','Specialist'),
                (132,132,'Terrestrial','morning','Specialist'),
                (132,132,'Terrestrial non-commercial','morning','Specialist'),
                (134,134,'Diginets','morning','Sports'),
                (134,134,'Diginets non-commercial','morning','Sports'),
                (140,140,'Other','morning','Sports'),
                (140,140,'Other non-commercial','morning','Sports'),
                (146,146,'Terrestrial','morning','Sports'),
                (146,146,'Terrestrial non-commercial','morning','Sports'),
                (154,154,'Diginets','morning','Unknown'),
                (154,154,'Diginets non-commercial','morning','Unknown'),
                (154,154,'Other','morning','Unknown'),
                (154,154,'Other non-commercial','morning','Unknown'),
                (154,154,'Terrestrial','morning','Unknown'),
                (154,154,'Terrestrial non-commercial','morning','Unknown'),
                (0,0,'Diginets','morning','na'),
                (0,0,'Diginets non-commercial','morning','na'),
                (0,0,'Other','morning','na'),
                (0,0,'Other non-commercial','morning','na'),
                (0,0,'Terrestrial','morning','na'),
                (0,0,'Terrestrial non-commercial','morning','na'),
				-------------------------------------------------------------------------
                (1,1,'Diginets','lunch','Children'),								--	(2,2,'Diginets','lunch','Children'),
                (7,7,'Diginets non-commercial','lunch','Children'),
                (11,11,'Other','lunch','Children'),
                (17,17,'Other non-commercial','lunch','Children'),
                (21,21,'Terrestrial','lunch','Children'),
                (21,21,'Terrestrial non-commercial','lunch','Children'),
                (24,24,'Diginets','lunch','Entertainment'),
                (30,30,'Diginets non-commercial','lunch','Entertainment'),
                (36,36,'Other','lunch','Entertainment'),
                (36,36,'Other non-commercial','lunch','Entertainment'),
                (43,43,'Terrestrial','lunch','Entertainment'),
                (50,50,'Terrestrial non-commercial','lunch','Entertainment'),
                (56,56,'Diginets','lunch','Movies'),
                (56,56,'Diginets non-commercial','lunch','Movies'),
                (63,63,'Other','lunch','Movies'),
                (63,63,'Other non-commercial','lunch','Movies'),
                (69,69,'Terrestrial','lunch','Movies'),
                (69,69,'Terrestrial non-commercial','lunch','Movies'),
                (78,78,'Diginets','lunch','Music & Radio'),
                (78,78,'Diginets non-commercial','lunch','Music & Radio'),
                (85,85,'Other','lunch','Music & Radio'),
                (85,85,'Other non-commercial','lunch','Music & Radio'),
                (90,90,'Terrestrial','lunch','Music & Radio'),
                (90,90,'Terrestrial non-commercial','lunch','Music & Radio'),
                (93,93,'Diginets','lunch','News & Documentaries'),
                (100,100,'Diginets non-commercial','lunch','News & Documentaries'),
                (107,107,'Other','lunch','News & Documentaries'),
                (107,107,'Other non-commercial','lunch','News & Documentaries'),
                (114,114,'Terrestrial','lunch','News & Documentaries'),
                (121,121,'Terrestrial non-commercial','lunch','News & Documentaries'),
                (126,126,'Diginets','lunch','Specialist'),						--	(127,127,'Diginets','lunch','Specialist'),
                (126,126,'Diginets non-commercial','lunch','Specialist'),		--	(127,127,'Diginets non-commercial','lunch','Specialist'),
                (130,130,'Other','lunch','Specialist'),
                (130,130,'Other non-commercial','lunch','Specialist'),
                (132,132,'Terrestrial','lunch','Specialist'),
                (132,132,'Terrestrial non-commercial','lunch','Specialist'),
                (134,134,'Diginets','lunch','Sports'),
                (134,134,'Diginets non-commercial','lunch','Sports'),
                (141,141,'Other','lunch','Sports'),
                (141,141,'Other non-commercial','lunch','Sports'),
                (146,146,'Terrestrial','lunch','Sports'),						-- 	(147,147,'Terrestrial','lunch','Sports'),
                (146,146,'Terrestrial non-commercial','lunch','Sports'),		--	(147,147,'Terrestrial non-commercial','lunch','Sports'),
                (154,154,'Diginets','lunch','Unknown'),
                (154,154,'Diginets non-commercial','lunch','Unknown'),
                (154,154,'Other','lunch','Unknown'),
                (154,154,'Other non-commercial','lunch','Unknown'),
                (154,154,'Terrestrial','lunch','Unknown'),
                (154,154,'Terrestrial non-commercial','lunch','Unknown'),
				(0,0,'Diginets','lunch','na'),
                (0,0,'Diginets non-commercial','lunch','na'),
                (0,0,'Other','lunch','na'),
                (0,0,'Other non-commercial','lunch','na'),
                (0,0,'Terrestrial','lunch','na'),
                (0,0,'Terrestrial non-commercial','lunch','na'),
				-------------------------------------------------------------------------
                (3,3,'Diginets','early prime','Children'),
                (8,8,'Diginets non-commercial','early prime','Children'),
                (12,12,'Other','early prime','Children'),
                (18,18,'Other non-commercial','early prime','Children'),
                (21,21,'Terrestrial','early prime','Children'),
                (21,21,'Terrestrial non-commercial','early prime','Children'),
                (25,25,'Diginets','early prime','Entertainment'),
                (30,30,'Diginets non-commercial','early prime','Entertainment'),
                (37,37,'Other','early prime','Entertainment'),
                (37,37,'Other non-commercial','early prime','Entertainment'),
                (44,44,'Terrestrial','early prime','Entertainment'),
                (51,51,'Terrestrial non-commercial','early prime','Entertainment'),
                (57,57,'Diginets','early prime','Movies'),
                (57,57,'Diginets non-commercial','early prime','Movies'),
                (64,64,'Other','early prime','Movies'),
                (64,64,'Other non-commercial','early prime','Movies'),
                (70,70,'Terrestrial','early prime','Movies'),
                (70,70,'Terrestrial non-commercial','early prime','Movies'),
                (78,78,'Diginets','early prime','Music & Radio'),						--	(79,79,'Diginets','early prime','Music & Radio'),
                (78,78,'Diginets non-commercial','early prime','Music & Radio'),		--	(79,79,'Diginets non-commercial','early prime','Music & Radio'),
                (86,86,'Other','early prime','Music & Radio'),
                (86,86,'Other non-commercial','early prime','Music & Radio'),
                (90,90,'Terrestrial','early prime','Music & Radio'),
                (90,90,'Terrestrial non-commercial','early prime','Music & Radio'),
                (94,94,'Diginets','early prime','News & Documentaries'),
                (101,101,'Diginets non-commercial','early prime','News & Documentaries'),
                (108,108,'Other','early prime','News & Documentaries'),
                (108,108,'Other non-commercial','early prime','News & Documentaries'),
                (115,115,'Terrestrial','early prime','News & Documentaries'),
                (122,122,'Terrestrial non-commercial','early prime','News & Documentaries'),
                (126,126,'Diginets','early prime','Specialist'),						--	(127,127,'Diginets','early prime','Specialist'),
                (126,126,'Diginets non-commercial','early prime','Specialist'),			--	(127,127,'Diginets non-commercial','early prime','Specialist'),
                (130,130,'Other','early prime','Specialist'),
                (130,130,'Other non-commercial','early prime','Specialist'),
                (132,132,'Terrestrial','early prime','Specialist'),
                (132,132,'Terrestrial non-commercial','early prime','Specialist'),
                (135,135,'Diginets','early prime','Sports'),
                (135,135,'Diginets non-commercial','early prime','Sports'),
                (142,142,'Other','early prime','Sports'),
                (142,142,'Other non-commercial','early prime','Sports'),
                (146,146,'Terrestrial','early prime','Sports'),							--	(147,147,'Terrestrial','early prime','Sports'),
                (146,146,'Terrestrial non-commercial','early prime','Sports'),			--	(147,147,'Terrestrial non-commercial','early prime','Sports'),
                (154,154,'Diginets','early prime','Unknown'),
                (154,154,'Diginets non-commercial','early prime','Unknown'),
                (154,154,'Other','early prime','Unknown'),
                (154,154,'Other non-commercial','early prime','Unknown'),
                (154,154,'Terrestrial','early prime','Unknown'),
                (154,154,'Terrestrial non-commercial','early prime','Unknown'),
				/*(155,155,'Diginets','early prime','Unknown'),
                (155,155,'Diginets non-commercial','early prime','Unknown'),
                (155,155,'Other','early prime','Unknown'),
                (155,155,'Other non-commercial','early prime','Unknown'),
                (155,155,'Terrestrial','early prime','Unknown'),
                (155,155,'Terrestrial non-commercial','early prime','Unknown'),*/
				-------------------------------------------------------------------------
                (0,0,'Diginets','early prime','na'),
                (0,0,'Diginets non-commercial','early prime','na'),
                (0,0,'Other','early prime','na'),
                (0,0,'Other non-commercial','early prime','na'),
                (0,0,'Terrestrial','early prime','na'),
                (0,0,'Terrestrial non-commercial','early prime','na'),
                (4,4,'Diginets','prime','Children'),
                (4,4,'Diginets non-commercial','prime','Children'),
                (13,13,'Other','prime','Children'),
                (19,19,'Other non-commercial','prime','Children'),
                (21,21,'Terrestrial','prime','Children'),
                (21,21,'Terrestrial non-commercial','prime','Children'),
                (26,26,'Diginets','prime','Entertainment'),
                (31,31,'Diginets non-commercial','prime','Entertainment'),
                (38,38,'Other','prime','Entertainment'),
                (38,38,'Other non-commercial','prime','Entertainment'),
                (45,45,'Terrestrial','prime','Entertainment'),
                (52,52,'Terrestrial non-commercial','prime','Entertainment'),
                (58,58,'Diginets','prime','Movies'),
                (58,58,'Diginets non-commercial','prime','Movies'),
                (65,65,'Other','prime','Movies'),
                (65,65,'Other non-commercial','prime','Movies'),
                (71,71,'Terrestrial','prime','Movies'),
                (71,71,'Terrestrial non-commercial','prime','Movies'),				--	(74,74,'Terrestrial non-commercial','prime','Movies'),
                (80,80,'Diginets','prime','Music & Radio'),
                (80,80,'Diginets non-commercial','prime','Music & Radio'),
                (87,87,'Other','prime','Music & Radio'),
                (87,87,'Other non-commercial','prime','Music & Radio'),
                (90,90,'Terrestrial','prime','Music & Radio'),
                (90,90,'Terrestrial non-commercial','prime','Music & Radio'),
                (95,95,'Diginets','prime','News & Documentaries'),
                (102,102,'Diginets non-commercial','prime','News & Documentaries'),
                (109,109,'Other','prime','News & Documentaries'),
                (109,109,'Other non-commercial','prime','News & Documentaries'),
                (116,116,'Terrestrial','prime','News & Documentaries'),
                (123,123,'Terrestrial non-commercial','prime','News & Documentaries'),
                (126,126,'Diginets','prime','Specialist'),							--	(127,127,'Diginets','prime','Specialist'),
                (126,126,'Diginets non-commercial','prime','Specialist'),			--	(127,127,'Diginets non-commercial','prime','Specialist'),
                (130,130,'Other','prime','Specialist'),
                (130,130,'Other non-commercial','prime','Specialist'),
                (132,132,'Terrestrial','prime','Specialist'),
                (132,132,'Terrestrial non-commercial','prime','Specialist'),
                (136,136,'Diginets','prime','Sports'),
                (136,136,'Diginets non-commercial','prime','Sports'),
                (143,143,'Other','prime','Sports'),
                (143,143,'Other non-commercial','prime','Sports'),
                (148,148,'Terrestrial','prime','Sports'),
                (148,148,'Terrestrial non-commercial','prime','Sports'),			--	 (151,151,'Terrestrial non-commercial','prime','Sports'),
                (154,154,'Diginets','prime','Unknown'),
                (154,154,'Diginets non-commercial','prime','Unknown'),
                (154,154,'Other','prime','Unknown'),
                (154,154,'Other non-commercial','prime','Unknown'),
                (154,154,'Terrestrial','prime','Unknown'),
                (154,154,'Terrestrial non-commercial','prime','Unknown'),
																					/*
																					 (156,156,'Diginets','prime','Unknown'),
																					(156,156,'Diginets non-commercial','prime','Unknown'),
																					(156,156,'Other','prime','Unknown'),
																					(156,156,'Other non-commercial','prime','Unknown'),
																					(156,156,'Terrestrial','prime','Unknown'),
																					(156,156,'Terrestrial non-commercial','prime','Unknown'),*/
                (0,0,'Diginets','prime','na'),
                (0,0,'Diginets non-commercial','prime','na'),
                (0,0,'Other','prime','na'),
                (0,0,'Other non-commercial','prime','na'),
                (0,0,'Terrestrial','prime','na'),
                (0,0,'Terrestrial non-commercial','prime','na'),
				-------------------------------------------------------------------------
                (4,4,'Diginets','late night','Children'),
                (4,4,'Diginets non-commercial','late night','Children'),
                (14,14,'Other','late night','Children'),
                (20,20,'Other non-commercial','late night','Children'),
                (21,21,'Terrestrial','late night','Children'),
                (21,21,'Terrestrial non-commercial','late night','Children'),
                (27,27,'Diginets','late night','Entertainment'),
                (32,32,'Diginets non-commercial','late night','Entertainment'),
                (39,39,'Other','late night','Entertainment'),
                (39,39,'Other non-commercial','late night','Entertainment'),
                (46,46,'Terrestrial','late night','Entertainment'),
                (53,53,'Terrestrial non-commercial','late night','Entertainment'),
                (59,59,'Diginets','late night','Movies'),
                (59,59,'Diginets non-commercial','late night','Movies'),
                (66,66,'Other','late night','Movies'),
                (66,66,'Other non-commercial','late night','Movies'),
                (72,72,'Terrestrial','late night','Movies'),
                (75,75,'Terrestrial non-commercial','late night','Movies'),
                (80,80,'Diginets','late night','Music & Radio'),					--	(81,81,'Diginets','late night','Music & Radio'),
                (80,80,'Diginets non-commercial','late night','Music & Radio'),		--	(81,81,'Diginets non-commercial','late night','Music & Radio'),
                (88,88,'Other','late night','Music & Radio'),
                (88,88,'Other non-commercial','late night','Music & Radio'),
                (90,90,'Terrestrial','late night','Music & Radio'),
                (90,90,'Terrestrial non-commercial','late night','Music & Radio'),
                (96,96,'Diginets','late night','News & Documentaries'),
                (103,103,'Diginets non-commercial','late night','News & Documentaries'),
                (110,110,'Other','late night','News & Documentaries'),
                (110,110,'Other non-commercial','late night','News & Documentaries'),
                (117,117,'Terrestrial','late night','News & Documentaries'),
                (124,124,'Terrestrial non-commercial','late night','News & Documentaries'),
                (126,126,'Diginets','late night','Specialist'),						-- (127,127,'Diginets','late night','Specialist'),
                (126,126,'Diginets non-commercial','late night','Specialist'),		--	(127,127,'Diginets non-commercial','late night','Specialist'),
                (130,130,'Other','late night','Specialist'),
                (130,130,'Other non-commercial','late night','Specialist'),
                (132,132,'Terrestrial','late night','Specialist'),
                (132,132,'Terrestrial non-commercial','late night','Specialist'),
                (137,137,'Diginets','late night','Sports'),
                (137,137,'Diginets non-commercial','late night','Sports'),
                (144,144,'Other','late night','Sports'),
                (144,144,'Other non-commercial','late night','Sports'),
                (149,149,'Terrestrial','late night','Sports'),
                (149,149,'Terrestrial non-commercial','late night','Sports'),		-- 	(152,152,'Terrestrial non-commercial','late night','Sports'),
                (157,157,'Diginets','late night','Unknown'),
                (157,157,'Diginets non-commercial','late night','Unknown'),
                (157,157,'Other','late night','Unknown'),
                (157,157,'Other non-commercial','late night','Unknown'),
                (157,157,'Terrestrial','late night','Unknown'),
                (157,157,'Terrestrial non-commercial','late night','Unknown'),
                (0,0,'Diginets','late night','na'),
                (0,0,'Diginets non-commercial','late night','na'),
                (0,0,'Other','late night','na'),
                (0,0,'Other non-commercial','late night','na'),
                (0,0,'Terrestrial','late night','na'),
                (0,0,'Terrestrial non-commercial','late night','na'),
				-------------------------------------------------------------------------
                (4,4,'Diginets','night','Children'),
                (4,4,'Diginets non-commercial','night','Children'),
                (14,14,'Other','night','Children'),
                (20,20,'Other non-commercial','night','Children'),
                (21,21,'Terrestrial','night','Children'),
                (21,21,'Terrestrial non-commercial','night','Children'),
                (28,28,'Diginets','night','Entertainment'),
                (33,33,'Diginets non-commercial','night','Entertainment'),
                (40,40,'Other','night','Entertainment'),
                (40,40,'Other non-commercial','night','Entertainment'),
                (47,47,'Terrestrial','night','Entertainment'),
                (54,54,'Terrestrial non-commercial','night','Entertainment'),
                (60,60,'Diginets','night','Movies'),
                (60,60,'Diginets non-commercial','night','Movies'),
                (67,67,'Other','night','Movies'),
                (67,67,'Other non-commercial','night','Movies'),
                (60,60,'Terrestrial','night','Movies'),								--	(73,73,'Terrestrial','night','Movies'),
                (60,60,'Terrestrial non-commercial','night','Movies'),				--	(76,76,'Terrestrial non-commercial','night','Movies'),
                (80,80,'Diginets','night','Music & Radio'),							--	(82,82,'Diginets','night','Music & Radio'),	
                (80,80,'Diginets non-commercial','night','Music & Radio'),			--	(82,82,'Diginets non-commercial','night','Music & Radio'),
                (89,89,'Other','night','Music & Radio'),
                (89,89,'Other non-commercial','night','Music & Radio'),
                (90,90,'Terrestrial','night','Music & Radio'),
                (90,90,'Terrestrial non-commercial','night','Music & Radio'),
                (97,97,'Diginets','night','News & Documentaries'),					
                (97,97,'Diginets non-commercial','night','News & Documentaries'),	--	(104,104,'Diginets non-commercial','night','News & Documentaries'),
                (111,111,'Other','night','News & Documentaries'),
                (111,111,'Other non-commercial','night','News & Documentaries'),
                (118,118,'Terrestrial','night','News & Documentaries'),
                (125,125,'Terrestrial non-commercial','night','News & Documentaries'),
                (126,126,'Diginets','night','Specialist'),							--	(128,128,'Diginets','night','Specialist'),
                (126,126,'Diginets non-commercial','night','Specialist'),			--	(128,128,'Diginets non-commercial','night','Specialist'),
                (131,131,'Other','night','Specialist'),
                (131,131,'Other non-commercial','night','Specialist'),
                (132,132,'Terrestrial','night','Specialist'),						--	(133,133,'Terrestrial','night','Specialist'),
                (132,132,'Terrestrial non-commercial','night','Specialist'),		--	(133,133,'Terrestrial non-commercial','night','Specialist'),
                (137,137,'Diginets','night','Sports'),								--	(138,138,'Diginets','night','Sports'),
                (137,137,'Diginets non-commercial','night','Sports'),				--	(138,138,'Diginets non-commercial','night','Sports'),
                (145,145,'Other','night','Sports'),
                (145,145,'Other non-commercial','night','Sports'),
                (150,150,'Terrestrial','night','Sports'),
                (150,150,'Terrestrial non-commercial','night','Sports'),			--	(153,153,'Terrestrial non-commercial','night','Sports'),
                (157,157,'Diginets','night','Unknown'),
                (157,157,'Diginets non-commercial','night','Unknown'),
                (157,157,'Other','night','Unknown'),
                (157,157,'Other non-commercial','night','Unknown'),
                (157,157,'Terrestrial','night','Unknown'),
                (157,157,'Terrestrial non-commercial','night','Unknown'),
                (0,0,'Diginets','night','na'),
                (0,0,'Diginets non-commercial','night','na'),
                (0,0,'Other','night','na'),
                (0,0,'Other non-commercial','night','na'),
                (0,0,'Terrestrial','night','na'),
                (0,0,'Terrestrial non-commercial','night','na'),
                (0,0,'Diginets','na','Children'),
                (0,0,'Diginets non-commercial','na','Children'),
                (0,0,'Other','na','Children'),
                (0,0,'Other non-commercial','na','Children'),
                (0,0,'Terrestrial','na','Children'),
                (0,0,'Terrestrial non-commercial','na','Children'),
                (0,0,'Diginets','na','Entertainment'),
                (0,0,'Diginets non-commercial','na','Entertainment'),
                (0,0,'Other','na','Entertainment'),
                (0,0,'Other non-commercial','na','Entertainment'),
                (0,0,'Terrestrial','na','Entertainment'),
                (0,0,'Terrestrial non-commercial','na','Entertainment'),
                (0,0,'Diginets','na','Movies'),
                (0,0,'Diginets non-commercial','na','Movies'),
                (0,0,'Other','na','Movies'),
                (0,0,'Other non-commercial','na','Movies'),
                (0,0,'Terrestrial','na','Movies'),
                (0,0,'Terrestrial non-commercial','na','Movies'),
                (0,0,'Diginets','na','Music & Radio'),
                (0,0,'Diginets non-commercial','na','Music & Radio'),
                (0,0,'Other','na','Music & Radio'),
                (0,0,'Other non-commercial','na','Music & Radio'),
                (0,0,'Terrestrial','na','Music & Radio'),
                (0,0,'Terrestrial non-commercial','na','Music & Radio'),
                (0,0,'Diginets','na','News & Documentaries'),
                (0,0,'Diginets non-commercial','na','News & Documentaries'),
                (0,0,'Other','na','News & Documentaries'),
                (0,0,'Other non-commercial','na','News & Documentaries'),
                (0,0,'Terrestrial','na','News & Documentaries'),
                (0,0,'Terrestrial non-commercial','na','News & Documentaries'),
                (0,0,'Diginets','na','Specialist'),
                (0,0,'Diginets non-commercial','na','Specialist'),
                (0,0,'Other','na','Specialist'),
                (0,0,'Other non-commercial','na','Specialist'),
                (0,0,'Terrestrial','na','Specialist'),
                (0,0,'Terrestrial non-commercial','na','Specialist'),
                (0,0,'Diginets','na','Sports'),
                (0,0,'Diginets non-commercial','na','Sports'),
                (0,0,'Other','na','Sports'),
                (0,0,'Other non-commercial','na','Sports'),
                (0,0,'Terrestrial','na','Sports'),
                (0,0,'Terrestrial non-commercial','na','Sports'),
                (0,0,'Diginets','na','Unknown'),
                (0,0,'Diginets non-commercial','na','Unknown'),
                (0,0,'Other','na','Unknown'),
                (0,0,'Other non-commercial','na','Unknown'),
                (0,0,'Terrestrial','na','Unknown'),
                (0,0,'Terrestrial non-commercial','na','Unknown'),
                (0,0,'Diginets','na','na'),
                (0,0,'Diginets non-commercial','na','na'),
                (0,0,'Other','na','na'),
                (0,0,'Other non-commercial','na','na'),
                (0,0,'Terrestrial','na','na'),
                (0,0,'Terrestrial non-commercial','na','na')

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
      row_id                INT         IDENTITY
    , account_number        VARCHAR(20) NOT NULL
    , cb_key_household      BIGINT      NOT NULL
    , exp_cb_key_db_person  BIGINT      
    , cb_key_individual     BIGINT
    , cb_key_db_person      BIGINT
    , cb_address_line_1     VARCHAR (200)
    , HH_person_number      TINYINT
    , person_gender         CHAR (1)                
    , person_age            TINYINT
    , person_ageband        varchar(10)            
    , exp_person_head       TINYINT                
    , person_income         NUMERIC
    , person_head           char(1)     DEFAULT '0'   
    , household_size        TINYINT
    , demographic_ID        TINYINT
    , Updated_On            DATETIME    DEFAULT TIMESTAMP
    , Updated_By            VARCHAR(30) DEFAULT user_name())
        
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
    account_number          VARCHAR(20) NOT NULL
    , cb_key_household      BIGINT      NOT NULL
    , cb_address_line_1     VARCHAR (200)
    , HH_composition        TINYINT
    , Children_count        TINYINT     DEFAULT 0
    , non_matching_flag     BIT         DEFAULT 0
    , edited_add_flag       BIT         DEFAULT 0
    , Updated_On            DATETIME    DEFAULT TIMESTAMP
    , Updated_By            VARCHAR(30) DEFAULT user_name())
        
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
                  row_id                                INT identity
                , source                                VARCHAR (6)
        , the_day                               DATE 
        , service_key                   INT
        , person_ageband                INT
        , person_gender                 INT
        , session_daypart               INT
                , channel_name                  VARCHAR(200)
        , weighted_duration_mins DOUBLE
                , Updated_On            DATETIME    DEFAULT TIMESTAMP
                , Updated_By            VARCHAR(30) DEFAULT user_name()
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
                        pk_viewing_prog_instance_fact           bigint 
                        ,dk_event_start_datehour_dim            integer
                        ,dk_event_end_datehour_dim                      integer
                        ,dk_broadcast_start_Datehour_dim        integer
                        ,dk_instance_start_datehour_dim         integer
                        ,dk_viewing_event_dim                           integer
                        ,duration                                                       integer
                        ,genre_description                                      varchar(20)
                        ,service_key                                            integer
                        ,cb_key_household                                       bigint
                        ,event_start_date_time_utc                      timestamp
                        ,event_end_date_time_utc                        timestamp
                        ,account_number                                         varchar(20)
                        ,subscriber_id                                          integer
                        ,service_instance_id                            varchar(1)
                        ,programme_name                                         varchar(100)
                        ,capping_end_Date_time_utc                      timestamp
                        ,broadcast_start_date_time_utc          timestamp
                        ,broadcast_end_date_time_utc            timestamp
                        ,instance_start_date_time_utc           timestamp
                        ,instance_end_date_time_utc                     timestamp
                )

                commit
                
                create unique index key1 on v289_M06_dp_raw_data(pk_viewing_prog_instance_fact)
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
                        
                        account_number          varchar(20) not null
                        ,subscriber_id          decimal(10) not null
                        ,event_id                       bigint          default null
                        ,event_Start_utc        timestamp       not null
                        ,event_end_utc          timestamp       not null
                        ,chunk_start            timestamp       default null
                        ,chunk_end                      timestamp       default null
                        ,event_duration_seg     int                     not null
                        ,chunk_duration_seg int                 default null
                        ,programme_genre        varchar(20)     default null
                        ,session_daypart        varchar(11)     default null
                        ,hhsize                         tinyint         default 0
                        ,channel_pack           varchar(40) default null
                        ,segment_id                     int                     default null
                        ,Overlap_batch          int                     default null
                        ,session_size           tinyint         default 0
                        ,event_start_dim        int                     not null
                        ,event_end_dim          int                     not null
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
                        sequencer       integer         default autoincrement
                        ,task           varchar(50) not null
                        ,status         bit         default 0
                        ,exe_date       date            
                        ,audit_date     date        not null
                )
                commit

                insert  into v289_m01_t_process_manager(task,audit_date) Values('v289_m08_Experian_data_preparation',cast(now() as date))
                insert  into v289_m01_t_process_manager(task,audit_date) Values('v289_m04_barb_data_preparation',cast(now() as date))
                insert  into v289_m01_t_process_manager(task,audit_date) Values('v289_m05_barb_Matrices_generation',cast(now() as date))
                insert  into v289_m01_t_process_manager(task,audit_date) Values('v289_m06_DP_data_extraction',cast(now() as date))
                insert  into v289_m01_t_process_manager(task,audit_date) Values('v289_m07_dp_data_preparation',cast(now() as date))
                insert  into v289_m01_t_process_manager(task,audit_date) Values('v289_m09_Session_size_definition',cast(now() as date))
                insert  into v289_m01_t_process_manager(task,audit_date) Values('v289_M10_individuals_selection',cast(now() as date))
                insert  into v289_m01_t_process_manager(task,audit_date) Values('V289_M11_01_SC3_v1_1__do_weekly_segmentation',cast(now() as date))
                insert  into v289_m01_t_process_manager(task,audit_date) Values('V289_M11_02_SC3_v1_1__prepare_panel_members',cast(now() as date))
                insert  into v289_m01_t_process_manager(task,audit_date) Values('V289_M11_03_SC3I_v1_1__add_individual_data',cast(now() as date))
                insert  into v289_m01_t_process_manager(task,audit_date) Values('V289_M11_04_SC3I_v1_1__make_weights_BARB',cast(now() as date))
                insert  into v289_m01_t_process_manager(task,audit_date) Values('v289_m12_validation',cast(now() as date))
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
                                id                      int
                                ,scaling_variable        varchar(20)
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
                        scaling_segment_id      int
                        ,sky_base_universe      varchar(30)
                        ,isba_tv_region         varchar(30)
                        ,hhcomposition          varchar(30)
                        ,package                        varchar(30)
                        ,head_of_hhd            varchar(1)
                        ,gender                         varchar(1)
                        ,age_band                       varchar(10)
                        ,viewed_tv              char(1)
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
                        account_number                  varchar(20)
                        ,profiling_date                 date
                        ,HH_person_number               tinyint
                        ,population_scaling_segment_id  int
                        ,vespa_scaling_segment_id       int
                        ,expected_boxes                 int
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
                        account_number          varchar(20)
                        ,HH_person_number       tinyint
                        ,scaling_segment_id     int
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
                        scaling_segment_id      INT             primary key
                        ,sky_base_universe      VARCHAR(50)
                        ,sky_base_accounts      DOUBLE          not null
                        ,vespa_panel            DOUBLE          default 0
                        ,category_weight        DOUBLE
                        ,sum_of_weights         DOUBLE
                        ,segment_weight         DOUBLE
                        ,indices_actual         DOUBLE
                        ,indices_weighted       DOUBLE
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
                         sky_base_universe      VARCHAR(50)
                        ,profile                VARCHAR(50)
                        ,value                  VARCHAR(70)
                        ,sky_base_accounts      DOUBLE
                        ,vespa_panel            DOUBLE
                        ,category_weight        DOUBLE
                        ,sum_of_weights         DOUBLE
                        ,convergence_flag       TINYINT     DEFAULT 1
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
                         scaling_date           date
                        ,sky_base_universe      VARCHAR(50)
                        ,profile                VARCHAR(50)
                        ,value                  VARCHAR(70)
                        ,sky_base_accounts      DOUBLE
                        ,vespa_panel            DOUBLE
                        ,category_weight        DOUBLE
                        ,sum_of_weights         DOUBLE
                        ,convergence            TINYINT
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
                        scaling_date           DATE
                        ,iterations            int
                        ,convergence           tinyint
                        ,max_weight            float
                        ,av_weight             float
                        ,sum_of_weights        float
                        ,sky_base              bigint
                        ,vespa_panel           bigint
                        ,non_scalable_accounts bigint
                        ,sum_of_convergence    float
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
                          scaling_date           DATE
                         ,scaling_segment_id     int
                         ,difference             float
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
                        ,vespa_accounts             bigint          default 0       -- Vespa panel accounts in this segment reporting back for this day
                        ,sky_base_accounts          bigint          not null        -- Sky base accounts for this day by segment
                        ,weighting                  double          default null    -- The weight for an account in this segment
                        ,sum_of_weights             double          default null    -- The total weight for all accounts in this segment
                        ,indices_actual             double
                        ,indices_weighted           double
                        ,convergence                tinyint
                        ,primary key (scaling_day, scaling_segment_ID)
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
                        ageband                 varchar(10)
                        ,gender                 char(1)
                        ,viewed_tv              char(1)
                        ,head_of_hhd            char(1)
                        ,barb_weight    double
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
                        ,vespa_accounts             bigint          default 0       -- Vespa panel accounts in this segment reporting back for this day
                        ,sky_base_accounts          bigint          not null        -- Sky base accounts for this day by segment
                        ,weighting                  double          default null    -- The weight for an account in this segment
                        ,sum_of_weights             double          default null    -- The total weight for all accounts in this segment
                        ,indices_actual             double
                        ,indices_weighted           double
                        ,convergence                tinyint
                        ,primary key (scaling_day, scaling_segment_ID)
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
                        ,population_scaling_segment_id  bigint
                        ,vespa_scaling_segment_id       bigint
                        ,expected_boxes                 tinyint                     -- number of boxes in household; need to check they're all reporting
                        ,primary key (account_number, profiling_date)
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
                         account_number                     VARCHAR(20)     primary key
                        ,cb_key_household                   BIGINT          not null            -- Needed for VIQ interim solution
                        ,cb_key_individual                  BIGINT          not null            -- For ConsumerView linkage
                        ,consumerview_cb_row_id             BIGINT                              -- Not really needed for consumerview linkage, but whatever
                        ,universe                           VARCHAR(30)                         -- Single or multiple box household. Look at trying to make obsolete
                        ,sky_base_universe                  VARCHAR(30)                         -- Not adsmartable, Adsmartable with consent, Adsmartable but no consent household
                        ,vespa_universe                     VARCHAR(30)                         -- Universe used for Vespa
                        ,weighting_universe                 VARCHAR(30)                         -- Universe used for weighting purposes
                        ,isba_tv_region                     VARCHAR(20)                         -- Scaling variable 1 : Region
                        ,hhcomposition                      VARCHAR(2)      DEFAULT 'U'         -- Scaling variable 2: Household composition, originally from Experian Consumerview, collated later
                        ,tenure                             VARCHAR(15)     DEFAULT 'E) Unknown'-- Scaling variable 3: Tenure 'Unknown' removed from vespa panel
                        ,num_mix                            INT
                        ,mix_pack                           VARCHAR(20)
                        ,package                            VARCHAR(20)                         -- Scaling variable 4: Package
                        ,boxtype                            VARCHAR(35)                         -- Old scaling variable 5: Look at ways to make obsolete.
                        ,no_of_stbs                         VARCHAR(15)                         -- Scaling variable 5: No of set top boxes
                        ,hd_subscription                    VARCHAR(5)                          -- Scaling variable 6: HD subscription
                        ,pvr                                VARCHAR(5)                          -- Scaling variable 6: Is the box pvr capable?
                        ,population_scaling_segment_id      INT             DEFAULT NULL        -- segment scaling id for identifying segments in population
                        ,vespa_scaling_segment_id       INT             DEFAULT NULL        -- segment scaling id for identifying segments used in rim weighting
                        ,mr_boxes                           INT
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
                        ,sky_base_universe      VARCHAR(50)
                        ,sky_base_accounts      DOUBLE          not null
                        ,vespa_panel            DOUBLE          default 0
                        ,category_weight        DOUBLE
                        ,sum_of_weights         DOUBLE
                        ,segment_weight         DOUBLE
                        ,indices_actual         DOUBLE
                        ,indices_weighted       DOUBLE
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
                         sky_base_universe      VARCHAR(50)
                        ,profile                VARCHAR(50)
                        ,value                  VARCHAR(70)
                        ,sky_base_accounts      DOUBLE
                        ,vespa_panel            DOUBLE
                        ,category_weight        DOUBLE
                        ,sum_of_weights         DOUBLE
                        ,convergence_flag       TINYINT     DEFAULT 1
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
                         scaling_date           date
                        ,sky_base_universe      VARCHAR(50)
                        ,profile                VARCHAR(50)
                        ,value                  VARCHAR(70)
                        ,sky_base_accounts      DOUBLE
                        ,vespa_panel            DOUBLE
                        ,category_weight        DOUBLE
                        ,sum_of_weights         DOUBLE
                        ,convergence            TINYINT
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
                         scaling_date           DATE
                         ,iterations            int
                         ,convergence           tinyint
                         ,max_weight            float
                         ,av_weight             float
                         ,sum_of_weights        float
                         ,sky_base              bigint
                         ,vespa_panel           bigint
                         ,non_scalable_accounts bigint
                         ,sum_of_convergence    float
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
                          scaling_date           DATE
                         ,scaling_segment_id     int
                         ,difference             float
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
        
        
        
        
        
        
        MESSAGE cast(now() as timestamp)||' | Begining M00.1 - Initialising Tables DONE' TO CLIENT      
        
COMMIT                  
-----------------------------
-- M00.2 - Initialising Views
-----------------------------

        MESSAGE cast(now() as timestamp)||' | Begining M00.2 - Initialising Views' TO CLIENT
	       
-- barb_weights

        MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table barb_weights' TO CLIENT
        
		if object_id('barb_weights') is not null
			drop table barb_weights
		
		select  distinct *
		into	barb_weights
		from    BARB_Panel_Member_Responses_Weights_and_Viewing_Categories 
		where   file_creation_date = (select max(file_Creation_date) from BARB_Panel_Member_Responses_Weights_and_Viewing_Categories)
		and     reporting_panel_code = 50
		and		date_of_activity_db1 = cast(replace(cast(@processing_date as varchar(10)),'-','') as integer)
		
		commit
		grant select on barb_weights to vespa_group_low_security
		commit
		
		MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating Table barb_weights DONE' TO CLIENT

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

