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
**Project Name:                         Skyview H2I
**Analysts:                             Angel Donnarumma    (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jason Thompson      (Jason.Thompson@skyiq.co.uk)
                                        ,Hoi Yu Tang        (HoiYu.Tang@skyiq.co.uk)
                                        ,Jose Pitteloud     (jose.pitteloud@skyiq.co.uk)
**Stakeholder:                          SkyIQ
                                        ,Jose Loureda       (Jose.Loureda@skyiq.co.uk)
**Due Date:                             11/07/2014
**Project Code (Insight Collation):     v289
**Sharepoint Folder:    

    http://sp-department.bskyb.com/sites/SIGEvolved/Shared%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FSIGEvolved%2FShared%20Documents%2F01%20Analysis%20Requests%2FV289%20-%20Skyview%20Futures%2F01%20Plans%20Briefs%20and%20Project%20Admin
                                                                        
**Business Brief:

    This module is in charge create the working views that are used across the rest of the process. It will create a full view from a available table to the user. 

**Module:
    000     -   Prevalidaton module 
    
    ----- EXECUTE v289_m000_Prevalidation  '2013-09-18'
--------------------------------------------------------------------------------------------------------------
*/

-----------------------------------
-- M01.0 - Initialising Environment
-----------------------------------

CREATE OR REPLACE procedure v289_m000_Prevalidation
    @viewing_date DATE
AS BEGIN 
    DECLARE @table_name VARCHAR(200)
    DECLARE @schema_    VARCHAR(200)
    DECLARE @sql_       VARCHAR(2000)
    DECLARE @sql_1      VARCHAR(2000)
    DECLARE @option_    TINYINT 
    DECLARE @cont       TINYINT 
    DECLARE @max_it         TINYINT 
    DECLARE @exe_status     INT
    DECLARE @view_name      VARCHAR(200)

    /*      ********** Table with tables to be checked creation     */
    IF object_id('V289_Tables_check') IS NULL 
    BEGIN
        CREATE TABLE V289_Tables_check
        ( row_id        INT IDENTITY
        , table_name    VARCHAR(200)
        , view_name     VARCHAR(200)
        , processed     BIT DEFAULT 0)
    END 

    COMMIT

    TRUNCATE TABLE V289_Tables_check
            MESSAGE '1.- Initialization Done' TO CLIENT 
    -------------------------------------------------------
    SET @table_name = 'vespa_dp_prog_viewed_'||DATEFORMAT(@viewing_date, 'YYYYMM')
    INSERT INTO V289_Tables_check (table_name, view_name) ---- INSERTING Viewing Table
     VALUES
     ('BARB_Channel_Map','BARB_Channel_Map_V')
    ,('CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES','CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_V')
    ,('cust_entitlement_lookup','cust_entitlement_lookup_V')
    ,('CUST_SET_TOP_BOX','CUST_SET_TOP_BOX_V')
    ,('cust_single_account_view','cust_single_account_view_V')
    ,('cust_subs_hist','cust_subs_hist_V')
    ,('experian_consumerview','experian_consumerview_V')
    ,('PI_BARB_IMPORT','PI_BARB_IMPORT_V')
    ,('PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD','PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD_V')
    ,('SC3I_Variables_lookup_v1_1','SC3I_Variables_lookup_v1_1_V')
    ,('VESPA_PROGRAMME_SCHEDULE','VESPA_PROGRAMME_SCHEDULE_V')
    
    INSERT INTO V289_Tables_check (table_name, view_name) ---- INSERTING Viewing Table
    SELECT @table_name, 'V289_viewing_data_view'
    
    COMMIT
            MESSAGE '2.- Tables to check Inserted' TO CLIENT 
    --------------------------------------------------------
    WHILE EXISTS  (SELECT top 1 table_name FROM V289_Tables_check WHERE processed = 0)
    BEGIN 
        SET @table_name = (SELECT top 1 REPLACE (TRIM(table_name),' ' ,'' ) FROM V289_Tables_check WHERE processed = 0) 
        SET @view_name  = (SELECT top 1 REPLACE (TRIM(view_name),' ' ,'' ) FROM V289_Tables_check WHERE table_name = @table_name)   
        SET @schema_    = ' '
        SET @exe_status = -1
        
        SET @sql_  = 'EXECUTE @exe_status = SELECT top 1 * INTO #t1 FROM '||@schema_||@table_name
        --MESSAGE 'EXECUTING:           '||@sql_  TO CLIENT 
            
        EXECUTE (@sql_ )

    --  MESSAGE 'Result: '|| @exe_status   TO CLIENT 
        IF @exe_status <> 0 
        BEGIN                           ----------------------------------  Prefix needed section START
    --      MESSAGE '2.1 .- Prefix Needed '  TO CLIENT 
                        
            SELECT
                row_number()  OVER (ORDER BY user_name,table_type) row_id
                , TRIM(user_name)   AS user_name_
                , TRIM(table_name)  AS table_name_
                , TRIM(table_type)  AS table_type_
                , CAST (0  AS BIT)  Checked
                , CAST (0  AS BIT)  Working
                , CAST (0  AS TINYINT)  Selected
            INTO #tables
            FROM systable
            INNER JOIN sysuser ON systable.creator = sysuser.user_id
            WHERE UPPER (table_name) LIKE UPPER (@table_name)
            AND UPPER(user_name) NOT LIKE '%REPROCESS%'
            ORDER BY user_name, table_type DESC
            
        --  MESSAGE '2.2 .- #Tables created: Rows:'||@@rowcount  TO CLIENT 
            


            SET @max_it = (SELECT MAX(row_id) FROM #tables)
            SET @cont = 1 
            
        --  MESSAGE '2.3 .-  VALIDATION Section STARTED'  TO CLIENT 
            
            WHILE EXISTS (SELECT top 1 row_id FROM #tables WHERE Checked =0) 
            BEGIN                                       ------------------- VALIDATION Section START                                                                        
                
                IF @cont > @max_it break
                
				SET @schema_ 		= (SELECT top 1  REPLACE (TRIM(user_name_),' ' ,'' )		FROM #tables WHERE row_id = @cont)
                SET @table_name     = (SELECT top 1  REPLACE (TRIM(table_name_),' ' ,'' )   FROM #tables WHERE row_id = @cont)
                
                SET @exe_status = -1
                SET @sql_ = 'SELECT top 1 * INTO #t1 FROM '||@schema_||'.'||@table_name
                
                SET @sql_1  = 'EXECUTE @exe_status = v289_validation_table @sql_'
                
            --  MESSAGE '3.- EXECUTING:         '||@sql_  TO CLIENT 
                EXECUTE (@sql_1)
                
                if @exe_status <> 0
                    SET @cont = @cont
                
                IF OBJECT_ID ('#t1') IS NOT NULL DROP TABLE #t1
                
                UPDATE #tables
                SET   Checked = 1
                    , Working = CASE WHEN @exe_status = 0 THEN 1 ELSE 0  END 
                WHERE row_id = @cont
                        
                SET @cont = @cont +1 

            END                                         ------------------- VALIDATION Section END
        --  MESSAGE '2.3 .-  VALIDATION Section ENDED'  TO CLIENT 
            

                     
                     ----- In case no table is available SECTION START
            IF (SELECT SUM(Working) FROM #tables) = 0   BEGIN 
                    MESSAGE '4.- ERROR - No table Available:    '||@table_name TO CLIENT 
                    goto anytable
                END 

                ------ In case no table is available SECTION END 
            ELSE 
        --      MESSAGE '2.4 .- No available validation ENDED'  TO CLIENT 
            
        --  MESSAGE '2.5 .- Prioritation Started '  TO CLIENT 
            UPDATE  #tables                                 ------ Establishing schema priority selection
            SET Selected = CASE WHEN UPPER(user_name_) = 'SK_PROD' THEN 1
                                WHEN UPPER(user_name_) = 'VESPA_ANALYSTS' THEN 2
                                WHEN UPPER(user_name_) = 'VESPA_SHARED' THEN 3
                                WHEN UPPER(user_name_) = 'SK_PROD_VESPA_RESTRICTED' THEN 4
                                WHEN UPPER(user_name_) LIKE 'SK_PROD_%' THEN 4
                                WHEN UPPER(user_name_) = user_name() THEN 5
                                WHEN UPPER(user_name_) IN ('THOMPSONJA', 'ANGELD', 'PITTELOUDJ', 'TANGHOI', 'SPENCERC2') THEN 6
                                ELSE 10
                                END 
            WHERE Working = 1                   ------ Only working tables
            
            --SELECT * INTO tablee FROM #tables 
            
        --  MESSAGE '2.5 .- Prioritation ENDED  '  TO CLIENT 
            COMMIT 
            
            SET @schema_    = (SELECT top 1 user_name_  FROM #tables WHERE Working = 1 ORDER BY Selected ASC)       ------Defining the chosen one
            
            DROP TABLE #tables
        END                             ----------------------------------- Prefix needed section END 
        
        
        SET @sql_ = 'IF object_id('||''''||@view_name||''''||') is not null DROP VIEW '||@view_name||';'
        SET @sql_ = @sql_||' CREATE VIEW '||@view_name||' AS SELECT * FROM '||@schema_||'.'||@table_name||';commit'
            
        --  MESSAGE '2.5 .- EXECUTING:      '||@sql_  TO CLIENT 
            
        SET @exe_status = -1 
        EXECUTE @exe_status = v289_validation_table @sql_
        COMMIT 
        
        IF @exe_status = 0 
            MESSAGE '5.- Table Processed: '||@schema_||@table_name TO CLIENT 
        ELSE 
            MESSAGE '6.- ERROR Table creation failed CODE: '||@exe_status||'   '||@schema_||'.'||@table_name TO CLIENT
            
        anytable:                           -------------       Label in case no table is found
        
        UPDATE V289_Tables_check
        SET processed =1 
        WHERE view_name = @view_name
        
        
        
    END    
        


        --------------------------------------------------
        -- Pre-requisites for E2E in non-prod environments
        --------------------------------------------------
        IF (SELECT USER) <> 'angeld'
            BEGIN
        
                -- skybarb views
                CREATE OR REPLACE VIEW skybarb_fullview
                AS
                SELECT * FROM angeld.skybarb_fullview
                

                CREATE OR REPLACE VIEW skybarb
                AS
                SELECT * FROM angeld.skybarb
                

                
                -- Cope entire BARB_Panel_Member_Responses_Weights_and_Viewing_Categories table from Angel's schema
                IF OBJECT_ID('BARB_Panel_Member_Responses_Weights_and_Viewing_Categories') IS NOT NULL
                    DROP TABLE BARB_Panel_Member_Responses_Weights_and_Viewing_Categories
                
                SELECT  * 
                INTO    BARB_Panel_Member_Responses_Weights_and_Viewing_Categories 
                FROM    angeld.BARB_Panel_Member_Responses_Weights_and_Viewing_Categories
                COMMIT


                
                -- Cope entire PI_BARB_import table from Angel's schema
                IF OBJECT_ID('PI_BARB_import') IS NOT NULL
                    DROP TABLE PI_BARB_import
                    
                SELECT  *
                INTO    PI_BARB_import
                FROM    angeld.PI_BARB_import
                COMMIT


            END
        COMMIT

                
        
        
     

    COMMIT
END; --- END of the procedure 
COMMIT;
GRANT EXECUTE ON v289_m000_Prevalidation TO vespa_group_low_security;
commit;


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
                (68,68,'Terrestrial','breakfast','Movies'),
                (68,68,'Terrestrial non-commercial','breakfast','Movies'),
                (77,77,'Diginets','breakfast','Music & Radio'),
                (77,77,'Diginets non-commercial','breakfast','Music & Radio'),
                (83,83,'Other','breakfast','Music & Radio'),
                (83,83,'Other non-commercial','breakfast','Music & Radio'),
                (90,90,'Terrestrial','breakfast','Music & Radio'),
                (90,90,'Terrestrial non-commercial','breakfast','Music & Radio'),
                (91,91,'Diginets','breakfast','News & Documentaries'),
                (98,98,'Diginets non-commercial','breakfast','News & Documentaries'),
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
                (2,2,'Diginets','morning','Children'),
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
                (68,68,'Terrestrial','morning','Movies'),
                (68,68,'Terrestrial non-commercial','morning','Movies'),
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
                (127,127,'Diginets','morning','Specialist'),
                (127,127,'Diginets non-commercial','morning','Specialist'),
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
                (2,2,'Diginets','lunch','Children'),
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
                (127,127,'Diginets','lunch','Specialist'),
                (127,127,'Diginets non-commercial','lunch','Specialist'),
                (130,130,'Other','lunch','Specialist'),
                (130,130,'Other non-commercial','lunch','Specialist'),
                (132,132,'Terrestrial','lunch','Specialist'),
                (132,132,'Terrestrial non-commercial','lunch','Specialist'),
                (134,134,'Diginets','lunch','Sports'),
                (134,134,'Diginets non-commercial','lunch','Sports'),
                (141,141,'Other','lunch','Sports'),
                (141,141,'Other non-commercial','lunch','Sports'),
                (147,147,'Terrestrial','lunch','Sports'),
                (147,147,'Terrestrial non-commercial','lunch','Sports'),
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
                (79,79,'Diginets','early prime','Music & Radio'),
                (79,79,'Diginets non-commercial','early prime','Music & Radio'),
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
                (127,127,'Diginets','early prime','Specialist'),
                (127,127,'Diginets non-commercial','early prime','Specialist'),
                (130,130,'Other','early prime','Specialist'),
                (130,130,'Other non-commercial','early prime','Specialist'),
                (132,132,'Terrestrial','early prime','Specialist'),
                (132,132,'Terrestrial non-commercial','early prime','Specialist'),
                (135,135,'Diginets','early prime','Sports'),
                (135,135,'Diginets non-commercial','early prime','Sports'),
                (142,142,'Other','early prime','Sports'),
                (142,142,'Other non-commercial','early prime','Sports'),
                (147,147,'Terrestrial','early prime','Sports'),
                (147,147,'Terrestrial non-commercial','early prime','Sports'),
                (155,155,'Diginets','early prime','Unknown'),
                (155,155,'Diginets non-commercial','early prime','Unknown'),
                (155,155,'Other','early prime','Unknown'),
                (155,155,'Other non-commercial','early prime','Unknown'),
                (155,155,'Terrestrial','early prime','Unknown'),
                (155,155,'Terrestrial non-commercial','early prime','Unknown'),
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
                (74,74,'Terrestrial non-commercial','prime','Movies'),
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
                (127,127,'Diginets','prime','Specialist'),
                (127,127,'Diginets non-commercial','prime','Specialist'),
                (130,130,'Other','prime','Specialist'),
                (130,130,'Other non-commercial','prime','Specialist'),
                (132,132,'Terrestrial','prime','Specialist'),
                (132,132,'Terrestrial non-commercial','prime','Specialist'),
                (136,136,'Diginets','prime','Sports'),
                (136,136,'Diginets non-commercial','prime','Sports'),
                (143,143,'Other','prime','Sports'),
                (143,143,'Other non-commercial','prime','Sports'),
                (148,148,'Terrestrial','prime','Sports'),
                (151,151,'Terrestrial non-commercial','prime','Sports'),
                (156,156,'Diginets','prime','Unknown'),
                (156,156,'Diginets non-commercial','prime','Unknown'),
                (156,156,'Other','prime','Unknown'),
                (156,156,'Other non-commercial','prime','Unknown'),
                (156,156,'Terrestrial','prime','Unknown'),
                (156,156,'Terrestrial non-commercial','prime','Unknown'),
                (0,0,'Diginets','prime','na'),
                (0,0,'Diginets non-commercial','prime','na'),
                (0,0,'Other','prime','na'),
                (0,0,'Other non-commercial','prime','na'),
                (0,0,'Terrestrial','prime','na'),
                (0,0,'Terrestrial non-commercial','prime','na'),
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
                (81,81,'Diginets','late night','Music & Radio'),
                (81,81,'Diginets non-commercial','late night','Music & Radio'),
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
                (127,127,'Diginets','late night','Specialist'),
                (127,127,'Diginets non-commercial','late night','Specialist'),
                (130,130,'Other','late night','Specialist'),
                (130,130,'Other non-commercial','late night','Specialist'),
                (132,132,'Terrestrial','late night','Specialist'),
                (132,132,'Terrestrial non-commercial','late night','Specialist'),
                (137,137,'Diginets','late night','Sports'),
                (137,137,'Diginets non-commercial','late night','Sports'),
                (144,144,'Other','late night','Sports'),
                (144,144,'Other non-commercial','late night','Sports'),
                (149,149,'Terrestrial','late night','Sports'),
                (152,152,'Terrestrial non-commercial','late night','Sports'),
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
                (73,73,'Terrestrial','night','Movies'),
                (76,76,'Terrestrial non-commercial','night','Movies'),
                (82,82,'Diginets','night','Music & Radio'),
                (82,82,'Diginets non-commercial','night','Music & Radio'),
                (89,89,'Other','night','Music & Radio'),
                (89,89,'Other non-commercial','night','Music & Radio'),
                (90,90,'Terrestrial','night','Music & Radio'),
                (90,90,'Terrestrial non-commercial','night','Music & Radio'),
                (97,97,'Diginets','night','News & Documentaries'),
                (104,104,'Diginets non-commercial','night','News & Documentaries'),
                (111,111,'Other','night','News & Documentaries'),
                (111,111,'Other non-commercial','night','News & Documentaries'),
                (118,118,'Terrestrial','night','News & Documentaries'),
                (125,125,'Terrestrial non-commercial','night','News & Documentaries'),
                (128,128,'Diginets','night','Specialist'),
                (128,128,'Diginets non-commercial','night','Specialist'),
                (131,131,'Other','night','Specialist'),
                (131,131,'Other non-commercial','night','Specialist'),
                (133,133,'Terrestrial','night','Specialist'),
                (133,133,'Terrestrial non-commercial','night','Specialist'),
                (138,138,'Diginets','night','Sports'),
                (138,138,'Diginets non-commercial','night','Sports'),
                (145,145,'Other','night','Sports'),
                (145,145,'Other non-commercial','night','Sports'),
                (150,150,'Terrestrial','night','Sports'),
                (153,153,'Terrestrial non-commercial','night','Sports'),
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
        
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating View v289_genderage_lookup' TO CLIENT

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
                
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating View v289_genderage_lookup DONE' TO CLIENT
                
        end

        MESSAGE cast(now() as timestamp)||' | Begining M00.1 - Initialising Views DONE' TO CLIENT
        

-- v289_M06_dp_raw_data

        if object_id('v289_M06_dp_raw_data') is null
        begin
                
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table v289_M06_dp_raw_data' TO CLIENT
                
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

                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table v289_M06_dp_raw_data DONE' TO CLIENT
                
        end

        
        
-- V289_M07_dp_data

        if object_id('V289_M07_dp_data') is null
        begin
        
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table V289_M07_dp_data' TO CLIENT
                
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
                
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table V289_M07_dp_data DONE' TO CLIENT
                
        end
        
-- v289_m01_t_process_manager

        if object_id('v289_m01_t_process_manager') is null
        begin
        
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table v289_m01_t_process_manager' TO CLIENT
                
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
                
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table v289_m01_t_process_manager DONE' TO CLIENT
                
        end
        
        
-- SC3I_Variables_lookup_v1_1

        if object_id('SC3I_Variables_lookup_v1_1') is null
        begin
        
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3I_Variables_lookup_v1_1' TO CLIENT
                
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
                
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3I_Variables_lookup_v1_1 DONE' TO CLIENT
                
        end             
        
        
-- SC3I_Segments_lookup_v1_1

        if object_id('SC3I_Segments_lookup_v1_1') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3I_Segments_lookup_v1_1' TO CLIENT

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
                
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3I_Segments_lookup_v1_1 DONE' TO CLIENT
                
        end     
        
        
-- SC3I_Sky_base_segment_snapshots

        if object_id('SC3I_Sky_base_segment_snapshots') is null
        begin
        
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3I_Sky_base_segment_snapshots' TO CLIENT
                
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
                
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3I_Sky_base_segment_snapshots DONE' TO CLIENT
                
        end     
        
        
-- SC3I_Todays_panel_members

        if object_id('SC3I_Todays_panel_members') is null
        begin
        
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3I_Todays_panel_members' TO CLIENT
                
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
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3I_Todays_panel_members DONE' TO CLIENT
                
        end     
        
        
-- SC3I_weighting_working_table

        if object_id('SC3I_weighting_working_table') is null
        begin
        
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3I_weighting_working_table' TO CLIENT
                
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
                
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3I_weighting_working_table DONE' TO CLIENT
                
        end     
        
        
-- SC3I_category_working_table

        if object_id('SC3I_category_working_table') is null
        begin
        
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3I_category_working_table' TO CLIENT
                
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
                
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3I_category_working_table DONE' TO CLIENT
                
        end     
        
        
-- SC3I_category_subtotals

        if object_id('SC3I_category_subtotals') is null
        begin
        
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3I_category_subtotals' TO CLIENT
                
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
                
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3I_category_subtotals DONE' TO CLIENT
                
        end     
        
        
-- SC3I_metrics

        if object_id('SC3I_metrics') is null
        begin
        
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3I_metrics' TO CLIENT
                
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
                
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3I_metrics DONE' TO CLIENT
                
        end     
        
        
-- SC3I_non_convergences

        if object_id('SC3I_non_convergences') is null
        begin
        
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3I_non_convergences' TO CLIENT
                
                CREATE TABLE SC3I_non_convergences (
                          scaling_date           DATE
                         ,scaling_segment_id     int
                         ,difference             float
                )
                commit
                
                create index indx_date on SC3I_non_convergences(scaling_date)
                commit
                
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3I_non_convergences DONE' TO CLIENT
                
        end     
        

-- SC3I_Weightings

        if object_id('SC3I_Weightings') is null
        begin
        
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3I_Weightings' TO CLIENT
                
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
                
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3I_Weightings DONE' TO CLIENT
                
        end
        
        
-- SC3I_Intervals

        if object_id('SC3I_Intervals') is null
        begin
        
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3I_Intervals' TO CLIENT
                
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
                
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3I_Intervals DONE' TO CLIENT
                
        end     
        
        
-- V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING

        if object_id('V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING') is null
        begin
        
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING' TO CLIENT
                
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
                
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING DONE' TO CLIENT
                
        end     
        
        
-- V289_M11_04_Barb_weighted_population

        if object_id('V289_M11_04_Barb_weighted_population') is null
        begin
        
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table V289_M11_04_Barb_weighted_population' TO CLIENT
                
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
                
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table V289_M11_04_Barb_weighted_population DONE' TO CLIENT
                
        end     
        
        
-- SC3_Weightings

        if object_id('SC3_Weightings') is null
        begin
        
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3_Weightings' TO CLIENT
                
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
                
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3_Weightings DONE' TO CLIENT
                
        end     
        
        
-- SC3_Intervals

        if object_id('SC3_Intervals') is null
        begin
        
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3_Intervals' TO CLIENT
                
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
                
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3_Intervals DONE' TO CLIENT
                
        end     
        
        
-- VESPA_HOUSEHOLD_WEIGHTING

        if object_id('VESPA_HOUSEHOLD_WEIGHTING') is null
        begin
        
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table VESPA_HOUSEHOLD_WEIGHTING' TO CLIENT
                
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
                
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table VESPA_HOUSEHOLD_WEIGHTING DONE' TO CLIENT
                
        end     
        
        
-- SC3_Sky_base_segment_snapshots

        if object_id('SC3_Sky_base_segment_snapshots') is null
        begin
        
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3_Sky_base_segment_snapshots' TO CLIENT
                
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
                
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3_Sky_base_segment_snapshots DONE' TO CLIENT
                
        end     
        
        
-- SC3_Todays_panel_members

        if object_id('SC3_Todays_panel_members') is null
        begin
        
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3_Todays_panel_members' TO CLIENT
                
                create table SC3_Todays_panel_members (
                        account_number              varchar(20)     not null primary key
                        ,scaling_segment_id         bigint          not null
                )
                commit
                
                grant select on SC3_Todays_panel_members to vespa_group_low_security
                commit
                
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3_Todays_panel_members DONE' TO CLIENT
                
        end     
        
        
-- SC3_Todays_segment_weights

        if object_id('SC3_Todays_segment_weights') is null
        begin
        
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3_Todays_segment_weights' TO CLIENT
                
                create table SC3_Todays_segment_weights (
                        scaling_segment_id          bigint          not null primary key
                        ,scaling_weighting          float           not null
                )
                commit
                
                grant select on SC3_Todays_segment_weights to vespa_group_low_security
                commit
                
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3_Todays_segment_weights DONE' TO CLIENT
                
        end     
        
        
-- SC3_scaling_weekly_sample

        if object_id('SC3_scaling_weekly_sample') is null
        begin
        
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3_scaling_weekly_sample' TO CLIENT
                
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
                
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3_scaling_weekly_sample DONE' TO CLIENT
                
        end     
        
        
-- SC3_weighting_working_table

        if object_id('SC3_weighting_working_table') is null
        begin
        
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3_weighting_working_table' TO CLIENT
                
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
                
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3_weighting_working_table DONE' TO CLIENT
                
        end     
        

-- SC3_category_working_table

        if object_id('SC3_category_working_table') is null
        begin
        
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3_category_working_table' TO CLIENT
                
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
                
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3_category_working_table DONE' TO CLIENT
                
        end     
        
        
-- SC3_category_subtotals

        if object_id('SC3_category_subtotals') is null
        begin
        
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3_category_subtotals' TO CLIENT
                
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
                
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3_category_subtotals DONE' TO CLIENT
                
        end     
        
        
-- SC3_metrics

        if object_id('SC3_metrics') is null
        begin
        
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3_metrics' TO CLIENT
                
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
                
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3_metrics DONE' TO CLIENT
                
        end     
        
        
-- SC3_non_convergences

        if object_id('SC3_non_convergences') is null
        begin
        
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3_non_convergences' TO CLIENT
                
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
                
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table SC3_non_convergences DONE' TO CLIENT
                
        end     
        
        
-- BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories

        if object_id('BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories') is null
        begin
        
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories' TO CLIENT
                
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
                
                MESSAGE cast(now() as timestamp)||' | @ M00.1: Creating Table BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories DONE' TO CLIENT
                
        end
        
        
        
        
        
        
        MESSAGE cast(now() as timestamp)||' | Begining M00.1 - Initialising Tables DONE' TO CLIENT      
        
COMMIT                  
-----------------------------
-- M00.2 - Initialising Views
-----------------------------
		
        MESSAGE cast(now() as timestamp)||' | Begining M00.2 - Initialising Views' TO CLIENT	
        
		-- Local variables for when needed...
		declare @sql_	varchar(5000)
		
-- barb_weights
		
		/*
			now forcing the drop of this view to make sure we get the weights for the
			day we are processing... should be in line with Barb feed on One-View
		*/
        if object_id('barb_weights') is not null
			drop view barb_weights
			
		commit
        
		MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating View barb_weights' TO CLIENT
		
		set @sql_ = 'create  view barb_weights as '||
					'select  household_number '||
							',person_number '||
							',processing_weight/10 as processing_weight '||
					'from    sk_prod_confidential_customer.BARB_PANEL_MEM_RESP_WGHT '||
					'where   date_of_activity ='''|| @processing_date||''' '||
					'and     reporting_panel_code = 50'
					
		execute (@sql_)
		
		commit
		grant select on barb_weights to vespa_group_low_security
		commit
		
		MESSAGE cast(now() as timestamp)||' | @ M00.2: Creating View barb_weights DONE' TO CLIENT
           

-- barb_rawview

/*
	AD - 10-12-2014:
	To be replaced later on for below logic:
	
	select  household_number
			,person_1_viewing
			,person_2_viewing
			,person_3_viewing
			,person_4_viewing
			,person_5_viewing
			,person_6_viewing
			,person_7_viewing
			,person_8_viewing
			,person_9_viewing
			,person_10_viewing
			,person_11_viewing
			,person_12_viewing
			,person_13_viewing
			,person_14_viewing
			,person_15_viewing
			,person_16_viewing
			--,start_time_of_session
			--,end_time_of_session
			,session_duration   --,duration_of_the_session
			,db1_station_code
	From    BARB_PVF_VWREC_PANEL_MEM
	
	NOTE: So far seems we would like to derive the start time and end time of the session since the table does not have this info 
	on a straight forward readable way... need to double check with Jason Thompson
*/

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
**Project Name:							Skyview H2I
**Analysts:                             Angel Donnarumma	(angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jason Thompson		(Jason.Thompson@skyiq.co.uk)
										,Hoi Yu Tang		(HoiYu.Tang@skyiq.co.uk)
										,Jose Pitteloud		(jose.pitteloud@skyiq.co.uk)
**Stakeholder:                          SkyIQ
										,Jose Loureda		(Jose.Loureda@skyiq.co.uk)
**Due Date:                             11/07/2014
**Project Code (Insight Collation):     v289
**Sharepoint Folder:    

	http://sp-department.bskyb.com/sites/SIGEvolved/Shared%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FSIGEvolved%2FShared%20Documents%2F01%20Analysis%20Requests%2FV289%20-%20Skyview%20Futures%2F01%20Plans%20Briefs%20and%20Project%20Admin
                                                                        
**Business Brief:

	This module is in charge of managing the execution of all other modules in the right sequence to
	automate the production of the H2I view, and to act as a centralized point of execution (rather
	than executing all modules manually one after another)...

**Module:
	
	M01: Process Manager
			M01.0 - Initialising Environment
			M01.1 - Identifying Pending TasksHousekeeping
			M01.2 - Tasks Execution
			M01.3 - Returning results
	
--------------------------------------------------------------------------------------------------------------
*/


-----------------------------------
-- M01.0 - Initialising Environment
-----------------------------------

create or replace procedure v289_m01_process_manager
	@fresh_start    bit         = 0 
    ,@proc_date     date        = null
    ,@sample_prop   smallint    = 100
as begin

	MESSAGE cast(now() as timestamp)||' | Begining  M01.0 - Initialising Environment' TO CLIENT
	
	-- Variables
	declare @thetask    varchar(100)
	declare @sql_       varchar(2000)
	declare @exe_status	integer
	declare @log_id		bigint
	declare @gtg_flag	bit
	declare	@Module_id	varchar(3)
	declare @thursday	date
	
	-- we currently need this Thursday for when processing scaling...
	select  @thursday = cast((dateadd(day,(5-datepart(weekday,@proc_date)),@proc_date)) as date)
	
	set @Module_id = 'M01'
	set @exe_status = -1
	
	if @fresh_start = 1
	begin
		
		MESSAGE cast(now() as timestamp)||' | @ M01.0: Fresh Start, Dropping tables' TO CLIENT
		
		drop table V289_M12_Skyview_weighted_duration	
		drop table v289_M06_dp_raw_data	
		drop table V289_M07_dp_data	
		drop table SC3I_Todays_panel_members
		drop table SC3I_weighting_working_table
		drop table SC3I_category_working_table
		drop table V289_M11_04_Barb_weighted_population
		drop table SC3_Weightings
		drop table SC3_Intervals
		drop table VESPA_HOUSEHOLD_WEIGHTING
		drop table SC3_Sky_base_segment_snapshots
		drop table SC3_Todays_panel_members
		drop table SC3_Todays_segment_weights
		drop table SC3_scaling_weekly_sample
		drop table SC3_weighting_working_table
		drop table SC3_category_working_table
		drop table SC3_category_subtotals
		drop table SC3_metrics
		drop table SC3_non_convergences
		drop table V289_PIV_Grouped_Segments_desc
		drop table SC3I_Variables_lookup_v1_1
		commit
		
		MESSAGE cast(now() as timestamp)||' | @ M01.0: Fresh Start, Dropping tables DONE' TO CLIENT
		
	end
	
	execute @exe_status = v289_m00_initialisation @proc_date
	
	if	@exe_status = 0
	begin
	
		set @exe_status = -1
	
		execute @exe_status = v289_m02_housekeeping @fresh_start, @log_id output
		
		--execute citeam.logger_add_event @log_ID, 1, @Module_id || ' : EXE M02', @exe_status
		
		if	@exe_status = 0
		begin
		
			MESSAGE cast(now() as timestamp)||' | @ M01.0: Initialising Environment DONE' TO CLIENT
			
		------------------------------------------------
		-- M01.1 - Identifying Pending TasksHousekeeping
		------------------------------------------------

			MESSAGE cast(now() as timestamp)||' | Begining  M01.1 - Identifying Pending TasksHousekeeping' TO CLIENT
			
			while exists    (
								select 	first status
								from	v289_m01_t_process_manager 
								where	status = 0			--> Any tasks Pending?...
							)
			begin
			
				MESSAGE cast(now() as timestamp)||' | @ M01.1: Pending Tasks Found' TO CLIENT

				-- What task to execute?...
				select  @thetask = task
				from    v289_m01_t_process_manager
				where   sequencer = (
										select  min(sequencer)
										from    v289_m01_t_process_manager
										where   status = 0
									)
				
				MESSAGE cast(now() as timestamp)||' | @ M01.1: Task '||@thetask||' Pending' TO CLIENT
			
			
				MESSAGE cast(now() as timestamp)||' | @ M01.1: Identifying Pending TasksHousekeeping DONE' TO CLIENT

		--------------------------
		-- M01.2 - Tasks Execution
		--------------------------

				MESSAGE cast(now() as timestamp)||' | Begining  M01.2 - Tasks Execution' TO CLIENT
				
				MESSAGE cast(now() as timestamp)||' | @ M01.2: Executing ->'||@thetask TO CLIENT
				
				set @exe_status = -1
				
				set @sql_ = 'execute @exe_status = '||  case    when @thetask = 'v289_m04_barb_data_preparation'				then @thetask||' '''||@proc_date||''''
																when @thetask = 'v289_m06_DP_data_extraction' 					then @thetask||' '''||@proc_date||''','||@sample_prop
																when @thetask = 'V289_M11_01_SC3_v1_1__do_weekly_segmentation'	then @thetask||' '''||@thursday||''','||@log_ID||','''||today()||''''
																when @thetask = 'V289_M11_02_SC3_v1_1__prepare_panel_members'	then @thetask||' '''||@thursday||''','''||@proc_date||''','''||today()||''','||@log_ID
																when @thetask = 'V289_M11_03_SC3I_v1_1__add_individual_data'	then @thetask||' '''||@thursday||''','''||today()||''','||@log_ID
																when @thetask = 'V289_M11_04_SC3I_v1_1__make_weights_BARB'		then @thetask||' '''||@thursday||''','''||@proc_date||''','''||today()||''','||@log_ID
																else @thetask
														end
				MESSAGE cast(now() as timestamp)||' | @ M01.2 - SQL :'||@sql_ TO CLIENT
				
				execute (@sql_)
				
				if @exe_status = 0
				begin
					update	v289_m01_t_process_manager
					set		status 		= 1
							,audit_date	= today()
					where	task = @thetask
					and		status = 0
					
					MESSAGE cast(now() as timestamp)||' | @ M01.2: '||@thetask||' DONE' TO CLIENT
					--execute citeam.logger_add_event @log_ID, 1, @Module_id || ' : EXE '||@thetask||' DONE', @exe_status
					
					commit
				end
				else
				begin
					MESSAGE cast(now() as timestamp)||' | @ M01.2: '||@thetask||' FAILED('||@exe_status||')' TO CLIENT
					--execute citeam.logger_add_event @log_ID, 1, @Module_id || ' : EXE '||@thetask||' FAILED', @exe_status
					break
				end		
				
				MESSAGE cast(now() as timestamp)||' | @ M01.2: Tasks Execution DONE' TO CLIENT
				
			end
		end
		else
		begin
			
			MESSAGE cast(now() as timestamp)||' | @ M01.3: Housekeeping (M02) failure!!!' TO CLIENT
		
		end
	end
	else
	begin
	
		MESSAGE cast(now() as timestamp)||' | @ M01.3: Initialisation (M00) failure!!!' TO CLIENT
	
	end
----------------------------
-- M01.3 - Returning results
----------------------------

	MESSAGE cast(now() as timestamp)||' | Begining  M01.3 - Returning results' TO CLIENT
	MESSAGE cast(now() as timestamp)||' | @ M01.3: Returning results DONE' TO CLIENT
	
	MESSAGE cast(now() as timestamp)||' | M01 Finished' TO CLIENT
	commit

end;

commit;
grant execute on v289_m01_process_manager to vespa_group_low_security;
commit;/*


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
**Project Name:							Skyview H2I
**Analysts:                             Angel Donnarumma	(angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jason Thompson		(Jason.Thompson@skyiq.co.uk)
										,Hoi Yu Tang		(HoiYu.Tang@skyiq.co.uk)
										,Jose Pitteloud		(jose.pitteloud@skyiq.co.uk)
**Stakeholder:                          SkyIQ
										,Jose Loureda		(Jose.Loureda@skyiq.co.uk)
**Due Date:                             11/07/2014
**Project Code (Insight Collation):     v289
**Sharepoint Folder:    

	http://sp-department.bskyb.com/sites/SIGEvolved/Shared%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FSIGEvolved%2FShared%20Documents%2F01%20Analysis%20Requests%2FV289%20-%20Skyview%20Futures%2F01%20Plans%20Briefs%20and%20Project%20Admin                                                        
	                                                          
**Business Brief:

	This Module goal is to assign a session size to all the events using Monte Carlo simulation process. 

**Module:
	
	M02: Housekeeping
			M02.0 - Initialising Environment
			M02.1 - Checking for Fresh Start flag 
			M02.2 - Maintaining Base tables
			M02.3 - Initialising the logger
			M02.4 - Returning Results
	
--------------------------------------------------------------------------------------------------------------
*/


-----------------------------------
-- M02.0 - Initialising Environment
-----------------------------------
create or replace procedure v289_m02_housekeeping
	@fresh_start	bit	= 0
	,@log_id		bigint 	output
as begin

	MESSAGE cast(now() as timestamp)||' | Begining  M02.0 - Initialising Environment' TO CLIENT
	
	-- variables
	declare	@tasks_done		smallint
	declare @total_tasks	smallint
	declare @logbatch_id	varchar(20)
	declare @logrefres_id	varchar(40)
	
	MESSAGE cast(now() as timestamp)||' | @ M02.0: Initialising Environment DONE' TO CLIENT

----------------------------------------
-- M02.1 - Checking for Fresh Start flag
----------------------------------------

	MESSAGE cast(now() as timestamp)||' | Begining  M02.1 - Checking for Fresh Start flag' TO CLIENT
	
	if @fresh_start = 1
	begin
	
		MESSAGE cast(now() as timestamp)||' | @ M02.1: Fresh Start requested: Resting process table' TO CLIENT
		
		update	v289_m01_t_process_manager
		set		status = 0
		
		commit
		
		MESSAGE cast(now() as timestamp)||' | @ M02.1: Checking for Fresh Start flag DONE' TO CLIENT
		
	end
	
----------------------------------
-- M02.2 - Maintaining Base tables
----------------------------------
	
	else
	begin
	
		/*
			checking if all tasks have been executed so meaning we need to restart
			their status else they will never get executed
			
			status = 1 means DONE
			status = 0 means PENDING
		*/
	
		MESSAGE cast(now() as timestamp)||' | @ M02.1: No Fresh Start requested' TO CLIENT
		MESSAGE cast(now() as timestamp)||' | Begining  M02.2 - Maintaining Base tables' TO CLIENT
		
		select	@tasks_done = count(1) from v289_m01_t_process_manager where status > 0
		select	@total_tasks = count(1) from v289_m01_t_process_manager
		
		if @tasks_done = @total_tasks
		begin
			
			MESSAGE cast(now() as timestamp)||' | @ M02.2: Reseting Status in Process Table' TO CLIENT
			
			update 	v289_m01_t_process_manager
			set		status = 0
			
			commit
			
			MESSAGE cast(now() as timestamp)||' | @ M02.2: Reseting Status in Process Table DONE' TO CLIENT
			
		end
		
	end
	
	if	( @fresh_start = 1 or @tasks_done = @total_tasks)
	begin
		
		MESSAGE cast(now() as timestamp)||' | @ M02.2: Cleaning Base tables' TO CLIENT
		
		/*
		if object_id('PI_BARB_import') is not null drop table PI_BARB_import	
		if object_id('BARB_Individual_Panel_Member_Details') is not null drop table BARB_Individual_Panel_Member_Details	
		if object_id('BARB_Panel_Member_Responses_Weights_and_Viewing_Categories') is not null drop table BARB_Panel_Member_Responses_Weights_and_Viewing_Categories	
		if object_id('BARB_PVF_Viewing_Record_Panel_Members') is not null drop table BARB_PVF_Viewing_Record_Panel_Members	
		if object_id('BARB_PVF06_Viewing_Record_Panel_Members') is not null drop table BARB_PVF06_Viewing_Record_Panel_Members	
		if object_id('BARB_Panel_Demographic_Data_TV_Sets_Characteristics') is not null drop table BARB_Panel_Demographic_Data_TV_Sets_Characteristics	
		if object_id('BARB_PVF04_Individual_Member_Details') is not null drop table BARB_PVF04_Individual_Member_Details
		if object_id('BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories') is not null	truncate table BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories		
		*/
		--if object_id('V289_PIV_Grouped_Segments_desc') is not null			drop table V289_PIV_Grouped_Segments_desc	 			[STATIC]
		--if object_id('V289_M08_SKY_HH_composition') is not null 			truncate table V289_M08_SKY_HH_composition					[SEMI-STATIC]
		--if object_id('V289_M08_SKY_HH_view') is not null 					truncate table V289_M08_SKY_HH_view							[SEMI-STATIC]
		if object_id('V289_M12_Skyview_weighted_duration') is not null		truncate table V289_M12_Skyview_weighted_duration	
		if object_id('v289_M06_dp_raw_data') is not null 					truncate table v289_M06_dp_raw_data	
		if object_id('V289_M07_dp_data') is not null 						truncate table V289_M07_dp_data	
		--if object_id('SC3I_Variables_lookup_v1_1') is not null 				truncate table SC3I_Variables_lookup_v1_1				[STATIC]
		--if object_id('SC3I_Segments_lookup_v1_1') is not null 				truncate table SC3I_Segments_lookup_v1_1	    		[I THINK IS STATIC]
		--if object_id('SC3I_Sky_base_segment_snapshots') is not null 		truncate table SC3I_Sky_base_segment_snapshots				[STATIC]
		if object_id('SC3I_Todays_panel_members') is not null 				truncate table SC3I_Todays_panel_members
		if object_id('SC3I_weighting_working_table') is not null 			truncate table SC3I_weighting_working_table
		if object_id('SC3I_category_working_table') is not null 			truncate table SC3I_category_working_table
		--if object_id('SC3I_category_subtotals') is not null 				truncate table SC3I_category_subtotals						[STATIC]
		--if object_id('SC3I_metrics') is not null 							truncate table SC3I_metrics									[STATIC]
		--if object_id('SC3I_non_convergences') is not null 					truncate table SC3I_non_convergences					[STATIC]
		--if object_id('SC3I_Weightings') is not null 						truncate table SC3I_Weightings								[STATIC]
		--if object_id('SC3I_Intervals') is not null 							truncate table SC3I_Intervals							[STATIC]
		--if object_id('V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING') is not null 	truncate table V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING	[STATIC]
		if object_id('V289_M11_04_Barb_weighted_population') is not null 	truncate table V289_M11_04_Barb_weighted_population
		if object_id('SC3_Weightings') is not null 							truncate table SC3_Weightings
		if object_id('SC3_Intervals') is not null 							truncate table SC3_Intervals
		if object_id('VESPA_HOUSEHOLD_WEIGHTING') is not null 				truncate table VESPA_HOUSEHOLD_WEIGHTING
		if object_id('SC3_Sky_base_segment_snapshots') is not null 			truncate table SC3_Sky_base_segment_snapshots
		if object_id('SC3_Todays_panel_members') is not null 				truncate table SC3_Todays_panel_members
		if object_id('SC3_Todays_segment_weights') is not null 				truncate table SC3_Todays_segment_weights
		if object_id('SC3_scaling_weekly_sample') is not null 				truncate table SC3_scaling_weekly_sample
		if object_id('SC3_weighting_working_table') is not null 			truncate table SC3_weighting_working_table
		if object_id('SC3_category_working_table') is not null 				truncate table SC3_category_working_table
		if object_id('SC3_category_subtotals') is not null 					truncate table SC3_category_subtotals
		--if object_id('SC3_metrics') is not null 							truncate table SC3_metrics
		if object_id('SC3_non_convergences') is not null 					truncate table SC3_non_convergences
	
		commit
		MESSAGE cast(now() as timestamp)||' | @ M02.2: Cleaning Base tables DONE' TO CLIENT
	
	end
	
	MESSAGE cast(now() as timestamp)||' | @ M02.2: Maintaining Base tables DONE' TO CLIENT
	
----------------------------------
-- M02.3 - Initialising the logger
----------------------------------	
	
	MESSAGE cast(now() as timestamp)||' | Begining  M02.3 - Initialising the logger' TO CLIENT
	
	-- Now automatically detecting if it's a test build and logging appropriately...
	
	if lower(user) = 'vespa_analysts'
		set @logbatch_id = 'H2I'
	else
		set @logbatch_id = 'H2I test ' || upper(right(user,1)) || upper(left(user,2))

	set @logrefres_id = convert(varchar(10),today(),123) || ' H2I refresh'
	
	execute citeam.logger_create_run @logbatch_id, @logrefres_id, @log_ID output

	--execute citeam.logger_add_event @log_ID, 3, 'M02: Log initialised'
	
	MESSAGE cast(now() as timestamp)||' | @ M02.3: Initialising the logger DONE' TO CLIENT
	
----------------------------
-- M02.4 - Returning Results
----------------------------

	MESSAGE cast(now() as timestamp)||' | Begining  M02.4 - Returning Results' TO CLIENT
	MESSAGE cast(now() as timestamp)||' | @ M02.4: Returning Results DONE' TO CLIENT
	
	MESSAGE cast(now() as timestamp)||' | M02 Finished' TO CLIENT	

end;

commit;
grant execute on v289_m02_housekeeping to vespa_group_low_security;
commit; /*


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
	
	SET @file_creation_date = (SELECT CAST(substr(imported_text,7,8) AS Date)
                                FROM PI_BARB_import
                                WHERE substr(imported_text,1,2) = '01')

	SET @file_creation_time = (SELECT CAST(substr(imported_text,15,2) || ':' || substr(imported_text,17,2) || ':' || substr(imported_text,19,2)  AS Time)
									FROM PI_BARB_import
									WHERE substr(imported_text,1,2) = '01')

	SET @file_type = (SELECT substr(imported_text,21,12)
									FROM PI_BARB_import
									WHERE substr(imported_text,1,2) = '01')

	SET @File_Version = (SELECT CAST(substr(imported_text,33,3) AS Int)
									FROM PI_BARB_import
									WHERE substr(imported_text,1,2) = '01')

	SET @Filename = (SELECT substr(imported_text,36,13)
									FROM PI_BARB_import
									WHERE substr(imported_text,1,2) = '01')

	MESSAGE cast(now() as timestamp)||' | @ M03.0: Initialising Environment DONE' TO CLIENT

--------------------------
-- M03.1 - Extracting Data
--------------------------

	MESSAGE cast(now() as timestamp)||' | Begining M03.1 - Extracting Data' TO CLIENT
	
-- BARB_Individual_Panel_Member_Details

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
	
	commit
	MESSAGE cast(now() as timestamp)||' | @ M03.1: Table BARB_PVF_Viewing_Record_Panel_Members LOADED' TO CLIENT
	
	
-- BARB_Panel_Demographic_Data_TV_Sets_Characteristics	
	
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
**Project Name:							Skyview H2I
**Analysts:                             Angel Donnarumma	(angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jason Thompson		(Jason.Thompson@skyiq.co.uk)
										,Hoi Yu Tang		(HoiYu.Tang@skyiq.co.uk)
										,Jose Pitteloud		(jose.pitteloud@skyiq.co.uk)
**Stakeholder:                          SkyIQ
										,Jose Loureda		(Jose.Loureda@skyiq.co.uk)
**Due Date:                             11/07/2014
**Project Code (Insight Collation):     v289
**Sharepoint Folder:    

	http://sp-department.bskyb.com/sites/SIGEvolved/Shared%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FSIGEvolved%2FShared%20Documents%2F01%20Analysis%20Requests%2FV289%20-%20Skyview%20Futures%2F01%20Plans%20Briefs%20and%20Project%20Admin                                                        
	                                                                
**Business Brief:

	This Module is to prepare the extracted BARB data into a more suitable data structure for analysis...

**Module:
	
	M04: Barb Data Preparation
			M04.0 - Initialising Environment
			M04.1 - Preparing transient tables
			M04.2 - Final BARB Data Preparation
			M04.3 - Returning Results
	
--------------------------------------------------------------------------------------------------------------
*/


-----------------------------------
-- M04.0 - Initialising Environment
-----------------------------------

create or replace procedure v289_m04_barb_data_preparation
	@processing_date date = null
as begin

	MESSAGE cast(now() as timestamp)||' | Begining M04.0 - Initialising Environment' TO CLIENT
	
	
    declare @a int
	
    select	@a = count(1)
    from	barb_weights
    
	if @a > 0
	begin
	
		MESSAGE cast(now() as timestamp)||' | @ M04.0: Initialising Environment DONE' TO CLIENT
		
-------------------------------------
-- M04.1 - Preparing transient tables
-------------------------------------

		MESSAGE cast(now() as timestamp)||' | Begining M04.1 - Preparing transient tables' TO CLIENT
		
		/*
			Extracting the sample of panellists from Barb with Sky as the base for any analysis for the project
			at this stage we are only interested on the household demographic (hh size, sex and age of people withing the hH)
		*/

		if object_id('skybarb') is not null
			drop table skybarb
			
		commit
		
		select  demo.household_number										as house_id
				,demo.person_number											as person
				,datepart(year,today())-datepart(year,demo.date_of_birth) 	as age
				,case   when demo.sex_code = 1 then 'Male'
						when demo.sex_code = 2 then 'Female'
						else 'Unknown'
				end     as sex
				,case   when demo.household_status in (4,2)  then 1
						else 0
				end     as head
		into	skybarb
		from    BARB_INDV_PANELMEM_DET  as demo
				inner join  (
								select  distinct household_number
								from    BARB_PANEL_DEMOGR_TV_CHAR
								where   @processing_date between date_valid_from and date_valid_to
								and     reception_capability_code_1 = 2
							)   as barb_sky_panelists
				on  demo.household_number   = barb_sky_panelists.household_number
		where   @processing_date between demo.date_valid_from and demo.date_valid_to

		commit
		
		create hg index hg1	on skybarb(house_id)
		create lf index lf1	on skybarb(person)
		commit
		
		grant select on skybarb to vespa_group_low_security
		commit

		MESSAGE cast(now() as timestamp)||' | @ M04.1: Preparing transient tables DONE' TO CLIENT
		
--------------------------------------
-- M04.2 - Final BARB Data Preparation
--------------------------------------
		
		MESSAGE cast(now() as timestamp)||' | Begining M04.2 - Final BARB Data Preparation' TO CLIENT
		
		
		/*
			Now constructing a table to be able to check minutes watched across all households based on Barb (weighted to show UK):
			Channel pack, household size, programme genre and the part of the day where these actions happened (breakfast, lunch, etc...)
		*/

		if object_id('skybarb_fullview') is not null
			drop table skybarb_fullview

		commit

		select  mega.*
				,z.sex
				,case   when z.age between 1 and 19		then '0-19'
						when z.age between 20 and 24 	then '20-24'
						when z.age between 25 and 34 	then '25-34'
						when z.age between 35 and 44 	then '35-44'
						when z.age between 45 and 64 	then '45-64'
						when z.age >= 65              	then '65+'  
				end     as ageband
		into    skybarb_fullview
		from    (
					select  ska.service_key
							,barbskyhhsize.thesize	as hhsize
							,base.*
					from    (
								-- multiple aggregations to derive part of the day where the viewing session took place
								-- and a workaround to get the minutes watched per each person in the household multiplied
								-- by their relevant weights to show the minutes watched by UK (as per barb scaling exercise)...
								select  viewing.household_number
										,viewing.programme_name
										,local_start_time_of_session	as start_time_of_session
										,local_end_time_of_session		as end_time_of_session
										,local_tv_instance_start_date_time
										,local_tv_instance_end_date_time
										,duration_of_session
										,db1_station_code
										,case when local_start_time_of_recording is null then local_start_time_of_session else local_start_time_of_recording end as session_start_date_time
										,case when local_start_time_of_recording is null then local_end_time_of_session else dateadd(mi, Duration_of_session, local_start_time_of_recording) end as session_end_date_time -- -1 because of minute attribution
										,case   when cast(local_start_time_of_session as time) between '00:00:00.000' and '05:59:00.000' then 'night'
												when cast(local_start_time_of_session as time) between '06:00:00.000' and '08:59:00.000' then 'breakfast'
												when cast(local_start_time_of_session as time) between '09:00:00.000' and '11:59:00.000' then 'morning'
												when cast(local_start_time_of_session as time) between '12:00:00.000' and '14:59:00.000' then 'lunch'
												when cast(local_start_time_of_session as time) between '15:00:00.000' and '17:59:00.000' then 'early prime'
												when cast(local_start_time_of_session as time) between '18:00:00.000' and '20:59:00.000' then 'prime'
												when cast(local_start_time_of_session as time) between '21:00:00.000' and '23:59:00.000' then 'late night'
										end     as session_daypart
										,viewing.channel_pack
										,viewing.genre_description
										,weights.person_number
										,weights.processing_weight	as theweight
										,case when person_1_viewing   = 1 and person_number = 1   then theweight*duration_of_session else 0 end as person_1
										,case when person_2_viewing   = 1 and person_number = 2   then theweight*duration_of_session else 0 end as person_2
										,case when person_3_viewing   = 1 and person_number = 3   then theweight*duration_of_session else 0 end as person_3
										,case when person_4_viewing   = 1 and person_number = 4   then theweight*duration_of_session else 0 end as person_4
										,case when person_5_viewing   = 1 and person_number = 5   then theweight*duration_of_session else 0 end as person_5
										,case when person_6_viewing   = 1 and person_number = 6   then theweight*duration_of_session else 0 end as person_6
										,case when person_7_viewing   = 1 and person_number = 7   then theweight*duration_of_session else 0 end as person_7
										,case when person_8_viewing   = 1 and person_number = 8   then theweight*duration_of_session else 0 end as person_8
										,case when person_9_viewing   = 1 and person_number = 9   then theweight*duration_of_session else 0 end as person_9
										,case when person_10_viewing  = 1 and person_number = 10  then theweight*duration_of_session else 0 end as person_10
										,case when person_11_viewing  = 1 and person_number = 11  then theweight*duration_of_session else 0 end as person_11
										,case when person_12_viewing  = 1 and person_number = 12  then theweight*duration_of_session else 0 end as person_12
										,case when person_13_viewing  = 1 and person_number = 13  then theweight*duration_of_session else 0 end as person_13
										,case when person_14_viewing  = 1 and person_number = 14  then theweight*duration_of_session else 0 end as person_14
										,case when person_15_viewing  = 1 and person_number = 15  then theweight*duration_of_session else 0 end as person_15
										,case when person_16_viewing  = 1 and person_number = 16  then theweight*duration_of_session else 0 end as person_16
										,person_1+person_2+person_3+person_4+person_5+person_6+person_7+person_8+person_9+person_10+person_11+person_12+person_13+person_14+person_15+person_16 as theflag
										,case when  session_start_date_time >= local_tv_instance_start_date_time then session_start_date_time else local_tv_instance_start_date_time end as x
										,case when  local_tv_instance_end_date_time <= session_end_date_time then local_tv_instance_end_date_time else session_end_date_time end as y
										,datediff(minute,x,y)	        as progwatch_duration
										,progwatch_duration * theweight as progscaled_duration
                                        ,broadcast_start_date_time_local
                                        ,broadcast_end_date_time_local
								from    ripolile.latest_barb_viewing_table  as viewing
										inner join  barb_weights			as weights
										on  viewing.household_number    = weights.household_number
								where   viewing.sky_stb_holder_hh = 'Y'
								and		cast(viewing.local_start_time_of_session as date) = @processing_date
							)   as base
							inner join	(
											-- fixing barb sample to only barb panellists with Sky (table from prior step)
											select  house_id
													,max(person) as thesize
											from    skybarb
											group   by  house_id
										)   as barbskyhhsize
							on	base.household_number	= barbskyhhsize.house_id
							inner join  (
											-- mapping the db1 station code to the actual service key to find meta data for service key
											-- done on the join after this one...
											select  db1_station_code, service_key
											from    thompsonja.BARB_Channel_Map
											where   main_sk = 'Y'
										)   as map
							on  base.db1_station_code   = map.db1_station_code
							inner join  (
											-- getting metadata for service key
											select  service_key
													,channel_genre
													,channel_pack
											from    vespa_Analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES
											where   activex = 'Y'
										)   as ska
							on  map.service_key         = ska.service_key
					where   base.theflag > 0
				)   as mega
				inner join  skybarb as z
				on  mega.household_number   = z.house_id
				and mega.person_number      = z.person


		commit

		create hg index hg1 on skybarb_fullview     (service_key)
		create hg index hg2 on skybarb_fullview     (household_number)
		create lf index lf1 on skybarb_fullview     (channel_pack)
		--create lf index lf2 on skybarb_fullview     (programme_genre)
		create dttm index dt1 on skybarb_fullview   (start_time_of_session)
		create dttm index dt2 on skybarb_fullview   (end_time_of_session)
		create dttm index dt3 on skybarb_fullview   (session_start_date_time)
		create dttm index dt4 on skybarb_fullview   (session_end_date_time)
		commit

		grant select on skybarb_fullview to vespa_group_low_security
		commit
				
		MESSAGE cast(now() as timestamp)||' | @ M04.1: Final BARB Data Preparation DONE' TO CLIENT
	
	
	end
	
	else
	begin
	
		MESSAGE cast(now() as timestamp)||' | @ M04.0: Missing Data on base tables for Data Preparation Stage!!!' TO CLIENT
		
	end

	
----------------------------
-- M04.3 - Returning Results	
----------------------------

	MESSAGE cast(now() as timestamp)||' | M04 Finished' TO CLIENT	
	
end;

commit;
grant execute on v289_m04_barb_data_preparation to vespa_group_low_security;
commit; /*


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
**Project Name:							Skyview H2I
**Analysts:                             Angel Donnarumma	(angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jason Thompson		(Jason.Thompson@skyiq.co.uk)
										,Hoi Yu Tang		(HoiYu.Tang@skyiq.co.uk)
										,Jose Pitteloud		(jose.pitteloud@skyiq.co.uk)
**Stakeholder:                          SkyIQ
										,Jose Loureda		(Jose.Loureda@skyiq.co.uk)
**Due Date:                             11/07/2014
**Project Code (Insight Collation):     v289
**Sharepoint Folder:    

	http://sp-department.bskyb.com/sites/SIGEvolved/Shared%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FSIGEvolved%2FShared%20Documents%2F01%20Analysis%20Requests%2FV289%20-%20Skyview%20Futures%2F01%20Plans%20Briefs%20and%20Project%20Admin                                                        
	                                                          
**Business Brief:

	This Module goal is to generate the probability matrices from BARB data to be used for identifying
	the most likely candidate(s) of been watching TV at a given event...

**Module:
	
	M05: Barb Matrices Generation
			M05.0 - Initialising Environment
			M05.1 - Aggregating transient tables 
			M05.2 - Generating Matrices
			M05.3 - Returning Results
	
--------------------------------------------------------------------------------------------------------------
*/

-----------------------------------
-- M05.0 - Initialising Environment
-----------------------------------

create or replace procedure v289_m05_barb_Matrices_generation
as begin

	MESSAGE cast(now() as timestamp)||' | Begining M05.0 - Initialising Environment' TO CLIENT
	MESSAGE cast(now() as timestamp)||' | @ M05.0: Initialising Environment DONE' TO CLIENT

	
---------------------------------------
-- M05.1 - Aggregating transient tables
---------------------------------------

	MESSAGE cast(now() as timestamp)||' | Begining M05.1 - Aggregating transient tables' TO CLIENT
	
	/*
		Aggregating hours watched for all dimensions, this will be the base for slicing the 
		probabilities when generating the Matrices...
	*/
	
	select  cast(start_time_of_session as date) as thedate
			,lookup.segment_id
			,skybarb.hhsize
			,skybarb.sex
			,coalesce(skybarb.ageband,'Undefined') as ageband
			,cast((sum(distinct skybarb.progscaled_duration)/60.0) as integer)  as uk_hhwatched
	into	#base
	from    skybarb_fullview    as skybarb
            inner join V289_PIV_Grouped_Segments_desc    as lookup
            on  skybarb.session_daypart = lookup.daypart
            and skybarb.channel_pack    = lookup.channel_pack
            and skybarb.programme_genre = lookup.genre
	group   by  thedate
				,lookup.segment_id
				,skybarb.hhsize
				,skybarb.sex
				,skybarb.ageband
	
	commit
	
	create lf index lf1 on #base(segment_id)
	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M05.1: Base Table Generation DONE' TO CLIENT
	
	
	/*
		Identifying sessions on the viewing data...
	*/
	
	select  *
			,rank() over    (
								partition by    household_number
								order by        start_time_of_session
							)   as session_id
	into	#pseudo_base2
	from    skybarb_fullview
	
	commit
	
	create lf index lf1 on #pseudo_base2(session_daypart)
	create lf index lf2 on #pseudo_base2(channel_pack)
	create lf index lf3 on #pseudo_base2(programme_genre)
	commit
	
	select  cast(skybarb.start_time_of_session as date) as thedate
            ,lookup.segment_id
            ,skybarb.hhsize
            ,skybarb.session_id
            ,count(distinct skybarb.person_number)  as session_size
            ,cast((sum(distinct skybarb.progscaled_duration)/60.0) as integer)  as uk_hhwatched
    into    #base2
    from    #pseudo_base2                                            as skybarb
            inner join V289_PIV_Grouped_Segments_desc    as lookup
            on  skybarb.session_daypart = lookup.daypart
            and skybarb.channel_pack    = lookup.channel_pack
            and skybarb.programme_genre = lookup.genre
    group   by  thedate
                ,lookup.segment_id
                ,skybarb.hhsize
                ,skybarb.session_id
    
    
    commit
    
    create lf index lf1 on #base2(segment_id)
	create lf index lf2 on #base2(hhsize)
	commit
	
	drop table #pseudo_base2
	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M05.1: Base2 Table Generation DONE' TO CLIENT
	
	MESSAGE cast(now() as timestamp)||' | @ M05.1: Aggregating transient tables DONE' TO CLIENT

------------------------------
-- M05.2 - Generating Matrices
------------------------------

	MESSAGE cast(now() as timestamp)||' | Begining M05.1 - Generating Matrices' TO CLIENT
	
	-- PIV sex/age
	
	/*
		Now generating the matrix to identify who is most likely to be watching TV based
		BARB distributions by sex and age over specific part of the day, channel pack
		and programme genre...
	*/
	
	if object_id('v289_genderage_matrix') is not null
		drop table v289_genderage_matrix
		
	commit
	
	select  base.*
			,cast(base.uk_hhwatched as decimal(10,2)) / cast(totals.tot_uk_hhwatched as decimal(10,2)) as PIV
	into	v289_genderage_matrix
	from    #base        as base
			inner join  (
							select  thedate
									,segment_id
									,sum(uk_hhwatched)  as tot_uk_hhwatched
							from    #base
							group   by  thedate
										,segment_id
						)   as totals
			on  base.thedate            = totals.thedate
			and base.segment_id         = totals.segment_id
	where   totals.tot_uk_hhwatched > 0 
	
	commit
	
	create lf index lf1 on v289_genderage_matrix(segment_id)
	commit
	
	grant select on v289_genderage_matrix to vespa_group_low_security
	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M05.1: Sex/Age Matrix Generation DONE (v289_genderage_matrix)' TO CLIENT
	
	-- PIV Session size
	
	/*
		This is the probability matrix to determine the size of the session, how many
		people were watching TV on a given date, an specific part of the day, household
		size, channel pack and programme genre. All based on BARB distributions...
	*/
	
	if object_id('v289_sessionsize_matrix') is not null
		drop table v289_sessionsize_matrix
		
	commit
	
	select  base.*
			,cast(base.uk_hhwatched as decimal(10,2)) / cast(totals.tot_uk_hhwatched as decimal(10,2)) as proportion
			,coalesce((SUM (proportion) OVER (PARTITION BY base.thedate,base.segment_ID, base.hhsize ORDER BY base.session_size ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)),0) AS Lower_limit
            ,SUM (proportion) OVER (PARTITION BY base.thedate,base.segment_ID, base.hhsize ORDER BY base.session_size ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Upper_limit
	into	v289_sessionsize_matrix
	from    (
                select  thedate
                        ,segment_id
                        ,hhsize
                        ,session_size
                        ,sum(uk_hhwatched) as uk_hhwatched
                from    #base2
                group   by  thedate
                            ,segment_id
                            ,hhsize
                            ,session_size
            )   as base
			inner join  (
							select  thedate
                                    ,segment_id
									,hhsize
									,sum(uk_hhwatched) as tot_uk_hhwatched
							from    #base2
							group   by  thedate
										,segment_id
										,hhsize
						)   as totals
			on  base.thedate            = totals.thedate
			and base.segment_id         = totals.segment_id
			and base.hhsize             = totals.hhsize
    where   totals.tot_uk_hhwatched > 0
	
	commit
    
    create lf index lf1 on v289_sessionsize_matrix(segment_id)
	create lf index lf2 on v289_sessionsize_matrix(hhsize)
	commit
	
	grant select on v289_sessionsize_matrix to vespa_group_low_security
	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M05.1: Session size Matrix Generation DONE (v289_sessionsize_matrix)' TO CLIENT

	
	-- DEFAULT session size matrix
			
	DECLARE @min DECIMAL(8,7)   

    if object_id('v289_sessionsize_matrix_ID') is not null
        drop table v289_sessionsize_matrix_ID

    commit

    SELECT  segment_id
			,session_size
			,hhsize
			,SUM(uk_hhwatched) uk_hhwatched
			,SUM(uk_hhwatched) OVER (PARTITION BY segment_id, hhsize) AS total_watched
			,CAST (uk_hhwatched AS DECIMAL(11,1)) / CAST (total_watched  AS DECIMAL(11,1)) AS proportion
    INTO    v289_sessionsize_matrix_ID
	FROM    v289_sessionsize_matrix
    group   by  segment_id
    			,session_size
    			,hhsize    
	
	SET @min  = (
                    SELECT  min(proportion) 
                    FROM    v289_sessionsize_matrix_ID
                    where   proportion >0 /2
                )


	if object_id('v289_sessionsize_matrix_default') is not null 
        drop table v289_sessionsize_matrix_default

	COMMIT
	SELECT  a.segment_ID
    		,b.hhsize
    		,sx.session_size
    		,COALESCE(c.proportion, @min)    AS proportion
    		,SUM (proportion)  OVER    (PARTITION BY a.segment_ID, b.hhsize )  AS norm
    		,coalesce((SUM (proportion)  OVER    (PARTITION BY a.segment_ID, b.hhsize  ORDER BY sx.session_size ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) / norm),0)    AS Lower_limit
    		,coalesce((SUM (proportion)  OVER    (PARTITION BY a.segment_ID, b.hhsize  ORDER BY sx.session_size ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / norm),0)    AS Upper_limit
	INTO    v289_sessionsize_matrix_default
	FROM    (
                SELECT  DISTINCT 
                        segment_ID 
                FROM    v289_sessionsize_matrix_ID
            )   as a
	        CROSS JOIN  (
                            SELECT  DISTINCT 
                                    CASE    WHEN hhsize >8 THEN 8 
                                            ELSE hhsize 
                                    END     as hhsize 
                            FROM    v289_sessionsize_matrix_ID
                        )   AS b
	        CROSS JOIN  (
                            SELECT  DISTINCT 
                                    CASE    WHEN hhsize >8 THEN 8 
                                            ELSE hhsize 
                                    END     as session_size 
                            FROM    v289_sessionsize_matrix_ID
                        )   AS sx
	        LEFT JOIN   v289_sessionsize_matrix_ID AS c 
            ON  a.segment_id = c.segment_id 
            AND b.hhsize = c.hhsize 
            AND c.session_size = sx.session_size 
            AND c. proportion > 0
	WHERE   b.hhsize >= sx.session_size

	DELETE FROM v289_sessionsize_matrix_default
	WHERE session_size > hhsize

	COMMIT

	CREATE LF INDEX UW ON v289_sessionsize_matrix_default(segment_ID)
	CREATE LF INDEX UQ ON v289_sessionsize_matrix_default(hhsize)
	
	COMMIT

	DROP TABLE v289_sessionsize_matrix_ID
	
	GRANT ALL ON v289_sessionsize_matrix_default  TO vespa_group_low_security	

	COMMIT 
		
		
	MESSAGE cast(now() as timestamp)||' | @ M05.1: DEFAULT Session size Matrix Generation DONE (v289_sessionsize_matrix_default)' TO CLIENT
	
	MESSAGE cast(now() as timestamp)||' | @ M05.1: Generating Matrices DONE' TO CLIENT	
	
	
----------------------------
-- M05.3 - Returning Results
----------------------------

	MESSAGE cast(now() as timestamp)||' | M05 Finished' TO CLIENT	

end;

commit;
grant execute on v289_m05_barb_Matrices_generation to vespa_group_low_security;
commit; /*


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
**Project Name:							Skyview H2I
**Analysts:                             Angel Donnarumma	(angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jason Thompson		(Jason.Thompson@skyiq.co.uk)
										,Hoi Yu Tang		(HoiYu.Tang@skyiq.co.uk)
										,Jose Pitteloud		(jose.pitteloud@skyiq.co.uk)
**Stakeholder:                          SkyIQ
										,Jose Loureda		(Jose.Loureda@skyiq.co.uk)
**Due Date:                             11/07/2014
**Project Code (Insight Collation):     v289
**Sharepoint Folder:    

	http://sp-department.bskyb.com/sites/SIGEvolved/Shared%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FSIGEvolved%2FShared%20Documents%2F01%20Analysis%20Requests%2FV289%20-%20Skyview%20Futures%2F01%20Plans%20Briefs%20and%20Project%20Admin                                                        
	
**Business Brief:

	This Module goal is to generate the probability matrices from BARB data to be used for identifying
	the most likely candidate(s) of been watching TV at a given event...

**Module:
	
	M06: DP Data Extraction
			M06.0 - Initialising Environment
			M06.1 - Composing Table Name 
			M06.2 - Data Extraction
			M06.3 - Trimming Sample
			M06.4 - Returning Results
	
--------------------------------------------------------------------------------------------------------------
*/

-----------------------------------
-- M06.0 - Initialising Environment
-----------------------------------

create or replace procedure v289_m06_DP_data_extraction
	@event_date date = null
	,@sample_proportion smallint = 100
as begin

	MESSAGE cast(now() as timestamp)||' | Begining M06.0 - Initialising Environment' TO CLIENT
	

	declare @dp_tname 	varchar(50)
	declare @query		varchar(3000)
	declare @from_dt	integer
	declare @to_dt		integer
	
	set @dp_tname = 'SK_PROD.VESPA_DP_PROG_VIEWED_'
	select  @from_dt 	= cast((dateformat(@Event_date,'YYYYMMDD')||'00') as integer)
	select  @to_dt 		= cast((dateformat(@Event_date,'YYYYMMDD')||'23') as integer)
	
	if @Event_date is null
	begin
		MESSAGE cast(now() as timestamp)||' | @ M06.0: You need to provide a Date for extraction !!!' TO CLIENT
	end
	else
	begin
	
		MESSAGE cast(now() as timestamp)||' | @ M06.0: Initialising Environment DONE' TO CLIENT
-------------------------------
-- M06.1 - Composing Table Name
-------------------------------

		MESSAGE cast(now() as timestamp)||' | Begining M06.1 - Composing Table Name' TO CLIENT

		set @dp_tname = @dp_tname||datepart(year,@Event_date)||right(('00'||cast(datepart(month,@event_date) as varchar(2))),2) 

		MESSAGE cast(now() as timestamp)||' | @ M06.1: Composing Table Name DONE: '||@dp_tname  TO CLIENT
		
--------------------------
-- M06.2 - Data Extraction
--------------------------

		MESSAGE cast(now() as timestamp)||' | Begining M06.2 - Data Extraction' TO CLIENT

		if object_id('v289_M06_dp_raw_data') is not null
			truncate table v289_M06_dp_raw_data
			
		commit

		set @query =    'insert  into v289_M06_dp_raw_data  ('||
                                                            'pk_viewing_prog_instance_fact'||
                                                            ',dk_event_start_datehour_dim'||
															',dk_event_end_datehour_dim'||
                                                            ',dk_broadcast_start_Datehour_dim'||
                                                            ',dk_instance_start_datehour_dim'||
                                                            ',dk_viewing_event_dim'||
                                                            ',duration'||
                                                            ',genre_description'||
                                                            ',service_key'||
                                                            ',cb_key_household'||
                                                            ',event_start_date_time_utc'||
                                                            ',event_end_date_time_utc'||
                                                            ',account_number'||
                                                            ',subscriber_id'||
                                                            ',service_instance_id'||
															',programme_name'||
															',capping_end_Date_time_utc'||
															',broadcast_start_date_time_utc'||
															',broadcast_end_date_time_utc'||
															',instance_start_date_time_utc'||
															',instance_end_date_time_utc'||
                                                        ') '||
                        'select  pk_viewing_prog_instance_fact'||
                                ',dk_event_start_datehour_dim'||
								',dk_event_end_datehour_dim'||
                                ',dk_broadcast_start_Datehour_dim'||
                                ',dk_instance_start_datehour_dim'||
                                ',dk_viewing_event_dim'||
                                ',duration'||
                                ',case when genre_description in (''Undefined'',''Unknown'') then ''Unknown'' else genre_description end'||
                                ',service_key'||
                                ',c.household_key'||
                                ',event_start_date_time_utc'||
                                ',event_end_date_time_utc'||
                                ',a.account_number'||
                                ',subscriber_id'||
                                ',service_instance_id'||
								',programme_name'||
								',capping_end_Date_time_utc'||
								',broadcast_start_date_time_utc'||
								',broadcast_end_date_time_utc'||
								',instance_start_date_time_utc'||
								',instance_end_date_time_utc'||
                        ' from    '||@dp_tname||' as a '
								||'inner join  (
													select  account_number
															,min(cb_key_household)  as household_key
													from    V289_M08_SKY_HH_composition 
													group   by  account_number
												)   as c 
								on a.account_number = c.account_number '||
						
						'where dk_event_start_datehour_dim between '||@from_dt||' and '||@to_dt
						
						
		execute (@query)
		
		commit
		
		MESSAGE cast(now() as timestamp)||' | @ M06.2: Data Extraction DONE ROWS;'||@@rowcount  TO CLIENT
			
	
--------------------------
-- M06.3 - Trimming Sample
--------------------------
		
		if @sample_proportion < 100
		begin
				
			MESSAGE cast(now() as timestamp)||' | Begining M06.3 - Trimming Sample' TO CLIENT
			
			select  account_number
					,cast(account_number as float)          as random
			into	#aclist
			from    v289_M06_dp_raw_data
			group   by   account_number

			commit

			update  #aclist
			set     random  = rand(cast(account_number as float)+datepart(us, getdate()))

			commit

			select  distinct account_number
			into    #sample
			from    (
						select  *
								,row_number() over( order by random) as therow
						from    #aclist
					)   as base
			where   therow <=   (
									select  (count(1)*@sample_proportion)/100
									from    #aclist
								)

			commit
			
			delete  v289_M06_dp_raw_data
			where   account_number not in   (
												select  distinct
														account_number
												from    #sample
											)
											
			commit
		
			MESSAGE cast(now() as timestamp)||' | @ M06.3: Trimming Sample DONE' TO CLIENT
		
		end	
		
	
----------------------------
-- M06.4 - Returning Results
----------------------------

	end

	MESSAGE cast(now() as timestamp)||' | M06 Finished' TO CLIENT

end;

commit;
grant execute on v289_m06_DP_data_extraction to vespa_group_low_security;
commit; /*


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
**Project Name:							Skyview H2I
**Analysts:                             Angel Donnarumma	(angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jason Thompson		(Jason.Thompson@skyiq.co.uk)
										,Hoi Yu Tang		(HoiYu.Tang@skyiq.co.uk)
										,Jose Pitteloud		(jose.pitteloud@skyiq.co.uk)
**Stakeholder:                          SkyIQ
										,Jose Loureda		(Jose.Loureda@skyiq.co.uk)
**Due Date:                             11/07/2014
**Project Code (Insight Collation):     v289
**Sharepoint Folder:    

	http://sp-department.bskyb.com/sites/SIGEvolved/Shared%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FSIGEvolved%2FShared%20Documents%2F01%20Analysis%20Requests%2FV289%20-%20Skyview%20Futures%2F01%20Plans%20Briefs%20and%20Project%20Admin                                                        
	
**Business Brief:

	This Module goal is to generate the probability matrices from BARB data to be used for identifying
	the most likely candidate(s) of been watching TV at a given event...

**Module:
	
	M07: DP Data Preparation
			M07.0 - Initialising Environment
			M07.1 - Compacting Data at Event level
			M07.2 - Appending Dimensions
			M07.3 - Assembling batches of overlaps
			M07.4 - Returning Results
	
--------------------------------------------------------------------------------------------------------------
*/

-----------------------------------
-- M07.0 - Initialising Environment
-----------------------------------

create or replace procedure v289_m07_dp_data_preparation
as begin

    MESSAGE cast(now() as timestamp)||' | Begining  M07.0 - Initialising Environment' TO CLIENT
	
	/*
		To prepare the DP data we first need to make sure we actually have something to prepare...
	*/
	
	if	exists
		(
			select	top 1 *
			from	v289_M06_dp_raw_data
		)
	begin
		
		truncate table V289_M07_dp_data
		commit
		
		MESSAGE cast(now() as timestamp)||' | @ M07.0: Initialising Environment DONE' TO CLIENT
		
-----------------------------------------
-- M07.1 - Compacting Data at Event level
-----------------------------------------

		MESSAGE cast(now() as timestamp)||' | Begining  M07.1 - Compacting Data at Event level' TO CLIENT
		
		
		if object_id('v289_m07_dp_data_tempshelf') is not null
			drop table v289_m07_dp_data_tempshelf
			
		commit
		
		select  *
		into    v289_m07_dp_data_tempshelf
		from    v289_m07_dp_data
		
		commit
		
		insert  into v289_m07_dp_data_tempshelf	(
													account_number
													,subscriber_id
													,event_id
													,event_start_utc
													,event_end_utc
													,event_start_dim
													,event_end_dim
													,event_duration_seg
													,programme_genre 
												)
		select  base.*
				,lookup.genre_description	as programme_genre -- Taking advantage here of getting this dimension already in place...
		from    (
					select  account_number
							,subscriber_id
							,min(pk_viewing_prog_instance_fact)         as event_id
							,event_start_date_time_utc			        as event_start_utc
							,case   when min(capping_end_Date_time_utc) is not null then min(capping_end_Date_time_utc)
									else event_end_date_time_utc			
							end     as event_end_utc
							,min(dk_event_start_datehour_dim)           as dk_event_start_dim
							,min(dk_event_end_datehour_dim)             as dk_event_end_dim
							,datediff(ss,event_start_utc,event_end_utc) as duration
					from    v289_M06_dp_raw_data
					group   by  account_number
								,subscriber_id
								,event_start_date_time_utc
								,event_end_date_time_utc   
				)   as base
				inner join v289_M06_dp_raw_data as lookup
				on  base.event_id   =   lookup.pk_viewing_prog_instance_fact

		commit
		
		create hg index hg1 on	v289_m07_dp_data_tempshelf(account_number)
		create hg index hg2 on	v289_m07_dp_data_tempshelf(subscriber_id)
		create hg index hg3 on	v289_m07_dp_data_tempshelf(event_id)
		create dttm index dttm1 on	v289_m07_dp_data_tempshelf(event_start_utc)
		create dttm index dttm2 on	v289_m07_dp_data_tempshelf(event_end_utc)
		commit
		
		MESSAGE cast(now() as timestamp)||' | @ M07.1: Compacting Data at Event level DONE' TO CLIENT
		
-------------------------------
-- M07.2 - Appending Dimensions
-------------------------------
		/*
			All these dimensions appended in this part are those needed for the matrices...
			They are currently 4:
			
			+ session_daypart 	[DONE]
			+ hhsize			[DONE]
			+ channel_pack		[DONE]
			+ programme_genre --> Appended on above section (M07.1)
			+ segment_id		[DONE]
			
		*/
		
		MESSAGE cast(now() as timestamp)||' | Begining  M07.2 - Appending Dimensions' TO CLIENT
		
		-- Session_daypart
		
		update  v289_m07_dp_data_tempshelf
		set     session_daypart =   case    when cast(event_start_utc as time) between '00:00:00.000' and '05:59:59.000' then 'night'
											when cast(event_start_utc as time) between '06:00:00.000' and '08:59:59.000' then 'breakfast'
											when cast(event_start_utc as time) between '09:00:00.000' and '11:59:59.000' then 'morning'
											when cast(event_start_utc as time) between '12:00:00.000' and '14:59:59.000' then 'lunch'
											when cast(event_start_utc as time) between '15:00:00.000' and '17:59:59.000' then 'early prime'
											when cast(event_start_utc as time) between '18:00:00.000' and '20:59:59.000' then 'prime'
											when cast(event_start_utc as time) between '21:00:00.000' and '23:59:59.000' then 'late night'
									end
									
		commit
		
		MESSAGE cast(now() as timestamp)||' | @ M07.2: Appending Session_Daypart DONE' TO CLIENT
		
		-- Channel_pack
		
		update  v289_m07_dp_data_tempshelf                                          as dpdata
		set     channel_pack    = cm.channel_pack
		from    v289_M06_dp_raw_data                                                    as dpraw
				inner join vespa_Analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES   as cm
				on  dpraw.service_key   = cm.service_key
		where   dpraw.pk_viewing_prog_instance_fact = dpdata.event_id

		commit
		
		MESSAGE cast(now() as timestamp)||' | @ M07.2: Appending Channel_Pack DONE' TO CLIENT
		
		
		-- hhsize		
		/*
			Due the size of the HH Composition table we need to treat this separately
			and store the result into a temp table to move on
		*/
                                              
        update  v289_m07_dp_data_tempshelf    dpdata
		set     hhsize = base.household_size
		from    V289_M08_SKY_HH_composition as base
		where   base.account_number = dpdata.account_number

		commit
		
		MESSAGE cast(now() as timestamp)||' | @ M07.2: Appending HHSize DONE' TO CLIENT
		
		
		-- Segment_id
		
		update  v289_m07_dp_data_tempshelf    as dpdata
		set     dpdata.segment_id   = seglookup.segment_id
		from    V289_PIV_Grouped_Segments_desc   as seglookup
		where   seglookup.daypart       = dpdata.session_daypart
		and     seglookup.genre         = dpdata.programme_genre
		and     seglookup.channel_pack  = dpdata.channel_pack

		commit
				
		MESSAGE cast(now() as timestamp)||' | @ M07.2: Appending Segment_ID DONE' TO CLIENT
		
		
		MESSAGE cast(now() as timestamp)||' | @ M07.2: Appending Dimensions DONE' TO CLIENT
		
-----------------------------------------
-- M07.3 - Assembling batches of overlaps
-----------------------------------------


		MESSAGE cast(now() as timestamp)||' | Begining  M07.3 - Flagging Overlapping Events' TO CLIENT
				
		-- Finding events that are overlapping with each others for each account
		
	if object_id('v289_m07_events_overlap') is not null
		drop table v289_m07_events_overlap
			
		commit
		
		select  side_a.*
				,side_b.event_start_utc	as event_start_b
				,side_b.event_end_utc 	as event_end_b
				,dense_rank() over  (
										partition by    side_a.account_number
										order by        side_a.event_id
									)   as event_index
		into    v289_m07_events_overlap
		from    (
					select  account_number
							,subscriber_id
							,event_id
							,event_start_utc
							,event_end_utc
					from    v289_m07_dp_data_tempshelf 
				)   side_A
				inner join  (
								select  account_number
										,event_id
										,event_start_utc
										,event_end_utc
								from    v289_m07_dp_data_tempshelf 
							)   as side_b
				on  side_a.account_number   = side_b.account_number
				and (
						(side_a.event_start_utc	>	side_b.event_Start_utc and side_a.event_start_utc	<	side_b.event_end_utc)
                        or
                        (side_a.event_end_utc	>	side_b.event_Start_utc and side_a.event_end_utc		<	side_b.event_end_utc)
                        or
                        (side_b.event_Start_utc	>	side_a.event_start_utc and side_b.event_Start_utc	<	side_a.event_end_utc)
                        or
                        (side_b.event_end_utc	>	side_a.event_Start_utc and side_b.event_end_utc		<	side_a.event_end_utc)
                    )
		
					
		commit
		
		create hg index hg1 	on v289_m07_events_overlap(account_number)
		create hg index hg2		on v289_m07_events_overlap(subscriber_id)
		create hg index hg3		on v289_m07_events_overlap(event_id)
		create dttm index dttm1 on v289_m07_events_overlap(event_start_utc)
		create dttm index dttm2 on v289_m07_events_overlap(event_end_utc)
		create dttm index dttm3	on v289_m07_events_overlap(event_start_b)
		create dttm index dttm4 on v289_m07_events_overlap(event_end_b)
		commit
		
		grant select on v289_m07_events_overlap to vespa_group_low_security
		commit
		
		MESSAGE cast(now() as timestamp)||' | @ M07.3: Flagging Overlapping Events DONE' TO CLIENT
		
		-- breaking overlapping events into chunks
		/*
			this bit stack all start and end dates on top of each other for each event coming from an account
			with the idea of setting the timeline for the chunks, overlapping timestamps are sorted ascendantly 
			and identifying where the event starts (theflag = 1) then we can easly create the chunks by lead/lagging
			the dates...
		*/
		
		if object_id('v289_m07_overlaps_chunks') is not null
			drop table v289_m07_overlaps_chunks
			
		commit
			
		select  *
				,min(chunk_start) over	(
											partition by    account_number
															,event_id
											order by        chunk_start
											rows between    1 following and 1 following
										)   as chunk_end
		into    v289_m07_overlaps_chunks
		from    (
					select  distinct *
					from    (
								select  account_number
										,event_id
										,event_start_utc	as chunk_start
										,event_index
										,1 as theflag
								from    v289_m07_events_overlap
								union   all
								select  account_number
										,event_id
										,event_end_utc		as chunk_start
										,event_index
										,0 as theflag
								from    v289_m07_events_overlap
								union   all
								select  account_number
										,event_id
										,event_start_b		as chunk_start
										,event_index
										,0 as theflag
								from    v289_m07_events_overlap
								where   event_start_b > event_start_utc
								union   all
								select  account_number
										,event_id
										,event_end_b		as chunk_start
										,event_index
										,0 as theflag
								from    v289_m07_events_overlap
								WHERE event_end_b <= event_end_utc
							)   as base
				)   as base2
		commit
		
		create hg index hg1		on v289_m07_overlaps_chunks(account_number)
		create hg index hg2 	on v289_m07_overlaps_chunks(event_id)
		create dttm index dttm1	on v289_m07_overlaps_chunks(chunk_start)
		create dttm index dttm2	on v289_m07_overlaps_chunks(chunk_end)
		commit
		
		grant select on v289_m07_overlaps_chunks to vespa_group_low_security
		commit
		
		drop table v289_m07_events_overlap
		commit
		
		MESSAGE cast(now() as timestamp)||' | @ M07.3: Breaking Overlapping Events into Chunks DONE' TO CLIENT
		
		-- Identifying batches of overlaps for each account
		/*
			chunks of events starting/ending at the same time overlap, hence they all get wrapped up into a single
			batch... batches can also be made of 1 single chunk and that means is the head,body or tail of an event 
			that is not overlapping with others
		*/
		
		if object_id('v289_m07_overlap_batches') is not null
			drop table v289_m07_overlap_batches
			
		commit
		
		select  side_a.*
				,dense_rank() over  (
										partition by    side_a.account_number
										order by        side_a.chunk_start
									)   as thebatch
		into	v289_m07_overlap_batches
		from    v289_m07_overlaps_chunks    as side_a
				inner join  (
								select  distinct
										account_number
										,event_id
										,chunk_start
								from    v289_m07_overlaps_chunks
								where   theflag = 1
							)   as side_b
				on  side_a.account_number    = side_b.account_number
				and side_a.event_id          = side_b.event_id
		where   side_a.chunk_end is not null
		and     side_b.chunk_start <= side_a.chunk_start
		and     side_a.chunk_start <> side_a.chunk_end
		
		commit
		
		create hg index hg1	on v289_m07_overlap_batches(account_number)
		create hg index hg2 on v289_m07_overlap_batches(event_id)
		create dttm index dttm1	on v289_m07_overlap_batches(chunk_start)
		create dttm index dttm2	on v289_m07_overlap_batches(chunk_end)
		create lf index lf1		on v289_m07_overlap_batches(thebatch)
		commit
		
		grant select on v289_m07_overlap_batches to vespa_group_low_security
		commit
		
		drop table v289_m07_overlaps_chunks
		commit
		
		MESSAGE cast(now() as timestamp)||' | @ M07.3: Assembling batches of overlaps DONE' TO CLIENT

		
----------------------------
-- M07.4 - Returning Results
----------------------------

		MESSAGE cast(now() as timestamp)||' | Begining  M07.4 - Returning Results' TO CLIENT

		insert  into V289_M07_dp_data
		select  dpdata.account_number
				,dpdata.subscriber_id
				,dpdata.event_id
				,dpdata.event_start_utc
				,dpdata.event_end_utc
				,overlap.chunk_start
				,overlap.Chunk_end
				,dpdata.event_duration_seg
				,case   when overlap.chunk_start is not null then datediff(second,overlap.chunk_start,overlap.chunk_end)    
						else null
				end     as chunk_duration_seg
				,dpdata.programme_genre
				,dpdata.session_daypart
				,dpdata.hhsize
				,dpdata.channel_pack
				,dpdata.segment_id
				,overlap.thebatch
				,0                      as session_size
				,dpdata.event_start_dim
				,dpdata.event_end_dim
		from    v289_m07_dp_data_tempshelf          as dpdata
				left join v289_m07_overlap_batches  as overlap
				on  dpdata.account_number   = overlap.account_number
				and dpdata.event_id         = overlap.event_id
				
		commit
		
		drop table v289_m07_dp_data_tempshelf
		commit
		
		MESSAGE cast(now() as timestamp)||' | @ M07.4: Output table V289_M07_DP_DATA DONE' TO CLIENT
		
	end
	else
	begin
	
		MESSAGE cast(now() as timestamp)||' | @ M07.0: Missing DP Viewing Data to prepare( v289_M06_dp_raw_data empty)!!!' TO CLIENT
	
	end

	MESSAGE cast(now() as timestamp)||' | M07 Finished' TO CLIENT	

end;

commit;
grant execute on v289_m07_dp_data_preparation to vespa_group_low_security;
commit; /*


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

        This Module goal is to extract and prepare the household and individual data from Experian 

**Module:
        
        M08: Barb Matrices Generation
                        M08.0 - Initialising Environment
                        M08.1 - Account extraction from SAV
                        M08.2 - Experian HH Info Extraction (1st round - Only hh_key and address line matching accounts)
                        M08.3 - Experian HH Info Extraction (2nd round - Non-matching address line accounts AND hh > 10 people) 
                        M08.4 - Experian HH Info Extraction (3nd round - Non-matching address line accounts)
                        M08.5 - Individual TABLE POPULATION
                        M08.6 - Add Head of Household
                        M08.7 - Add Individual Children
                        M08.8 - Final Tidying of Data
                
--------------------------------------------------------------------------------------------------------------
*/

-----------------------------------
-- M08.0 - Initialising Environment
-----------------------------------

create or replace procedure v289_m08_Experian_data_preparation
AS BEGIN

        MESSAGE cast(now() as timestamp)||' | Begining M08.0 - Initialising Environment' TO CLIENT

        -- variables
        declare @last_update_dt date    commit

        select @last_update_dt =  max(updated_on) from V289_M08_SKY_HH_view     commit -- '2000-01-01' commit

        declare @female_probability decimal(8,6) -- approx this percentage of head of hhds selected will be female
        commit

        set @female_probability = 0.800000
        commit

        /*
                this module is not needed to be refreshed every single day so we came up with
                the resolution of only executing it if the last update date is at least 1 week 
                ago from today...
        */
        if (datediff(day,@last_update_dt,today()) > 6 or @last_update_dt is null)
        begin
                        
                MESSAGE cast(now() as timestamp)||' | @ M08.0: Initialising Environment DONE' TO CLIENT
                        
                --------------------------------------
                -- M08.1 - Account extraction from SAV
                --------------------------------------
                
                MESSAGE cast(now() as timestamp)||' | Begining M08.1 - Account extraction from SAV' TO CLIENT
                
                
                IF object_id('V289_M08_SKY_HH_view') IS NOT NULL  TRUNCATE TABLE V289_M08_SKY_HH_view   commit
                
                
                INSERT INTO V289_M08_SKY_HH_view (account_number, cb_key_household, cb_address_line_1)
                SELECT DISTINCT
                          sav.account_number
                        , sav.cb_key_household
                        , sav.cb_address_line_1
                INTO    V289_M08_SKY_HH_view
                FROM  CUST_SINGLE_ACCOUNT_VIEW as sav
                WHERE sav.cb_key_household > 0
                AND   sav.cb_key_household IS NOT NULL
                AND   sav.account_number IS NOT NULL

                COMMIT

                
                MESSAGE cast(now() as timestamp)||' | @ M08.1 TABLE V289_M08_SKY_HH_view Populated' TO CLIENT
                        
                --------------------------------------------------------------------------------------------------
                -- M08.2 - Experian HH Info Extraction (1st round - Only hh_key and address line matching accounts)
                --------------------------------------------------------------------------------------------------

                MESSAGE cast(now() as timestamp)||' | Begining M08.2 - Experian HH Info Extraction' TO CLIENT
                
                SELECT account_number
                        , vh.cb_key_household
                        , vh.cb_address_line_1
                        , COUNT(DISTINCT ex.cb_key_db_person) + MAX(CAST(h_number_of_children_in_household_2011 as INT))  AS     HH_composition
                        , Children_count        = MAX(CAST(h_number_of_children_in_household_2011 as INT))
                INTO #t1
                FROM V289_M08_SKY_HH_view AS vh
                JOIN EXPERIAN_CONSUMERVIEW as ex ON ex.cb_key_household = vh.cb_key_household AND ex.cb_address_line_1 = vh.cb_address_line_1
                GROUP BY account_number
                        , vh.cb_key_household
                        , vh.cb_address_line_1
                COMMIT

                CREATE HG INDEX idhh ON #t1(cb_key_household)           commit
                CREATE HG INDEX idac ON #t1(account_number)                     commit
                CREATE HG INDEX idal ON #t1(cb_address_line_1)          commit

                COMMIT
                ---------------------   Table Update
                UPDATE V289_M08_SKY_HH_view
                SET     a.Children_count        = b.Children_count
                        ,   a.HH_composition        = b.HH_composition
                        ,   a.non_matching_flag     = 1
                FROM V289_M08_SKY_HH_view as a
                JOIN #t1 as b ON a.account_number = b.account_number  AND a.cb_key_household = b.cb_key_household AND a.cb_address_line_1 = b.cb_address_line_1

                COMMIT
                

                -- Clean up
                drop table #t1  commit

           MESSAGE cast(now() as timestamp)||' | @ M08.2 1st round finished ' TO CLIENT
           
                ----------------------------------------------------------------------------------------------------------
                -- M08.3 - Experian HH Info Extraction (2nd round - Non-matching address line accounts AND hh > 10 people) 
                ----------------------------------------------------------------------------------------------------------

                MESSAGE cast(now() as timestamp)||' | Begining M08.3 - Experian HH Info Extraction (2nd round)' TO CLIENT
                
                SELECT vh.account_number
                        , vh.cb_key_household
                        , vh.cb_address_line_1
                        , ex.cb_address_line_1 AS linex
                        , COUNT(DISTINCT ex.cb_key_db_person) + MAX(CAST(h_number_of_children_in_household_2011 as INT))  AS  HH_composition
                        , Children_count        = MAX(CAST(h_number_of_children_in_household_2011 as INT))
                        , RANK () OVER (PARTITION BY vh.cb_key_household ORDER by HH_composition DESC) as rank_1
                INTO #t2
                FROM V289_M08_SKY_HH_view as vh
                JOIN EXPERIAN_CONSUMERVIEW as ex ON ex.cb_key_household = vh.cb_key_household
                WHERE (vh.non_matching_flag = 0)
                GROUP BY vh.account_number
                        , vh.cb_key_household
                        , vh.cb_address_line_1
                        , linex
                HAVING HH_composition <= 10

                COMMIT

                CREATE HG INDEX idhh ON #t2(cb_key_household)           commit
                CREATE HG INDEX idac ON #t2(account_number)                     commit
                CREATE HG INDEX idal ON #t2(cb_address_line_1)          commit
                COMMIT
                ---------------------   Table Update
                UPDATE V289_M08_SKY_HH_view
                SET     a.Children_count        = b.Children_count
                        ,   a.HH_composition        = b.HH_composition
                        ,   a.cb_address_line_1     = b.linex
                        ,   a.non_matching_flag     = 1
                        ,   a.edited_add_flag       = 1
                FROM V289_M08_SKY_HH_view as a
                JOIN #t2 as b ON a.account_number = b.account_number  AND a.cb_key_household = b.cb_key_household AND a.cb_address_line_1 = b.cb_address_line_1 and rank_1 = 1

                COMMIT
                
                -- Clean up
                drop table #t2  commit

           MESSAGE cast(now() as timestamp)||' | @ M08.3 2nd round finished ' TO CLIENT 

                -----------------------------------------------------------------------------------------
                -- M08.4 - Experian HH Info Extraction (3nd round - Non-matching address line accounts A)
                -----------------------------------------------------------------------------------------

                MESSAGE cast(now() as timestamp)||' | Begining M08.4 - Experian HH Info Extraction (3nd round)' TO CLIENT
                
                SELECT vh.account_number
                        , vh.cb_key_household
                        , vh.cb_address_line_1
                        , ex.cb_address_line_1 AS linex
                        , COUNT(DISTINCT ex.cb_key_db_person) + MAX(CAST(h_number_of_children_in_household_2011 as INT))  AS  HH_composition
                        , Children_count        = MAX(CAST(h_number_of_children_in_household_2011 as INT))
                        , RANK () OVER (PARTITION BY vh.cb_key_household ORDER by HH_composition ASC) as rank_1
                INTO #t3
                FROM V289_M08_SKY_HH_view as vh
                JOIN EXPERIAN_CONSUMERVIEW as ex ON ex.cb_key_household = vh.cb_key_household
                WHERE (vh.non_matching_flag = 0)
                GROUP BY vh.account_number
                        , vh.cb_key_household
                        , vh.cb_address_line_1
                        , linex


                COMMIT

                CREATE HG INDEX idhh ON #t3(cb_key_household)           commit
                CREATE HG INDEX idac ON #t3(account_number)                     commit
                CREATE HG INDEX idal ON #t3(cb_address_line_1)          commit
                COMMIT
                ---------------------   Table Update
                UPDATE V289_M08_SKY_HH_view
                SET     a.Children_count        = b.Children_count
                        ,   a.HH_composition        = b.HH_composition
                        ,   a.cb_address_line_1     = b.linex
                        ,   a.non_matching_flag     = 1
                        ,   a.edited_add_flag       = 1
                FROM V289_M08_SKY_HH_view as a
                JOIN #t3 as b ON a.account_number = b.account_number  AND a.cb_key_household = b.cb_key_household AND a.cb_address_line_1 = b.cb_address_line_1 and rank_1 = 1

                COMMIT
                
                -- Clean up
                drop table #t3  commit

                MESSAGE cast(now() as timestamp)||' | @ M08.4 3rd round finished ' TO CLIENT
                
                --------------------------------------
                -- M08.5 - Individual TABLE POPULATION
                --------------------------------------
                
                MESSAGE cast(now() as timestamp)||' | Begining M08.5 - Individual TABLE POPULATION' TO CLIENT
                
                IF object_id('V289_M08_SKY_HH_composition') IS NOT NULL  TRUNCATE TABLE V289_M08_SKY_HH_composition
                
                INSERT INTO V289_M08_SKY_HH_composition (account_number, cb_key_household, exp_cb_key_db_person, cb_address_line_1
                                                                                                , cb_key_db_person, person_age, person_ageband, HH_person_number, person_gender, person_income, demographic_ID)
                SELECT
                          vh.account_number
                        , vh.cb_key_household
                        , ex.exp_cb_key_db_person
                        , vh.cb_address_line_1
                        , ex.cb_key_db_person
                        , person_age                = ex.p_actual_age
                        , person_ageband            = CASE WHEN person_age <= 19 then '0-19'
                                                                                           WHEN person_age BETWEEN 20 AND 24 then '20-24'
                                                                                           WHEN person_age BETWEEN 25 AND 34 then '25-34'
                                                                                           WHEN person_age BETWEEN 35 AND 44 then '35-44'
                                                                                           WHEN person_age BETWEEN 45 AND 64 then '45-64'
                                                                                           WHEN person_age >= 65 then '65+'
                                                                                  END
                        , HH_person_number          = RANK () OVER(PARTITION BY  vh.account_number ORDER BY person_age, p_gender, ex.cb_key_db_person)
                        , person_gender             = CASE  WHEN ex.p_gender = '0' THEN 'M'
                                                                                                WHEN ex.p_gender = '1' THEN 'F'
                                                                                                ELSE 'U' END
                        , person_income             = ex.p_personal_income_value
                        , demographic_ID    = CASE  WHEN p_gender = '0' AND p_actual_age <= 19                      THEN 7
                                                                                WHEN p_gender = '0' AND p_actual_age BETWEEN 20 AND 24          THEN 6
                                                                                WHEN p_gender = '0' AND p_actual_age BETWEEN 25 AND 34          THEN 5
                                                                                WHEN p_gender = '0' AND p_actual_age BETWEEN 35 AND 44          THEN 4
                                                                                WHEN p_gender = '0' AND p_actual_age BETWEEN 45 AND 64          THEN 3
                                                                                WHEN p_gender = '0' AND p_actual_age >= 65                      THEN 2
                                                                                ---------- FEMALES
                                                                                WHEN p_gender = '1' AND p_actual_age <= 19                      THEN 14
                                                                                WHEN p_gender = '1' AND p_actual_age BETWEEN 20 AND 24          THEN 13
                                                                                WHEN p_gender = '1' AND p_actual_age BETWEEN 25 AND 34          THEN 12
                                                                                WHEN p_gender = '1' AND p_actual_age BETWEEN 35 AND 44          THEN 11
                                                                                WHEN p_gender = '1' AND p_actual_age BETWEEN 45 AND 64          THEN 10
                                                                                WHEN p_gender = '1' AND p_actual_age >= 65                      THEN 9
                                                                                ---------- UNDEFINED GENDER
                                                                                WHEN p_gender = 'U' AND p_actual_age <= 19                      THEN 15
                                                                                WHEN p_gender = 'U' AND p_actual_age BETWEEN 20 AND 24          THEN 16
                                                                                WHEN p_gender = 'U' AND p_actual_age BETWEEN 25 AND 34          THEN 17
                                                                                WHEN p_gender = 'U' AND p_actual_age BETWEEN 35 AND 44          THEN 18
                                                                                WHEN p_gender = 'U' AND p_actual_age BETWEEN 45 AND 64          THEN 19
                                                                                WHEN p_gender = 'U' AND p_actual_age >= 65                      THEN 20
                                                                                ---------- UNDEFINED AGE
                                                                                WHEN p_gender = '1' AND p_actual_age IS NULL                    THEN 21
                                                                                WHEN p_gender = '0' AND p_actual_age IS NULL                    THEN 22
                                                                                ---------- UNDIFINED ALL
                                                                                WHEN p_gender = 'U' AND p_actual_age IS NULL                    THEN 23
                                                                                ELSE 0 END
                FROM V289_M08_SKY_HH_view AS vh
                JOIN EXPERIAN_CONSUMERVIEW as ex ON ex.cb_key_household = vh.cb_key_household AND ex.cb_address_line_1 = vh.cb_address_line_1

                COMMIT

                
                MESSAGE cast(now() as timestamp)||' | @ M08.5 Individual table populated' TO CLIENT

                --------------------------------
                -- M08.6 - Add Head of Household
                --------------------------------

                MESSAGE cast(now() as timestamp)||' | Begining M08.6 - Add Head of Household' TO CLIENT
                
                --------        Get Experian Head of Household
                UPDATE  V289_M08_SKY_HH_composition s
                SET     exp_person_head = p_head_of_household
                FROM    sk_prod.PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD e
                WHERE   s.exp_cb_key_db_person = e.exp_cb_key_db_person
                COMMIT


                 ---------------------------------------------------------------------------------
                --- Based upon Experian Head of hhd select a single head of hhd for each hhd
                --- A hhd is defined by cb_key_household and cb_address_line_1
                --- Experian generally assigns BOTH a male and a female as head of hhd
                --- We need to select ONE. This is done based upon highest personal income by gender
                --- Then selecting a random male or female based upon a probability assigned to female
                ---------------------------------------------------------------------------------


                -- Identify highest personal income from indiviuals in a hhd who are head of hhd - by gender
                -- Also assign a probabity for selecting either experian head of hhd based upon gender
                select account_number, exp_cb_key_db_person, cb_key_household, cb_address_line_1
                                ,rank() OVER (PARTITION by account_number, cb_key_household, cb_address_line_1, person_gender ORDER BY person_income DESC, exp_cb_key_db_person DESC ) rank_1
                                ,case when person_gender = 'F' then @female_probability else 1 - @female_probability end as probability
                into #a1_1
                from V289_M08_SKY_HH_composition
                where exp_person_head = 1
                commit

                create hg index ind0 on #a1_1(account_number)
                create hg index ind1 on #a1_1(exp_cb_key_db_person)
                create hg index ind2 on #a1_1(cb_key_household)
                create hg index ind3 on #a1_1(cb_address_line_1)

                commit

                -- Calculate total probability by hhd.
                -- In some cases may only have 1 gender assigned as experian head of hhd. So need to deal with this
                select account_number, cb_key_household, cb_address_line_1, sum(probability) as tot_probability
                into #a1_2
                from #a1_1
                where rank_1 = 1
                group by account_number, cb_key_household, cb_address_line_1
                commit

                create hg index ind0 on #a1_2(account_number)
                create hg index ind1 on #a1_2(cb_key_household)
                create hg index ind2 on #a1_2(cb_address_line_1)
                commit

                -- For each experian head of household calculate bounds to apply probability to
                select #a1_1.exp_cb_key_db_person, #a1_1.account_number, #a1_1.cb_key_household, #a1_1.cb_address_line_1, rank_1
                                ,case
                                        when probability/tot_probability > 0.5 then 0.000000
                                        else cast(1-probability/tot_probability as decimal(8,6)) end as low_limit
                                ,case
                                        when probability/tot_probability > 0.5 then cast(probability/tot_probability as decimal(8,6))
                                        else 1.000000 end as high_limit
                into #a1_3
                from #a1_1 inner join #a1_2
                on #a1_1.cb_key_household = #a1_2.cb_key_household
                and #a1_1.cb_address_line_1 = #a1_2.cb_address_line_1
                and #a1_1.account_number = #a1_2.account_number
                where rank_1 = 1

                commit

                create hg index ind1 on #a1_3(exp_cb_key_db_person)
                create hg index ind2 on #a1_3(cb_key_household)
                create hg index ind3 on #a1_3(cb_address_line_1)
                create hg index ind4 on #a1_3(account_number)

                commit



                -- Generate a random number per hhd
                select distinct cb_key_household, cb_address_line_1, 0.000001 as random_number
                into #r1
                from V289_M08_SKY_HH_composition
                commit

                update #r1 set random_number = RAND(cb_key_household + DATEPART(us, GETDATE()))
                commit

                create hg index ind1 on #r1(cb_key_household)
                create hg index ind2 on #r1(cb_address_line_1)
                commit


                -- Assign a single individual in each hhd as head of hhd based upon above
                update V289_M08_SKY_HH_composition e
                                set person_head =  '1'
                                from #a1_3 a, #r1 r
                                where e.exp_cb_key_db_person = a.exp_cb_key_db_person
                                and a.rank_1 = 1
                                and e.cb_key_household = r.cb_key_household
                                and e.cb_address_line_1 = r.cb_address_line_1
                                and random_number >= low_limit and random_number < high_limit
                commit


                drop table #a1_1
                drop table #a1_2
                drop table #a1_3
                commit

                MESSAGE cast(now() as timestamp)||' | @ M08.6 Head of household added where Experian head exists' TO CLIENT


                --- Not all hhds have a defined head of hhd from Experian. So will assign highest personal income in these cases
                -- First count number of heads of hhd as per our definition for each hhd
                select account_number, cb_key_household, cb_address_line_1
                                , sum(case when person_head = '1' then 1 else 0 end) as head_count
                into #b1
                from V289_M08_SKY_HH_composition
                group by account_number, cb_key_household, cb_address_line_1
                commit

                create hg index ind1 on #b1(cb_key_household)
                create hg index ind2 on #b1(cb_address_line_1)
                create lf index ind3 on #b1(head_count)
                create hg index ind4 on #b1(account_number)

                commit


                -- Those hhds where above is zero need to be allocated individual with highest income by gender
                select p.exp_cb_key_db_person, p.account_number, p.cb_key_household, p.cb_address_line_1
                                ,rank() OVER (PARTITION by p.account_number, p.cb_key_household, p.cb_address_line_1, person_gender ORDER BY p.person_income DESC, p.exp_cb_key_db_person DESC ) rank_1
                                ,case when person_gender = 'F' then @female_probability else 1 - @female_probability end as probability
                into #b1_1
                from
                                V289_M08_SKY_HH_composition p
                         inner join
                                #b1 b
                         on p.cb_key_household = b.cb_key_household and p.cb_address_line_1 = b.cb_address_line_1
                where b.head_count = 0
                commit

                create hg index ind1 on #b1_1(exp_cb_key_db_person)
                commit


                -- Calulate total probabilty by hhd
                select account_number, cb_key_household, cb_address_line_1, sum(probability) as tot_probability
                into #b1_2
                from #b1_1
                where rank_1 = 1
                group by account_number, cb_key_household, cb_address_line_1
                commit

                create hg index ind1 on #b1_2(cb_key_household)
                create hg index ind2 on #b1_2(cb_address_line_1)
                create hg index ind3 on #b1_2(account_number)

                commit

                -- Calculate lower and upper bounds for each potential head of hhd individual
                select #b1_1.exp_cb_key_db_person, #b1_1.account_number, #b1_1.cb_key_household, #b1_1.cb_address_line_1, rank_1
                                ,case
                                        when probability/tot_probability > 0.5 then 0.000000
                                        else cast(1-probability/tot_probability as decimal(8,6)) end as low_limit
                                ,case
                                        when probability/tot_probability > 0.5 then cast(probability/tot_probability as decimal(8,6))
                                        else 1.000000 end as high_limit
                into #b1_3
                from #b1_1 inner join #b1_2
                on #b1_1.cb_key_household = #b1_2.cb_key_household
                and #b1_1.cb_address_line_1 = #b1_2.cb_address_line_1
                and #b1_1.account_number = #b1_2.account_number
                where rank_1 = 1

                commit

                create hg index ind1 on #b1_3(exp_cb_key_db_person)
                create hg index ind2 on #b1_3(cb_key_household)
                create hg index ind3 on #b1_3(cb_address_line_1)
                create hg index ind4 on #b1_3(account_number)

                commit


                -- Assign individual as head of hhd
                update V289_M08_SKY_HH_composition e
                                set person_head =  '1'
                                from #b1_3 b, #r1 r
                                where e.exp_cb_key_db_person = b.exp_cb_key_db_person
                                and b.rank_1 = 1
                                and e.cb_key_household = r.cb_key_household
                                and e.cb_address_line_1 = r.cb_address_line_1
                                and random_number >= low_limit and random_number < high_limit
                commit

                drop table #r1

                drop table #b1
                drop table #b1_1
                drop table #b1_2
                drop table #b1_3
                commit

                MESSAGE cast(now() as timestamp)||' | @ M08.6 Head of household added' TO CLIENT


                ----------------------------------
                -- M08.7 - Add Individual Children
                ----------------------------------

                MESSAGE cast(now() as timestamp)||' | Begining M08.7 - Add Individual Children' TO CLIENT

                -- Experian tables do not have individual data for children less than 17
                ---- Need to append rows for these
                --- They cannot be head of hhd either so can be run after that code

                -- Will need to add a row for each child, these multiple rows in this table will enable
                -- the right number of individuals to be added to the data
                select 1 as number_of_kids, 1 as unique_row into #PIV_append_kids_rows
                commit

                create lf index ind1 on #PIV_append_kids_rows(number_of_kids)
                commit

                insert into #PIV_append_kids_rows values (2, 2)
                insert into #PIV_append_kids_rows values (2, 3)
                insert into #PIV_append_kids_rows values (3, 4)
                insert into #PIV_append_kids_rows values (3, 5)
                insert into #PIV_append_kids_rows values (3, 6)
                insert into #PIV_append_kids_rows values (4, 7)
                insert into #PIV_append_kids_rows values (4, 8)
                insert into #PIV_append_kids_rows values (4, 9)
                insert into #PIV_append_kids_rows values (4, 10)
                insert into #PIV_append_kids_rows values (5, 11)
                insert into #PIV_append_kids_rows values (5, 12)
                insert into #PIV_append_kids_rows values (5, 13)
                insert into #PIV_append_kids_rows values (5, 14)
                insert into #PIV_append_kids_rows values (5, 15)
                insert into #PIV_append_kids_rows values (6, 16)
                insert into #PIV_append_kids_rows values (6, 17)
                insert into #PIV_append_kids_rows values (6, 18)
                insert into #PIV_append_kids_rows values (6, 19)
                insert into #PIV_append_kids_rows values (6, 20)
                insert into #PIV_append_kids_rows values (6, 21)
                insert into #PIV_append_kids_rows values (7, 22)
                insert into #PIV_append_kids_rows values (7, 23)
                insert into #PIV_append_kids_rows values (7, 24)
                insert into #PIV_append_kids_rows values (7, 25)
                insert into #PIV_append_kids_rows values (7, 26)
                insert into #PIV_append_kids_rows values (7, 27)
                insert into #PIV_append_kids_rows values (7, 28)
                insert into #PIV_append_kids_rows values (8, 29)
                insert into #PIV_append_kids_rows values (8, 30)
                insert into #PIV_append_kids_rows values (8, 31)
                insert into #PIV_append_kids_rows values (8, 32)
                insert into #PIV_append_kids_rows values (8, 33)
                insert into #PIV_append_kids_rows values (8, 34)
                insert into #PIV_append_kids_rows values (8, 35)
                insert into #PIV_append_kids_rows values (8, 36)
                insert into #PIV_append_kids_rows values (9, 37)
                insert into #PIV_append_kids_rows values (9, 38)
                insert into #PIV_append_kids_rows values (9, 39)
                insert into #PIV_append_kids_rows values (9, 40)
                insert into #PIV_append_kids_rows values (9, 41)
                insert into #PIV_append_kids_rows values (9, 42)
                insert into #PIV_append_kids_rows values (9, 43)
                insert into #PIV_append_kids_rows values (9, 44)
                insert into #PIV_append_kids_rows values (9, 45)
                insert into #PIV_append_kids_rows values (10, 46)
                insert into #PIV_append_kids_rows values (10, 47)
                insert into #PIV_append_kids_rows values (10, 48)
                insert into #PIV_append_kids_rows values (10, 49)
                insert into #PIV_append_kids_rows values (10, 50)
                insert into #PIV_append_kids_rows values (10, 51)
                insert into #PIV_append_kids_rows values (10, 52)
                insert into #PIV_append_kids_rows values (10, 53)
                insert into #PIV_append_kids_rows values (10, 54)
                insert into #PIV_append_kids_rows values (10, 55)
                insert into #PIV_append_kids_rows values (11, 56)
                insert into #PIV_append_kids_rows values (11, 57)
                insert into #PIV_append_kids_rows values (11, 58)
                insert into #PIV_append_kids_rows values (11, 59)
                insert into #PIV_append_kids_rows values (11, 60)
                insert into #PIV_append_kids_rows values (11, 61)
                insert into #PIV_append_kids_rows values (11, 62)
                insert into #PIV_append_kids_rows values (11, 63)
                insert into #PIV_append_kids_rows values (11, 64)
                insert into #PIV_append_kids_rows values (11, 65)
                insert into #PIV_append_kids_rows values (11, 66)
                insert into #PIV_append_kids_rows values (12, 67)
                insert into #PIV_append_kids_rows values (12, 68)
                insert into #PIV_append_kids_rows values (12, 69)
                insert into #PIV_append_kids_rows values (12, 70)
                insert into #PIV_append_kids_rows values (12, 71)
                insert into #PIV_append_kids_rows values (12, 72)
                insert into #PIV_append_kids_rows values (12, 73)
                insert into #PIV_append_kids_rows values (12, 74)
                insert into #PIV_append_kids_rows values (12, 75)
                insert into #PIV_append_kids_rows values (12, 76)
                insert into #PIV_append_kids_rows values (12, 77)
                insert into #PIV_append_kids_rows values (12, 78)
                insert into #PIV_append_kids_rows values (13, 79)
                insert into #PIV_append_kids_rows values (13, 80)
                insert into #PIV_append_kids_rows values (13, 81)
                insert into #PIV_append_kids_rows values (13, 82)
                insert into #PIV_append_kids_rows values (13, 83)
                insert into #PIV_append_kids_rows values (13, 84)
                insert into #PIV_append_kids_rows values (13, 85)
                insert into #PIV_append_kids_rows values (13, 86)
                insert into #PIV_append_kids_rows values (13, 87)
                insert into #PIV_append_kids_rows values (13, 88)
                insert into #PIV_append_kids_rows values (13, 89)
                insert into #PIV_append_kids_rows values (13, 90)
                insert into #PIV_append_kids_rows values (13, 91)
                insert into #PIV_append_kids_rows values (14, 92)
                insert into #PIV_append_kids_rows values (14, 93)
                insert into #PIV_append_kids_rows values (14, 94)
                insert into #PIV_append_kids_rows values (14, 95)
                insert into #PIV_append_kids_rows values (14, 96)
                insert into #PIV_append_kids_rows values (14, 97)
                insert into #PIV_append_kids_rows values (14, 98)
                insert into #PIV_append_kids_rows values (14, 99)
                insert into #PIV_append_kids_rows values (14, 100)
                insert into #PIV_append_kids_rows values (14, 101)
                insert into #PIV_append_kids_rows values (14, 102)
                insert into #PIV_append_kids_rows values (14, 103)
                insert into #PIV_append_kids_rows values (14, 104)
                insert into #PIV_append_kids_rows values (14, 105)
                insert into #PIV_append_kids_rows values (15, 106)
                insert into #PIV_append_kids_rows values (15, 107)
                insert into #PIV_append_kids_rows values (15, 108)
                insert into #PIV_append_kids_rows values (15, 109)
                insert into #PIV_append_kids_rows values (15, 110)
                insert into #PIV_append_kids_rows values (15, 111)
                insert into #PIV_append_kids_rows values (15, 112)
                insert into #PIV_append_kids_rows values (15, 113)
                insert into #PIV_append_kids_rows values (15, 114)
                insert into #PIV_append_kids_rows values (15, 115)
                insert into #PIV_append_kids_rows values (15, 116)
                insert into #PIV_append_kids_rows values (15, 117)
                insert into #PIV_append_kids_rows values (15, 118)
                insert into #PIV_append_kids_rows values (15, 119)
                insert into #PIV_append_kids_rows values (15, 120)
                insert into #PIV_append_kids_rows values (16, 121)
                insert into #PIV_append_kids_rows values (16, 122)
                insert into #PIV_append_kids_rows values (16, 123)
                insert into #PIV_append_kids_rows values (16, 124)
                insert into #PIV_append_kids_rows values (16, 125)
                insert into #PIV_append_kids_rows values (16, 126)
                insert into #PIV_append_kids_rows values (16, 127)
                insert into #PIV_append_kids_rows values (16, 128)
                insert into #PIV_append_kids_rows values (16, 129)
                insert into #PIV_append_kids_rows values (16, 130)
                insert into #PIV_append_kids_rows values (16, 131)
                insert into #PIV_append_kids_rows values (16, 132)
                insert into #PIV_append_kids_rows values (16, 133)
                insert into #PIV_append_kids_rows values (16, 134)
                insert into #PIV_append_kids_rows values (16, 135)
                insert into #PIV_append_kids_rows values (16, 136)
                insert into #PIV_append_kids_rows values (17, 137)
                insert into #PIV_append_kids_rows values (17, 138)
                insert into #PIV_append_kids_rows values (17, 139)
                insert into #PIV_append_kids_rows values (17, 140)
                insert into #PIV_append_kids_rows values (17, 141)
                insert into #PIV_append_kids_rows values (17, 142)
                insert into #PIV_append_kids_rows values (17, 143)
                insert into #PIV_append_kids_rows values (17, 144)
                insert into #PIV_append_kids_rows values (17, 145)
                insert into #PIV_append_kids_rows values (17, 146)
                insert into #PIV_append_kids_rows values (17, 147)
                insert into #PIV_append_kids_rows values (17, 148)
                insert into #PIV_append_kids_rows values (17, 149)
                insert into #PIV_append_kids_rows values (17, 150)
                insert into #PIV_append_kids_rows values (17, 151)
                insert into #PIV_append_kids_rows values (17, 152)
                insert into #PIV_append_kids_rows values (17, 153)
                insert into #PIV_append_kids_rows values (18, 154)
                insert into #PIV_append_kids_rows values (18, 155)
                insert into #PIV_append_kids_rows values (18, 156)
                insert into #PIV_append_kids_rows values (18, 157)
                insert into #PIV_append_kids_rows values (18, 158)
                insert into #PIV_append_kids_rows values (18, 159)
                insert into #PIV_append_kids_rows values (18, 160)
                insert into #PIV_append_kids_rows values (18, 161)
                insert into #PIV_append_kids_rows values (18, 162)
                insert into #PIV_append_kids_rows values (18, 163)
                insert into #PIV_append_kids_rows values (18, 164)
                insert into #PIV_append_kids_rows values (18, 165)
                insert into #PIV_append_kids_rows values (18, 166)
                insert into #PIV_append_kids_rows values (18, 167)
                insert into #PIV_append_kids_rows values (18, 168)
                insert into #PIV_append_kids_rows values (18, 169)
                insert into #PIV_append_kids_rows values (18, 170)
                insert into #PIV_append_kids_rows values (18, 171)
                insert into #PIV_append_kids_rows values (19, 172)
                insert into #PIV_append_kids_rows values (19, 173)
                insert into #PIV_append_kids_rows values (19, 174)
                insert into #PIV_append_kids_rows values (19, 175)
                insert into #PIV_append_kids_rows values (19, 176)
                insert into #PIV_append_kids_rows values (19, 177)
                insert into #PIV_append_kids_rows values (19, 178)
                insert into #PIV_append_kids_rows values (19, 179)
                insert into #PIV_append_kids_rows values (19, 180)
                insert into #PIV_append_kids_rows values (19, 181)
                insert into #PIV_append_kids_rows values (19, 182)
                insert into #PIV_append_kids_rows values (19, 183)
                insert into #PIV_append_kids_rows values (19, 184)
                insert into #PIV_append_kids_rows values (19, 185)
                insert into #PIV_append_kids_rows values (19, 186)
                insert into #PIV_append_kids_rows values (19, 187)
                insert into #PIV_append_kids_rows values (19, 188)
                insert into #PIV_append_kids_rows values (19, 189)
                insert into #PIV_append_kids_rows values (19, 190)
                insert into #PIV_append_kids_rows values (20, 191)
                insert into #PIV_append_kids_rows values (20, 192)
                insert into #PIV_append_kids_rows values (20, 193)
                insert into #PIV_append_kids_rows values (20, 194)
                insert into #PIV_append_kids_rows values (20, 195)
                insert into #PIV_append_kids_rows values (20, 196)
                insert into #PIV_append_kids_rows values (20, 197)
                insert into #PIV_append_kids_rows values (20, 198)
                insert into #PIV_append_kids_rows values (20, 199)
                insert into #PIV_append_kids_rows values (20, 200)
                insert into #PIV_append_kids_rows values (20, 201)
                insert into #PIV_append_kids_rows values (20, 202)
                insert into #PIV_append_kids_rows values (20, 203)
                insert into #PIV_append_kids_rows values (20, 204)
                insert into #PIV_append_kids_rows values (20, 205)
                insert into #PIV_append_kids_rows values (20, 206)
                insert into #PIV_append_kids_rows values (20, 207)
                insert into #PIV_append_kids_rows values (20, 208)
                insert into #PIV_append_kids_rows values (20, 209)
                insert into #PIV_append_kids_rows values (20, 210)
                commit


                INSERT INTO V289_M08_SKY_HH_composition (account_number, cb_key_household, cb_address_line_1
                                                                                                                                , person_gender, person_ageband, demographic_ID)
                select
                                hh.account_number
                                ,hh.cb_key_household
                                ,hh.cb_address_line_1
                                ,'U'
                                ,'0-19'
                           ,15 -- demographic_ID for gender ='U' and age <=19
                from
                                V289_M08_SKY_HH_view hh
                         inner join
                                #PIV_append_kids_rows k
                         on hh.children_count = k.number_of_kids
                         
                commit

                -- Clean up
                drop table #PIV_append_kids_rows        commit



                ---- There are a small number of 0-19 in the Experian data (these were 18-19 in Experian data)
                --- These will have a gender. But because they are a small number distort the scaling
                --- Change the gender of these to U

                update V289_M08_SKY_HH_composition
                set person_gender = 'U'
                where person_ageband = '0-19'
                commit

                MESSAGE cast(now() as timestamp)||' | @ M08.7 kids data added' TO CLIENT
                
                --------------------------------
                -- M08.8 - Final Tidying of Data
                --------------------------------

                MESSAGE cast(now() as timestamp)||' | Begining M08.8 - Final Tidying of Data' TO CLIENT
                
                -- Everyone with the same account_number gets a unique number
                select     row_id
                                   ,RANK () OVER (PARTITION BY  account_number ORDER BY person_head DESC, row_id) as rank1
                into       #a4
                from        V289_M08_SKY_HH_composition
                group by    account_number, person_head, row_id
                commit

                create hg index ind1 on #a4(row_id)
                commit

                update V289_M08_SKY_HH_composition h
                set HH_person_number = rank1
                from #a4 r
                where h.row_id = r.row_id
                commit
                
                -- Clean up
                drop table #a4


                -- Calculate household size and delete any > 15

                select account_number, count(1) as hhd_count
                into #a5
                from V289_M08_SKY_HH_composition
                group by account_number
                commit

                update V289_M08_SKY_HH_composition c
                set household_size = hhd_count
                from #a5 a
                where c.account_number = a.account_number
                commit
                
                delete from V289_M08_SKY_HH_composition
                where household_size > 15
                commit
                
                -- Clean up
                drop table #a5

                MESSAGE cast(now() as timestamp)||' | @ M08.8: Final Tidying of Data DONE' TO CLIENT
                
        end
        else
        begin
        
                MESSAGE cast(now() as timestamp)||' | @ M08.0: Data still valid, last update was less than a week ago' TO CLIENT
                MESSAGE cast(now() as timestamp)||' | @ M08.0: Initialising Environment DONE' TO CLIENT
        
        end
        
        
        MESSAGE cast(now() as timestamp)||' | M08.8 Process completed' TO CLIENT
        
END;                    --      END of Proc


COMMIT;
GRANT EXECUTE   ON v289_m08_Experian_data_preparation   TO vespa_group_low_security;
COMMIT;






 -- NOTE THESE QA NUMBERS NEED TO BE REFRESHED
---------------------------------  QA
 ---------------------------------  V289_M08_SKY_HH_view
 ---- account_number             9,929,864
 ---- cb_key_household           9,542,183
 ---- cb_address_line_1          6,036,344
 ---- matching_flag              8,734,222
 ---- edited_add_flag              365,499
 ---- HH Children_count          3,207,144
 ---- COUNT()                            9,929,864
---------------------------------
--------------------------------- V289_M08_SKY_HH_composition (individuals)
---- account_number      8,734,222
---- cb_key_household    8,615,848
---- cb_address_line_1   5,465,916
---- cb_key_db_person   18,375,549
---- individual                 17,898,812
---- COUNT(*)                   19,087,944
--------------------------------------
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
**Project Name:							Skyview H2I
**Analysts:                             Angel Donnarumma	(angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jason Thompson		(Jason.Thompson@skyiq.co.uk)
										,Hoi Yu Tang		(HoiYu.Tang@skyiq.co.uk)
										,Jose Pitteloud		(jose.pitteloud@skyiq.co.uk)
**Stakeholder:                          SkyIQ
										,Jose Loureda		(Jose.Loureda@skyiq.co.uk)
**Due Date:                             11/07/2014
**Project Code (Insight Collation):     v289
**Sharepoint Folder:    

	http://sp-department.bskyb.com/sites/SIGEvolved/Shared%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FSIGEvolved%2FShared%20Documents%2F01%20Analysis%20Requests%2FV289%20-%20Skyview%20Futures%2F01%20Plans%20Briefs%20and%20Project%20Admin                                                        
	                                                          
**Business Brief:

	This Module goal is to assign a session size to all the events using Monte Carlo simulation process. 

**Module:
	
	M09: Session Size Assignment process
			M09.0 - Initialising Environment
			M09.1 - Creating transient tables 
			M09.2 - Single box events update
			M09.3 - Multi Box events update
			M09.4 - Main event tables update
	
--------------------------------------------------------------------------------------------------------------
*/

-----------------------------------
-- M09.0 - Initialising Environment
-----------------------------------

create or replace procedure v289_m09_Session_size_definition
AS BEGIN

	MESSAGE cast(now() as timestamp)||' | Begining M09.0 - Initialising Environment' TO CLIENT
		


	-----------------	Session VARIABLEs Definition
        DECLARE @account     varchar(20)     ---Account Number
        DECLARE @subs        decimal(10)     ---Subscriber ID
        DECLARE @iter        tinyint         ---Max iteration x accounts
        DECLARE @cont        tinyint         ---counter for subs iteration
        DECLARE @event       bigint          ---for event iteration
        DECLARE @length      DECIMAL(7,6)    ---MC proportional length
        DECLARE @random      FLOAT           ---MC Random number
        DECLARE @s_size      tinyint         ---Event session size
        DECLARE @adj_hh      tinyint         ---Adjusted HH size (for MC Multibox process only)
        DECLARE @hh_size     tinyint         ---HH size
        DECLARE @segment     tinyint         ---Segment_ID
        DECLARE @batch       tinyint         ---Overlap Batch
        DECLARE @row_id          INT
        DECLARE @event_id    BIGINT
        commit


	---------------------------------------
	-- M09.1 - Creating transient tables
	---------------------------------------


	-----------------	temp_event Table Creation
	
	SELECT
              count (event_id)  AS overlap_size
            , account_number
            , Overlap_batch
    into	#tmp1
    FROM V289_M07_dp_data
    GROUP BY Overlap_batch, account_number
    commit
	
    create hg index tmp1_idx_1 on #tmp1(account_number)
    create lf index tmp1_idx_2 on #tmp1(overlap_batch)
	commit
	
	IF OBJECT_ID('temp_event') IS NOT NULL DROP TABLE temp_event
	SELECT            			   
			event_ID
			, dt.account_number
			, dt.subscriber_id
			, CAST(event_start_utc AS DATE) event_dt
			, CASE WHEN hhsize > 8 THEN 8 ELSE hhsize END as hhsize
			, COALESCE (dt.segment_ID, 157) AS segment_ID
			, random1       =   RAND(dt.event_id + DATEPART(us, GETDATE()))
			, overlap       =   ov.overlap_size 
			, COALESCE(dt.overlap_batch,0) 	AS overlap_batch 
			, box_rank      =   dense_rank() OVER (PARTITION BY dt.account_number, dt.Overlap_batch ORDER BY subscriber_id, event_end_utc  DESC)
			, CAST(0 AS tinyint) session_size
	INTO temp_event
	FROM 
					V289_M07_dp_data 	AS	dt
		LEFT JOIN	#tmp1				AS	ov 	ON	ov.account_number = dt.account_number 
												AND	ov.Overlap_batch = dt.overlap_batch
	WHERE hhsize > 0 and session_size = 0
	
	MESSAGE cast(now() as timestamp)||' | @ M09.1: temp_Event Table created: '||@@rowcount TO CLIENT
		
	COMMIT

	CREATE HG INDEX ide1 ON temp_event(event_ID)
	CREATE LF INDEX ide2 ON temp_event(overlap_batch)
	CREATE LF INDEX ide3 ON temp_event(segment_ID)
	CREATE LF INDEX ide4 ON temp_event(hhsize)
	COMMIT
	
	-- cleanup
	drop table #tmp1
	commit




	------------------------------
	-- M09.2 - Single box events update
	------------------------------

	UPDATE temp_event
	SET ev.session_size = COALESCE(sm.session_size, mx.session_size)
	FROM temp_event as ev
	LEFT JOIN v289_sessionsize_matrix 			AS sm ON  sm.segment_ID = ev.segment_id 
															AND ev.hhsize 	= 	sm.hhsize 
															AND random1 	>   sm.lower_limit 
															AND random1 	<=  sm.upper_limit
															AND ev.event_dt = 	sm.thedate
	JOIN v289_sessionsize_matrix_default   	AS mx ON  mx.segment_ID = ev.segment_id 
															AND ev.hhsize 	= 	mx.hhsize 
															AND random1 	> 	mx.lower_limit 
															AND random1 	<= 	mx.upper_limit
	WHERE Overlap_batch = 0 OR overlap = 1


	MESSAGE cast(now() as timestamp)||' | @ M09.2: Single Box events done: '||@@rowcount TO CLIENT

	
	
	--------------------------
	-- M09.3: Multi Box events
	--------------------------
	
	-----------------   MULTI box events update
	-----------------   Primary box processing
	MESSAGE cast(now() as timestamp)||' | @ M09.3: Multi Box events started '||@@rowcount TO CLIENT
	
	IF OBJECT_ID('events_1_box') IS NOT NULL DROP TABLE events_1_box
	SELECT
          *
        , row_id        = row_number() over(order by subscriber_id)
        , ev_proc_flag  =   CAST (0 AS BIT)
        , adj_hh        = hhsize - overlap + 1
        , length_1      = CAST (0 as DECIMAL (7,6))
	INTO events_1_box
	FROM temp_event
	WHERE       session_size = 0
			AND hhsize is not null
			AND box_rank = 1
	ORDER BY account_number, subscriber_id, overlap_batch
	
	MESSAGE cast(now() as timestamp)||' | @ M09.3: Multi Box primary box table populated: '||@@rowcount TO CLIENT
	
	COMMIT
	CREATE HG       INDEX idxe1     ON events_1_box (event_ID)
	CREATE LF       INDEX id1       ON events_1_box (overlap_batch)
	CREATE HG       INDEX id2       ON events_1_box (subscriber_id)
	CREATE LF       INDEX box       ON events_1_box (box_rank)
	CREATE HG       INDEX box1      ON events_1_box (length_1)
	CREATE LF       INDEX box2      ON events_1_box (adj_hh)

	COMMIT
	
	UPDATE events_1_box
    SET adj_hh = 1
    WHERE adj_hh < 1
	

	UPDATE events_1_box
	SET     length_1 = upper_limit
			, random1 = random1 * upper_limit
	FROM events_1_box AS ev
	JOIN v289_sessionsize_matrix_default as mx ON mx.segment_ID = ev.segment_id
																	AND ev.hhsize = mx.hhsize
																	AND ev.adj_hh = mx.session_size

	COMMIT
	

	/*
	UPDATE temp_event
	SET ev.session_size =  COALESCE(sm.session_size, mx.session_size)
	FROM temp_event as ev
	INNER JOIN events_1_box as ev1 ON   ev.event_ID = ev1.event_ID
									AND ev.overlap_batch = ev1.overlap_batch
	LEFT JOIN v289_sessionsize_matrix 			AS sm ON  sm.segment_ID = ev.segment_id 
															AND ev.hhsize 	= 	sm.hhsize 
															AND ev1.adj_hh 	>= 	sm.session_size
															AND ev1.random1 	>   sm.lower_limit 
															AND ev1.random1 	<=  sm.upper_limit
															AND ev.event_dt = 	sm.thedate
	INNER JOIN v289_sessionsize_matrix_default   AS mx ON  mx.segment_ID = ev.segment_id
															AND ev.hhsize = mx.hhsize
															AND ev1.random1 >  mx.lower_limit
															AND ev1.random1 <= mx.upper_limit
	*/
	
	-- Separated version of the above 4-table join:
	select
			ev.event_ID
		,	ev.overlap_batch
		,	ev.segment_id
		,	ev.hhsize
		,	ev.event_dt
		,	ev1.adj_hh
		,	ev1.random1
	into	#tmp1
	FROM
					temp_event 		as	ev
		INNER JOIN 	events_1_box 	as	ev1		ON		ev.event_ID = ev1.event_ID
												AND 	ev.overlap_batch = ev1.overlap_batch
	commit
	
	create hg index tmp1_idx_1 on #tmp1(event_ID)
	create lf index tmp1_idx_2 on #tmp1(overlap_batch)
	commit
	
	
	select
			tmp.*
		,	mx.session_size		as	mx_session_size
	into	#tmp2
	from
					#tmp1								as	tmp
		INNER JOIN 	v289_sessionsize_matrix_default   	AS 	mx 	ON  mx.segment_ID = tmp.segment_id
																AND tmp.hhsize = mx.hhsize
																AND tmp.random1 >  mx.lower_limit
																AND tmp.random1 <= mx.upper_limit
	commit
	
	create hg index tmp2_idx_1 on #tmp2(event_ID)
	create lf index tmp2_idx_2 on #tmp2(overlap_batch)
	commit
	
	
	select
			tmp.*
		,	sm.session_size		as	sm_session_size
		,	COALESCE(sm_session_size, mx_session_size)	as	ev_session_size
	into	#tmp3
	from
					#tmp2						as	tmp
		left join	v289_sessionsize_matrix		as	sm		ON  sm.segment_ID = tmp.segment_id 
															AND tmp.hhsize 	= 	sm.hhsize 
															AND tmp.adj_hh 	>= 	sm.session_size
															AND tmp.random1 	>   sm.lower_limit 
															AND tmp.random1 	<=  sm.upper_limit
															AND tmp.event_dt = 	sm.thedate
	commit
	
	create hg index tmp3_idx_1 on #tmp3(event_ID)
	create lf index tmp3_idx_2 on #tmp3(overlap_batch)
	commit
	
	
	update temp_event
	SET ev.session_size =  tmp.ev_session_size
	FROM 
					temp_event 	as 	ev
		inner join	#tmp3		as	tmp		on		ev.event_ID = tmp.event_ID
											and		ev.overlap_batch = tmp.overlap_batch
											and		ev.segment_id = tmp.segment_id
											and		ev.hhsize = tmp.hhsize
											and		ev.event_dt = tmp.event_dt

	-- Clean up
	drop table #tmp1
	drop table #tmp2
	drop table #tmp3
	commit


											
	MESSAGE cast(now() as timestamp)||' | @ M09.3: Multi Box primary box events updated: '||@@rowcount TO CLIENT
	
	COMMIT
	
		

	-----------------   Secondary box processing - Other Boxes
	MESSAGE cast(now() as timestamp)||' | @ M09.4: Multi Box Other boxes loop started ' TO CLIENT
	SET @cont = 2
	
	WHILE EXISTS (SELECT top 1 event_ID FROM temp_event WHERE session_size = 0 AND hhsize is not null)
	BEGIN
		
		MESSAGE cast(now() as timestamp)||' | @ M09.4: Multi Box start box #: '||@cont TO CLIENT
		
		-- This table is reused from above
		IF OBJECT_ID('events_1_box') IS NOT NULL 
			truncate TABLE events_1_box
		
		commit
		
		SELECT  Overlap_batch
				,account_number
				,SUM (session_size) s_size
				,COUNT (subscriber_id) boxes
		into	#tmp1
		FROM 	temp_event
		GROUP 	BY	Overlap_batch
					,account_number
		commit
		
		create hg index tmp1_idx_1 on #tmp1(account_number)
		create lf index tmp2_idx_2 on #tmp1(overlap_batch)
		commit

		insert	INTO events_1_box
		SELECT	te.*
				,row_id        = row_number() over(order by subscriber_id)
				,ev_proc_flag  = CAST (0 AS BIT)
				,adj_hh        = te.hhsize - v.s_size -(v.boxes -@cont)
				,length_1      = CAST (0 as DECIMAL (7,6))
		FROM 
						temp_event 	as te
			INNER JOIN	#tmp1		AS v 	ON	v.Overlap_batch = te.Overlap_batch  
											AND v.account_number = te.account_number
		WHERE	te.session_size = 0
		AND 	te.hhsize is not null
		AND 	te.box_rank = @cont
		ORDER 	BY	te.account_number
					,te.subscriber_id
					,te.overlap_batch	
		
		commit
		
		-- cleanup
		drop table #tmp1
		commit
		
		MESSAGE cast(now() as timestamp)||' | @ M09.4: Multi Box events_1_box table populated: '||@@rowcount TO CLIENT
		
		UPDATE events_1_box
		SET adj_hh = 1
		WHERE adj_hh < 1
		
		UPDATE events_1_box
		SET     length_1 = upper_limit
				, random1 = random1 * upper_limit
		FROM events_1_box AS ev
		JOIN v289_sessionsize_matrix_default as mx ON mx.segment_ID = ev.segment_id
																		AND ev.hhsize = mx.hhsize
																		AND ev.adj_hh = mx.session_size
		
		/*
		UPDATE temp_event
		SET ev.session_size =  COALESCE(sm.session_size, mx.session_size)
		FROM temp_event as ev
		INNER JOIN events_1_box as ev1 ON   ev.event_ID = ev1.event_ID
										AND ev.overlap_batch = ev1.overlap_batch
		LEFT JOIN v289_sessionsize_matrix 			AS sm ON  sm.segment_ID = ev.segment_id 
															AND ev.hhsize 	= 	sm.hhsize 
															AND ev1.adj_hh 	>= 	sm.session_size
															AND ev1.random1 	>   sm.lower_limit 
															AND ev1.random1 	<=  sm.upper_limit
															AND ev.event_dt = 	sm.thedate										
		INNER JOIN v289_sessionsize_matrix_default   AS mx ON  mx.segment_ID = ev.segment_id
																AND ev.hhsize = mx.hhsize
																AND ev1.random1 > mx.lower_limit
																AND ev1.random1 <= mx.upper_limit
		*/
		
		-- Separated version of the above 4-table join:
		select
				ev.event_ID
			,	ev.overlap_batch
			,	ev.segment_id
			,	ev.hhsize
			,	ev.event_dt
			,	ev1.adj_hh
			,	ev1.random1
		into	#tmp1
		FROM
						temp_event 		as	ev
			INNER JOIN 	events_1_box 	as	ev1		ON		ev.event_ID = ev1.event_ID
													AND 	ev.overlap_batch = ev1.overlap_batch
		commit
		
		create hg index tmp1_idx_1 on #tmp1(event_ID)
		create lf index tmp1_idx_2 on #tmp1(overlap_batch)
		commit
		
		
		select
				tmp.*
			,	mx.session_size		as	mx_session_size
		into	#tmp2
		from
						#tmp1								as	tmp
			INNER JOIN 	v289_sessionsize_matrix_default   	AS 	mx 	ON  mx.segment_ID = tmp.segment_id
																	AND tmp.hhsize = mx.hhsize
																	AND tmp.random1 >  mx.lower_limit
																	AND tmp.random1 <= mx.upper_limit
		commit
		
		create hg index tmp2_idx_1 on #tmp2(event_ID)
		create lf index tmp2_idx_2 on #tmp2(overlap_batch)
		commit
		
		
		select
				tmp.*
			,	sm.session_size		as	sm_session_size
			,	COALESCE(sm_session_size, mx_session_size)	as	ev_session_size
		into	#tmp3
		from
						#tmp2						as	tmp
			left join	v289_sessionsize_matrix		as	sm		ON  sm.segment_ID = tmp.segment_id 
																AND tmp.hhsize 	= 	sm.hhsize 
																AND tmp.adj_hh 	>= 	sm.session_size
																AND tmp.random1 	>   sm.lower_limit 
																AND tmp.random1 	<=  sm.upper_limit
																AND tmp.event_dt = 	sm.thedate
		commit
		
		create hg index tmp3_idx_1 on #tmp3(event_ID)
		create lf index tmp3_idx_2 on #tmp3(overlap_batch)
		commit
		
		
		update temp_event
		SET ev.session_size =  tmp.ev_session_size
		FROM 
						temp_event 	as 	ev
			inner join	#tmp3		as	tmp		on		ev.event_ID = tmp.event_ID
												and		ev.overlap_batch = tmp.overlap_batch
												and		ev.segment_id = tmp.segment_id
												and		ev.hhsize = tmp.hhsize
												and		ev.event_dt = tmp.event_dt

		-- Clean up
		drop table #tmp1
		drop table #tmp2
		drop table #tmp3
		commit


		MESSAGE cast(now() as timestamp)||' | @ M09.3: Multi Box box#: '||@cont||'  events updated: '||@@rowcount TO CLIENT
	    COMMIT

		SET @cont = @cont +1
	END

	
	------------------------------
	-- M09.4 - Main event tables update
	------------------------------
	
	UPDATE V289_M07_dp_data
	SET dt.session_size = te.session_size
	FROM V289_M07_dp_data AS dt
	INNER JOIN temp_event AS te ON te.event_id = dt.event_id AND te.overlap_batch = dt.overlap_batch
	
	MESSAGE cast(now() as timestamp)||' | @ M09.4: Multi Box events updated: '||@@rowcount TO CLIENT
	
	UPDATE V289_M07_dp_data
	SET dt.session_size = te.session_size
	FROM V289_M07_dp_data AS dt
	INNER JOIN temp_event AS te ON te.event_id = dt.event_id 
	WHERE te.overlap_batch = 0
	
	MESSAGE cast(now() as timestamp)||' | @ M09.4: Single Box events updated: '||@@rowcount TO CLIENT
	
	--DROP TABLE temp_event 

	COMMIT

	

END;


COMMIT;
GRANT EXECUTE 	ON v289_m09_Session_size_definition 	TO vespa_group_low_security;
COMMIT;



/*          QA
SELECT top 100 * FROM temp_event WHERE session_size is not null
SELECT top 100 * FROM accounts
SELECT top 100 * FROM events
SELECT top 100 * FROM MC_event
*//*


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
**Project Name:							SkyView Futures
**Analysts:                             Angel Donnarumma
**Lead(s):                              Jason Thompson, Jose Pitteloud, Hoi Yu Tang
**Stakeholder:                          SkyIQ, Sky Media
**Due Date:                             31/07/2014
**Project Code (Insight Collation):     V289
**Sharepoint Folder:                    
                                                                        
**Business Brief:

This script embodies one module within the larger framework of a Household-to-Individual algorithm that takes
Vespa viewing data, and assigns an audience at the granularity of an individual viewer. Audience determination
begins by compilation of probability matrices based on BARB viewing data, where the viewing habits of individuals
are recorded. Monte Carlo selection of individuals available to a household based upon these probability matrices
then gives us gender and age assigns per Vespa viewing event.

The algorithm presented in this script forms one of the final stages within the household-to-individual 
algorithm, in that it takes as inputs the various probability matrices and viewing events with an assigned 
audience size, and determines the individuals that make up that audience.

**Sections:
	S0		- Initialise tables
	S1		- Initialise variables
	S2		- Produce date-wise and date-agnostic PIV matrices and re-normalise
	S3		- Prepare viewing data
	S4 		- Assign audience for single-occupancy households and whole-household audiences
	S5 		- Assign audience for non-lapping events
	S6 		- Assign audience for overlapping events from the same account
	SXX		- Finish
	
**Notes:
	When testing and debugging the code in script-form, simply uncomment the block-commented query terminators 

--------------------------------------------------------------------------------------------------------------
*/

-- Create procedure
create or replace procedure v289_M10_individuals_selection as begin

MESSAGE cast(now() as timestamp)||' | M10 - Individuals assignment module start' TO CLIENT


-- Initialise progress log and update
if object_id('V289_M10_log') is not null drop table V289_M10_log commit --(^_^ )!

create table V289_M10_log(
		section_id			varchar(255)		default	null
	,	dt_completed		datetime			default	null
	,	completed			bit					default 0
	)
commit --(^_^ )!

grant select on V289_M10_log to vespa_group_low_security commit --(^_^ )!

insert into V289_M10_log(section_id)
select section_id
from
	(
		select 'JOB START' 																					as section_id, -1 as num
		union
		select 'S0 - INITIALISE TABLES' 																	as section_id, 0 as num
		union
		select 'S1 - INITIALISE VARIABLES' 																	as section_id, 1 as num
		union
		select 'S2 - Produce date-agnostic PIV and re-normalise' 											as section_id, 2 as num
		union
		select 'S3 - PREPARE VIEWING DATA' 																	as section_id, 3 as num
		union
		select 'S4 - Assign audience for single-occupancy households and whole-household audiences' 		as section_id, 4 as num
		union
		select 'S5 - Assign audience for non-overlapping events' 											as section_id, 5 as num
		union
		select 'S6 - Assign audience for overlapping events from the same account' 							as section_id, 6 as num
		union
		select 'SXX - FINISH' 																				as section_id, 9999 as num
	)	as	t
order by num
commit --(^_^ )!

update V289_M10_log
set
		dt_completed	=	now()
	,	completed		=	1
where section_id = 'JOB START'
commit --(^_^ )!



-- Check for empty input tables 
if
    (
		select
			sum(
				case
					when n > 0	then 1
					else 0
				end
				)
		from
			(SELECT count() as n	from	V289_M07_dp_data
				union
				select count() as n	from	V289_M08_SKY_HH_composition
				union
				select count() as n	from	V289_PIV_Grouped_Segments_desc
				union
				select count() as n	from	v289_genderage_matrix
			)	as	t
	) < 4	
		begin
			insert into V289_M10_log(
					section_id
				,	dt_completed
				)
			select 
					'At least one input table is empty! Please check data.'
				,	now()
			commit
			
		end -- if
commit --(^_^ )!

-------------------------
-------------------------
-- S0 - INITIALISE TABLES
-------------------------
-------------------------

-- The module dependencies, input and output tables are defined and described here.
MESSAGE cast(now() as timestamp)||' | M10 S0.0 - Initialise tables' TO CLIENT

------------------------------------------------------------------
--	S0.1
--	Input tables - Just for definition of what data is required...
------------------------------------------------------------------
/*
[angeld].V289_M07_dp_data(	
		account_number		varchar(20) not null
	,	subscriber_id		decimal(10) not null
	,	event_id			bigint 		not null
	,	event_Start_utc		timestamp 	not null
	,	event_end_utc		timestamp	not null
	,	event_start_dim		int			not null
	,	event_end_dim		int			not null
	,	duration			int			not null
	,	programme_genre		varchar(20)	default null
	,	session_daypart		varchar(11)	default null
	,	hhsize				tinyint		default 0
	,	channel_pack		varchar(40) default null
	,	segment_id			int			default null
	,	Overlap_batch		int			default null
	,	session_size		int			default null
	)
;



-- The probability matrix for a particular age-gender combination to be in the audience
[angeld].v289_genderage_matrix(
		thedate
	,	session_daypart
	,	hhsize
	,	channel_pack
	,	programme_genre
	,	household_number
	,	person_number
	,	sex
	,	ageband
	,	uk_hhwatched
	,	segment_ID???
	,	PIV
	)
;



-- Experian reference data
[thompsonja].V289_M08_SKY_HH_composition(
		row_id
	,	account_number
	,	cb_key_household
	,	exp_cb_key_db_person
	,	cb_key_individual
	,	cb_key_db_person
	,	db_address_line_1
	,	HH_person_number
	,	person_gender
	,	person_age
	,	person_ageband
	,	exp_person_head
	,	person_income
	,	person_head
	,	demographic_ID
	,	Updated_On
	,	Updated_By
	,	household_size
	)
;



-- Segment definitions
[pitteloudj].V289_PIV_Grouped_Segments_desc(
		row_id
	,	channel_pack
	,	daypart
	,	Genre
	,	segment_id
	,	active_flag
	,	Updated_On
	,	Updated_By
	)
;



-- Copy reference tables into local schema for testing
if object_id('V289_M07_dp_data') is not null drop table V289_M07_dp_data; 
select * into V289_M07_dp_data from [angeld].V289_M07_dp_data;

if object_id('v289_genderage_matrix') is not null drop table v289_genderage_matrix;
select * into v289_genderage_matrix from [angeld].v289_genderage_matrix;

if object_id('V289_M08_SKY_HH_composition') is not null drop table V289_M08_SKY_HH_composition;
select * into V289_M08_SKY_HH_composition from [angeld].V289_M08_SKY_HH_composition;

if object_id('V289_PIV_Grouped_Segments_desc') is not null drop table V289_PIV_Grouped_Segments_desc;
select * into V289_PIV_Grouped_Segments_desc from [angeld].V289_PIV_Grouped_Segments_desc;

*/



--------------------
--	S0.2
--	Transient tables
--------------------
MESSAGE cast(now() as timestamp)||' | M10 S0.2 - Initialise transient tables' TO CLIENT



-- Combined event data that gives all possible audience individuals from the household of each viewing event and their PIV
if object_id('V289_M10_combined_event_data') is not null drop table V289_M10_combined_event_data commit --(^_^ )!

create table V289_M10_combined_event_data(
        account_number			varchar(20)		not	null
	,	hh_person_number		tinyint			not	null
    ,   subscriber_id			decimal(10)		not	null
    ,   event_id				bigint			not	null
    ,   event_start_utc			datetime		not	null
    ,   chunk_start				datetime		default	null
    ,   overlap_batch			int				default	null
    ,   programme_genre			varchar(20)		default	null
    ,   session_daypart			varchar(11)		default	null
    ,   channel_pack			varchar(40)		default	null
    ,   segment_id				int				default	null
    ,   numrow					int				not	null
    ,   session_size			tinyint			default	null
    ,   person_gender			varchar(1)		default	null
    ,   person_ageband			varchar(10)		default	null
    ,   household_size			tinyint			default	null
    ,   assigned                bit             default 0
    ,   dt_assigned             datetime        default null
	,	PIV						double			default null
	,	individuals_assigned	int				default	0
	)
commit --(^_^ )!

create hg 	index 	V289_M10_combined_event_data_hg_idx_1 	on V289_M10_combined_event_data(account_number) commit --(^_^ )!
create hg 	index 	V289_M10_combined_event_data_hg_idx_2 	on V289_M10_combined_event_data(event_id) commit --(^_^ )!
create hg 	index 	V289_M10_combined_event_data_hg_idx_3 	on V289_M10_combined_event_data(numrow) commit --(^_^ )!
create lf 	index 	V289_M10_combined_event_data_lf_idx_4 	on V289_M10_combined_event_data(session_size) commit --(^_^ )!
create lf 	index 	V289_M10_combined_event_data_lf_idx_5 	on V289_M10_combined_event_data(person_gender) commit --(^_^ )!
create lf 	index 	V289_M10_combined_event_data_lf_idx_6 	on V289_M10_combined_event_data(person_ageband) commit --(^_^ )!
create lf 	index 	V289_M10_combined_event_data_lf_idx_7 	on V289_M10_combined_event_data(household_size) commit --(^_^ )!

grant select on V289_M10_combined_event_data to vespa_group_low_security commit --(^_^ )!




-- Date-agnostic gender-age PIV (default)
if object_id('V289_M10_PIV_default') is not null drop table V289_M10_PIV_default commit --(^_^ )!

create table V289_M10_PIV_default(
		hhsize						int				default	null
	,	segment_id					int				default	null
	,   sex							varchar(10)		default	null
	,   ageband						varchar(5)		default	null
	,   sum_hours_watched			int				default	null
	,   sum_hours_over_all_demog	int				default	null
    ,   PIV_default					double			default	null
	)
commit --(^_^ )!


-- Date-agnostic gender-age PIV (by date)
if object_id('V289_M10_PIV_by_date') is not null drop table V289_M10_PIV_by_date commit --(^_^ )!

create table V289_M10_PIV_by_date(
		thedate						date			default	null
	,	hhsize						int				default	null
	,	segment_id					int				default	null
	,   sex							varchar(10)		default	null
	,   ageband						varchar(5)		default	null
	,   sum_hours_watched			int				default	null
	,   sum_hours_over_all_demog	int				default	null
    ,   PIV_by_date					double			default	null
	)
commit --(^_^ )!



-- Working PIV matrix per event. This will be continually truncated and inserted into within the loop.
create table #working_PIV(
		account_number			varchar(20)		not		null
	,	subscriber_id			decimal(10)		default	null
	,	event_id				bigint			default	null
	,	overlap_batch			int				default	null	-- will only be used when assigning individuals to overlapping events
	,	hh_person_number		tinyint			not		null	-- unique identifier of a person for a given account_number
	,	cumsum_PIV				double			default	null	-- cumulative sum of PIV
	,	norm_total				double			default	null	-- sum of PIV for normalisation
	,	PIV_range				double			default	null	-- transformed PIV range covering all individuals
	,	rnd						double			default	null	-- random number between 0 and 1
	)
commit --(^_^ )!



-----------------
--	S0.3
--	Output tables
-----------------
MESSAGE cast(now() as timestamp)||' | M10 S0.3 - Initialise output tables' TO CLIENT


-- Gender and age assignments per viewing event
-- This table will be continuously appended to as the audience individuals are assigned. 
-- It will only viewing events from the input table [V289_M07_dp_data] if certain conditions are met, such as there being a valid session_size etc.
if object_id('V289_M10_session_individuals') is not null drop table V289_M10_session_individuals
commit --(^_^ )!

create table V289_M10_session_individuals(
		event_date				date		default	null
    ,   event_id        		bigint      default null
	,	account_number			varchar(20)	default null
    ,   overlap_batch			int			default	null
    ,   chunk_start				datetime	default null
    ,   person_ageband         	varchar(5)  default null
    ,   person_gender          	varchar(10) default null
    ,	hh_person_number		tinyint		default	null
	,	last_modified_dt		datetime	default	null
    )
commit --(^_^ )!


create hg index 		V289_M10_session_individuals_hg_idx_1 		on V289_M10_session_individuals(event_id) commit --(^_^ )!
create hg index 		V289_M10_session_individuals_hg_idx_2 		on V289_M10_session_individuals(account_number) commit --(^_^ )!
create lf index 		V289_M10_session_individuals_lf_idx_3 		on V289_M10_session_individuals(overlap_batch) commit --(^_^ )!
create dttm index 		V289_M10_session_individuals_dttm_idx_4 	on V289_M10_session_individuals(chunk_start) commit --(^_^ )!
create date index 		V289_M10_session_individuals_dttm_idx_5 	on V289_M10_session_individuals(event_date) commit --(^_^ )!
create dttm index 		V289_M10_session_individuals_dttm_idx_6 	on V289_M10_session_individuals(last_modified_dt) commit --(^_^ )!

grant select on V289_M10_session_individuals to vespa_group_low_security commit --(^_^ )!



-- Update log
update V289_M10_log
set
		dt_completed 	= now()
	,	completed 		= 1
where section_id = 'S0 - INITIALISE TABLES'
commit --(^_^ )!



----------------------------
----------------------------
-- S1 - INITIALISE VARIABLES
----------------------------
----------------------------

-- Create and initialise variables
MESSAGE cast(now() as timestamp)||' | M10 S1.0 - Initialise variables' TO CLIENT


-- -- For dev/testing in script form...
-- create variable	@total_number_of_events		int;			-- the total number of viewing events - this will also act as the iterator limit
-- create variable @i                  		int;			-- loop counter for iterations over each unique viewing event/chunk
-- create variable @j                  		tinyint;		-- loop counter for iterations over each audience member when assigning individuals to a viewing event
-- create variable @event_id					bigint;			-- unique identifier of Vespa viewing event
-- create variable @account_number				varchar(20);	-- account number
-- create variable @segment_id         		int;			-- segment ID capturing the daypart, channel_pack and genre of the viewing event
-- create variable @session_size       		int;			-- the audience size for a given viewing event
-- create variable @household_size     		tinyint;		-- the number of occupants associated with a customer account
-- create variable @overlap_batch      		tinyint;		-- an identifier for concurrent events corresponding to a single account

-- create variable @j_person_gender			varchar(6);		-- the gender of the j-th individual during the MC audience assignment process
-- create variable @j_person_ageband 			varchar(5);		-- the ageband of the j-th individual during the MC audience assignment process
-- create variable @j_hh_person_number			tinyint;		-- the unique person identifier within a given household 

-- create variable	@max_household_size			tinyint;		-- maximum allowable household_size to limit the iteration
-- set	@max_household_size	=	15;

-- create variable  @max_chunk_session_size    tinyint;        -- maximum chunk audience size to limit the iteration for overlapping events


-- For execution as a stored procedure...
declare @total_number_of_events 	int
declare @i 							int
declare @j                			tinyint
declare @event_id					bigint
declare @account_number				varchar(20)
declare @segment_id       			int
declare @session_size     			int
declare @household_size   			tinyint
declare @overlap_batch    			tinyint

declare @j_person_gender		varchar(6)
declare @j_person_ageband 		varchar(5)
declare @j_hh_person_number		tinyint

declare @max_household_size		tinyint
set	@max_household_size	=	15

declare @max_chunk_session_size    tinyint

commit --(^_^ )!



-- Update log
update V289_M10_log
set
		dt_completed 	= now()
	,	completed 		= 1
where section_id = 'S1 - INITIALISE VARIABLES'
commit --(^_^ )!





------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------
-- S2 - Produce date-agnostic PIV and re-normalise to create default matrix as well the probabilites by date
------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------
--	S2.1
--	This produces a date-agnostic PIV, and can be treated as a "default" probability matrix
-- 	(We can derive this from the date-wise PIV in S2.2 if we calculate that first...)
-------------------------------------------------------------------------------------------
MESSAGE cast(now() as timestamp)||' | M10 S2.1 - Calculate default PIV' TO CLIENT

SELECT row_num AS hhsize 			INTO #t15   FROM sa_rowgenerator( 1, 15 )
SELECT DISTINCT segment_id  		INTO #tseg  FROM V289_PIV_Grouped_Segments_desc
SELECT DISTINCT sex  , ageband      INTO #tsex  FROM v289_genderage_matrix
INSERT INTO #tsex
	 SELECT 'Undefined' sex, '0-19' age

COMMIT



insert into V289_M10_PIV_default (
		hhsize
	,	segment_id
    ,	sex
    ,	ageband
    ,	sum_hours_watched
    ,	sum_hours_over_all_demog
    ,	PIV_default)
SELECT hhsize
	,	segment_id
    ,	sex
    ,	ageband
    ,	0 		AS sum_hours_watched
    ,	0		AS sum_hours_over_all_demog
    ,	1e-3 	AS PIV_default
FROM 		#tseg as b
CROSS JOIN 	#tsex as c
CROSS JOIN 	#t15 as d
commit 
DELETE FROM V289_M10_PIV_default
WHERE sex not like '%Undef%' AND ageband like '0-19%'
COMMIT 

SELECT
		hhsize
	,	segment_id
	,	sex
	,	ageband
	,	sum_hours_watched
	,	sum_hours_over_all_demog
	,	PIV_default
INTO #PIV
FROM (SELECT 
					hhsize
				,	segment_id
				,	CAST(case	ageband
						when	'0-19'	then	'Undefined'
						else	sex
					end	AS VARCHAR(10))																as	sex
				,	ageband
				,   uk_hhwatched
				,   case
						when (uk_hhwatched = 0 or uk_hhwatched is null) then    1e-3
						else    uk_hhwatched
					end                                                                 as  uk_hhwatched_nonzero
				,	sum(uk_hhwatched_nonzero)	over	(	partition by
																			segment_id
																		,	hhsize
																		,	sex
																		,	ageband
														)								as	sum_hours_watched
				,	sum(uk_hhwatched_nonzero)	over	(	partition by
																			segment_id
																		,	hhsize
														)								as	sum_hours_over_all_demog
				,	1.0 * sum_hours_watched / sum_hours_over_all_demog					as	PIV_default
		FROM 	v289_genderage_matrix
		WHERE 	ageband	<> 'Undefined'
		)	as	t
GROUP BY 
		hhsize
	,	segment_id
	,	sex
	,	ageband
	,	sum_hours_watched
	,	sum_hours_over_all_demog
	,	PIV_default
MESSAGE cast(now() as timestamp)||' #PIV generated: '||@@rowcount TO CLIENT

UPDATE V289_M10_PIV_default
SET   a.sum_hours_watched 			= j.sum_hours_watched
	, a.sum_hours_over_all_demog 	= j.sum_hours_over_all_demog
	, a.PIV_default					= j.PIV_default
FROM V289_M10_PIV_default 		AS a
INNER JOIN #PIV AS j 	ON  a.hhsize = j.hhsize 
					AND a.segment_id = j.segment_id
					AND LEFT(a.sex,1) = LEFT(j.sex,1)
					AND LEFT(a.ageband,2) = LEFT(j.ageband,2)
	
	MESSAGE cast(now() as timestamp)||' V289_M10_PIV_default Table Updated: ' ||@@rowcount TO CLIENT
commit --(^_^ )!




-----------------------------
--	S2.2
--	This produces PIV by date
-----------------------------
MESSAGE cast(now() as timestamp)||' | M10 S2.2 - Calculate date-wise PIV' TO CLIENT

insert into V289_M10_PIV_by_date
select
		thedate
	,	hhsize
	,	segment_id
    ,	sex
    ,	ageband
    ,	sum_hours_watched
    ,	sum_hours_over_all_demog
    ,	PIV_by_date
from	(
			select
					thedate
				,	hhsize
				,	segment_id
				,	case	ageband
						when	'0-19'	then	'Undefined'
						else	sex
					end																	as	sex
				,	ageband
				,   uk_hhwatched
				,   case
						when (uk_hhwatched = 0 or uk_hhwatched is null) then    1e-3
						else    uk_hhwatched
					end                                                                 as  uk_hhwatched_nonzero
				,	sum(uk_hhwatched_nonzero)	over	(	partition by
																			thedate
																		,	hhsize
																		,	segment_id
																		,	sex
																		,	ageband
														)								as	sum_hours_watched
				,	sum(uk_hhwatched_nonzero)	over	(	partition by
																			thedate
																		,	hhsize
																		,	segment_id
														)								as	sum_hours_over_all_demog
				,	1.0 * sum_hours_watched / sum_hours_over_all_demog					as	PIV_by_date
			from	v289_genderage_matrix
			where	ageband	<> 'Undefined'
		)	as	t
group by
		thedate
	,	hhsize
	,	segment_id
    ,	sex
    ,	ageband
    ,	sum_hours_watched
    ,	sum_hours_over_all_demog
    ,	PIV_by_date
commit --(^_^ )!




/* Checks...

select top 20 * from V289_M10_PIV_default;

*/

-- Update log
update V289_M10_log
set
		dt_completed 	= now()
	,	completed 		= 1
where section_id = 'S2 - Produce date-agnostic PIV and re-normalise'
commit --(^_^ )!


----------------------------
----------------------------
-- S3 - PREPARE VIEWING DATA
----------------------------
----------------------------

----------------------------------------------------------------------------------------------------------------
--	S3.1
--	Join events to Experian individual data to give all possible audience members within a single working table. 
-- 	We do this so as to avoid having to perform the same kind of join as we iterate through each viewing event.
----------------------------------------------------------------------------------------------------------------
MESSAGE cast(now() as timestamp)||' | M10 S3.1 - Join all possible individuals to viewing data' TO CLIENT

insert into V289_M10_combined_event_data(
        account_number
	,	hh_person_number
    ,   subscriber_id
    ,   event_id
    ,   event_start_utc
    ,   chunk_start
    ,   overlap_batch
    ,   programme_genre
    ,   session_daypart
    ,   channel_pack
    ,   segment_id
    ,   numrow
    ,   session_size
    ,   person_gender
    ,   person_ageband
    ,   household_size
	)
select
        a.account_number
	,	hh_person_number
    ,   a.subscriber_id
    ,   a.event_id
    ,   a.event_start_utc
    ,   a.chunk_start
    ,   a.overlap_batch
    ,   a.programme_genre
    ,   a.session_daypart
    ,   a.channel_pack
    ,   a.segment_id
    ,   a.numrow
    ,   a.session_size
    ,   b.person_gender
    ,   b.person_ageband
    ,   a.hhsize
from
                (
                    select
                            account_number
                        ,   subscriber_id
                        ,   event_id
                        ,   event_start_utc
                        ,   chunk_start
                        ,   overlap_batch
                        ,   programme_genre
                        ,   session_daypart
                        ,   channel_pack
                        ,   segment_id
                        ,   session_size
                        ,   hhsize
                        ,   row_number()    over    (order by account_number, subscriber_id, event_id, overlap_batch)   as  numrow  -- won't need this anymore once we move away from an event-wise iteration
                    from    V289_M07_dp_data
                    where
                            session_size > 0                -- ignore events without an assign audience size
                        and segment_id is not null          -- ignore any events without a valid segment ID
                )   as  a
    inner join  (
                    select
                            *
                        ,   count() over (partition by account_number)  as  valid_individuals
                    from    V289_M08_SKY_HH_composition
                    where
								person_ageband is not null
                        -- and     person_gender <> 'U'
                        and     hh_person_number is not null
                )   as  b   	on      a.account_number    =   b.account_number
								and     a.hhsize            =   b.valid_individuals
where   session_size <= hhsize
commit --(^_^ )!

MESSAGE cast(now() as timestamp)||' | M10 S3.1 - V289_M10_combined_event_data Table populated: '||@@rowcount TO CLIENT



/*
-------------------------------------------------------------------------------------------------------
--	S3.2
--	Filter: For overlapping events, keep only those where the total session sizes are <= household_size
-------------------------------------------------------------------------------------------------------


select
		account_number
	,	household_size
	,   overlap_batch
	,   sum(session_size)   as  total_session_size
into 	#V289_M10_overlap_accounts_to_remove
from    V289_M10_combined_event_data --V289_M07_dp_data
where 	overlap_batch is not null
group by
		account_number
	,	household_size
	,   overlap_batch
having 	total_session_size > household_size
commit --(^_^ )!

-- select count() from #V289_M10_overlap_accounts_to_remove;
-- select top 200 * from #V289_M10_overlap_accounts_to_remove;


-- Now remove those rows where these bad session_size assignments occur
delete from V289_M10_combined_event_data
from
				V289_M10_combined_event_data		as	a
	inner join	#V289_M10_overlap_accounts_to_remove	as	b		on	a.account_number 	= b.account_number
																	and	a.overlap_batch 	= b.overlap_batch
commit --(^_^ )!
*/



-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--	S3.3
--	Filter: For overlapping batches, keep only those where the number of occurrences of any overlap_batch number is greater than the number of available boxes for that account
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
MESSAGE cast(now() as timestamp)||' | M10 S3.3 - Filter out overlapping events with more overlaps than available STBs' TO CLIENT
/*
select
		a.account_number
	,	overlap_batch
    -- ,	boxes
	,	count()		as	batch_occurances
into #V289_M10_batch_overcount
from
					V289_M10_combined_event_data	as	a
	inner join	(
    				select
                    		account_number
                    	,	count(distinct subscriber_id)	as	boxes
                    from	V289_M10_combined_event_data
                    group by account_number
                )									as	b	on	a.account_number = b.account_number
where	overlap_batch is not null
group by
		a.account_number
	,	overlap_batch
	,	boxes
having	batch_occurances > boxes
commit --(^_^ )!

MESSAGE cast(now() as timestamp)||' | M10 S3.1 - #V289_M10_batch_overcount Table populated: '||@@rowcount TO CLIENT

-- Now remove those rows where these apparent batch overcounts occur
delete from V289_M10_combined_event_data
from
				V289_M10_combined_event_data		as	a
	inner join	#V289_M10_batch_overcount				as	b		on	a.account_number 	= b.account_number
																	and	a.overlap_batch 	= b.overlap_batch
commit --(^_^ )!
*/

SELECT DISTINCT  b.event_id
    , a.overlap_batch
    , a.account_number
    , a.subscriber_id
    , b.numrow
    , dense_rank() OVER (PARTITION BY         a.account_number         , a.overlap_batch        , a.subscriber_id ORDER BY  b.event_id DESC) AS rankk
INTO #temp_del
FROM (SELECT
          account_number
        , overlap_batch
        , subscriber_id
        , COUNT(DISTINCT event_id) hits
    FROM V289_M10_combined_event_data
    WHERE overlap_batch is not null
    GROUP BY
          account_number
        , overlap_batch
        , subscriber_id
    HAVING hits > 1)                AS  a
JOIN V289_M10_combined_event_data   AS  b   ON a.overlap_batch = b.overlap_batch
                                        AND a.account_number = b.account_number

DELETE FROM V289_M10_combined_event_data
FROM V289_M10_combined_event_data   AS a
JOIN #temp_del                      AS b   ON a.overlap_batch = b.overlap_batch
                                          AND a.account_number = b.account_number
WHERE rankk > 1

MESSAGE cast(now() as timestamp)||' | M10 S3.1 - #V289_M10_batch_overcount overcounts removed: '||@@rowcount TO CLIENT


--------------------------------------------------------------------------------------------
--	S3.4
-- Add the default PIV per individual, reverting to the latest current value where available
--------------------------------------------------------------------------------------------
MESSAGE cast(now() as timestamp)||' | M10 S3.4 - Append PIVs to individuals' TO CLIENT

UPDATE V289_M10_combined_event_data
SET		PIV = 	c.PIV_by_date	
FROM 	V289_M10_combined_event_data		AS 	a
JOIN	V289_M10_PIV_by_date           		AS 	c	ON 	DATE (a.event_start_utc) = c.thedate	
														AND a.segment_id = c.segment_id
														AND a.household_size = c.hhsize
														AND a.person_gender = left(c.sex,1)
														AND LEFT(a.person_ageband,2) = LEFT(c.ageband,2)

SELECT DISTINCT event_id 
INTO #tev1
FROM V289_M10_combined_event_data
WHERE PIV is null 

COMMIT
CREATE HG INDEX evi ON #tev1(event_id)
COMMIT

UPDATE V289_M10_combined_event_data
SET		PIV = 	b.PIV_default
FROM 	V289_M10_combined_event_data		AS 	a	
JOIN 	#tev1 								AS  z 	ON  a.event_id = z.event_id													
JOIN 	V289_M10_PIV_default            	AS  b   ON 	a.segment_id = b.segment_id		
													AND a.household_size = b.hhsize
													AND a.person_gender = left(b.sex,1)
													AND LEFT(a.person_ageband,2) = LEFT(b.ageband,2)
COMMIT --(^_^ )!

DROP TABLE #tev1

delete from V289_M10_combined_event_data
where PIV is null
commit --(^_^ )!


MESSAGE cast(now() as timestamp)||' | M10 S3.1 - Deleted from V289_M10_combined_event_data due to null PIV: '||@@rowcount TO CLIENT


---------------------------------------------------------------------------------
--	S3.5
--	Delete rows where there are less expected individuals than the household size
---------------------------------------------------------------------------------
MESSAGE cast(now() as timestamp)||' | M10 S3.5 - Filter out accounts with fewer individuals than the expected household size' TO CLIENT

select	event_id
into #tmp
from
	(
        select
        		*
        	,	count()		over	(partition by event_id)	as	individuals_with_PIV
        from		V289_M10_combined_event_data
	)	as	t
where individuals_with_PIV < household_size
commit --(^_^ )!

delete from V289_M10_combined_event_data
from
				V289_M10_combined_event_data	as	a
	inner join	#tmp								as	b	on	a.event_id = b.event_id
commit --(^_^ )!

MESSAGE cast(now() as timestamp)||' | M10 S3.1 - Deleted from V289_M10_combined_event_data due to less expected individuals than the household size: '||@@rowcount TO CLIENT

drop table #tmp commit --(^_^ )!




---------------------------------------------------------------------------------------------
--	S3.6
--	Remove any results from the central output table for the dates that the input data covers
---------------------------------------------------------------------------------------------
MESSAGE cast(now() as timestamp)||' | M10 S3.6 - Delete existing results from current date of data' TO CLIENT

delete from 	V289_M10_session_individuals
where	event_date	in	(
							select	date(event_start_utc)	as	event_date
							from	V289_M10_combined_event_data
							group by	event_date
						)
commit --(^_^ )!



--------------
--	S3.X
--	Update log
--------------
update V289_M10_log
set
		dt_completed 	= now()
	,	completed 		= 1
where section_id = 'S3 - PREPARE VIEWING DATA'
commit --(^_^ )!




-------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------
-- S4 - Assign audience for single-occupancy households and whole-household audiences
-------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------
MESSAGE cast(now() as timestamp)||' | M10 S4.0 - Assign audience for single-occupancy households and whole-household audiences' TO CLIENT

-- Simply append all of the household individuals per event to the output table
insert into V289_M10_session_individuals(
		event_date
	,	event_id
	,	account_number
    ,   person_ageband
    ,   person_gender
    ,	hh_person_number
	,	last_modified_dt
    )
select
		date(event_start_utc)	as	event_date
    ,   event_id
	,	account_number
    ,   person_ageband
    ,   person_gender
    ,	hh_person_number
	,	now()					as	last_modified_dt
from    V289_M10_combined_event_data
where	household_size = session_size
group by
		event_date
    ,   event_id
	,	account_number
    ,   person_ageband
    ,   person_gender
    ,	hh_person_number
	,	last_modified_dt
commit --(^_^ )!

MESSAGE cast(now() as timestamp)||' | M10 S4.0 - Individual assigned due to single-occupancy households and whole-household audiences'|| @@rowcount TO CLIENT


-- 157976 Row(s) affected
-- ~0.58s


-- Update combined events table
update V289_M10_combined_event_data
set
		assigned 				=	1
	,	dt_assigned 			=	now()
	,	individuals_assigned 	=	session_size
where	household_size = session_size
commit --(^_^ )!


-- Update log
update V289_M10_log
set
		dt_completed 	= now()
	,	completed 		= 1
where section_id = 'S4 - Assign audience for single-occupancy households and whole-household audiences'
commit --(^_^ )!




--------------------------------------------------
--------------------------------------------------
-- S5 - Assign audience for non-overlapping events
--------------------------------------------------
--------------------------------------------------
MESSAGE cast(now() as timestamp)||' | M10 S5.0 - Assign audience for non-overlapping events' TO CLIENT

---------------------------------------------------------------------------------
--	S5.1
-- 	Loop over individuals instead of events (first tackle non-overlapping events)
---------------------------------------------------------------------------------

set @i = 0 commit --(^_^ )!

while @i < @max_household_size begin

	set @i = @i + 1

	-- select @i
	
	-- Get all events that (still) need an individual assigned, transform the available PIVs into a 0->1 range, then select an individual via MC
	insert into #working_PIV(
			account_number
		,	event_id
		,	hh_person_number
		,	cumsum_PIV
		,	norm_total
		,	PIV_range
		,	rnd
		)
	select
			account_number
		,	event_id
		,	hh_person_number
		,	cumsum_PIV
		,	norm_total
		,	PIV_range
		,	rnd	
	from
		(
			select *
			from
				(
					select
							*
						,   row_number() over (partition by event_id order by PIV_range)    as  rnk
					from
						(
							select
									account_number
								,	event_id
								,	hh_person_number
								,   sum(PIV)    over    (
															partition by event_id
															rows between unbounded preceding and current row
														)                                                       as  cumsum_PIV
								,   sum(PIV)    over    (   partition by event_id   )                           as  norm_total
								,   cumsum_PIV / norm_total                                                     as  PIV_range
								,   rand    (
													numrow
												+   datepart(us,now())
											)                                                                   as  rnd
							from	V289_M10_combined_event_data
							where	
										overlap_batch is null                   -- non-lapping events only
								and     individuals_assigned < session_size     -- select events that still need to be filled
								and     assigned = 0                        	-- select the individuals yet to be assigned
						)   as  t1
					where	rnd < PIV_range
				)   as  t2
			where   rnk = 1
		)	as	t3

	
    -- Now join back into the global combined viewing event data and update the individuals that have been assigned
    update V289_M10_combined_event_data
    set
            assigned = 1
        ,   dt_assigned = now()
    from
                    V289_M10_combined_event_data    as  a
        inner join  #working_PIV                                as  b   on  a.event_id = b.event_id
                                                                and a.account_number = b.account_number
                                                                and a.hh_person_number = b.hh_person_number
    
	
	-- Clean up
	truncate table #working_PIV

    
	-- Update number of assigned individuals per event
    update	V289_M10_combined_event_data
    set		individuals_assigned 	=	b.total_assigned
	from
    				V289_M10_combined_event_data	as	a
    	inner join	(
                    	select
                        		event_id
                            ,	sum(cast(assigned as int))	as	total_assigned
                        from	V289_M10_combined_event_data
                        where	overlap_batch is null
                        group by	event_id
					)									as	b	on	a.event_id = b.event_id
	
	
	-- Break out of while loop if assignments are complete
	-- if	(
    if	not	exists	(
						select	1
						from	V289_M10_combined_event_data
						where	
									overlap_batch is null
							and		individuals_assigned < session_size
					)	break

																
end	-- while @i < @max_household_size begin
commit --(^_^ )!


------------------------------------------------------------------------
--	S5.2
--	Finally, remove the unassigned individuals from the processed events
------------------------------------------------------------------------
delete from V289_M10_combined_event_data
where	
			overlap_batch is null
	and		individuals_assigned = session_size
	and		assigned = 0
commit --(^_^ )!



/* Checks
select
		dt_assigned
    ,	count()
from	V289_M10_combined_event_data
group by dt_assigned


*/



------------------------------------------------------------------
--	S5.3
-- 	Append results from non-overlapping events to the output table
------------------------------------------------------------------
insert into V289_M10_session_individuals(
		event_date
	,	event_id
	,	account_number
    ,   person_ageband
    ,   person_gender
    ,	hh_person_number
	,	last_modified_dt
    )
select
		date(event_start_utc)	as	event_date
    ,   event_id
	,	account_number
    ,   person_ageband
    ,   person_gender
    ,	hh_person_number
	,	now()					as	last_modified_dt
from    V289_M10_combined_event_data
where	
			household_size <> session_size
	and		overlap_batch is null
	and		assigned = 1
group by
		event_date
    ,   event_id
	,	account_number
    ,   person_ageband
    ,   person_gender
    ,	hh_person_number
	,	last_modified_dt
commit --(^_^ )!



--------------
--	S5.X
-- 	Update log
--------------
update V289_M10_log
set
		dt_completed 	= now()
	,	completed 		= 1
where section_id = 'S5 - Assign audience for non-overlapping events'
commit --(^_^ )!




----------------------------------------------
----------------------------------------------
-- S6 - Assign audience for overlapping events
----------------------------------------------
----------------------------------------------
MESSAGE cast(now() as timestamp)||' | M10 S6.0 - Assign audience for overlapping events' TO CLIENT


-----------------------------------------------------------------------------------------------------------
--	S6.1
-- 	Define the maximum number of iterations that we'll need to cover all individuals for overlapping events
-----------------------------------------------------------------------------------------------------------
select @max_chunk_session_size = max(chunk_session_size)
from
    (
        select
                account_number
            ,   overlap_batch
            ,   sum(session_size)   as  chunk_session_size
        into    #chunk_sessions
        from    V289_M07_dp_data
        where   overlap_batch   is not null
        group by
                account_number
            ,   overlap_batch
    )   as  t
commit --(^_^ )!

-- select @max_chunk_session_size;




------------------------------------------------------------
--	S6.2
--	Iterate over individuals per chunk of overlapping events
------------------------------------------------------------

set @i = 0 commit --(^_^ )!

while @i < @max_chunk_session_size  begin

	set @i = @i + 1

	-- select @i
	
	-- Get all events that (still) need an individual assigned, transform the available PIVs into a 0->1 range, then select an individual via MC
	insert into #working_PIV(
			account_number
		,	subscriber_id
		,	overlap_batch
		,	hh_person_number
		,	cumsum_PIV
		,	norm_total
		,	PIV_range
		,	rnd
		)
	select
			account_number
		,	subscriber_id
		,	overlap_batch
		,	hh_person_number
		,	cumsum_PIV
		,	norm_total
		,	PIV_range
		,	rnd	
	from
		(
			select *
			from
				(
					select
							*
						,   row_number() over (partition by account_number, overlap_batch	order by PIV_range)    as  rnk
					from
						(
							select
									account_number
								,	subscriber_id
								,	overlap_batch
								,	hh_person_number
								,   sum(PIV)    over    (
															partition by account_number, overlap_batch
															rows between unbounded preceding and current row
														)                                                       as  cumsum_PIV
								,   sum(PIV)    over    (   partition by account_number, overlap_batch   )		as  norm_total
								,   cumsum_PIV / norm_total                                                     as  PIV_range
								,   rand    (
													numrow
												+   hh_person_number
												+   datepart(us,now())
											)                                                                   as  rnd
							from V289_M10_combined_event_data   as  a
							where
										overlap_batch is not null				-- non-lapping events only
								and     individuals_assigned < session_size		-- select events that still need to be filled
								and     assigned = 0                        	-- select the individuals yet to be assigned
						)   as  t1
					where	rnd < PIV_range
				)   as  t2
			where   rnk = 1
		)	as	t3
	
	
    -- Now join back into the global combined viewing event data and update the individuals that have been assigned TO THAT PARTICULAR BOX
    update V289_M10_combined_event_data
    set
            assigned = 1
        ,   dt_assigned = now()
    from
                    V289_M10_combined_event_data    as  a
        inner join  #working_PIV					as  c   	on  a.account_number    =   c.account_number
                                                                and a.subscriber_id     =   c.subscriber_id
                                                                and a.hh_person_number  =   c.hh_person_number
                                                                and a.overlap_batch     =   c.overlap_batch

    
    -- Now we'll also need to remove those individuals to avoid being assigned to other overlapping events
    delete from V289_M10_combined_event_data
    from
                    V289_M10_combined_event_data    as  a
        inner join  #working_PIV					as  c		on  a.account_number    =   c.account_number
                                                                and a.subscriber_id     <>  c.subscriber_id
                                                                and a.hh_person_number  =   c.hh_person_number
                                                                and a.overlap_batch     =   c.overlap_batch
    where   a.assigned = 0


    -- Clean up
    truncate table #working_PIV


    -- Update number of assigned individuals per account_number-overlap_batch combination
    update  V289_M10_combined_event_data
    set     individuals_assigned    =   b.total_assigned
    from
                    V289_M10_combined_event_data    as  a
        inner join  (
                        select
                                account_number
                            ,   subscriber_id
                            ,   overlap_batch
                            ,   sum(cast(assigned as int))  as  total_assigned
                        from    V289_M10_combined_event_data
                        where   overlap_batch is not null
                        group by
                                account_number
                            ,   subscriber_id
                            ,   overlap_batch
                    )                                   as  b   on  a.account_number    =   b.account_number
                                                                and a.subscriber_id     =   b.subscriber_id
                                                                and a.overlap_batch     =   b.overlap_batch


    -- Break out of while loop if assignments are complete
    if  not exists	(
						select  1
						from    V289_M10_combined_event_data
						where
									overlap_batch is not null
							and		individuals_assigned < session_size
					)	break

end -- @i < @max_household_size begin
commit --(^_^ )!



------------------------------------------------------------------
--	S6.3
-- 	Append results from non-overlapping events to the output table
------------------------------------------------------------------
insert into V289_M10_session_individuals(
		event_date
	,	event_id
	,	account_number
	,	overlap_batch
    ,   person_ageband
    ,   person_gender
    ,	hh_person_number
	,	last_modified_dt
    )
select
		date(event_start_utc)	as	event_date
    ,   event_id
	,	account_number
	,	overlap_batch
    ,   person_ageband
    ,   person_gender
    ,	hh_person_number
	,	now()					as	last_modified_dt
from    V289_M10_combined_event_data
where	
			overlap_batch is not null
	and		assigned = 1
group by
		event_date
    ,   event_id
	,	account_number
	,	overlap_batch
    ,   person_ageband
    ,   person_gender
    ,	hh_person_number
	,	last_modified_dt
commit --(^_^ )!



--------------
--	S6.X
--	Update log
--------------
update V289_M10_log
set
		dt_completed 	= now()
	,	completed 		= 1
where section_id = 'S6 - Assign audience for overlapping events from the same account'
commit --(^_^ )!




---------------
---------------
-- SXX - FINISH
---------------
---------------

-- Update log
update V289_M10_log
set
		dt_completed 	= now()
	,	completed 		= 1
where section_id = 'SXX - FINISH'
commit --(^_^ )!

MESSAGE cast(now() as timestamp)||' | M10 - Individuals assignment complete!' TO CLIENT


end; -- create or replace procedure H2I_M10_individuals_selection as begin

commit;
grant execute on v289_M10_individuals_selection to vespa_group_low_security;
commit;
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
**Project Name:							Skyview H2I
**Analysts:                             Angel Donnarumma	(angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jason Thompson		(Jason.Thompson@skyiq.co.uk)
										,Hoi Yu Tang		(HoiYu.Tang@skyiq.co.uk)
										,Jose Pitteloud		(jose.pitteloud@skyiq.co.uk)
**Stakeholder:                          SkyIQ
										,Jose Loureda		(Jose.Loureda@skyiq.co.uk)
**Due Date:                             11/07/2014
**Project Code (Insight Collation):     v289
**Sharepoint Folder:    

	http://sp-department.bskyb.com/sites/IQSKY/SIG/Insight Collation Documents/01 Analysis Requests/V289 - Skyview Futures
                                                                        
**Business Brief:

	This brief module serves to produce a high-level summary of the H2I algorithm outputs, giving average
	viewing hours per individual across the gender-age classes, and average hours per household to comparison
	against an equivalent metric as derived from the dp_prog_viewing_* tables.
	
	The main procedure can be executed to target particular table names (backups for e.g.), where in the 
	absence of input parameters will automatically attempt to analyse the standard tables:
		V289_M10_session_individuals
		V289_M07_dp_data
	within the user's own schema.
	
	Syntax:
		execute V289_H2I_summarise_avg_viewing;
		execute V289_H2I_summarise_avg_viewing 
				'thompsonja.V289_M10_session_individuals_backup_50pc_01'
			,	'thompsonja.V289_M07_dp_data_backup_50pc_01'
		;
		

**Module:
	
	M10b: Process Manager
			M10b.0 - Create simple function to test for data existence within target tables
			M10b.1 - Main procedure to calculate average viewing hours per individual and household

	
--------------------------------------------------------------------------------------------------------------
*/



----------------------------------------------------------------------------------
-- M10b.0 - Create simple function to test for data existence within target tables
----------------------------------------------------------------------------------
create or replace function V289_H2I_check_M10_validation_tables(
        @table_session_individuals   varchar(255)   =   'V289_M10_session_individuals'
    ,   @table_dp_data               varchar(255)   =   'V289_M07_dp_data'
    )
returns bit
as  begin

    declare @result bit

    execute(
    +   '   select  '
    +   '       @result =   case    '
    +   '                       when    '
    +   '                               (exists (select top 1 1 from ' + @table_session_individuals + '))   '
    +   '                           and (exists (select top 1 1 from ' + @table_dp_data + '))   '
    +   '                           then 1  '
    +   '                           else 0  '
    +   '                   end ')

    return @result

end
;
commit;

grant execute on V289_H2I_check_M10_validation_tables to vespa_group_low_security;
commit;


------------------------------------------------------------------------------------------
-- M10b.1 - Main procedure to calculate average viewing hours per individual and household
------------------------------------------------------------------------------------------
create or replace procedure V289_H2I_summarise_avg_viewing
        @table_session_individuals   varchar(255)   =   'V289_M10_session_individuals'
    ,   @table_dp_data               varchar(255)   =   'V289_M07_dp_data'
as  begin


	-- Check for data before proceeding
	
	message cast(now() as timestamp) || ' | M10b - Checking for M07 and M10 output tables' to client
	
	if  V289_H2I_check_M10_validation_tables(
				@table_session_individuals
			,   @table_dp_data
			)   =   1
		begin


			-- Create temporary views on the input tables
			
			message cast(now() as timestamp) || ' | M10b - Creating temporary views' to client
			
			execute ('create or replace view tmp_view_m10 as select * from ' + @table_session_individuals)
			execute ('create or replace view tmp_view_m07 as select * from ' + @table_dp_data)


			
			-- Prepare base table (still no aggregations as yet - just pulling together all of the relevant data on an event/individual level)
			select
					a.person_gender
				,   a.person_ageband
				,   a.account_number
				,   a.hh_person_number
				,   a.account_number + '-' + cast(a.hh_person_number as varchar)    as  person_id
				,	c.scaling_weighting
				,   a.event_id
				,	trim(b.session_daypart)											as	daypart
				,   case
						when    a.overlap_batch is null then b.event_start_utc
						else    b.chunk_start
					end     														as  viewing_start_dt
				,   case
						when    a.overlap_batch is null then b.event_end_utc
						else    b.chunk_end
					end     														as  viewing_end_dt
				,   datediff(second,viewing_start_dt,viewing_end_dt)        		as  viewing_seconds
			into	#tmp
			from
							tmp_view_m10   											as  a
				inner join  tmp_view_m07                							as  b   on  a.account_number = b.account_number
																							and a.event_id = b.event_id
																							and case when a.overlap_batch is null then 0 else a.overlap_batch end = case when b.overlap_batch is null then 0 else b.overlap_batch end
				inner join	V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING					as	c	on	a.account_number = c.account_number
																							and	a.hh_person_number = c.hh_person_number
																							and	c.build_date = (select	max(build_date) from	V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING)
            -- where a.overlap_batch is null
			
			
			-- Vespa viewing per individual (aggregated over the entire day of data)
			
			message cast(now() as timestamp) || ' | M10b - Calculating average viewing per individual gender-age class' to client

			select
					person_gender
				,   person_ageband
				,   count()                         							as  individuals
				,   count(distinct account_number)  							as  households
				,   sum(viewing_hours)              							as  total_viewing_hours
				,   avg(viewing_hours)              							as  average_viewing_hours
				,	sum(scaling_weighting)										as	individuals_scaled
				,	sum(viewing_hours_scaled)									as	total_viewing_hours_scaled
				,   total_viewing_hours_scaled / individuals_scaled				as	average_viewing_hours_scaled
				,	(select count(distinct account_number) from tmp_view_m10)	as	OLAP_unique_households
			from
				(	-- aggregated over individuals to get their respective viewing total and scaled total consumption
					select
							person_gender
						,   person_ageband
						,   person_id
						,	scaling_weighting
						,   account_number
						,   sum(viewing_seconds) / 3600.0  		as  viewing_hours
						,	viewing_hours * scaling_weighting	as	viewing_hours_scaled
					from	#tmp	as  t0
					group by
							person_gender
						,   person_ageband
						,   person_id
						,	scaling_weighting
						,   account_number
				)   as  t1
			group by
					person_gender
				,   person_ageband
			order by
					person_gender
				,   person_ageband


			
			
			message cast(now() as timestamp) || ' | M10b - Calculating average viewing per individual gender-age class and daypart' to client

			select
					person_gender
				,   person_ageband
				,	daypart
				,   count()                         							as  individuals
				,   count(distinct account_number)  							as  households
				,   sum(viewing_hours)              							as  total_viewing_hours
				,   avg(viewing_hours)              							as  average_viewing_hours
				,	sum(scaling_weighting)										as	individuals_scaled
				,	sum(viewing_hours_scaled)									as	total_viewing_hours_scaled
				,   total_viewing_hours_scaled / individuals_scaled				as	average_viewing_hours_scaled
				,	(select count(distinct account_number) from tmp_view_m10)	as	OLAP_unique_households
			from
				(	-- aggregated over individuals to get their respective viewing total and scaled total consumption per daypart
					select
							person_gender
						,   person_ageband
						,	daypart
						,   person_id
						,	scaling_weighting
						,   account_number
						,   sum(viewing_seconds) / 3600.0  		as  viewing_hours
						,	viewing_hours * scaling_weighting	as	viewing_hours_scaled
					from	#tmp	as  t0
					group by
							person_gender
						,   person_ageband
						,	daypart
						,   person_id
						,	scaling_weighting
						,   account_number
				)   as  t1
			group by
					person_gender
				,   person_ageband
				,	daypart
			order by
					person_gender
				,   person_ageband
				,	daypart
			
			
			-- Count distinct accounts
			
			message cast(now() as timestamp) || ' | M10b - Counting distinct accounts' to client
			
			select  count(distinct account_number)  as  unique_accounts
			from    tmp_view_m10

			
			
			
			
			-- Viewing per household - dedupe individuals per event
			
			message cast(now() as timestamp) || ' | M10b - Calculating average viewing per household' to client
			
			select
					count()                     as  rows
				,   count(distinct account_number)  as  accounts
				,   sum(viewing_hours)          as  total_hours
				,   avg(viewing_hours)          as  avg_hours_per_hh
				,   min(viewing_hours)          as  min_hours_per_hh
				,   max(viewing_hours)          as  max_hours_per_hh
				,   stddev(viewing_hours)       as  std_hours_per_hh
			from
				(
					select
							account_number
						,   sum(viewing_hours)  as  viewing_hours
					from
						(
							select
									account_number
								,   sum(viewing_seconds / 3600.0)   as  viewing_hours
							from
								(
									select
											a.account_number
										,   a.event_id
										,   datediff(second,b.event_start_utc,b.event_end_utc)      as  viewing_seconds
									from
													tmp_view_m10        as  a
										inner join  tmp_view_m07      	as  b   on  a.account_number = b.account_number
																				and a.event_id = b.event_id
																				and case when a.overlap_batch is null then 0 else a.overlap_batch end = case when b.overlap_batch is null then 0 else b.overlap_batch end
									where a.overlap_batch is null
									group by
											a.account_number
										,   a.event_id
										,   viewing_seconds
								)   as  t0
							group by account_number
							union all
							select
									account_number
								,   sum(viewing_seconds / 3600.0)   as  viewing_hours
							from
								(
									select
											a.account_number
										,   a.overlap_batch
										,   datediff(second,b.chunk_start,b.chunk_end)              as  viewing_seconds
									from
													tmp_view_m10        as  a
										inner join  tmp_view_m07       	as  b   on  a.account_number = b.account_number
																				and a.event_id = b.event_id
																				and case when a.overlap_batch is null then 0 else a.overlap_batch end = case when b.overlap_batch is null then 0 else b.overlap_batch end
									where a.overlap_batch is not null
									group by
											a.account_number
										,   a.overlap_batch
										,   viewing_seconds
								)   as  t0
							group by account_number
						)   as  t1
					group by account_number
				)   as  t2

		end -- if begin
	else
		message cast(now() as timestamp) || ' | M10b - Required input tables not detected. Exiting.' to client


end -- begin procedure
;
commit;

grant execute on V289_H2I_summarise_avg_viewing to vespa_group_low_security;
commit;



/*

execute V289_H2I_summarise_avg_viewing;

execute V289_H2I_summarise_avg_viewing 'tanghoi.V289_M10_session_individuals' , 'tanghoi.V289_M07_dp_data';

*/
/*
exec V289_M11_01_SC3_v1_1__do_weekly_segmentation '2013-09-19', 26, '2014-07-29' -- thurs, logid, batch date

exec V289_M11_02_SC3_v1_1__prepare_panel_members '2013-09-18', '2013-09-18', '2014-07-29', 26 -- thurs, scaling, batch date, logid
exec V289_M11_03_SC3I_v1_1__add_individual_data '2013-09-19', '2014-07-29', 26  -- Thur, Batch logid

-- exec V289_M11_04_SC3I_v1_1__make_weights '2013-09-23', '2014-07-22', 23
exec V289_M11_04_SC3I_v1_1__make_weights_BARB '2013-09-19', '2013-09-18', '2014-07-29', 26 -- Thur, Scaling, Batch Date, logid

select * from thompsonja.z_logger_events
*/

/***********************************************************************************************************************************************
************************************************************************************************************************************************
******* M11: SKYVIEW INDIVIDUAL AND HOUSEOLD LEVEL SCALING SCRIPT                                                                             *******
************************************************************************************************************************************************
***********************************************************************************************************************************************/


--- Skyview scaling uses 2 of the Scaling 3.0 procedures. See the repository for more details
-- \Git_repository\Vespa\ad_hoc\V154 - Scaling 3.0\Vespa Analysts - SC3\SC3 - 3 - refresh procedures [v1.1].sql

-- These procs prepare the Skybase accounts (to be done once a week for a Thursday) and valid Vespa accounts (to be run each day)
--        SC3_v1_1__do_weekly_segmentation  SKYVIEW VERSION: V289_M11_01_SC3_v1_1__do_weekly_segmentation
--        SC3_v1_1__prepare_panel_members   SKYVIEW VERSION: V289_M11_02_SC3_v1_1__prepare_panel_members


--- A new procedure has been written to add individual level data to the scaling tables
--     V289_M11_03_SC3I_v1_1__add_individual_data

--- An existing Scaling 3.0 proc has been ammended to work for SkyView
-- This proc calculates the weights using a RIM Weighting process
--         SC3_v1_1__make_weights           SKYVIEW VERSION: V289_M11_04_SC3I_v1_1__make_weights




/**************** PART L: WEEKLY SEGMENTATION BUILD ****************/

IF object_id('V289_M11_01_SC3_v1_1__do_weekly_segmentation') IS NOT NULL THEN DROP PROCEDURE V289_M11_01_SC3_v1_1__do_weekly_segmentation END IF;

create procedure V289_M11_01_SC3_v1_1__do_weekly_segmentation
    @profiling_thursday         date = null         -- Day on which to do sky base profiling
    ,@Scale_refresh_logging_ID  bigint = null       -- Might pass the log ID in as an argument if it's a big historical build, otherwise we'll make a new one
    ,@batch_date                datetime = now()    -- Day on which build was kicked off
as
begin

     declare @QA_catcher                 integer         -- For control totals along the way
     declare @tablespacename             varchar(40)

     execute logger_add_event @Scale_refresh_logging_ID, 3, 'SC3: Profiling Sky UK base as of ' || dateformat(@profiling_thursday,'yyyy-mm-dd') || '.'
     commit

     -- Clear out the processing tables and suchlike

     DELETE FROM SC3_scaling_weekly_sample
     COMMIT

     -- Decide when we're doing the profiling, if it's not passed in as a parameter
     if @profiling_thursday is null
     begin
         execute vespa_analysts.Regulars_Get_report_end_date @profiling_thursday output  -- proc returns a Saturday
         set @profiling_thursday = @profiling_thursday - 2                               -- but we want a Thursday
     end
     commit

     -- Get us a refresh logging ID thing if one wasn't assigned
--      if @Scale_refresh_logging_ID is null
--      begin
--          execute Regulars_whats_my_namespace @tablespacename output
--          if @tablespacename = 'vespa_analysts'
--              EXECUTE logger_create_run 'ScalingSegmentation'           , 'SC3: Segmentation build for ' || dateformat(@profiling_thursday, 'yyyy-mm-dd') || '.', @Scale_refresh_logging_ID output
--          else
--          begin
--              set @tablespacename = coalesce(@tablespacename, user)
--              EXECUTE logger_create_run 'SC3 Dev ' || @tablespacename || 'SC3: Segmentation build for  ' || dateformat(@profiling_thursday, 'yyyy-mm-dd') || '.', @Scale_refresh_logging_ID output
--          end
--      end
     commit

     -- So this bit is not stable for the VIQ builds since we can't delete weights from there,
     -- but for dev builds within analytics this is required.
     DELETE FROM SC3_Sky_base_segment_snapshots where profiling_date = @profiling_thursday
     commit

     /**************** L01: ESTABLISH POPULATION ****************/
     -- We need the segmentation over the whole Sky base so we can scale up


     -- Captures all active accounts in cust_subs_hist
     SELECT   account_number
             ,cb_key_household
             ,cb_key_individual
             ,current_short_description
             ,rank() over (PARTITION BY account_number ORDER BY effective_from_dt desc, cb_row_id) AS rank
             ,convert(bit, 0)  AS uk_standard_account
             ,convert(VARCHAR(30), NULL) AS isba_tv_region
       INTO #weekly_sample
       FROM /*sk_prod.*/cust_subs_hist
      WHERE subscription_sub_type IN ('DTV Primary Viewing')
        AND status_code IN ('AC','AB','PC')
        AND effective_from_dt    <= @profiling_thursday
        AND effective_to_dt      > @profiling_thursday
        AND effective_from_dt    <> effective_to_dt
        AND EFFECTIVE_FROM_DT    IS NOT NULL
        AND cb_key_household     > 0
        AND cb_key_household     IS NOT NULL
        AND cb_key_individual    IS NOT NULL
        AND account_number       IS NOT NULL
        AND service_instance_id  IS NOT NULL

     -- De-dupes accounts
     COMMIT
     DELETE FROM #weekly_sample WHERE rank > 1
     COMMIT

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from #weekly_sample

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L01: Midway 1/2 (Weekly sample)', coalesce(@QA_catcher, -1)
     commit

     -- Create indices
     CREATE UNIQUE INDEX fake_pk ON #weekly_sample (account_number)
     CREATE INDEX for_package_join ON #weekly_sample (current_short_description)
     COMMIT

     -- Take out ROIs (Republic of Ireland) and non-standard accounts as these are not currently in the scope of Vespa
     UPDATE #weekly_sample
     SET
         uk_standard_account = CASE
             WHEN b.acct_type='Standard' AND b.account_number <>'?' AND b.pty_country_code ='GBR' THEN 1
             ELSE 0 END
        -- Insert SC3 TV regions
         ,isba_tv_region = case
                when b.isba_tv_region = 'Border' then'NI, Scotland & Border'
                when b.isba_tv_region = 'Central Scotland' then'NI, Scotland & Border'
                when b.isba_tv_region = 'East Of England' then'Wales & Midlands'
                when b.isba_tv_region = 'HTV Wales' then'Wales & Midlands'
                when b.isba_tv_region = 'HTV West' then'South England'
                when b.isba_tv_region = 'London' then'London'
                when b.isba_tv_region = 'Meridian (exc. Channel Islands)' then'South England'
                when b.isba_tv_region = 'Midlands' then'Wales & Midlands'
                when b.isba_tv_region = 'North East' then'North England'
                when b.isba_tv_region = 'North Scotland' then'NI, Scotland & Border'
                when b.isba_tv_region = 'North West' then'North England'
                when b.isba_tv_region = 'Not Defined' then'Not Defined'
                when b.isba_tv_region = 'South West' then'South England'
                when b.isba_tv_region = 'Ulster' then'NI, Scotland & Border'
                when b.isba_tv_region = 'Yorkshire' then'North England'
                else 'Not Defined'
          end
--          ,isba_tv_region = b.isba_tv_region
         -- Grab the cb_key_individual we need for consumerview linkage at the same time
         ,cb_key_individual = b.cb_key_individual
     FROM #weekly_sample AS a
     inner join /*sk_prod.*/cust_single_account_view AS b
     ON a.account_number = b.account_number

     COMMIT
     DELETE FROM #weekly_sample WHERE uk_standard_account=0
     COMMIT

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from #weekly_sample

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L01: Complete! (Population)', coalesce(@QA_catcher, -1)
     commit

     -- hahaha, no, there are lots of key dupes here, we're just going to
     -- have to cull them during the piping into the VIQ build
     --CREATE UNIQUE INDEX for_ilu_joining ON #weekly_sample (cb_key_household)
     --commit

     /**************** L02: ASSIGN VARIABLES ****************/
     -- Since "h_household_composition" & "p_head_of_household" are now in two separate tables, an intemidiary table is created
     -- so both variables are available for ranking function in the next step
     SELECT
          cv.cb_key_household,
          cv.cb_key_family,
          cv.cb_key_individual,
          min(cv.cb_row_id) as cb_row_id,
          max(cv.h_household_composition) as h_household_composition,
          max(pp.p_head_of_household) as p_head_of_household
      INTO #cv_pp
      FROM /*sk_prod.*/EXPERIAN_CONSUMERVIEW cv,
           /*sk_prod.*/PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD pp
     WHERE cv.exp_cb_key_db_individual = pp.exp_cb_key_db_individual
       AND cv.cb_key_individual is not null
     GROUP BY cv.cb_key_household, cv.cb_key_family, cv.cb_key_individual
     COMMIT

     CREATE LF INDEX idx1 on #cv_pp(p_head_of_household)
     CREATE HG INDEX idx2 on #cv_pp(cb_key_family)
     CREATE HG INDEX idx3 on #cv_pp(cb_key_individual)

     -- We grabbed the cb_key_individual mark from SAV in the previuos build, so
     -- now we need the ConsumerView treatment from the customer group wiki:
     -- Update for SC3 build. Use case statement to consolidate old scaling segments into new.

     -- Approx 25% of accounts do not match when using cb_key_individual meaning these default to D) Uncassified
     -- Instead match at household level

/*     SELECT   cb_key_individual
             ,cb_row_id
             ,rank() over(partition by cb_key_family     ORDER BY p_head_of_household desc,  cb_row_id desc) as rank_fam
             ,rank() over(partition by cb_key_individual ORDER BY p_head_of_household desc,  cb_row_id desc) as rank_ind
             ,case
                     when h_household_composition = '00' then 'A) Families'
                     when h_household_composition = '01' then 'A) Families'
                     when h_household_composition = '02' then 'A) Families'
                     when h_household_composition = '03' then 'A) Families'
                     when h_household_composition = '04' then 'B) Singles'
                     when h_household_composition = '05' then 'B) Singles'
                     when h_household_composition = '06' then 'C) Homesharers'
                     when h_household_composition = '07' then 'C) Homesharers'
                     when h_household_composition = '08' then 'C) Homesharers'
                     when h_household_composition = '09' then 'A) Families'
                     when h_household_composition = '10' then 'A) Families'
                     when h_household_composition = '11' then 'C) Homesharers'
                     when h_household_composition = 'U'  then 'D) Unclassified HHComp'
                     else 'D) Unclassified HHComp'
             end as h_household_composition
--              ,h_household_composition -- may as well pull out the item we need given we're ranking and deleting
     INTO #cv_keys
     FROM #cv_pp
     WHERE cb_key_individual IS not NULL
       AND cb_key_individual <> 0
*/

     SELECT   cb_key_household
             ,cb_row_id
             ,rank() over(partition by cb_key_family     ORDER BY p_head_of_household desc,  cb_row_id desc) as rank_fam
             ,rank() over(partition by cb_key_household ORDER BY p_head_of_household desc,  cb_row_id desc) as rank_hhd
             ,case
                     when h_household_composition = '00' then 'A) Families'
                     when h_household_composition = '01' then 'A) Families'
                     when h_household_composition = '02' then 'A) Families'
                     when h_household_composition = '03' then 'A) Families'
                     when h_household_composition = '04' then 'B) Singles'
                     when h_household_composition = '05' then 'B) Singles'
                     when h_household_composition = '06' then 'C) Homesharers'
                     when h_household_composition = '07' then 'C) Homesharers'
                     when h_household_composition = '08' then 'C) Homesharers'
                     when h_household_composition = '09' then 'A) Families'
                     when h_household_composition = '10' then 'A) Families'
                     when h_household_composition = '11' then 'C) Homesharers'
                     when h_household_composition = 'U'  then 'D) Unclassified HHComp'
                     else 'D) Unclassified HHComp'
             end as h_household_composition
--              ,h_household_composition -- may as well pull out the item we need given we're ranking and deleting
     INTO #cv_keys
     FROM #cv_pp
--     WHERE cb_key_individual IS not NULL
--       AND cb_key_individual <> 0
     WHERE cb_key_household IS not NULL
     AND cb_key_household <> 0






     -- This is a cleaned out version of http://mktskyportal/Campaign%20Handbook/ConsumerView.aspx
     -- since we only need the individual stuff for this linkage.

     commit
--     DELETE FROM #cv_keys WHERE rank_fam != 1 AND rank_ind != 1
       DELETE FROM #cv_keys WHERE rank_fam != 1 AND rank_hhd != 1
     commit

--     CREATE INDEX index_ac on #cv_keys (cb_key_individual)
       CREATE INDEX index_ac on #cv_keys (cb_key_household)
     COMMIT

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from #cv_keys

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 1/8 (Consumerview Linkage)', coalesce(@QA_catcher, -1)
     commit

     -- Populate Package & ISBA TV Region

     INSERT INTO SC3_scaling_weekly_sample (
         account_number
         ,cb_key_household
         ,cb_key_individual
         ,universe    --scaling variables removed. Use later to set no_of_stbs
         ,sky_base_universe  -- Need to include this as they form part of a big index
         ,vespa_universe  -- Need to include this as they form part of a big index
         ,isba_tv_region
         ,hhcomposition
         ,tenure
         ,num_mix
         ,mix_pack
         ,package
         ,boxtype
         ,no_of_stbs
         ,hd_subscription
         ,pvr
     )
     SELECT
         fbp.account_number
         ,fbp.cb_key_household
         ,fbp.cb_key_individual
         ,'A) Single box HH' -- universe
         ,'Not adsmartable'  -- sky_base_universe
         ,'Non-Vespa'   -- Vespa Universe
         ,fbp.isba_tv_region -- isba_tv_region
         ,'D)'  -- hhcomposition
         ,'D) Unknown' -- tenure
         ,cel.Variety + cel.Knowledge + cel.Kids + cel.Style_Culture + cel.Music + cel.News_Events as num_mix
         ,CASE
                         WHEN Num_Mix IS NULL OR Num_Mix=0                           THEN 'Entertainment Pack'
                         WHEN (cel.variety=1 OR cel.style_culture=1)  AND Num_Mix=1  THEN 'Entertainment Pack'
                         WHEN (cel.variety=1 AND cel.style_culture=1) AND Num_Mix=2  THEN 'Entertainment Pack'
                         WHEN Num_Mix > 0                                            THEN 'Entertainment Extra'
                     END AS mix_pack -- Basic package has recently been split into the Entertainment and Entertainment Extra packs
         ,CASE
             WHEN cel.prem_sports = 2 AND cel.prem_movies = 2 THEN 'Movies & Sports' --'Top Tier'
             WHEN cel.prem_sports = 2 AND cel.prem_movies = 0 THEN 'Sports' --'Dual Sports'
             WHEN cel.prem_sports = 0 AND cel.prem_movies = 2 THEN 'Movies' --'Dual Movies'
             WHEN cel.prem_sports = 1 AND cel.prem_movies = 0 THEN 'Sports' --'Single Sports'
             WHEN cel.prem_sports = 0 AND cel.prem_movies = 1 THEN 'Movies' --'Single Movies'
             WHEN cel.prem_sports > 0 OR  cel.prem_movies > 0 THEN 'Movies & Sports' --'Other Premiums'
             WHEN cel.prem_movies = 0 AND cel.prem_sports = 0 AND mix_pack = 'Entertainment Pack'  THEN 'Basic' --'Basic - Ent'
             WHEN cel.prem_movies = 0 AND cel.prem_sports = 0 AND mix_pack = 'Entertainment Extra' THEN 'Basic' --'Basic - Ent Extra'
             ELSE 'Basic' END --                                                  'Basic - Ent' END -- package
          ,'D) FDB & No_secondary_box' -- boxtype
          ,'Single' --no_of_stbs
          ,'No' --hd_subscription
          ,'No' --pvr
     FROM #weekly_sample AS fbp
     left join /*sk_prod.*/cust_entitlement_lookup AS cel
         ON fbp.current_short_description = cel.short_description
     WHERE fbp.cb_key_household IS NOT NULL
       AND fbp.cb_key_individual IS NOT NULL

     commit
     drop table #weekly_sample
     commit

     -- Populate sky_base_universe according to SQL code used to find adsmartable bozes in weekly reports
     select  account_number
            ,case
                when flag = 1 and cust_viewing_data_capture_allowed = 'Y' then 'Adsmartable with consent'
                when flag = 1 and cust_viewing_data_capture_allowed <> 'Y' then 'Adsmartable but no consent'
                else 'Not adsmartable'
                end as sky_base_universe
        into  #cv_sbu
        from (
                 select  sav.account_number as account_number, adsmart.flag, cust_viewing_data_capture_allowed
                    from    (
                                select      distinct account_number, cust_viewing_data_capture_allowed
                                     from   /*sk_prod.*/CUST_SINGLE_ACCOUNT_VIEW
                                    where   CUST_ACTIVE_DTV = 1                     -- this field implies -> prod_latest_dtv_status_code IN ('AC','AB','PC')
                                      and   pty_country_code = 'GBR'
                            )as sav
                                    left join       (
                                            ----------------------------------------------------------
                                            -- B03: Flag Adsmartable boxes based on Adsmart definition
                                            ----------------------------------------------------------
                                                select  account_number
                                                                ,max(   CASE    WHEN x_pvr_type ='PVR6'                                 THEN 1
                                                                                WHEN x_pvr_type ='PVR5'                                 THEN 1
                                                                                WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Samsung'  THEN 1
                                                                                WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Pace'     THEN 1
                                                                                ELSE 0
                                                                                END) AS flag
                                                from    (
                                                        --------------------------------------------------------------------------
                                                        -- B02: Extracting Active Boxes per account (one line per box per account)
                                                        --------------------------------------------------------------------------
                                                        select  *
                                                        from    (
                                                                --------------------------------------------------------------------
                                                                -- B01: Ranking STB based on service instance id to dedupe the table
                                                                --------------------------------------------------------------------
                                                                Select  account_number
                                                                                ,x_pvr_type
                                                                                ,x_personal_storage_capacity
                                                                                ,currency_code
                                                                                ,x_manufacturer
                                                                                ,rank () over (partition by service_instance_id order by ph_non_subs_link_sk desc) active_flag
                                                                from    /*sk_prod.*/CUST_SET_TOP_BOX

                                                        )       as base
                                                        where   active_flag = 1

                                                )       as active_boxes
                                        where   currency_code = 'GBP'
                                        group   by      account_number

                                    )       as adsmart
                                    on      sav.account_number = adsmart.account_number
        ) as sub1
     commit

     UPDATE SC3_scaling_weekly_sample
     SET
         stws.sky_base_universe = cv.sky_base_universe
     FROM SC3_scaling_weekly_sample AS stws
     inner join #cv_sbu AS cv
     ON stws.account_number = cv.account_number

--      -- Update vespa universe from SC3_account_universe
--      UPDATE SC3_scaling_weekly_sample
--      SET    samp.vespa_universe     = acc.vespa_universe
--      FROM   SC3_scaling_weekly_sample samp
--      INNER JOIN SC3_account_universe acc
--      ON    samp.account_number = acc.account_number

     -- Update vespa universe
     UPDATE SC3_scaling_weekly_sample
     SET    vespa_universe     =
              case
                when sky_base_universe = 'Not adsmartable' then 'Vespa not Adsmartable'
                when sky_base_universe = 'Adsmartable with consent' then 'Vespa adsmartable'
                when sky_base_universe = 'Adsmartable but no consent' then 'Vespa but no consent'
                else 'Non-Vespa'
              end
        commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC3_scaling_weekly_sample
     where sky_base_universe is not null and vespa_universe is not null

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 2a/8 (Accounts with no universe)', coalesce(@QA_catcher, -1)
     commit

     delete from SC3_scaling_weekly_sample
     where sky_base_universe is null or vespa_universe is null

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC3_scaling_weekly_sample

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 2/8 (Package & ISBA region)', coalesce(@QA_catcher, -1)
     commit

     -- HHcomposition

     UPDATE SC3_scaling_weekly_sample
     SET
         stws.hhcomposition = cv.h_household_composition
     FROM SC3_scaling_weekly_sample AS stws
     inner join #cv_keys AS cv
     -- ON stws.cb_key_individual = cv.cb_key_individual
     ON stws.cb_key_household = cv.cb_key_household

     commit
     drop table #cv_keys
     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC3_scaling_weekly_sample
     where left(hhcomposition, 2) <> 'D)'

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 3/8 (HH composition)', coalesce(@QA_catcher, -1)
     commit

     -- Tenure

     -- Tenure has been grouped according to its relationship with viewing behaviour

     UPDATE SC3_scaling_weekly_sample t1
     SET
         tenure = CASE   WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) <=  730 THEN 'A) 0-2 Years'
                         WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) <= 3650 THEN 'B) 3-10 Years'
                         WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) > 3650 THEN  'C) 10 Years+'
                         ELSE 'D) Unknown'
                  END
     FROM /*sk_prod.*/cust_single_account_view sav
     WHERE t1.account_number=sav.account_number
     COMMIT

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC3_scaling_weekly_sample
     where tenure <> 'D) Unknown'

     -- Added SC3 line to remove Unknown tenure
     delete from SC3_scaling_weekly_sample
     where tenure = 'D) Unknown'

     -- Added SC3 line to remove Not Defined region
     delete from SC3_scaling_weekly_sample
     where isba_tv_region = 'Not Defined'


     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 4/8 (Tenure)', coalesce(@QA_catcher, -1)
     commit

     -- Boxtype & Universe

     -- Boxtype is defined as the top two boxtypes held by a household ranked in the following order
     -- 1) HD, 2) HDx, 3) Skyplus, 4) FDB

     -- Capture all active boxes for this week
     SELECT    csh.service_instance_id
             , csh.account_number
             , subscription_sub_type
             , rank() over (PARTITION BY csh.service_instance_id ORDER BY csh.account_number, csh.cb_row_id desc) AS rank
       INTO #accounts -- drop table #accounts
       FROM /*sk_prod.*/cust_subs_hist as csh
             inner join SC3_scaling_weekly_sample AS ss ON csh.account_number = ss.account_number
      WHERE  csh.subscription_sub_type IN ('DTV Primary Viewing','DTV Extra Subscription')     --the DTV sub Type
        AND csh.status_code IN ('AC','AB','PC')                  --Active Status Codes
        AND csh.effective_from_dt <= @profiling_thursday
        AND csh.effective_to_dt > @profiling_thursday
        AND csh.effective_from_dt<>effective_to_dt

     -- De-dupe active boxes
     DELETE FROM #accounts WHERE rank>1
     COMMIT

     -- Create indices on list of boxes
     CREATE hg INDEX idx1 ON #accounts(service_instance_id)
     CREATE hg INDEX idx2 ON #accounts(account_number)
     commit

     -- Identify HD & 1TB/2TB HD boxes
     SELECT  stb.service_instance_id
            ,SUM(CASE WHEN current_product_description LIKE '%HD%' THEN 1
                     ELSE 0
                  END) AS HD
            ,SUM(CASE WHEN x_description IN ('Amstrad HD PVR6 (1TB)', 'Amstrad HD PVR6 (2TB)') THEN 1
                     ELSE 0
                  END) AS HD1TB
     INTO #hda -- drop table #hda
     FROM /*sk_prod.*/CUST_SET_TOP_BOX AS stb INNER JOIN #accounts AS acc
                                                  ON stb.service_instance_id = acc.service_instance_id
     WHERE box_installed_dt <= @profiling_thursday
     AND box_replaced_dt   > @profiling_thursday
     AND current_product_description like '%HD%'
     GROUP BY stb.service_instance_id

     -- Create index on HD table
     COMMIT
     CREATE UNIQUE hg INDEX idx1 ON #hda(service_instance_id)
     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from #hda

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 5/8 (HD boxes)', coalesce(@QA_catcher, -1)
     commit

     -- Identify PVR boxes
     SELECT  acc.account_number
            ,MAX(CASE WHEN x_box_type LIKE '%Sky+%' THEN 'Yes'
                     ELSE 'No'
                  END) AS PVR
     INTO #pvra -- drop table #pvra
     FROM /*sk_prod.*/CUST_SET_TOP_BOX AS stb INNER JOIN #accounts AS acc
                                                  ON stb.service_instance_id = acc.service_instance_id
     WHERE box_installed_dt <= @profiling_thursday
     AND box_replaced_dt   > @profiling_thursday
     GROUP by acc.account_number

     -- Create index on PVR table
     COMMIT
     CREATE hg INDEX pvidx1 ON #pvra(account_number)
     commit

     -- PVR
     UPDATE SC3_scaling_weekly_sample
     SET
         stws.pvr = cv.pvr
     FROM SC3_scaling_weekly_sample AS stws
     inner join #pvra AS cv
     ON stws.account_number = cv.account_number

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from #pvra

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 6/8 (PVR boxes)', coalesce(@QA_catcher, -1)
     commit

       -- Set default value when account cannot be found
      update SC3_scaling_weekly_sample
         set pvr = case
                when sky_base_universe like 'Adsmartable%' then 'Yes'
                else 'No'
         end
       where pvr is null
      commit

     --Further check to ensure that when PVR is No then the box is Not Adsmartable
     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC3_scaling_weekly_sample
     where pvr = 'No' and sky_base_universe like 'Adsmartable%'

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 6a/8 (Non-PVR boxes which are adsmartable)', coalesce(@QA_catcher, -1)
     commit

       -- Update PVR when PVR says 'No' and universe is an adsmartable one.
      update SC3_scaling_weekly_sample
         set pvr = 'Yes'
       where pvr = 'No' and sky_base_universe like 'Adsmartable%'
      commit

     SELECT  --acc.service_instance_id,
            acc.account_number
            ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Extra Subscription' THEN 1 ELSE 0  END) AS MR
            ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Sky+'               THEN 1 ELSE 0  END) AS SP
            ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV HD'                 THEN 1 ELSE 0  END) AS HD
            ,MAX(CASE  WHEN #hda.HD = 1                                         THEN 1 ELSE 0  END) AS HDstb
            ,MAX(CASE  WHEN #hda.HD1TB = 1                                      THEN 1 ELSE 0  END) AS HD1TBstb
     INTO #scaling_box_level_viewing
     FROM /*sk_prod.*/cust_subs_hist AS csh
            INNER JOIN #accounts AS acc ON csh.service_instance_id = acc.service_instance_id --< Limits to your universe
            LEFT OUTER JOIN /*sk_prod.*/cust_entitlement_lookup cel
                            ON csh.current_short_description = cel.short_description
            LEFT OUTER JOIN #hda ON csh.service_instance_id = #hda.service_instance_id --< Links to the HD Set Top Boxes
      WHERE csh.effective_FROM_dt <= @profiling_thursday
        AND csh.effective_to_dt    > @profiling_thursday
        AND csh.status_code IN  ('AC','AB','PC')
        AND csh.SUBSCRIPTION_SUB_TYPE IN ('DTV Primary Viewing','DTV Sky+', 'DTV Extra Subscription','DTV HD' )
        AND csh.effective_FROM_dt <> csh.effective_to_dt
     GROUP BY acc.service_instance_id ,acc.account_number

     commit
     drop table #accounts
     drop table #hda
     commit

     -- Identify boxtype of each box and whether it is a primary or a secondary box
     SELECT  tgt.account_number
            ,SUM(CASE WHEN MR=1 THEN 1 ELSE 0 END) AS mr_boxes
            ,MAX(CASE WHEN MR=0 AND ((tgt.HD =1 AND HD1TBstb = 1) OR (tgt.HD =1 AND HDstb = 1))         THEN 4 -- HD ( inclusive of HD1TB)
                      WHEN MR=0 AND ((tgt.SP =1 AND tgt.HD1TBstb = 1) OR (tgt.SP =1 AND tgt.HDstb = 1)) THEN 3 -- HDx ( inclusive of HD1TB)
                      WHEN MR=0 AND tgt.SP =1                                                           THEN 2 -- Skyplus
                      ELSE                                                                              1 END) AS pb -- FDB
            ,MAX(CASE WHEN MR=1 AND ((tgt.HD =1 AND HD1TBstb = 1) OR (tgt.HD =1 AND HDstb = 1))         THEN 4 -- HD ( inclusive of HD1TB)
                      WHEN MR=1 AND ((tgt.SP =1 AND tgt.HD1TBstb = 1) OR (tgt.SP =1 AND tgt.HDstb = 1)) THEN 3 -- HDx ( inclusive of HD1TB)
                      WHEN MR=1 AND tgt.SP =1                                                           THEN 2 -- Skyplus
                      ELSE                                                                              1 END) AS sb -- FDB
             ,convert(varchar(20), null) as universe
             ,convert(varchar(30), null) as boxtype
       INTO #boxtype_ac -- drop table #boxtype_ac
       FROM #scaling_box_level_viewing AS tgt
     GROUP BY tgt.account_number

     -- Create indices on box-level boxtype temp table
     COMMIT
     CREATE unique INDEX idx_ac ON #boxtype_ac(account_number)
     drop table #scaling_box_level_viewing
     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from #boxtype_ac

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Midway 6/8 (P/S boxes)', coalesce(@QA_catcher, -1)
     commit

     -- Build the combined flags
     update #boxtype_ac
     set universe = CASE WHEN mr_boxes = 0 THEN 'A) Single box HH'
                              ELSE 'B) Multiple box HH' END
         ,boxtype  =
             CASE WHEN       mr_boxes = 0 AND  pb =  3 AND sb =  1   THEN  'A) HDx & No_secondary_box'
                  WHEN       mr_boxes = 0 AND  pb =  4 AND sb =  1   THEN  'B) HD & No_secondary_box'
                  WHEN       mr_boxes = 0 AND  pb =  2 AND sb =  1   THEN  'C) Skyplus & No_secondary_box'
                  WHEN       mr_boxes = 0 AND  pb =  1 AND sb =  1   THEN  'D) FDB & No_secondary_box'
                  WHEN       mr_boxes > 0 AND  pb =  4 AND sb =  4   THEN  'E) HD & HD' -- If a hh has HD  then all boxes have HD (therefore no HD and HDx)
                  WHEN       mr_boxes > 0 AND (pb =  4 AND sb =  3) OR (pb =  3 AND sb =  4)  THEN  'E) HD & HD'
                  WHEN       mr_boxes > 0 AND (pb =  4 AND sb =  2) OR (pb =  2 AND sb =  4)  THEN  'F) HD & Skyplus'
                  WHEN       mr_boxes > 0 AND (pb =  4 AND sb =  1) OR (pb =  1 AND sb =  4)  THEN  'G) HD & FDB'
                  WHEN       mr_boxes > 0 AND  pb =  3 AND sb =  3                            THEN  'H) HDx & HDx'
                  WHEN       mr_boxes > 0 AND (pb =  3 AND sb =  2) OR (pb =  2 AND sb =  3)  THEN  'I) HDx & Skyplus'
                  WHEN       mr_boxes > 0 AND (pb =  3 AND sb =  1) OR (pb =  1 AND sb =  3)  THEN  'J) HDx & FDB'
                  WHEN       mr_boxes > 0 AND  pb =  2 AND sb =  2                            THEN  'K) Skyplus & Skyplus'
                  WHEN       mr_boxes > 0 AND (pb =  2 AND sb =  1) OR (pb =  1 AND sb =  2)  THEN  'L) Skyplus & FDB'
                             ELSE   'M) FDB & FDB' END

     commit

     /* Now building this differently; Sybase 15 didn't like it at all, even had trouble killing
     ** the thread after the query is cancelled. This weirdness has been replicated in other schemas
     ** and on the QA server and has been raised to Sybase. Apparently it's a known bug and there's
     ** a patch coming in 15.4, but for the meantime this method remains commented out even though
     ** the workaround is amazingly ugly...pvr
     UPDATE SC3_scaling_weekly_sample
     SET
         universe    = ac.universe
         ,boxtype    = ac.boxtype
         ,mr_boxes   = ac.mr_boxes
     FROM SC3_scaling_weekly_sample
     inner join #boxtype_ac AS ac
     on ac.account_number = SC3_scaling_weekly_sample.account_number
     */

     CREATE TABLE #SC3_weird_sybase_update_workaround (
          account_number                     VARCHAR(20)     primary key
         ,cb_key_household                   BIGINT          not null
         ,cb_key_individual                  BIGINT          not null
         ,consumerview_cb_row_id             BIGINT
         ,universe                           VARCHAR(30)                         -- Single or multiple box household. Reused for no_of_stbs
         ,sky_base_universe                  VARCHAR(30)                         -- Not adsmartable, Adsmartable with consent, Adsmartable but no consent household
         ,vespa_universe                     VARCHAR(30)                         -- Non-Vespa, Not Adsmartable, Vespa with consent, vespa but no consent household
         ,weighting_universe                 VARCHAR(30)                         -- Used when finding appropriate scaling segment - see note
         ,isba_tv_region                     VARCHAR(30)                         -- Scaling variable 1 : Region
         ,hhcomposition                      VARCHAR(2)      default 'D)'        -- Scaling variable 2: Household composition
         ,tenure                             VARCHAR(15)     DEFAULT 'D) Unknown'-- Scaling variable 3: Tenure
         ,num_mix                            INT
         ,mix_pack                           VARCHAR(20)
         ,package                            VARCHAR(20)                         -- Scaling variable 4: Package
         ,boxtype                            VARCHAR(35)                         -- Old Scaling variable 5: Household boxtype split into no_of_stbs, hd_subscription and pvr.
         ,no_of_stbs                         VARCHAR(15)                         -- Scaling variable 5: No of set top boxes
         ,hd_subscription                    VARCHAR(5)                          -- Scaling variable 6: HD subscription
         ,pvr                                VARCHAR(5)                          -- Scaling variable 7: Is the box pvr capable?
         ,population_scaling_segment_id      INT             DEFAULT NULL        -- segment scaling id for identifying segments
         ,vespa_scaling_segment_id           INT             DEFAULT NULL        -- segment scaling id for identifying segments
         ,mr_boxes                           INT
     --    ,complete_viewing                   TINYINT         DEFAULT 0           -- Flag for all accounts with complete viewing data
     )

     CREATE INDEX for_segment_identification_temp1 ON #SC3_weird_sybase_update_workaround (isba_tv_region)
     CREATE INDEX for_segment_identification_temp2 ON #SC3_weird_sybase_update_workaround (hhcomposition)
     CREATE INDEX for_segment_identification_temp3 ON #SC3_weird_sybase_update_workaround (tenure)
     CREATE INDEX for_segment_identification_temp4 ON #SC3_weird_sybase_update_workaround (package)
     CREATE INDEX for_segment_identification_temp5 ON #SC3_weird_sybase_update_workaround (boxtype)
     CREATE INDEX consumerview_joining ON #SC3_weird_sybase_update_workaround (consumerview_cb_row_id)
     CREATE INDEX for_temping1 ON #SC3_weird_sybase_update_workaround (population_scaling_segment_id)
     CREATE INDEX for_temping2 ON #SC3_weird_sybase_update_workaround (vespa_scaling_segment_id)
     COMMIT

     insert into #SC3_weird_sybase_update_workaround (
          account_number
         ,cb_key_household
         ,cb_key_individual
         ,consumerview_cb_row_id
         ,universe
         ,sky_base_universe
         ,vespa_universe
         ,isba_tv_region
         ,hhcomposition
         ,tenure
         ,num_mix
         ,mix_pack
         ,package
         ,boxtype
         ,mr_boxes
     )
     select
          sws.account_number
         ,sws.cb_key_household
         ,sws.cb_key_individual
         ,sws.consumerview_cb_row_id
         ,ac.universe
         ,sky_base_universe
         ,vespa_universe
         ,sws.isba_tv_region
         ,sws.hhcomposition
         ,sws.tenure
         ,sws.num_mix
         ,sws.mix_pack
         ,sws.package
         ,ac.boxtype
         ,ac.mr_boxes
     from SC3_scaling_weekly_sample as sws
     inner join #boxtype_ac AS ac
     on ac.account_number = sws.account_number
     WHERE sws.cb_key_household IS NOT NULL
       AND sws.cb_key_individual IS NOT NULL

     -- Update SC3 scaling variables in #SC3_weird_sybase_update_workaround according to Scaling 3.0 variables
     update #SC3_weird_sybase_update_workaround sws
             set sws.pvr = ac.pvr
            from #pvra AS ac
           where ac.account_number = sws.account_number

     -- This data is eventually going to go back into the SC3_scaling_weekly_sample,
     -- but there's some weird Sybase bug at the moment that means that updates don't
     -- work. And then the sessions can't be cancelled, for some bizarre reason.

     commit
     drop table #boxtype_ac
     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from #SC3_weird_sybase_update_workaround

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L02: Complete! (Variables)', coalesce(@QA_catcher, -1)
     commit

      /**************** L03: ASSIGN SCALING SEGMENT ID ****************/

     -- The SC3_Segments_lookup table can be used to append a segment_id to
     -- the SC3_scaling_weekly_sample table by matching on universe and each of the
     -- seven scaling variables (hhcomposition, isba_tv_region, package, boxtype, tenure, no_of_stbs, hd_subscription and pvr)

     -- Commented out code is for when we were looking to create a proxy group using adsmartable accounts to mimic those adsmartable
     -- accounts that had not given viewing consent. Code is kept here jsut in case we need to revert back to this method.

--      update #SC3_weird_sybase_update_workaround
--      SET    samp.sky_base_universe  = acc.sky_base_universe
--            ,samp.vespa_universe     = acc.vespa_universe
--      FROM   #SC3_weird_sybase_update_workaround samp
--      INNER JOIN SC3_account_universe acc
--      ON    samp.account_number = acc.account_number

     --Set default sky_base_universe, if, for some reason, it is null
     UPDATE #SC3_weird_sybase_update_workaround
        SET  sky_base_universe = 'Not adsmartable'
                where sky_base_universe is null

     UPDATE #SC3_weird_sybase_update_workaround
        SET  vespa_universe = 'Non-Vespa'
                where sky_base_universe is null

     UPDATE #SC3_weird_sybase_update_workaround
        SET  weighting_universe = 'Not adsmartable'
                where weighting_universe is null

--      UPDATE #SC3_weird_sybase_update_workaround
--         SET  weighting_universe = case
--                 when vespa_universe = 'Vespa but no consent' then 'Adsmartable but no consent'
--                 else sky_base_universe
--                 end

      -- Set default value when account cannot be found
      update #SC3_weird_sybase_update_workaround
         set pvr = case
                when sky_base_universe like 'Adsmartable%' then 'Yes'
                else 'No'
         end
       where pvr is null
      commit

       -- Update PVR when PVR says 'No' and universe is an adsmartable one.
      update #SC3_weird_sybase_update_workaround
         set pvr = 'Yes'
       where pvr = 'No' and sky_base_universe like 'Adsmartable%'
      commit

     update #SC3_weird_sybase_update_workaround
             set no_of_stbs =
             case
                when Universe like '%Single%' then 'Single'
                when Universe like '%Multiple%' then 'Multiple'
                else 'Single'
                end

     update #SC3_weird_sybase_update_workaround
             set hd_subscription =
             case
                when boxtype like 'B)%' or boxtype like 'E)%' or boxtype like 'F)%' or boxtype like 'G)%' then 'Yes'
                else 'No'
                end

     commit

     UPDATE #SC3_weird_sybase_update_workaround
        SET #SC3_weird_sybase_update_workaround.population_scaling_segment_ID = ssl.scaling_segment_ID
       FROM #SC3_weird_sybase_update_workaround
             inner join vespa_analysts.SC3_Segments_lookup_v1_1 AS ssl
                                  ON trim(lower(#SC3_weird_sybase_update_workaround.sky_base_universe)) = trim(lower(ssl.sky_base_universe))
                                 AND left(#SC3_weird_sybase_update_workaround.hhcomposition, 2)  = left(ssl.hhcomposition, 2)
                                 AND left(#SC3_weird_sybase_update_workaround.isba_tv_region, 20) = left(ssl.isba_tv_region, 20)
                                 AND #SC3_weird_sybase_update_workaround.Package        = ssl.Package
                                 AND left(#SC3_weird_sybase_update_workaround.tenure, 2)         = left(ssl.tenure, 2)
                                 AND #SC3_weird_sybase_update_workaround.no_of_stbs     = ssl.no_of_stbs
                                 AND #SC3_weird_sybase_update_workaround.hd_subscription = ssl.hd_subscription
                                 AND #SC3_weird_sybase_update_workaround.pvr            = ssl.pvr

     UPDATE #SC3_weird_sybase_update_workaround
        SET vespa_scaling_segment_id = population_scaling_segment_ID

--      UPDATE #SC3_weird_sybase_update_workaround
--         SET #SC3_weird_sybase_update_workaround.vespa_scaling_segment_id = ssl.scaling_segment_ID
--        FROM #SC3_weird_sybase_update_workaround
--              inner join vespa_analysts.SC3_Segments_lookup_v1_1 AS ssl
--                                   ON trim(lower(#SC3_weird_sybase_update_workaround.weighting_universe)) = trim(lower(ssl.sky_base_universe))
--                                  AND left(#SC3_weird_sybase_update_workaround.hhcomposition, 2)  = left(ssl.hhcomposition, 2)
--                                  AND left(#SC3_weird_sybase_update_workaround.isba_tv_region, 20) = left(ssl.isba_tv_region, 20)
--                                  AND #SC3_weird_sybase_update_workaround.Package        = ssl.Package
--                                  AND left(#SC3_weird_sybase_update_workaround.tenure, 2)         = left(ssl.tenure, 2)
--                                  AND #SC3_weird_sybase_update_workaround.no_of_stbs     = ssl.no_of_stbs
--                                  AND #SC3_weird_sybase_update_workaround.hd_subscription = ssl.hd_subscription
--                                  AND #SC3_weird_sybase_update_workaround.pvr            = ssl.pvr

     COMMIT

     -- Just checked one manual build, none of these are null, it should all work fine.

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from #SC3_weird_sybase_update_workaround
     where population_scaling_segment_ID is not null

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L03a: Midway (Population Segment lookup)', coalesce(@QA_catcher, -1)
     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from #SC3_weird_sybase_update_workaround
     where vespa_scaling_segment_id is not null

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L03b: Midway (Weighting Segment lookup)', coalesce(@QA_catcher, -1)
     commit

     -- Okay, no throw all of that back into the weekly sample table, because that's where
     -- the build expects it to be, were it not for that weird bug in Sybase:

     delete from SC3_scaling_weekly_sample
     commit

     insert into SC3_scaling_weekly_sample
     select *
     from #SC3_weird_sybase_update_workaround

     commit
     drop table #SC3_weird_sybase_update_workaround

     commit
     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC3_scaling_weekly_sample
     where population_scaling_segment_ID is not null and vespa_scaling_segment_id is not null
     commit

     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L03: Complete! (Segment ID assignment)', coalesce(@QA_catcher, -1)
     commit

     /**************** L04: PUBLISHING INTO INTERFACE STRUCTURES ****************/

     -- First off we need the accounts and their scaling segmentation IDs: generating
     -- some 10M such records a week, but we'd be able to cull them once we've finished
     -- the associated scaling builds. Only need to maintain it while we still have
     -- historic builds to do.

     insert into SC3_Sky_base_segment_snapshots
     select
         account_number
         ,@profiling_thursday
         ,cb_key_household   -- This guy still needs to be added to SC3_scaling_weekly_sample
         ,population_scaling_segment_id
         ,vespa_scaling_segment_id
         ,mr_boxes+1         -- Number of multiroom boxes plus 1 for the primary
     from SC3_scaling_weekly_sample
     where population_scaling_segment_id is not null and vespa_scaling_segment_id is not null -- still perhaps with the weird account from Eire?

     commit
     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC3_Sky_base_segment_snapshots
     where profiling_date = @profiling_thursday

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'L04: Complete! (Segments published)', coalesce(@QA_catcher, -1)
     commit

     -- We were summarising into scaling_segment_id and sky base count, but now we're
     -- doing that later during the actual weights build rather than keeping all the
     -- differrent profile date builds concurrent, we can recover it easily from the
     -- weekly segmentation anyway.

     -- Don't need to separately track the build dates, since they're on the interface
     -- tables and we'll just rely on those. (Gives us no visiblity of overwrites, but
     -- hey, it's okay, those only happen when stuff is being muddled with anyway.)

     execute logger_add_event @Scale_refresh_logging_ID, 3, 'SC3: base segmentation complete!'
     commit

end; -- of procedure "V289_M11_01_SC3_v1_1__do_weekly_segmentation"
commit;


/**************** PART A: PLACEHOLDER FOR VIRTUAL PANEL BALANCE ****************/

-- This section nominally decides which boxes are considered to be on the panel
-- for each day. There could be a bunch of influences here:
--   * Completeness of returned data in multiroom households
--   * Regularity of returned data for panel stability / box reliability
--   * Virtual panel balance decisions (using the wekly segmentation) - NYIP
-- The output is a table of account numbers and scaling segment IDs. Which is
-- the other reason why it depends on the segmentation build.
IF object_id('V289_M11_02_SC3_v1_1__prepare_panel_members') IS NOT NULL THEN DROP PROCEDURE V289_M11_02_SC3_v1_1__prepare_panel_members END IF;

create OR  REPLACE procedure V289_M11_02_SC3_v1_1__prepare_panel_members
     @profiling_date            date                  -- Thursday to use for scaling
    ,@scaling_day                date                -- Day for which to do scaling
    ,@batch_date                datetime = now()    -- Day on which build was kicked off
    ,@Scale_refresh_logging_ID  bigint = null       -- Might pass the log ID in as an argument if it's a big historical build, otherwise we'll make a new one
as
begin

     /**************** A00: CLEANING OUT ALL THE OLD STUFF ****************/

     delete from SC3_todays_panel_members
     commit

     /**************** A01: ACCOUNTS REPORTING LAST WEEK ****************/

     -- This code block is more jury-rigged in than the others because the structure
     -- has to change a bit to accomodate appropriate modularisation. And it'll all
     -- change again later when Phase 2 stuff gets rolled in. And probably further to
     -- acommodate this overnight batching thing, because we won't have data returned
     -- up to a week in the future.

     --declare @profiling_date             date            -- The relevant Thursday of SAV flip etc
     declare @QA_catcher                 integer         -- For control totals along the way

     -- The weekly profiling is called in a different build, so we'll
     -- just grab the most recent one prior to the date we're scaling
/*     select @profiling_date = max(profiling_date)
     from SC3_Sky_base_segment_snapshots
     where profiling_date <= @scaling_day
*/
     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'SC3: Deciding panel members for ' || dateformat(@scaling_day,'yyyy-mm-dd') || ' using profiling of ' || dateformat(@profiling_date,'yyyy-mm-dd') || '.'
     commit

     -- Prepare to catch the week's worth of logs:
     create table #raw_logs_dump_temp (
         account_number          varchar(20)         not null
         ,service_instance_id    varchar(30)         not null
     )
     commit

     -- In phase two, we don't have to worry about juggling things through the daily tables,
     -- so figuring out what's returned data is a lot easier.
     insert into #raw_logs_dump_temp
       select distinct account_number, service_instance_id
--         from sk_prod.vespa_dp_prog_viewed_201310
         from sk_prod.vespa_dp_prog_viewed_201309
--         from /*sk_prod.*/vespa_dp_prog_viewed_201310
--         from /*sk_prod.*/vespa_dp_prog_viewed_201309
        where event_start_date_time_utc between dateadd(hour, 6, @scaling_day) and dateadd(hour, 30, @scaling_day)
          and (panel_id = 12 or panel_id = 11)
          and account_number is not null
          and service_instance_id is not null
     commit

     create hg index idx1 on #raw_logs_dump_temp (account_number)
     create hg index idx2 on #raw_logs_dump_temp (service_instance_id)


     create table #raw_logs_dump (
         account_number          varchar(20)         not null
         ,service_instance_id    varchar(30)         not null
     )
     commit

     insert into #raw_logs_dump
       select distinct
             account_number,
             service_instance_id
         from #raw_logs_dump_temp
     commit

     create index some_key on #raw_logs_dump (account_number)
     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from #raw_logs_dump
     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'A01: Midway 1/2 (Log extracts)', coalesce(@QA_catcher, -1)
     commit

     select
         account_number
         ,count(distinct service_instance_id) as box_count
         ,convert(tinyint, null) as expected_boxes
         ,convert(int, null) as scaling_segment_id
     into #panel_options
     from #raw_logs_dump
     group by account_number

     commit
     create unique index fake_pk on #panel_options (account_number)
     drop table #raw_logs_dump
     commit

     -- Getting this list of accounts isn't enough, we also want to know if all the boxes
     -- of the household have returned data.

     update #panel_options
     set expected_boxes      = sbss.expected_boxes
         ,scaling_segment_id = sbss.vespa_scaling_segment_id
     from #panel_options
     inner join SC3_Sky_base_segment_snapshots as sbss
     on #panel_options.account_number = sbss.account_number
     where sbss.profiling_date = @profiling_date

     commit
     delete from SC3_todays_panel_members
     commit

     -- First moving the unique account numbers in...

     insert into SC3_todays_panel_members (account_number, scaling_segment_id)
     SELECT account_number, scaling_segment_id
     FROM #panel_options
     where expected_boxes >= box_count
     -- Might be more than we expect if NULL service_instance_ID's are distinct against
     -- populated ones (might get fixed later but for now the initial Phase 2 build
     -- doesn't populate them all yet)
     and scaling_segment_id is not null

     commit
     drop table #panel_options
     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC3_todays_panel_members

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'A01: Complete! (Panel members)', coalesce(@QA_catcher, -1)
     commit

     execute logger_add_event @Scale_refresh_logging_ID, 3, 'SC3: panel members prepared!'
     commit

end; -- of procedure "V289_M11_02_SC3_v1_1__prepare_panel_members"
commit;




--------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------
--- Adds indivdual level data in some of the scaling tables for Skyview before the Rim Weighting is applied


IF object_id('V289_M11_03_SC3I_v1_1__add_individual_data') IS NOT NULL THEN DROP PROCEDURE V289_M11_03_SC3I_v1_1__add_individual_data END IF;

create procedure V289_M11_03_SC3I_v1_1__add_individual_data
    @profiling_thursday                date                -- Day on which to do sky base profiling
    ,@batch_date                datetime = now()    -- Day on which build was kicked off
    ,@Scale_refresh_logging_ID  bigint = null       -- Might pass the log ID in as an argument if it's a big historical build, otherwise we'll make a new one
as
begin

        declare @QA_catcher                 integer         -- For control totals along the way


delete from SC3I_Sky_base_segment_snapshots where profiling_date = @profiling_thursday
commit


--- Skybase segments
-- We can convert the segments from Scaling 3.0 into Skyview scaling segments
insert into SC3I_Sky_base_segment_snapshots
select
        b.account_number
        ,b.profiling_date
        ,d.HH_person_number
        ,l_sc3i.scaling_segment_id
        ,l_sc3i.scaling_segment_id
        ,b.expected_boxes
from
        SC3_Sky_base_segment_snapshots b
     inner join
        V289_M08_SKY_HH_composition d
     on b.account_number = d.account_number
     inner join
        vespa_analysts.SC3_Segments_lookup_v1_1 l_sc3
     on b.population_scaling_segment_id = l_sc3.scaling_segment_id
     inner join
        vespa_analysts.SC3I_Segments_lookup_v1_1 l_sc3i
  --   on l_sc3.sky_base_universe = l_sc3i.sky_base_universe -- NOT USED IN THIS VERSION
     on l_sc3.isba_tv_region = l_sc3i.isba_tv_region
 --    and l_sc3.hhcomposition = l_sc3i.hhcomposition -- NOT USED IN THIS VERSION
     and l_sc3.package = l_sc3i.package
     and d.person_head = l_sc3i.head_of_hhd
    -- and d.person_gender = l_sc3i.gender -- NOT USED IN THIS VERSION
     and d.person_gender || ' ' || d.person_ageband = l_sc3i.age_band -- combine age and gender into a single attribute
     and l_sc3i.viewed_tv = 'Y' -- most people watch TV and for SKy Base we can't differentiate between the viewers and non-viewers. Will deal with non-viewers later
where
        b.profiling_date = @profiling_thursday
commit

/* IN this version we are not exlcuding any segments as this has already been taken care of in the segment definitions

--- We want to exclude some segments (and therefor accounts within these segments) from scaling to improve effective sample size
--- This will only effects segments which have low numbers of accounts
select distinct account_number
into #t1
from SC3I_Sky_base_segment_snapshots b inner join vespa_analysts.SC3I_Segments_lookup_v1_1 l on b.population_scaling_segment_id = l.scaling_segment_id
where (gender = 'U' and age_band <> '0-19') -- exclude U gender except for 0-19 (almost all 0-19 are U)
--        or hhcomposition = 'D) Unclassified HHComp' -- high numbers of zero vespa segemnts driving lower effective sample size
--        or l.sky_base_universe = 'Adsmartable but no consent' -- Oct test data has very few Adsmart No Consent on the panel so exclude
--        or (age_band = '20-24' and hhcomposition = 'B) Singles') -- Small segments
--        or (age_band = '65+' and hhcomposition = 'C) Homesharers') -- Small segments

commit

create hg index ind1 on #t1(account_number)
commit


-- Delete the excluded accounts
delete from SC3I_Sky_base_segment_snapshots
from SC3I_Sky_base_segment_snapshots b inner join #t1 t
on b.account_number = t.account_number
commit

*/

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC3I_Sky_base_segment_snapshots

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'M11_03: Skybase Individuals', coalesce(@QA_catcher, -1)
     commit



delete from SC3I_Todays_panel_members
commit


-- Ensure only accounts and individuals on Vespa extract is used
select account_number, hh_person_number
into #t3
from V289_M10_session_individuals -- changed from V289_M07_dp_data so only include accounts that make it through the whole H2I process
group by account_number, hh_person_number
commit

create hg index ind1 on #t3(account_number)
create lf index ind2 on #t3(hh_person_number)

commit

-- Vespa Viewers
insert into SC3I_Todays_panel_members
select
        p.account_number
        ,d.HH_person_number
        ,l_sc3i.scaling_segment_id
from
        SC3_Todays_panel_members p
     inner join
        V289_M08_SKY_HH_composition d
     on p.account_number = d.account_number
     inner join
        vespa_analysts.SC3_Segments_lookup_v1_1 l_sc3
     on p.scaling_segment_id = l_sc3.scaling_segment_id
     inner join
        vespa_analysts.SC3I_Segments_lookup_v1_1 l_sc3i
--     on l_sc3.sky_base_universe = l_sc3i.sky_base_universe -- NOT USED IN THIS VERSION
     on l_sc3.isba_tv_region = l_sc3i.isba_tv_region
--     and l_sc3.hhcomposition = l_sc3i.hhcomposition -- NOT USED IN THIS VERSION
     and l_sc3.package = l_sc3i.package
     and d.person_head = l_sc3i.head_of_hhd
--     and d.person_gender = l_sc3i.gender  -- NOT USED IN THIS VERSION
     and d.person_gender || ' ' || d.person_ageband = l_sc3i.age_band -- combine age and gender into a single attribute
     and l_sc3i.viewed_tv = 'Y' -- by definition all these guys watched tv .Will deal with non-viewers later
     inner join
        #t3 t
     on p.account_number = t.account_number
     and d.hh_person_number = t.hh_person_number

commit

-- Vespa Non-Viewers
-- Not sure we need this as won't be comparable to Barb non-viewers








/* IN this version we are not exlcuding any segments as this has already been taken care of in the segment definitions

--- We want to exclude some segments (and therefor accounts within these segments) from scaling to improve effective sample size
--- This will only effects segments which have low numbers of accounts
select distinct account_number
into #t2
from SC3I_Todays_panel_members p inner join vespa_analysts.SC3I_Segments_lookup_v1_1 l on p.scaling_segment_id = l.scaling_segment_id
where (gender = 'U' and age_band <> '0-19') -- exclude U gender except for 0-19 (almost all 0-19 are U)
--        or hhcomposition = 'D) Unclassified HHComp' -- high numbers of zero vespa segemnts driving lower effective sample size
--        or l.sky_base_universe = 'Adsmartable but no consent' -- Oct test data has very few Adsmart No Consent on the panel so exclude
--        or (age_band = '20-24' and hhcomposition = 'B) Singles') -- Small segments
--        or (age_band = '65+' and hhcomposition = 'C) Homesharers') -- Small segments
commit

create hg index ind1 on #t2(account_number)
commit

-- Delete the excluded accounts
delete from SC3I_Todays_panel_members
from SC3I_Todays_panel_members p inner join #t2 t
on p.account_number = t.account_number
commit

*/


     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC3I_Sky_base_segment_snapshots

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'M11_03: Panel Individuals', coalesce(@QA_catcher, -1)
     commit


end; -- of procedure "V289_M11_03_SC3I_v1_1__add_individual_data"
commit;


-----------------------------------------------------------------------------------------------------------------------------------------




IF object_id('V289_M11_04_SC3I_v1_1__make_weights') IS NOT NULL THEN DROP PROCEDURE V289_M11_04_SC3I_v1_1__make_weights END IF;

create procedure V289_M11_04_SC3I_v1_1__make_weights
    @scaling_day                date                -- Day for which to do scaling; this argument is mandatory
    ,@batch_date                datetime = now()    -- Day on which build was kicked off
    ,@Scale_refresh_logging_ID  bigint = null       -- Might pass the log ID in as an argument if it's a big historical build, otherwise we'll make a new one
as
begin


        -- Only need these if we can't get to execute as a Proc
/*        declare @scaling_day  date
        declare @batch_date date
        declare @Scale_refresh_logging_ID bigint
        set @scaling_day = '2013-09-26'
        set @batch_date = '2014-07-10'
        set @Scale_refresh_logging_ID = 5
*/


     -- So by this point we're assuming that the Sky base segmentation is done
     -- (for a suitably recent item) and also that today's panel members have
     -- been established, and we're just going to go calculate these weights.

     DECLARE @cntr           INT
     DECLARE @iteration      INT
     DECLARE @cntr_var       SMALLINT
     DECLARE @scaling_var    VARCHAR(30)
     DECLARE @scaling_count  SMALLINT
     DECLARE @convergence    TINYINT
     DECLARE @sky_base       DOUBLE
     DECLARE @vespa_panel    DOUBLE
     DECLARE @sum_of_weights DOUBLE
     declare @profiling_date date
     declare @QA_catcher     bigint

     commit



     /**************** PART B01: GETTING TOTALS FOR EACH SEGMENT ****************/

     -- Figure out which profiling info we're using;
     select @profiling_date = max(profiling_date)
     from SC3I_Sky_base_segment_snapshots
     where profiling_date <= @scaling_day

     commit

     -- Log the profiling date being used for the build
      execute logger_add_event @Scale_refresh_logging_ID, 3, 'SC3: Making weights for ' || dateformat(@scaling_day,'yyyy-mm-dd') || ' using profiling of ' || dateformat(@profiling_date,'yyyy-mm-dd') || '.'
      commit

     -- First adding in the Sky base numbers
     delete from SC3I_weighting_working_table
     commit

     INSERT INTO SC3I_weighting_working_table (scaling_segment_id, sky_base_accounts)
     select population_scaling_segment_id, count(1)
     from SC3I_Sky_base_segment_snapshots
     where profiling_date = @profiling_date
     group by population_scaling_segment_id

     commit


/**************** update SC3I_weighting_working_table
-- Keep the totals for age/gender groups the same but apply Barb %

        -- Get SkyBase ageband totals
        select
                'age_band' as profile
                ,age_band as value
                ,count(1) as weighted_population
                ,9.999 as percent_of_total_pop
        into
                #skybase_age_gender_totals
        from
                SC3I_weighting_working_table w
             inner join
                vespa_analysts.SC3I_Segments_lookup_v1_1 l
             on w.scaling_segment_id = l.scaling_segment_id
        group by
                profile
                ,value
                ,weighted_population

        -- Get SkyBase ageband totals
        insert into #skybase_age_gender_totals
        select
                'gender' as profile
                ,gender as value
                ,count(1) as weighted_population
        into
                #skybase_age_gender_totals
        from
                SC3I_weighting_working_table w
             inner join
                vespa_analysts.SC3I_Segments_lookup_v1_1 l
             on w.scaling_segment_id = l.scaling_segment_id
        group by
                profile
                ,value
                ,weighted_population

        -- Calculate the % of Sky base by age group and by gender group
        update #skybase_age_gender_totals sb
        set percent_of_total_pop = weighted_population / tot_weighted_population
        from
                (select profile, sum(weighted_population) as tot_weighted_population
                from #skybase_age_gender_totals
                group by profile) summary
        where sb.profile = summary.profile



TABLE #barb_age_gender_weighted_population
        profile                 e.g. ageband
        value                   e.g 34-45
        weighted_population     e.g 12,000,000
        percent_of_total_pop    e.g. 0.151


        -- Calculate the adjustment to apply to age and gender groups so that they have same Barb profile
        select sb.profile, value, (bb.percent_of_total_pop / sb.percent_of_total_pop) as sky_adjust
        into #skybase_adjust_for_barb
        from
                #skybase_age_gender_totals sb
             inner join
                #barb_age_gender_weighted_population bb
             on sb.profile = bb.profile and sb.value = bb.value






        update SC3I_weighting_working_table w
        set sky_base_accounts =
        from

        select scaling_segment_id,
        from
                #skybase_age_gender_totals sb
             inner join
                #barb_age_gender_weighted_population bb





*/





     -- Now tack on the universe flags; a special case of things coming out of the lookup

     update SC3I_weighting_working_table
     set sky_base_universe = sl.sky_base_universe
     from SC3I_weighting_working_table
--      inner join vespa_analysts.SC2_Segments_lookup_v1_1 as sl
     inner join vespa_analysts.SC3I_Segments_lookup_v1_1 as sl
     on SC3I_weighting_working_table.scaling_segment_id = sl.scaling_segment_id

     commit

     -- Mix in the Vespa panel counts as determined earlier
     select scaling_segment_id
         ,count(1) as panel_members
     into #segment_distribs
     from SC3I_Todays_panel_members
     where scaling_segment_id is not null
     group by scaling_segment_id

     commit
     create unique index fake_pk on #segment_distribs (scaling_segment_id)
     commit

     -- It defaults to 0, so we can just poke values in
     update SC3I_weighting_working_table
     set vespa_panel = sd.panel_members
     from SC3I_weighting_working_table
     inner join #segment_distribs as sd
     on SC3I_weighting_working_table.scaling_segment_id = sd.scaling_segment_id

     -- And we're done! log the progress.
     commit
     drop table #segment_distribs
     commit
     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC3I_weighting_working_table

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'B01: Complete! (Segmentation totals)', coalesce(@QA_catcher, -1)
     commit






     /**************** PART B02: ASSIGNING WEIGHTS TO EACH SEGMENT ****************/

     delete from SC3I_category_subtotals where scaling_date = @scaling_day
     delete from SC3I_metrics where scaling_date = @scaling_day
     commit

     -- Rim-weighting is an iterative process that iterates through each of the scaling variables
     -- individually until the category sum of weights converge to the population category subtotals

     SET @cntr           = 1
     SET @iteration      = 0
     SET @cntr_var       = 1
--      SET @scaling_var    = (SELECT scaling_variable FROM vespa_analysts.SC2_Variables_lookup_v2_1 WHERE id = @cntr)
     SET @scaling_var    = (SELECT scaling_variable FROM vespa_analysts.SC3I_Variables_lookup_v1_1 WHERE id = @cntr)
     SET @scaling_count  = (SELECT COUNT(scaling_variable) FROM vespa_analysts.SC3I_Variables_lookup_v1_1)

     -- The SC3I_weighting_working_table table contains subtotals and sum_of_weights for all segments represented by
     -- the sky base.
     -- Some segments are not represented by the vespa panel, these are allocated an arbitrary value of 0.000001
     -- to ensure convergence.

     -- arbitrary value to ensure convergence
     update SC3I_weighting_working_table
     set vespa_panel = 0.000001
     where vespa_panel = 0

     commit

     -- Initialise working columns
     update SC3I_weighting_working_table
     set sum_of_weights = vespa_panel

     commit

     -- The iterative part.
     -- This works by choosing a particular scaling variable and then summing across the categories
     -- of that scaling variable for the sky base, the vespa panel and the sum of weights.
     -- A Category weight is calculated by dividing the sky base subtotal by the vespa panel subtotal
     -- for that category.
     -- This category weight is then applied back to the segments table and the process repeats until
     -- the sum_of_weights in the category table converges to the sky base subtotal.

     -- Category Convergence is defined as the category sum of weights being +/- 3 away from the sky
     -- base category subtotal within 100 iterations.
     -- Overall Convergence for that day occurs when each of the categories has converged, or the @convergence variable = 0

     -- The @convergence variable represents how many categories did not converge.
     -- If the number of iterations = 100 and the @convergence > 0 then this means that the Rim-weighting
     -- has not converged for this particular day.
     -- In this scenario, the person running the code should send the results of the SC3I_metrics for that
     -- week to analytics team for review. ## What exactly are we checking? can we automate any of it?

     WHILE @cntr <= @scaling_count
     BEGIN
             DELETE FROM SC3I_category_working_table

             SET @cntr_var = 1
             WHILE @cntr_var <= @scaling_count
             BEGIN
                         SELECT @scaling_var = scaling_variable FROM vespa_analysts.SC3I_Variables_lookup_v1_1 WHERE id = @cntr_var

                         EXECUTE('
                         INSERT INTO SC3I_category_working_table (sky_base_universe,profile,value,sky_base_accounts,vespa_panel,sum_of_weights)
                             SELECT  srs.sky_base_universe
                                    ,@scaling_var
                                    ,ssl.'||@scaling_var||'
                                    ,SUM(srs.sky_base_accounts)
                                    ,SUM(srs.vespa_panel)
                                    ,SUM(srs.sum_of_weights)
                             FROM SC3I_weighting_working_table AS srs
                                     inner join vespa_analysts.SC3I_Segments_lookup_v1_1 AS ssl ON srs.scaling_segment_id = ssl.scaling_segment_id
                             GROUP BY srs.sky_base_universe,ssl.'||@scaling_var||'
                             ORDER BY srs.sky_base_universe
                         ')

                         SET @cntr_var = @cntr_var + 1
             END

             commit

             UPDATE SC3I_category_working_table
             SET  category_weight = sky_base_accounts / sum_of_weights
                 ,convergence_flag = CASE WHEN abs(sky_base_accounts - sum_of_weights) < 3 THEN 0 ELSE 1 END

             SELECT @convergence = SUM(convergence_flag) FROM SC3I_category_working_table
             SET @iteration = @iteration + 1
             SELECT @scaling_var = scaling_variable FROM vespa_analysts.SC3I_Variables_lookup_v1_1 WHERE id = @cntr

             EXECUTE('
             UPDATE SC3I_weighting_working_table
             SET  SC3I_weighting_working_table.category_weight = sc.category_weight
                 ,SC3I_weighting_working_table.sum_of_weights  = SC3I_weighting_working_table.sum_of_weights * sc.category_weight
             FROM SC3I_weighting_working_table
                     inner join vespa_analysts.SC3I_Segments_lookup_v1_1 AS ssl ON SC3I_weighting_working_table.scaling_segment_id = ssl.scaling_segment_id
                     inner join SC3I_category_working_table AS sc ON sc.value = ssl.'||@scaling_var||'
                                                                      AND sc.sky_base_universe = ssl.sky_base_universe
             ')

             commit

             IF @iteration = 100 OR @convergence = 0 SET @cntr = (@scaling_count + 1)
             ELSE

             IF @cntr = @scaling_count  SET @cntr = 1
             ELSE
             SET @cntr = @cntr+1

     END

     commit
     -- This loop build took about 4 minutes. That's fine.

     -- Calculate segment weight and corresponding indices

     -- This section calculates the segment weight which is the weight that should be applied to viewing data
     -- A couple of indices are also calculated so that we can keep track of the performance of the rim-weighting


     SELECT @sky_base = SUM(sky_base_accounts) FROM SC3I_weighting_working_table
     SELECT @vespa_panel = SUM(vespa_panel) FROM SC3I_weighting_working_table
     SELECT @sum_of_weights = SUM(sum_of_weights) FROM SC3I_weighting_working_table

     UPDATE SC3I_weighting_working_table
     SET  segment_weight = sum_of_weights / vespa_panel
         ,indices_actual = 100*(vespa_panel / @vespa_panel) / (sky_base_accounts / @sky_base)
         ,indices_weighted = 100*(sum_of_weights / @sum_of_weights) / (sky_base_accounts / @sky_base)

     commit

     -- OK, now catch those cases where stuff diverged because segments weren't reperesented:
     update SC3I_weighting_working_table
     set segment_weight  = 0.000001
     where vespa_panel   = 0.000001

     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC3I_weighting_working_table
     where segment_weight >= 0.001           -- Ignore the placeholders here to guarantee convergence

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'B02: Midway (Iterations)', coalesce(@QA_catcher, -1)
     commit

     -- Now push convergence details out to the tracking tables: the first one provides a convergence summary at a category level

     INSERT INTO SC3I_category_subtotals (scaling_date,sky_base_universe,profile,value,sky_base_accounts,vespa_panel,category_weight
                                              ,sum_of_weights, convergence)
     SELECT  @scaling_day
             ,sky_base_universe
             ,profile
             ,value
             ,sky_base_accounts
             ,vespa_panel
             ,category_weight
             ,sum_of_weights
             ,case when abs(sky_base_accounts - sum_of_weights) > 3 then 1 else 0 end
     FROM SC3I_category_working_table

     -- The SC3I_metrics table contains metrics for a particular scaling date. It shows whether the
     -- Rim-weighting process converged for that day and the number of iterations. It also shows the
     -- maximum and average weight for that day and counts for the sky base and the vespa panel.

     commit

     -- Apparently it should be reviewed each week, but what are we looking for?

     INSERT INTO SC3I_metrics (scaling_date, iterations, convergence, max_weight, av_weight,
                                  sum_of_weights, sky_base, vespa_panel, non_scalable_accounts)
     SELECT  @scaling_day
            ,@iteration
            ,@convergence
            ,MAX(segment_weight)
            ,sum(segment_weight * vespa_panel) / sum(vespa_panel)    -- gives the average weight by account (just uising AVG would give it average by segment id)
            ,SUM(segment_weight * vespa_panel)                       -- again need some math because this table has one record per segment id rather than being at acocunt level
            ,@sky_base
            ,sum(CASE WHEN segment_weight >= 0.001 THEN vespa_panel ELSE NULL END)
            ,sum(CASE WHEN segment_weight < 0.001  THEN vespa_panel ELSE NULL END)
     FROM SC3I_weighting_working_table

     update SC3I_metrics
        set sum_of_convergence = abs(sky_base - sum_of_weights)

     insert into SC3I_non_convergences(scaling_date,scaling_segment_id, difference)
     select @scaling_day
           ,scaling_segment_id
           ,abs(sum_of_weights - sky_base_accounts)
       from SC3I_weighting_working_table
      where abs((segment_weight * vespa_panel) - sky_base_accounts) > 3

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'B02: Complete (Calculate weights)', coalesce(@QA_catcher, -1)
     commit



     /**************** PART B03: PUBLISHING WEIGHTS INTO INTERFACE STRUCTURES ****************/

     -- Here is where that bit of interface code goes, including extending the intervals
     -- in the Scaling midway tables (which now happens one day ata time). Maybe this guy
     -- wants to go into a new and different stored procedure?

     -- Heh, this deletion process clears out everything *after* the scaling day, meaning we
     -- have to start from the beginning doing this processing... I guess we'll just manage
     -- the historical build like this. (This is because we'd otherwise have to manage adding
     -- additional records to the interval table when we re-run a day and break an interval
     -- that already exists, and that whole process would be annoying to manage.)

     -- Except we'll only nuke everything if we *rebuild* a day that's not already there.
     if (select count(1) from SC3I_Weightings where scaling_day = @scaling_day) > 0
     begin
         delete from SC3I_Weightings where scaling_day = @scaling_day

         delete from SC3I_Intervals where reporting_starts = @scaling_day

         update SC3I_Intervals set reporting_ends = dateadd(day, -1, @scaling_day) where reporting_ends >= @scaling_day
     end
     commit

     -- Part 1: Update the Vespa midway scaling tables. In Vespa Analysts? May as well
     -- also keep this in VIQ_prod too.
     insert into SC3I_Weightings
     select
         @scaling_day
         ,scaling_segment_id
         ,vespa_panel
         ,sky_base_accounts
         ,segment_weight
         ,sum_of_weights
         ,indices_actual
         ,indices_weighted
         ,case when abs(sky_base_accounts - sum_of_weights) > 3 then 1 else 0 end
     from SC3I_weighting_working_table
     -- Might have to check that the filter on segment_weight doesn't leave any orphaned
     -- accounts about the place...

     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC3I_Weightings
     where scaling_day = @scaling_day

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'B03: Midway 1/4 (Midway weights)', coalesce(@QA_catcher, -1)
     commit

     -- First off extend the intervals that are already in the table:

/*
     update SC3I_Intervals
     set reporting_ends = @scaling_day
     from SC3I_Intervals
     inner join SC3I_Todays_panel_members as tpm
     on SC3I_Intervals.account_number         = tpm.account_number
     and SC3I_Intervals.scaling_segment_ID    = tpm.scaling_segment_ID
     where reporting_ends = @scaling_day - 1

     -- Next step is adding in all the new intervals that don't appear
     -- as extensions on existing intervals. First off, isolate the
     -- intervals that got extended

     select account_number
     into #included_accounts
     from SC3I_Intervals
     where reporting_ends = @scaling_day

     commit
     create unique index fake_pk on #included_accounts (account_number)
     commit

     -- Now having figured out what already went in, we can throw in the rest:
     insert into SC3I_Intervals (
         account_number
         ,HH_person_number
         ,reporting_starts
         ,reporting_ends
         ,scaling_segment_ID
     )
     select
         tpm.account_number
         ,HH_person_number
         ,@scaling_day
         ,@scaling_day
         ,tpm.scaling_segment_ID
     from SC3I_Todays_panel_members as tpm
     left join #included_accounts as ia
     on tpm.account_number = ia.account_number
     where ia.account_number is null -- we don't want to add things already in the intervals table


     commit
     drop table #included_accounts
     commit
*/
     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC3I_Intervals where reporting_ends = @scaling_day

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'B03: Midway 2/4 (Midway intervals)', coalesce(@QA_catcher, -1)
     commit

     -- Part 2: Update the VIQ interface table (which needs the household key thing)
     if (select count(1) from V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING where scaling_date = @scaling_day) > 0
     begin
         delete from V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING where scaling_date = @scaling_day
     end
     commit

     insert into V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING
     select
         ws.account_number
         ,ws.HH_person_number
         ,@scaling_day
         ,wwt.segment_weight
         ,@batch_date
     from SC3I_weighting_working_table as wwt
     inner join SC3I_Sky_base_segment_snapshots as ws -- need this table to get the cb_key_household items
     on wwt.scaling_segment_id = ws.population_scaling_segment_id
     inner join SC3I_Todays_panel_members as tpm
     on ws.account_number = tpm.account_number       -- Filter for today's panel only
     and ws.profiling_date = @profiling_date

     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING
     where scaling_date = @scaling_day

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'B03: Midway 3/4 (VIQ interface)', coalesce(@QA_catcher, -1)
     commit

     execute logger_add_event @Scale_refresh_logging_ID, 3, 'B03: Complete! (Publish weights)'
     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'SC3: Weights made for ' || dateformat(@scaling_day, 'yyyy-mm-dd')
     commit

end; -- of procedure "V289_M11_04_SC3I_v1_1__make_weights"
commit;


/*******************************************************************************************************/


IF object_id('V289_M11_04_SC3I_v1_1__make_weights_BARB') IS NOT NULL THEN DROP PROCEDURE V289_M11_04_SC3I_v1_1__make_weights_BARB END IF;

create procedure V289_M11_04_SC3I_v1_1__make_weights_BARB
    @profiling_date             date                -- Thursday profilr date
    ,@scaling_day                date                -- Day for which to do scaling; this argument is mandatory
    ,@batch_date                datetime = now()    -- Day on which build was kicked off
    ,@Scale_refresh_logging_ID  bigint = null       -- Might pass the log ID in as an argument if it's a big historical build, otherwise we'll make a new one
as
begin


        -- Only need these if we can't get to execute as a Proc
/*        declare @scaling_day  date
        declare @batch_date date
        declare @Scale_refresh_logging_ID bigint
        set @scaling_day = '2013-09-26'
        set @batch_date = '2014-07-10'
        set @Scale_refresh_logging_ID = 5
*/


     -- So by this point we're assuming that the Sky base segmentation is done
     -- (for a suitably recent item) and also that today's panel members have
     -- been established, and we're just going to go calculate these weights.

     DECLARE @cntr           INT
     DECLARE @iteration      INT
     DECLARE @cntr_var       SMALLINT
     DECLARE @scaling_var    VARCHAR(30)
     DECLARE @scaling_count  SMALLINT
     DECLARE @convergence    TINYINT
     DECLARE @sky_base       DOUBLE
     DECLARE @vespa_panel    DOUBLE
     DECLARE @sum_of_weights DOUBLE
--     declare @profiling_date date
     declare @QA_catcher     bigint

     commit



     /**************** PART B01: GETTING TOTALS FOR EACH SEGMENT ****************/

     -- Figure out which profiling info we're using;
/*     select @profiling_date = max(profiling_date)
     from SC3I_Sky_base_segment_snapshots
     where profiling_date <= @scaling_day

     commit
*/
     -- Log the profiling date being used for the build
      execute logger_add_event @Scale_refresh_logging_ID, 3, 'SC3: Making weights for ' || dateformat(@scaling_day,'yyyy-mm-dd') || ' using profiling of ' || dateformat(@profiling_date,'yyyy-mm-dd') || '.'
      commit

     -- First adding in the Sky base numbers
     delete from SC3I_weighting_working_table
     commit

     INSERT INTO SC3I_weighting_working_table (scaling_segment_id, sky_base_accounts)
     select population_scaling_segment_id, count(1)
     from SC3I_Sky_base_segment_snapshots
     where profiling_date = @profiling_date
     group by population_scaling_segment_id

     commit


/**************** update SC3I_weighting_working_table
-- Re-scale Sky base to Barb age/gender totals
-- Will only rescale to barb households that have any viewing data for the day being scaled
-- and NOT the barb base
*/

-- Get individuals from Barb who have viewed tv
select household_number, person_number
into #barb_viewers
from skybarb_fullview
where date(start_time_of_session) = @scaling_day
group by household_number, person_number
commit


create hg index ind1 on #barb_viewers(household_number)
create lf index ind2 on #barb_viewers(person_number)

-- Get hhds that have some viewing
select household_number
into #barb_hhd_viewers
from #barb_viewers
group by household_number
commit

create hg index ind1 on #barb_hhd_viewers(household_number)




-- Get Barb individuals in Sky hhds
select
        h.house_id as household_number
        ,h.person as person_number
        ,h.age
        ,case when age <= 19 then 'U'
              when h.sex = 'Male' then 'M'
              when h.sex = 'Female' then 'F'
        end as gender
        ,case when age <= 19 then '0-19'
              when age BETWEEN 20 AND 24 then '20-24'
              WHEN age BETWEEN 25 AND 34 then '25-34'
              WHEN age BETWEEN 35 AND 44 then '35-44'
              WHEN age BETWEEN 45 AND 64 then '45-64'
              WHEN age >= 65 then '65+'
        end as ageband
        ,cast(h.head as char(1)) as head_of_hhd
        ,w.processing_weight / 10.0 as processing_weight
into #barb_inds_with_sky
from
        skybarb h
        inner join barb_weights w
                on h.house_id = w.household_number
                and h.person = w.person_number





/* NOT NEEDED
--- Find Barb hhds with Sky
select distinct household_number
into #barb_hhd_with_sky
from BARB_Panel_Demographic_Data_TV_Sets_Characteristics
where
        (Reception_Capability_Code1 = 2 or Reception_Capability_Code2 = 2 or  Reception_Capability_Code3 = 2 or
                        Reception_Capability_Code4 = 2 or Reception_Capability_Code5 = 2 or Reception_Capability_Code6 = 2 or
                        Reception_Capability_Code7 = 2 or Reception_Capability_Code8 = 2 or Reception_Capability_Code9 = 2 or
                        Reception_Capability_Code10 = 2)
        and Date_valid_for_DB1 = cast(
                                        cast(year(@scaling_day) as varchar(4)) ||
                                        case when month(@scaling_day) < 10 then '0' end || cast(month(@scaling_day) as varchar(2)) ||
                                        case when day(@scaling_day) < 10 then '0' end || cast(day(@scaling_day) as varchar(2))
                                as integer)
commit

create hg index ind1 on #barb_hhd_with_sky(household_number)
commit




-- Find weight of each Barb individual with Sky
select
        p.household_number
        ,p.person_number
        ,cast((date(@scaling_day) - p.date_of_birth)/365 as int) as age
        ,case when age <= 19 then 'U'
              when p.sex_code = 1 then 'M'
              when p.sex_code = 2 then 'F'
        end as gender
        ,case when age <= 19 then '0-19'
              when age BETWEEN 20 AND 24 then '20-24'
              WHEN age BETWEEN 25 AND 34 then '25-34'
              WHEN age BETWEEN 35 AND 44 then '35-44'
              WHEN age BETWEEN 45 AND 64 then '45-64'
              WHEN age >= 65 then '65+'
        end as ageband
        ,processing_weight
into #barb_inds_with_sky
from
        BARB_PVF04_Individual_Member_Details p
      inner join
        BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories w
      on p.household_number = w.household_number
      and p.person_number = w.person_number
      inner join
        #barb_hhd_with_sky s
      on p.household_number = s.household_number
where
        reporting_panel_code = 50 -- regional codes, 50 covers all UK
        and p.Person_membership_status = 0 -- individual still on the panel
        and p.Date_valid_for_DB1 = @scaling_day
        and w.Date_of_Activity_DB1 = @scaling_day
commit

create lf index ind1 on #barb_inds_with_sky(household_number)
create lf index ind2 on #barb_inds_with_sky(person_number)
commit


--- There are some Welsh speakers who are on panel 39 but not on panel 50. Add these in

select distinct
        p.household_number
        ,p.person_number
into #b2
from
        BARB_PVF04_Individual_Member_Details p
     left join
        #barb_inds_with_sky b
     on p.household_number = b.household_number
     and p.person_number = b.person_number
where
        p.Person_membership_status = 0 -- individual still on the panel
        and b.household_number is null
        and p.Date_valid_for_DB1 = @scaling_day
commit


insert into #barb_inds_with_sky
select
        p.household_number
        ,p.person_number
        ,cast((date(@scaling_day) - p.date_of_birth)/365 as int) as age
        ,case when age <= 19 then 'U'
              when p.sex_code = 1 then 'M'
              when p.sex_code = 2 then 'F'
        end as gender
        ,case when age <= 19 then '0-19'
              when age BETWEEN 20 AND 24 then '20-24'
              WHEN age BETWEEN 25 AND 34 then '25-34'
              WHEN age BETWEEN 35 AND 44 then '35-44'
              WHEN age BETWEEN 45 AND 64 then '45-64'
              WHEN age >= 65 then '65+'
        end as ageband
        ,processing_weight
from
        BARB_PVF04_Individual_Member_Details p
      inner join
        BARB_PVF05_Panel_Member_Responses_Weights_and_Viewing_Categories w
      on p.household_number = w.household_number
      and p.person_number = w.person_number
      inner join
        #b2 b
      on w.household_number = b.household_number
      and w.person_number = b.person_number
      inner join
        #barb_hhd_with_sky s
      on p.household_number = s.household_number
where
        w.reporting_panel_code = 39 -- regional codes, 39 covers Welsh speakers
        and p.Person_membership_status = 0 -- individual still on the panel
        and p.Date_valid_for_DB1 = @scaling_day
        and w.Date_of_Activity_DB1 = @scaling_day
commit


drop table #b2
drop table #barb_hhd_with_sky
commit

*/
-------------- Summaries Barb Data

delete from V289_M11_04_Barb_weighted_population
commit

insert into V289_M11_04_Barb_weighted_population
select
        (case when ageband = '0-19' then 'U' else gender end) || ' ' || ageband as gender_ageband
        ,'A' as gender1
        ,case when v.household_number is null then 'N' else 'Y' end as viewed_tv
        ,i.head_of_hhd
        ,sum(processing_weight)
from #barb_inds_with_sky i
left join #barb_viewers v on i.household_number = v.household_number and i.person_number = v.person_number
group by gender_ageband, gender1, viewed_tv, i.head_of_hhd
commit

drop table #barb_inds_with_sky
commit


----

-- Note that for the Skybase at this point there are no non-viewers of TV
select age_band, viewed_tv, head_of_hhd, cast(sum(sky_base_accounts) as double) as age_gender_sky_base
into #a1
from SC3I_weighting_working_table w inner join vespa_analysts.SC3I_Segments_lookup_v1_1 l
on w.scaling_segment_id = l.scaling_segment_id
group by age_band, viewed_tv, head_of_hhd
commit

create lf index ind1 on #a1(age_band)
-- create lf index ind2 on #a1(gender)
create lf index ind3 on #a1(viewed_tv)
create lf index ind4 on #a1(head_of_hhd)

commit

-- All Skybase has been set to tv viewers
-- This will rescale them to Barb viewers by age gender group
-- Do Head of HHD
update SC3I_weighting_working_table w
set sky_base_accounts = sky_base_accounts * (barb_weight / age_gender_sky_base)
from vespa_analysts.SC3I_Segments_lookup_v1_1 l, V289_M11_04_Barb_weighted_population b, #a1 a
where
        w.scaling_segment_id = l.scaling_segment_id
   --     and l.gender = b.gender
        and l.age_band = b.ageband
   --     and l.gender = a.gender
        and l.age_band = a.age_band
        and l.viewed_tv = 'Y'
        and a.viewed_tv = 'Y'
        and b.viewed_tv = 'Y'
        and l.head_of_hhd = '1'
        and a.head_of_hhd = '1'
        and b.head_of_hhd = '1'

commit

-- Do Non-Head of HHD
update SC3I_weighting_working_table w
set sky_base_accounts = sky_base_accounts * (barb_weight / age_gender_sky_base)
from vespa_analysts.SC3I_Segments_lookup_v1_1 l, V289_M11_04_Barb_weighted_population b, #a1 a
where
        w.scaling_segment_id = l.scaling_segment_id
   --     and l.gender = b.gender
        and l.age_band = b.ageband
   --     and l.gender = a.gender
        and l.age_band = a.age_band
        and l.viewed_tv = 'Y'
        and a.viewed_tv = 'Y'
        and b.viewed_tv = 'Y'
        and l.head_of_hhd = '0'
        and a.head_of_hhd = '0'
        and b.head_of_hhd = '0'

commit






/* Not Completed but don't think needed
-- Now Rescale Skybase tv viewers to Barb non-viewers by age gender
-- Non viewers are not yet in the table SC3I_weighting_working_table
insert into SC3I_weighting_working_table
select
        l.scaling_segment_id
        ,l.sky_base_universe
        ,sky_base_accounts * (barb_weight / age_gender_sky_base)
        ,

 w
set sky_base_accounts = sky_base_accounts * (barb_weight / age_gender_sky_base)
from vespa_analysts.SC3I_Segments_lookup_v1_1 l, V289_M11_04_Barb_weighted_population b, #a1 a
where
        w.scaling_segment_id = l.scaling_segment_id
   --     and l.gender = b.gender
        and l.age_band = b.ageband
   --     and l.gender = a.gender
        and l.age_band = a.age_band
        and l.viewed_tv = 'N' -- Set Skybase non-viewers
        and a.viewed_tv = 'Y' -- Using profile of Skybase viwers
        and b.viewed_tv = 'N' -- Scaled to Barb non-viewers
commit
*/

drop table #a1
commit

/***********************************************/


     -- Now tack on the universe flags; a special case of things coming out of the lookup

     update SC3I_weighting_working_table
     set sky_base_universe = sl.sky_base_universe
     from SC3I_weighting_working_table
--      inner join vespa_analysts.SC2_Segments_lookup_v1_1 as sl
     inner join vespa_analysts.SC3I_Segments_lookup_v1_1 as sl
     on SC3I_weighting_working_table.scaling_segment_id = sl.scaling_segment_id

     commit

     -- Mix in the Vespa panel counts as determined earlier
     select scaling_segment_id
         ,count(1) as panel_members
     into #segment_distribs
     from SC3I_Todays_panel_members
     where scaling_segment_id is not null
     group by scaling_segment_id

     commit
     create unique index fake_pk on #segment_distribs (scaling_segment_id)
     commit

     -- It defaults to 0, so we can just poke values in
     update SC3I_weighting_working_table
     set vespa_panel = sd.panel_members
     from SC3I_weighting_working_table
     inner join #segment_distribs as sd
     on SC3I_weighting_working_table.scaling_segment_id = sd.scaling_segment_id

     -- And we're done! log the progress.
     commit
     drop table #segment_distribs
     commit
     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC3I_weighting_working_table

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'B01: Complete! (Segmentation totals)', coalesce(@QA_catcher, -1)
     commit






     /**************** PART B02: ASSIGNING WEIGHTS TO EACH SEGMENT ****************/

     delete from SC3I_category_subtotals where scaling_date = @scaling_day
     delete from SC3I_metrics where scaling_date = @scaling_day
     commit

     -- Rim-weighting is an iterative process that iterates through each of the scaling variables
     -- individually until the category sum of weights converge to the population category subtotals

     SET @cntr           = 1
     SET @iteration      = 0
     SET @cntr_var       = 1
--      SET @scaling_var    = (SELECT scaling_variable FROM vespa_analysts.SC2_Variables_lookup_v2_1 WHERE id = @cntr)
     SET @scaling_var    = (SELECT scaling_variable FROM vespa_analysts.SC3I_Variables_lookup_v1_1 WHERE id = @cntr)
     SET @scaling_count  = (SELECT COUNT(scaling_variable) FROM vespa_analysts.SC3I_Variables_lookup_v1_1)

     -- The SC3I_weighting_working_table table contains subtotals and sum_of_weights for all segments represented by
     -- the sky base.
     -- Some segments are not represented by the vespa panel, these are allocated an arbitrary value of 0.000001
     -- to ensure convergence.

     -- arbitrary value to ensure convergence
     update SC3I_weighting_working_table
     set vespa_panel = 0.000001
     where vespa_panel = 0

     commit

     -- Initialise working columns
     update SC3I_weighting_working_table
     set sum_of_weights = vespa_panel

     commit

     -- The iterative part.
     -- This works by choosing a particular scaling variable and then summing across the categories
     -- of that scaling variable for the sky base, the vespa panel and the sum of weights.
     -- A Category weight is calculated by dividing the sky base subtotal by the vespa panel subtotal
     -- for that category.
     -- This category weight is then applied back to the segments table and the process repeats until
     -- the sum_of_weights in the category table converges to the sky base subtotal.

     -- Category Convergence is defined as the category sum of weights being +/- 3 away from the sky
     -- base category subtotal within 100 iterations.
     -- Overall Convergence for that day occurs when each of the categories has converged, or the @convergence variable = 0

     -- The @convergence variable represents how many categories did not converge.
     -- If the number of iterations = 100 and the @convergence > 0 then this means that the Rim-weighting
     -- has not converged for this particular day.
     -- In this scenario, the person running the code should send the results of the SC3I_metrics for that
     -- week to analytics team for review. ## What exactly are we checking? can we automate any of it?

     WHILE @cntr <= @scaling_count
     BEGIN
             DELETE FROM SC3I_category_working_table

             SET @cntr_var = 1
             WHILE @cntr_var <= @scaling_count
             BEGIN
                         SELECT @scaling_var = scaling_variable FROM vespa_analysts.SC3I_Variables_lookup_v1_1 WHERE id = @cntr_var

                         EXECUTE('
                         INSERT INTO SC3I_category_working_table (sky_base_universe,profile,value,sky_base_accounts,vespa_panel,sum_of_weights)
                             SELECT  srs.sky_base_universe
                                    ,@scaling_var
                                    ,ssl.'||@scaling_var||'
                                    ,SUM(srs.sky_base_accounts)
                                    ,SUM(srs.vespa_panel)
                                    ,SUM(srs.sum_of_weights)
                             FROM SC3I_weighting_working_table AS srs
                                     inner join vespa_analysts.SC3I_Segments_lookup_v1_1 AS ssl ON srs.scaling_segment_id = ssl.scaling_segment_id
                             GROUP BY srs.sky_base_universe,ssl.'||@scaling_var||'
                             ORDER BY srs.sky_base_universe
                         ')

                         SET @cntr_var = @cntr_var + 1
             END

             commit

             UPDATE SC3I_category_working_table
             SET  category_weight = sky_base_accounts / sum_of_weights
                 ,convergence_flag = CASE WHEN abs(sky_base_accounts - sum_of_weights) < 3 THEN 0 ELSE 1 END

             SELECT @convergence = SUM(convergence_flag) FROM SC3I_category_working_table
             SET @iteration = @iteration + 1
             SELECT @scaling_var = scaling_variable FROM vespa_analysts.SC3I_Variables_lookup_v1_1 WHERE id = @cntr

             EXECUTE('
             UPDATE SC3I_weighting_working_table
             SET  SC3I_weighting_working_table.category_weight = sc.category_weight
                 ,SC3I_weighting_working_table.sum_of_weights  = SC3I_weighting_working_table.sum_of_weights * sc.category_weight
             FROM SC3I_weighting_working_table
                     inner join vespa_analysts.SC3I_Segments_lookup_v1_1 AS ssl ON SC3I_weighting_working_table.scaling_segment_id = ssl.scaling_segment_id
                     inner join SC3I_category_working_table AS sc ON sc.value = ssl.'||@scaling_var||'
                                                                      AND sc.sky_base_universe = ssl.sky_base_universe
             ')

             commit

             IF @iteration = 100 OR @convergence = 0 SET @cntr = (@scaling_count + 1)
             ELSE

             IF @cntr = @scaling_count  SET @cntr = 1
             ELSE
             SET @cntr = @cntr+1

     END

     commit
     -- This loop build took about 4 minutes. That's fine.

     -- Calculate segment weight and corresponding indices

     -- This section calculates the segment weight which is the weight that should be applied to viewing data
     -- A couple of indices are also calculated so that we can keep track of the performance of the rim-weighting


     SELECT @sky_base = SUM(sky_base_accounts) FROM SC3I_weighting_working_table
     SELECT @vespa_panel = SUM(vespa_panel) FROM SC3I_weighting_working_table
     SELECT @sum_of_weights = SUM(sum_of_weights) FROM SC3I_weighting_working_table

     UPDATE SC3I_weighting_working_table
     SET  segment_weight = sum_of_weights / vespa_panel
         ,indices_actual = 100*(vespa_panel / @vespa_panel) / (sky_base_accounts / @sky_base)
         ,indices_weighted = 100*(sum_of_weights / @sum_of_weights) / (sky_base_accounts / @sky_base)

     commit

     -- OK, now catch those cases where stuff diverged because segments weren't reperesented:
     update SC3I_weighting_working_table
     set segment_weight  = 0.000001
     where vespa_panel   = 0.000001

     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC3I_weighting_working_table
     where segment_weight >= 0.001           -- Ignore the placeholders here to guarantee convergence

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'B02: Midway (Iterations)', coalesce(@QA_catcher, -1)
     commit

     -- Now push convergence details out to the tracking tables: the first one provides a convergence summary at a category level

     INSERT INTO SC3I_category_subtotals (scaling_date,sky_base_universe,profile,value,sky_base_accounts,vespa_panel,category_weight
                                              ,sum_of_weights, convergence)
     SELECT  @scaling_day
             ,sky_base_universe
             ,profile
             ,value
             ,sky_base_accounts
             ,vespa_panel
             ,category_weight
             ,sum_of_weights
             ,case when abs(sky_base_accounts - sum_of_weights) > 3 then 1 else 0 end
     FROM SC3I_category_working_table

     -- The SC3I_metrics table contains metrics for a particular scaling date. It shows whether the
     -- Rim-weighting process converged for that day and the number of iterations. It also shows the
     -- maximum and average weight for that day and counts for the sky base and the vespa panel.

     commit

     -- Apparently it should be reviewed each week, but what are we looking for?

     INSERT INTO SC3I_metrics (scaling_date, iterations, convergence, max_weight, av_weight,
                                  sum_of_weights, sky_base, vespa_panel, non_scalable_accounts)
     SELECT  @scaling_day
            ,@iteration
            ,@convergence
            ,MAX(segment_weight)
            ,sum(segment_weight * vespa_panel) / sum(vespa_panel)    -- gives the average weight by account (just uising AVG would give it average by segment id)
            ,SUM(segment_weight * vespa_panel)                       -- again need some math because this table has one record per segment id rather than being at acocunt level
            ,@sky_base
            ,sum(CASE WHEN segment_weight >= 0.001 THEN vespa_panel ELSE NULL END)
            ,sum(CASE WHEN segment_weight < 0.001  THEN vespa_panel ELSE NULL END)
     FROM SC3I_weighting_working_table

     update SC3I_metrics
        set sum_of_convergence = abs(sky_base - sum_of_weights)

     insert into SC3I_non_convergences(scaling_date,scaling_segment_id, difference)
     select @scaling_day
           ,scaling_segment_id
           ,abs(sum_of_weights - sky_base_accounts)
       from SC3I_weighting_working_table
      where abs((segment_weight * vespa_panel) - sky_base_accounts) > 3

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'B02: Complete (Calculate weights)', coalesce(@QA_catcher, -1)
     commit



     /**************** PART B03: PUBLISHING WEIGHTS INTO INTERFACE STRUCTURES ****************/

     -- Here is where that bit of interface code goes, including extending the intervals
     -- in the Scaling midway tables (which now happens one day ata time). Maybe this guy
     -- wants to go into a new and different stored procedure?

     -- Heh, this deletion process clears out everything *after* the scaling day, meaning we
     -- have to start from the beginning doing this processing... I guess we'll just manage
     -- the historical build like this. (This is because we'd otherwise have to manage adding
     -- additional records to the interval table when we re-run a day and break an interval
     -- that already exists, and that whole process would be annoying to manage.)

     -- Except we'll only nuke everything if we *rebuild* a day that's not already there.
     if (select count(1) from SC3I_Weightings where scaling_day = @scaling_day) > 0
     begin
         delete from SC3I_Weightings where scaling_day = @scaling_day

         delete from SC3I_Intervals where reporting_starts = @scaling_day

         update SC3I_Intervals set reporting_ends = dateadd(day, -1, @scaling_day) where reporting_ends >= @scaling_day
     end
     commit

     -- Part 1: Update the Vespa midway scaling tables. In Vespa Analysts? May as well
     -- also keep this in VIQ_prod too.
     insert into SC3I_Weightings
     select
         @scaling_day
         ,scaling_segment_id
         ,vespa_panel
         ,sky_base_accounts
         ,segment_weight
         ,sum_of_weights
         ,indices_actual
         ,indices_weighted
         ,case when abs(sky_base_accounts - sum_of_weights) > 3 then 1 else 0 end
     from SC3I_weighting_working_table
     -- Might have to check that the filter on segment_weight doesn't leave any orphaned
     -- accounts about the place...

     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC3I_Weightings
     where scaling_day = @scaling_day

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'B03: Midway 1/4 (Midway weights)', coalesce(@QA_catcher, -1)
     commit

     -- First off extend the intervals that are already in the table:
/*
     update SC3I_Intervals
     set reporting_ends = @scaling_day
     from SC3I_Intervals
     inner join SC3I_Todays_panel_members as tpm
     on SC3I_Intervals.account_number         = tpm.account_number
     and SC3I_Intervals.scaling_segment_ID    = tpm.scaling_segment_ID
     where reporting_ends = @scaling_day - 1

     -- Next step is adding in all the new intervals that don't appear
     -- as extensions on existing intervals. First off, isolate the
     -- intervals that got extended

     select account_number
     into #included_accounts
     from SC3I_Intervals
     where reporting_ends = @scaling_day

     commit
     create unique index fake_pk on #included_accounts (account_number)
     commit

     -- Now having figured out what already went in, we can throw in the rest:
     insert into SC3I_Intervals (
         account_number
         ,HH_person_number
         ,reporting_starts
         ,reporting_ends
         ,scaling_segment_ID
     )
     select
         tpm.account_number
         ,HH_person_number
         ,@scaling_day
         ,@scaling_day
         ,tpm.scaling_segment_ID
     from SC3I_Todays_panel_members as tpm
     left join #included_accounts as ia
     on tpm.account_number = ia.account_number
     where ia.account_number is null -- we don't want to add things already in the intervals table


     commit
     drop table #included_accounts
     commit
*/
     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC3I_Intervals where reporting_ends = @scaling_day

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'B03: Midway 2/4 (Midway intervals)', coalesce(@QA_catcher, -1)
     commit

     -- Part 2: Update the VIQ interface table (which needs the household key thing)
     if (select count(1) from V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING where scaling_date = @scaling_day) > 0
     begin
         delete from V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING where scaling_date = @scaling_day
     end
     commit

     insert into V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING
     select
         ws.account_number
         ,ws.HH_person_number
         ,@scaling_day
         ,wwt.segment_weight
         ,@batch_date
     from SC3I_weighting_working_table as wwt
     inner join SC3I_Sky_base_segment_snapshots as ws -- need this table to get the cb_key_household items
     on wwt.scaling_segment_id = ws.population_scaling_segment_id
     inner join SC3I_Todays_panel_members as tpm
     on ws.account_number = tpm.account_number       -- Filter for today's panel only
     and ws.hh_person_number = tpm.hh_person_number
     and ws.profiling_date = @profiling_date

     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING
     where scaling_date = @scaling_day

     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'B03: Midway 3/4 (VIQ interface)', coalesce(@QA_catcher, -1)
     commit

     execute logger_add_event @Scale_refresh_logging_ID, 3, 'B03: Complete! (Publish weights)'
     commit
     execute logger_add_event @Scale_refresh_logging_ID, 3, 'SC3: Weights made for ' || dateformat(@scaling_day, 'yyyy-mm-dd')
     commit

end; -- of procedure "V289_M11_04_SC3I_v1_1__make_weights_BARB"
commit;


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
**Project Name:							Skyview H2I
**Analysts:                             Angel Donnarumma	(angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jason Thompson		(Jason.Thompson@skyiq.co.uk)
										,Hoi Yu Tang		(HoiYu.Tang@skyiq.co.uk)
										,Jose Pitteloud		(jose.pitteloud@skyiq.co.uk)
**Stakeholder:                          SkyIQ
										,Jose Loureda		(Jose.Loureda@skyiq.co.uk)
**Due Date:                             11/07/2014
**Project Code (Insight Collation):     v289
**Sharepoint Folder:    

	http://sp-department.bskyb.com/sites/SIGEvolved/Shared%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FSIGEvolved%2FShared%20Documents%2F01%20Analysis%20Requests%2FV289%20-%20Skyview%20Futures%2F01%20Plans%20Briefs%20and%20Project%20Admin                                                        
	                                                          
**Business Brief:

	This Script goal is to generate metrics to compare the performance of the process vs. BARB 

**Sections:
	
	M12: Validation Process 
			M12.0 - Initialising Environment
			M12.1 - Slicing for weighted duration (Skyview)
			
		
--------------------------------------------------------------------------------------------------------------
*/
	
-----------------------------------	
-- M12.0 - Initialising Environment
-----------------------------------

create or replace procedure v289_m12_validation
as begin

	MESSAGE cast(now() as timestamp)||' | Begining M12.0 - Initialising Environment' TO CLIENT
		   
	MESSAGE cast(now() as timestamp)||' | @ M12.0: Building Stage1 table from M07' TO CLIENT
	
	if object_id('stage1') is not null
		drop table stage1

	commit
					
	select  'VESPA'                     as source
			,date(m07.event_start_utc)  as scaling_date
			,ska.channel_name
			,ska.channel_pack
			,trim(m07.session_daypart)  as daypart
			,m07.event_id
			,m07.overlap_batch
			,coalesce(m07.chunk_duration_seg,m07.event_duration_seg) as duration_seg
			,m07.account_number
	into    stage1
	from    V289_M07_dp_data                        as m07 --  11158541
			inner join  v289_m06_dp_raw_data        as m06 -- <-- we need this guy to get the service_key to then get the channel name
			on  m07.event_id    = m06.pk_viewing_prog_instance_fact
			left join vespa_Analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES    as ska
			on	m06.service_key = ska.service_key
			and m07.event_start_utc between ska.EFFECTIVE_FROM and ska.EFFECTIVE_TO

	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M12.0: Stage1 Table DONE' TO CLIENT
	
	MESSAGE cast(now() as timestamp)||' | @ M12.0: Building Overlaps_Side table' TO CLIENT
	
	if object_id('Overlaps_side') is not null
		drop table Overlaps_side

	commit

	select  s1.*
			,m10.hh_person_number
			,m10.person_ageband as age
			,case   when m10.person_gender = 'F' then 'Female'
					when m10.person_gender = 'M' then 'Male'
					else 'Undefined'
			end     as gender
			,m11.weight
	into    Overlaps_side -- 222862 row(s) affected
	from    stage1 as s1
			inner join V289_M10_session_individuals as m10 -- <-- we need this table to get the persons ids to get the scaling weights later on
			on  s1.event_id    		= m10.event_id
			and	s1.account_number	= m10.account_number
			and s1.overlap_batch   	= m10.overlap_batch 
			inner join  (
							select  distinct
									account_number
									,hh_person_number
									,scaling_date
									,scaling_weighting  as weight
							from    V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING
						)   AS M11
			on  m10.account_number      = m11.account_number
			and m10.hh_person_number    = m11.hh_person_number
			and s1.scaling_date         = m11.scaling_date 

	commit

	MESSAGE cast(now() as timestamp)||' | @ M12.0: Overlaps_Side table DONE' TO CLIENT
	
	MESSAGE cast(now() as timestamp)||' | @ M12.0: Building No_Overlaps_Side table' TO CLIENT
	
	if object_id('no_Overlaps_side') is not null
		drop table no_Overlaps_side

	commit

	select  s1.*
			,m10.hh_person_number
			,m10.person_ageband as age
			,case   when m10.person_gender = 'F' then 'Female'
					when m10.person_gender = 'M' then 'Male'
					else 'Undefined'
			end     as gender
			,m11.weight
	into    no_Overlaps_side -- 9037422 row(s) affected
	from    stage1 as s1
			inner join V289_M10_session_individuals as m10 -- <-- we need this table to get the persons ids to get the scaling weights later on
			on  s1.event_id    		= m10.event_id
			and	s1.account_number	= m10.account_number
			and m10.overlap_batch is null
			inner join  (
							select  distinct
									account_number
									,hh_person_number
									,scaling_date
									,scaling_weighting  as weight
							from    V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING
						)   AS M11
			on  m10.account_number      = m11.account_number
			and m10.hh_person_number    = m11.hh_person_number
			and s1.scaling_date         = m11.scaling_date 

	commit

	MESSAGE cast(now() as timestamp)||' | @ M12.0: No_Overlaps_Side table DONE' TO CLIENT
	
	MESSAGE cast(now() as timestamp)||' | @ M12.0: Building v289_m12_dailychecks_base table' TO CLIENT
	
	if	object_id('v289_m12_dailychecks_base') is not null
		drop table v289_m12_dailychecks_base
		
	commit

	select	*
	into	v289_m12_dailychecks_base
	from	(
				select	*
				from 	Overlaps_side
				union
				select	*
				from	no_Overlaps_side
			)	as base
			
	commit

	create hg index hg1 	on v289_m12_dailychecks_base(account_number)
	create hg index hg2 	on v289_m12_dailychecks_base(event_id)
	create hg index hg3 	on v289_m12_dailychecks_base(channel_name)
	create lf index lf1 	on v289_m12_dailychecks_base(hh_person_number)
	create lf index lf2 	on v289_m12_dailychecks_base(channel_pack)
	create lf index lf3 	on v289_m12_dailychecks_base(daypart)
	create date index dt1	on v289_m12_dailychecks_base(scaling_date)
	commit

	grant select on v289_m12_dailychecks_base to vespa_group_low_security
	commit

	MESSAGE cast(now() as timestamp)||' | @ M12.0: v289_m12_dailychecks_base table DONE' TO CLIENT
	
	drop table Overlaps_side
	drop table no_Overlaps_side
	drop table stage1
	commit

	MESSAGE cast(now() as timestamp)||' | @ M12.0: Creating table v289_S12_v_weighted_duration_skyview' TO CLIENT
	
	if object_id('v289_S12_weighted_duration_skyview') is not null
		drop table v289_S12_weighted_duration_skyview
		
	commit
		
	select	*
	into	v289_S12_weighted_duration_skyview
	from	(
				select  source                                                                         
						,scaling_date
						,age
						,trim(gender)   					as gender
						,daypart
						,account_number						as household
						,hh_person_number					as person
						,min(weight)                        as ukbase
						,sum(duration_seg)/60.00            as duration_mins
						,(sum(duration_seg)*ukbase)/60.00   as duration_weighted_mins
				from    v289_m12_dailychecks_base
				group   by  source
							,scaling_date
							,age
							,gender
							,daypart
							,account_number
							,hh_person_number
				UNION ALL 
				select	*
				from	(
							SELECT	'BARB'                                      as source 
									,DATE(v.start_time_of_session)	        as scaling_date
									,trim(v.ageband) 				            as age
									,trim(v.sex) 					            as gender
									,trim(v.session_daypart)		            as daypart
									,cast(v.household_number as varchar(12))    as household
									,v.person_number				            as person
									,min(v.processing_weight)		            as ukbase
									,sum(v.progwatch_duration)  	as duration_min
									,sum(v.progscaled_duration) 	as duration_weighted_min
							FROM 	skybarb_fullview as v
									LEFT JOIN	vespa_Analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES	AS mp	
									ON	mp.service_key = v.service_key
									AND DATE(broadcast_start_date_time_local) BETWEEN mp.EFFECTIVE_FROM AND mp.EFFECTIVE_TO
							GROUP BY	source
										,scaling_date
										,age
										,gender
										,daypart
										,v.household_number
										,v.person_number
						)	barb
				where	scaling_date = (select max(cast(event_start_date_time_utc as date)) from v289_M06_dp_raw_data)
			)	as final
			
	create hg index hg1 	on v289_S12_weighted_duration_skyview(household)
	create lf index lf1 	on v289_S12_weighted_duration_skyview(person)
	create lf index lf3 	on v289_S12_weighted_duration_skyview(daypart)
	create date index dt1	on v289_S12_weighted_duration_skyview(scaling_date)
	commit
	
	grant select on v289_S12_weighted_duration_skyview to vespa_group_low_security
	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M12.0: v289_S12_v_weighted_duration_skyview table DONE' TO CLIENT
	
	drop table v289_m12_dailychecks_base
	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M12.0: Initialising Environment DONE' TO CLIENT
	
-------------------------------------------------	
-- M12.1 - Slicing for weighted duration (Skyview)
-------------------------------------------------
	
	MESSAGE cast(now() as timestamp)||' | Begining M12.1 - Slicing for weighted duration (Skyview)' TO CLIENT
	
	MESSAGE cast(now() as timestamp)||' | @ M12.1: Creating View v289_s12_avgminwatched_x_genderage' TO CLIENT
	
	if object_id('v289_s12_avgminwatched_x_genderage') is not null
		drop view v289_s12_avgminwatched_x_genderage
	
	commit
		
	create view v289_s12_avgminwatched_x_genderage as
	    select  scaling_date
			,source
			,case when source = 'BARB' and age = '0-19' then 'Undefined' else gender end as gender
			,age
            ,count(distinct individuals)                        as sample
            ,sum(weights)                                       as weighted_sample
			,sum(minutes_watched)			                    as total_mins_watched
			,sum(minutes_watched_scaled)	                    as total_mins_scaled_watched
			,avg(minutes_watched)/60.00                         as avg_hh_watched
			,sum(minutes_watched_scaled)/weighted_sample/60.00  as avg_hh_watched_scaled
	from    (
				select  scaling_date
						,source
						,age
						,gender
						,household||'-'||person         as individuals
						,min(ukbase)                    as weights
						,sum(duration_mins)             as minutes_watched
						,sum(duration_weighted_mins)    as minutes_watched_scaled
				from    v289_S12_weighted_duration_skyview
				group   by  scaling_date
							,source
							,age
							,gender
							,individuals
			)   as base
	group   by  scaling_date
				,source
				,gender
				,age

	grant select on v289_s12_avgminwatched_x_genderage to vespa_group_low_security
	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M12.1: Creating View v289_s12_avgminwatched_x_genderage DONE' TO CLIENT
	
	MESSAGE cast(now() as timestamp)||' | @ M12.1: Creating View v289_s12_sovminwatched_x_dimensions' TO CLIENT
	
	if object_id('v289_s12_sovminwatched_x_dimensions') is not null
		drop view v289_s12_sovminwatched_x_dimensions
	
	commit
	
	create view v289_s12_sovminwatched_x_dimensions as
	select  source
			,scaling_date
			,age
			,case 	when source = 'BARB' and age = '0-19' then 'Undefined'
					else gender
			end		as gender
			,daypart
			,sum(duration_mins)             as minutes_watched
			,sum(duration_weighted_mins)    as minutes_weighted_watched
            ,count(Distinct cast((household||'-'||person) as varchar(30)))  as sample
            ,sum(ukbase)                                                    as weighted_sample
			,avg(duration_mins)				as avg_min_watched
	from    v289_S12_weighted_duration_skyview
	group   by  source
				,scaling_date
				,age
				,gender
				,daypart

	grant select on v289_s12_sovminwatched_x_dimensions to vespa_group_low_security
	commit

	MESSAGE cast(now() as timestamp)||' | @ M12.1: Creating View v289_s12_sovminwatched_x_dimensions DONE' TO CLIENT
	
	
-- V289_s12_v_hhsize_distribution

	MESSAGE cast(now() as timestamp)||' | @ M12.1: Creating View V289_s12_v_hhsize_distribution' TO CLIENT

	if object_id('V289_s12_v_hhsize_distribution') is not null	
		drop view V289_s12_v_hhsize_distribution
		
	commit

	create view V289_s12_v_hhsize_distribution as
	select  'VESPA' 		    as source
			,hhsize
			,count(1)   		as	hits
			,sum(hhweighted)	as	ukbase
	from    (
				select  m07.account_number
						,min(m08.household_size)		as	hhsize
						,sum(m11.scaling_weighting)	    as hhweighted
				from    (
							select  distinct
									account_number
							from    v289_m07_dp_data
						)   as 	m07
						inner join 	V289_M08_SKY_HH_composition as 	m08		
						on  m07.account_number = m08.account_number
						and	m08.person_head = '1'
						inner join	(
										select  distinct
												account_number
												,hh_person_number
												,scaling_date
												,scaling_weighting
										from    V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING
									)   as	m11		
					on  m07.account_number      = m11.account_number
					and	m11.HH_person_number    = m08.HH_person_number
					and	m11.scaling_date        = (select max(date(event_start_utc)) from	v289_m07_dp_data)
					group   by  m07.account_number
				)   as base
	group   by  source
				,hhsize
	union   
	select  'BARB'  as source
			,hhsize
			,count(1)           as hits
			,sum(hhweighted)    as ukbase
	from    (
				select  house_id
						,count(1) as hhsize
						,sum(weight.processing_weight)/10  as hhweighted
				from    angeld.skybarb	as	skybarb
						left join angeld.barb_weights as weight
						on  skybarb.house_id    = weight.household_number
						and skybarb.person      = weight.person_number
						and skybarb.head        = 1
				group   by  house_id
			)   as base
	group   by  source
				,hhsize
	
	
	grant select on V289_s12_v_hhsize_distribution to vespa_group_low_security
	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M12.1: Creating View V289_s12_v_hhsize_distribution DONE' TO CLIENT
	
	
-- V289_s12_v_genderage_distribution

	MESSAGE cast(now() as timestamp)||' | @ M12.1: Creating View V289_s12_v_genderage_distribution' TO CLIENT

	if object_id('V289_s12_v_genderage_distribution') is not null	
		drop view V289_s12_v_genderage_distribution
		
	commit

	create view V289_s12_v_genderage_distribution as
	select  'VESPA'                             as source
			,trim(m08.person_ageband)	        as ageband
			,case   when m08.person_gender = 'F' then 'Female'
					when m08.person_gender = 'M' then 'Male'
					else 'Undefined'
			end     as genre
			,count(1)   as hits
            ,cast(sum(m11.weight) as integer)   as sow
	from    V289_M08_SKY_HH_composition as m08
			inner join  (
							select  distinct
									account_number
							from    v289_m07_dp_data
						)   as m07
			on  m08.account_number = m07.account_number
            inner join	(
						    select  distinct
							        account_number
									,hh_person_number
									,scaling_date
									,scaling_weighting  as weight
							from    V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING
						)   as	m11		
			on  m08.account_number      = m11.account_number
			and	m08.HH_person_number    = m11.HH_person_number
			and	m11.scaling_date        = (select max(date(event_start_utc)) from	v289_m07_dp_data)
	group   by  source
				,ageband
				,genre
	union
	select  'BARB'                                                  as source
			,case   when age between 1 and 17	then '0-19'
					when age between 20 and 24 	then '20-24'
					when age between 25 and 34 	then '25-34'
					when age between 35 and 44 	then '35-44'
					when age between 45 and 64 	then '45-64'
					when age > 65              	then '65+'
					else 'Undefined'  
			end     as ageband
			,trim(sex)	                                            as sex_
			,count(1)                                               as hits
            ,cast((sum(weight.processing_weight)/10) as integer)    as hhweighted
	from    angeld.skybarb	as	skybarb
            left join angeld.barb_weights as weight
			on  skybarb.house_id    = weight.household_number
			and skybarb.person      = weight.person_number
	group   by  source
				,ageband
				,sex_

	
	grant select on V289_s12_v_genderage_distribution to vespa_group_low_security
	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M12.1: Creating View V289_s12_v_genderage_distribution DONE' TO CLIENT
	

-- v289_s12_overall_consumption

	MESSAGE cast(now() as timestamp)||' | @ M12.1: Creating View v289_s12_overall_consumption' TO CLIENT

	if object_id('v289_s12_overall_consumption') is not null	
		drop view v289_s12_overall_consumption
		
	commit

	create view v289_s12_overall_consumption as
	select  source
			,scaling_date
			,count(distinct individual)                         as sample
			,sum(weight)                                        as scaled_sample
			,sum(minutes_watched)                               as source_mins_watched
			,sum(minutes_watched_scaled)                        as source_scaled_mins_watched
			,avg(minutes_watched)/60.00                         as avg_mins_watched
			,source_scaled_mins_watched / scaled_sample / 60.00 as avg_scaled_mins_watched
	from    (   
				select  source
						,scaling_date
						,household||'-'||person         as individual
						,min(ukbase) as weight
						,sum(duration_mins)             as minutes_watched
						,sum(duration_weighted_mins)    as minutes_watched_scaled
				from    v289_S12_weighted_duration_skyview
				group   by  source
							,scaling_date
							,individual
			)   as base
	group   by  source
				,scaling_date
				
	grant select on v289_s12_overall_consumption to vespa_group_low_security
	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M12.1: Creating View v289_s12_overall_consumption DONE' TO CLIENT
	
	MESSAGE cast(now() as timestamp)||' | @ M12.1: Slicing for weighted duration (Skyview) DONE' TO CLIENT
	
end;


commit;
grant execute on v289_m12_validation to vespa_group_low_security;
commit;/*
Simple procedure to save output tables from H2I run for further analysis/backup

Syntax:
		execute v289_backup_H2I_tables;
		execute v289_backup_H2I_tables 50;

*/


create or replace procedure v289_backup_H2I_tables
    @pc     int     =   NULL -- Enter the sample size here as appropriate
    as  begin
        
        if @pc is not null  
            begin

                message cast(now() as timestamp) || ' | Saving H2I tables for sample size : ' || cast(@pc as varchar) || '% ...' to client
				
				
				-- Declare variables
                declare @table_name     varchar(255)                                            commit --(^_^ )!
                declare @datestr        varchar(255)    = dateformat(now(),'_yyyymmdd_HHMMSS_') commit --(^_^ )!
                declare @sql_           varchar(255)                                            commit --(^_^ )!
                declare @sql2_          varchar(255)                                            commit --(^_^ )!
				declare @sql3_ 			varchar(255)											commit --(^_^ )!
                declare @i              int             = 0                                     commit --(^_^ )!

				message cast(now() as timestamp) || ' | ' || @datestr || cast(@pc as varchar) || 'pc' to client

				
				-- Loop over tables to back up
                while @i < 6    begin

                    -- Progress counter
					set @i = @i + 1
                    commit --(^_^ )!

					-- Define target table name and relevant copy and permissions commands
                    set @table_name =   case @i
                                            when    1   then    'v289_M06_dp_raw_data'
                                            when    2   then    'V289_M07_dp_data'
                                            when    3   then    'V289_M10_session_individuals'
                                            when    4   then    'V289_M10_combined_event_data'
                                            when    5   then    'V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING'
                                            when    6   then    'SC3I_Weightings'
                                        end
                    commit --(^_^ )!

                    set @sql_   = 'select * into ' + @table_name + @datestr + cast(@pc as varchar) + 'pc from ' + @table_name 
                    commit --(^_^ )!
                    
                    set @sql2_  = 'grant select on ' + @table_name + @datestr + cast(@pc as varchar) + 'pc to vespa_group_low_security'
                    commit --(^_^ )!

					-- Show SQL to client
                    set	@sql3_ =	'select ' + @sql_	+ ' union select ' + @sql2_	commit
					message cast(now() as timestamp) || ' | SQL : ' || @sql3_ to client

                    -- Execute SQL
                    execute(@sql_)
                    commit --(^_^ )!
                    execute(@sql2_)
                    commit --(^_^ )!
					
					
                end -- while
				
                message cast(now() as timestamp) || ' | Saving H2I tables for sample size : ' || cast(@pc as varchar) || '% ... DONE!' to client

            end -- if begin
        else
            message cast(now() as timestamp) || ' | Sample size input required. Exiting.' to client
        
    end -- procedure
commit
;
