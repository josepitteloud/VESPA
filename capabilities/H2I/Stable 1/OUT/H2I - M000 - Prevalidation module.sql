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
                
                SET @schema_        = (SELECT top 1  REPLACE (TRIM(user_name_),' ' ,'' )        FROM #tables WHERE row_id = @cont)
                SET @table_name     = (SELECT top 1  REPLACE (TRIM(table_name_),' ' ,'' )       FROM #tables WHERE row_id = @cont)
                
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
        

        SET @sql_ = 'IF object_id('||''''||@view_name||''''||') is not null DROP VIEW '||@view_name--||';'
        SET @sql_ = @sql_||' CREATE VIEW '||@view_name||' AS SELECT * FROM '||@schema_||'.'||@table_name--||';commit'
            
        --  MESSAGE '2.5 .- EXECUTING:      '||@sql_  TO CLIENT 
            
        SET @exe_status = -1 
        EXECUTE @exe_status = v289_validation_table @sql_
        COMMIT 
        
        IF @exe_status = 0 
            MESSAGE '5.- Table Processed: '||@schema_||'.'||@table_name TO CLIENT 
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
/* 
        -- skybarb views
        CREATE OR REPLACE VIEW skybarb_fullview
        AS
        SELECT * FROM vespa_analysts.skybarb_fullview
        

        CREATE OR REPLACE VIEW skybarb
        AS
        SELECT * FROM vespa_analysts.skybarb
        

        
        -- Copy entire BARB_Panel_Member_Responses_Weights_and_Viewing_Categories table from Angel's schema
        IF OBJECT_ID('BARB_Panel_Member_Responses_Weights_and_Viewing_Categories') IS NOT NULL
            DROP TABLE BARB_Panel_Member_Responses_Weights_and_Viewing_Categories
        
        SELECT  * 
        INTO    BARB_Panel_Member_Responses_Weights_and_Viewing_Categories 
        FROM    angeld.BARB_Panel_Member_Responses_Weights_and_Viewing_Categories
        COMMIT
*/

        

        -- Copy entire PI_BARB_import table from vespa_shared schema
        IF OBJECT_ID('PI_BARB_import') IS NOT NULL
            DROP TABLE PI_BARB_import
            
        SELECT  *
        INTO    PI_BARB_import
        FROM    vespa_shared.PI_BARB_import
        COMMIT

        COMMIT

                
        
        
     

    COMMIT
END; --- END of the procedure 
COMMIT;
GRANT EXECUTE ON v289_m000_Prevalidation TO vespa_group_low_security;
commit;


