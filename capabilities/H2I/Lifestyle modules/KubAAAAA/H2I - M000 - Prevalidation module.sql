
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
    if NOT exists(  select tname from syscatalog
                where creator = user_name()
                and upper(tname) = 'V289_TABLES_CHECK'
                and tabletype = 'TABLE')
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
     ('BARB_Channel_Map','BARB_Channel_Map')
    ,('CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES','CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES')
    ,('cust_entitlement_lookup','cust_entitlement_lookup')
    ,('CUST_SET_TOP_BOX','CUST_SET_TOP_BOX')
    ,('cust_single_account_view','cust_single_account_view')
    ,('cust_subs_hist','cust_subs_hist')
    ,('experian_consumerview','experian_consumerview')
    --,('PI_BARB_IMPORT','PI_BARB_IMPORT_V')
    ,('PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD','PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD')
    --,('SC3I_Variables_lookup_v1_1','SC3I_Variables_lookup_v1_1')
    ,('VESPA_PROGRAMME_SCHEDULE','VESPA_PROGRAMME_SCHEDULE')
    ,('VIQ_VIEWING_DATA_SCALING','VIQ_VIEWING_DATA_SCALING')
    ,('BARB_INDV_PANELMEM_DET', 'BARB_INDV_PANELMEM_DET')
    ,('BARB_PANEL_DEMOGR_TV_CHAR','BARB_PANEL_DEMOGR_TV_CHAR')
    ,('BARB_PANEL_MEM_RESP_WGHT','BARB_PANEL_MEM_RESP_WGHT')
    ,('BARB_PANEL_DEMOGR_HOME_CHAR','BARB_PANEL_DEMOGR_HOME_CHAR')
    


    INSERT INTO V289_Tables_check (table_name, view_name) ---- INSERTING Viewing Table
    SELECT @table_name, 'V289_viewing_data_view'

    SET @table_name = 'VESPA_STREAM_VOD_VIEWING_PROG_FACT_'||datepart(year,@viewing_date)||right(('00'||cast(datepart(month,@viewing_date) as varchar(2))),2)
    INSERT INTO V289_Tables_check (table_name, view_name) ---- INSERTING PullVOD Table
    SELECT @table_name, @table_name


    COMMIT
            MESSAGE '2.- Tables to check Inserted' TO CLIENT
    --------------------------------------------------------
    WHILE EXISTS  (SELECT top 1 table_name FROM V289_Tables_check WHERE processed = 0)
    BEGIN
        SET @table_name = (SELECT top 1 REPLACE (TRIM(table_name),' ' ,'' ) FROM V289_Tables_check WHERE processed = 0)
        SET @view_name  = (SELECT top 1 REPLACE (TRIM(view_name),' ' ,'' ) FROM V289_Tables_check WHERE table_name = @table_name)
        SET @schema_    = ''

        SET @exe_status = -1
        SET @sql_  = 'EXECUTE @exe_status = SELECT top 1 * INTO #t1 FROM '||@table_name
        EXECUTE (@sql_ )

    --  MESSAGE 'Result: '|| @exe_status   TO CLIENT
        IF @exe_status <> 0
        BEGIN                           ----------------------------------  Prefix needed section START
--          MESSAGE '2.1 .- Prefix Needed '  TO CLIENT

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

--        MESSAGE '2.2 .- #Tables created: Rows:'||@@rowcount  TO CLIENT



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

                EXECUTE (@sql_1)
                if @exe_status <> 0
                SET @cont = @cont

                UPDATE #tables
                SET   Checked = 1
                    , Working = CASE WHEN @exe_status = 0 THEN 1 ELSE 0  END
                WHERE row_id = @cont
                COMMIT
                SET @cont = @cont +1

            END                                         ------------------- VALIDATION Section END
            ----- In case no table is available SECTION START

            IF (SELECT SUM(Working) FROM #tables) = 0   BEGIN
                    MESSAGE '4.- ERROR - No table Available:    '||@table_name TO CLIENT
                    goto anytable
                END

                ------ In case no table is available SECTION END
            ELSE
            SET @cont = @cont

            UPDATE  #tables                                 ------ Establishing schema priority selection
            SET Selected = CASE WHEN UPPER(user_name_) = 'SK_PROD' THEN 1
                                WHEN UPPER(user_name_) = 'VESPA_ANALYSTS' THEN 2
                                WHEN UPPER(user_name_) = 'VESPA_SHARED' THEN 3
                                WHEN UPPER(user_name_) = 'SK_PROD_VESPA_RESTRICTED' THEN 4
                                WHEN UPPER(user_name_) LIKE 'SK_PROD_%' THEN 4
                                WHEN UPPER(user_name_) = user_name() THEN 5
                                WHEN UPPER(user_name_) IN ('THOMPSONJA') THEN 6
                                ELSE 10
                                END
            WHERE Working = 1                   ------ Only working tables
            COMMIT



            --SELECT * INTO tablee FROM #tables
            COMMIT

            SET @schema_    = (SELECT top 1 user_name_  FROM #tables WHERE Working = 1 ORDER BY Selected ASC)       ------Defining the chosen one
            DROP TABLE #tables

        END                             ----------------------------------- Prefix needed section END
        SET @sql_ = 'IF EXISTS( select tname FROM syscatalog '
        SET @sql_ = @sql_ ||'where creator = user_name() and tabletype = ''TABLE'' and upper(tname) = upper('||''''||@view_name||''''||'))   DROP TABLE '||@view_name

        EXECUTE (@sql_)

        SET @sql_ = 'IF EXISTS( select tname FROM syscatalog '
        SET @sql_ = @sql_ ||'where creator = user_name() and tabletype = ''VIEW'' and upper(tname) = upper( '||''''||@view_name||''''||'))   DROP VIEW '||@view_name

        EXECUTE (@sql_)
        COMMIT

        SET @sql_ = 'CREATE VIEW '||@view_name||' AS SELECT * FROM '||@schema_||'.'||@table_name--||';commit'
        SET @exe_status = -1
        MESSAGE @sql_ TO CLIENT
        EXECUTE @exe_status = v289_validation_table @sql_
        IF @exe_status = 0
            MESSAGE '5.- Table Processed: '||@schema_||'.'||@table_name TO CLIENT
        ELSE
            MESSAGE '6.- ERROR Table creation failed CODE: '||@exe_status||'   '||@schema_||'.'||@table_name TO CLIENT

        anytable:                           -------------       Label in case no table is found
        COMMIT
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








    COMMIT
END; --- END of the procedure
COMMIT;
GRANT EXECUTE ON v289_m000_Prevalidation TO vespa_group_low_security;
commit;


