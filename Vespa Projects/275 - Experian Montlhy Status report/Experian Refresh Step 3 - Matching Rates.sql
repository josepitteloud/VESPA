/******************************************************************************
**
**  Project Vespa: Experian Data Quality Report
**
**  Intended to produce a repository with basic quality metrics. This will feed a Monthly Quality Report that will 
**  allow us to track any deviation on the data acquired
**
**  Code sections:
**      General Tables Metric
**      Experian Consumerview
**      Experian Lifstyle
**      Playpen Consumerview
**      Playpen Lifestyle
**      Loading dates
**      [Quasi]Null Columns
******************************************************************************/
--execute Experian_montlhy_report_3 0,0,0,1,1,1,1



CREATE OR REPLACE PROCEDURE Experian_montlhy_report_3
        @c5 bit
      , @c6 bit
      , @c7 bit
      , @c8 bit
      , @c9 bit
      , @c10 bit
      , @c11 bit
AS
BEGIN 


    DECLARE @run_id int
          , @sql2       VARCHAR(1000)
          , @Tablec     VARCHAR(100)
          , @ColID      INT
          , @ColumnN    VARCHAR(200)
          , @cont       INT  
          , @sql1     VARCHAR(1000)
          , @Table    VARCHAR(100)
          , @TableID  BIGINT      
    /*
    SET @c5 = 1 --EXPERIAN_CONSUMERVIEW Totals
    SET @c6 = 1 --Experian Match Rates
    SET @c7 = 1 --Experian Lifestyle Match Rates
    SET @c8 = 1 --Propensities Match rates
    SET @c9 = 1 --Playpen Match Rates
    SET @c10 = 1 --Playpen Lifestyle
    SET @c11 = 1 --VESPA ALL
    */
    SET @run_id   = ISNULL ((SELECT max(mValue)  FROM Experian_Refresh_log WHERE Description =  'Run ID'), 1)

    IF object_id('pitteloudj.Experian_Refresh_Results') is null 
    BEGIN
    CREATE TABLE Experian_Refresh_Results
      (MetricID int IDENTITY
      , MetricDescription varchar(80)
      , MetricValue float
      , Recordate Datetime
      , run_id int
      )
      END
    --------------------------------------------------------------------------------------
    --------------------------------------------------EXPERIAN CONSUMERVIEW Matching rates
    --------------Extracting EXPERIAN_CONSUMERVIEW Data
    IF @c5 = 1
    BEGIN 
        ----------------------------TOTAL Sky Accounts Metric
        INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Total SkyBase Accounts' 
       , count(DISTINCT account_number)
       , getdate()
       , @run_id
      FROM skybase 
       
      ----------------------------------------Get Total Rows in Experian Consumerview
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Total Experian ConsumerView individual'
         , count(DISTINCT cb_key_individual)
         , getdate()
         , @run_id run_id
      FROM exp_cv_data   
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Total Experian ConsumerView HouseHold' 
         , count(DISTINCT cb_key_household)
         , getdate()
         , @run_id 
      FROM exp_cv_data 
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Total Experian ConsumerView Postcodes' 
         , count(DISTINCT cb_address_postcode)
         , getdate()
         , @run_id
      FROM exp_cv_data
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Total Experian ConsumerView family' 
         , count(DISTINCT cb_key_family)
         , getdate()
         , @run_id run_id
      FROM exp_cv_data   
      INSERT INTO Experian_Refresh_log (mValue, Description, Date_log)
            VALUES (  @run_id  , 'EXPERIAN CONSUMERVIEW Total Values Done'      , getdate()) 
    commit
    END

    ----------------------------------------Get Sky Match Rate at Individual Level
    IF @c6 = 1
    BEGIN 
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Match Rate Individual Level - SkyBase vs experian_consumerview'
         , count(DISTINCT cb_key_individual)
         , getdate()
         , @run_id
      FROM skybase AS s
        INNER JOIN exp_cv_data AS e ON e.cb_key_individual = s.individual_key
        WHERE h_fss_v3_group is not null 
    -- Checking for Empty rows
    ----------------------------------------Get Sky Match Rate at Family Level
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Match Rate Family Level - SkyBase vs experian_consumerview'
         , count(DISTINCT cb_key_family)
         , getdate()
         , @run_id
      FROM skybase AS s
        INNER JOIN exp_cv_data AS e ON e.cb_key_family = s.family_key
        WHERE h_fss_v3_group is not null                                    -- Checking for Empty rows

      ----------------------------------------Get Sky Match Rate at Household Level
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Match Rate HouseHold Level - SkyBase vs experian_consumerview'
         , count(DISTINCT household_key)
         , getdate()
         , @run_id
      FROM skybase AS s
        INNER JOIN exp_cv_data AS e ON e.cb_key_household = s.household_key
        WHERE h_fss_v3_group is not null

      ------------------------------------Get Sky Match Rate at Postal Code  Level vs consumerview_postcode
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Total SkyBase Postcode' 
          , COUNT(*)
          , GETDATE()       
        , @run_id
      FROM Experian_Refresh_Sky_postcode

      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Match Rate Postcode Level - SkyBase vs consumerview_postcode'
          , COUNT(DISTINCT sky.postcode)
          , GETDATE()       
        , @run_id
      FROM Experian_Refresh_Sky_postcode AS sky 
      INNER JOIN consumerview_postcode AS pc
                  ON sky.postcode = TRIM(REPLACE(pc.cb_address_postcode,' ',''))
      WHERE pc.cb_address_postcode  IS NOT NULL
        AND pc.pc_fss_v3_group      IS NOT NULL


      ------------------------------------Get Total Postcodes on consumerview_postcode
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Total Rows Consumerview_Postcode'
          , COUNT(*)
          , GETDATE()       
        , @run_id
      FROM consumerview_postcode

        INSERT INTO Experian_Refresh_log (mValue, Description, Date_log)
            VALUES (  @run_id  , 'EXPERIAN CONSUMERVIEW vs Sky Matching rates Done'         , getdate()) 
    commit
    END

    ----------------------------------EXPERIAN LIEFSTYLE Matching rates vs SKY
    ---------------------------------Get Total individual, household and rows from Experian_lifestyle

    IF @c7 = 1 
    BEGIN
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Total Rows Experian_lifestyle' 
          , COUNT(*)
          , GETDATE()       
        , @run_id
      FROM exp_lifestyle 
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Total Experian_lifestyle Individual' 
         , count(DISTINCT cb_key_individual)
         , getdate()
         , @run_id
      FROM exp_lifestyle
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Total Experian_lifestyle Family' 
         , count(DISTINCT cb_key_family)
         , getdate()
         , @run_id
      FROM exp_lifestyle
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Total Experian_lifestyle Household' 
         , count(DISTINCT cb_key_household)
         , getdate()
         , @run_id
      FROM exp_lifestyle

    ------------------------------------Get Sky Match Rate at Individual Level
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Match Rate Individual Level - SkyBase vs Experian_Lifestyle' 
         , count(DISTINCT cb_key_individual)
         , getdate()
         , @run_id
      FROM skybase AS s
        INNER JOIN exp_lifestyle AS e ON e.cb_key_individual = s.individual_key
        WHERE cb_row_id is not null                          -- Checking for Empty rows

    ------------------------------------Get Sky Match Rate at Family Level
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Match Rate Individual Level - SkyBase vs Experian_Lifestyle' 
         , count(DISTINCT cb_key_family)
         , getdate()
         , @run_id
      FROM skybase AS s
        INNER JOIN exp_lifestyle AS e ON e.cb_key_family = s.family_key
        WHERE cb_row_id is not null                          -- Checking for Empty rows

        ------------------------------------Get Sky Match Rate at Household Level
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Match Rate Household Level - SkyBase vs Experian_Lifestyle' 
         , count(DISTINCT cb_key_household)
         , getdate()
         , @run_id
      FROM skybase AS s
        INNER JOIN exp_lifestyle AS e ON e.cb_key_household = s.household_key
        WHERE cb_row_id is not null                          -- Checking for Empty rows

        
        INSERT INTO Experian_Refresh_log (mValue, Description, Date_log)
            VALUES (  @run_id  , 'EXPERIAN Lifestyle vs Sky Matching rates Done'        , getdate()) 
    commit
    END               
    --------------------------------------------PERSON_PROPENSITIES_GRID_[CUR/NEW]
    --------------------------------------Extracting PERSON_PROPENSITIES_GRID_CUR Data Total rows
    IF @c8 = 1
      BEGIN 

      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Total Rows PERSON_PROPENSITIES_GRID_CUR'
         , count(*)
         , getdate()
         , @run_id
      FROM PERSON_PROPENSITIES_GRID_CUR
      ------------------------------------Get Sky Match Rate at Individual Level
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Match Rate - Skybase vs PERSON_PROPENSITIES_GRID_CUR' 
        , Count(*)
        , getdate()
        , @run_id
      FROM EXPERIAN_CONSUMERVIEW AS a
      INNER JOIN PERSON_PROPENSITIES_GRID_CUR AS b ON a.p_pixel_v2 = b.ppixel AND a.Pc_mosaic_uk_type = b.mosaicuk
      INNER JOIN skybase AS s ON a.cb_key_individual = s.individual_key


      --------------Extracting PERSON_PROPENSITIES_GRID_NEW Data
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Total Rows PERSON_PROPENSITIES_GRID_NEW'
         , count(*)
         , getdate()
         , @run_id
      FROM PERSON_PROPENSITIES_GRID_NEW
      ------------------------------------Get Sky Match Rate at Individual Level
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Match Rate - Skybase vs PERSON_PROPENSITIES_GRID_NEW' 
         , Count(*)
         , getdate()
         , @run_id
      FROM EXPERIAN_CONSUMERVIEW AS a
      INNER JOIN PERSON_PROPENSITIES_GRID_NEW AS b ON a.p_pixel_v2 = b.ppixel2011 AND a.Pc_mosaic_uk_type = b.mosaic_uk_2009_type
      INNER JOIN skybase AS s ON a.cb_key_individual = s.individual_key


      --------------------------------------------HOUSEHOLD_PROPENSITIES_GRID_[CUR/NEW]
      --------------Extracting HOUSEHOLD_PROPENSITIES_GRID_CUR Data
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Total Rows HOUSEHOLD_PROPENSITIES_GRID_CUR'
         , count(*)
         , getdate()
         , @run_id
      FROM HOUSEHOLD_PROPENSITIES_GRID_CUR
      ------------------------------------Get Sky Match Rate at Household Level
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
    SELECT 'Match Rate - Experian_Consumerview vs HOUSEHOLD_PROPENSITIES_GRID_CUR'
         , Count(distinct household_key)
            , getdate()
               , @run_id
      FROM EXPERIAN_CONSUMERVIEW AS a
      INNER JOIN HOUSEHOLD_PROPENSITIES_GRID_CUR AS b ON a.h_pixel_v2 = b.hpixel AND a.Pc_mosaic_uk_type = b.mosaicuk_type
      INNER JOIN skybase AS s ON a.cb_key_household = s.household_key  
      --------------Extracting HOUSEHOLD_PROPENSITIES_GRID_NEW Data
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Total Rows HOUSEHOLD_PROPENSITIES_GRID_NEW' 
         , Count(*)
         , getdate()
         , @run_id
      FROM HOUSEHOLD_PROPENSITIES_GRID_NEW
      ------------------------------------Get Sky Match Rate at Household Level
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Match Rate - Experian_Consumerview vs HOUSEHOLD_PROPENSITIES_GRID_NEW' 
         , Count(distinct household_key)
            , getdate()
          , @run_id
      FROM EXPERIAN_CONSUMERVIEW AS a
      INNER JOIN HOUSEHOLD_PROPENSITIES_GRID_NEW AS b ON a.h_pixel_v2 = b.hpixel2011 AND a.Pc_mosaic_uk_type = b.mosaic_uk_2009_type
      INNER JOIN skybase AS s ON a.cb_key_household = s.household_key  
      
        INSERT INTO Experian_Refresh_log (mValue, Description, Date_log)
            VALUES (  @run_id  , 'Propensities vs Sky Matching rates Done'      , getdate()) 
    commit
    END

    --------------------------------------------PLAYPEN TABLES
    --------------------------------------------PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD
    IF @c9 = 1
    BEGIN 
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Total Rows PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD' 
         , count(*)
         , getdate()
         , @run_id
      FROM sk_prod.PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD

      ------------------------------------Get Sky Match Rate at Household Level
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Total Playpen ConsumerView individual' 
         , COUNT(DISTINCT cb_key_individual)
         , getdate()
         , @run_id
      FROM play_cv_data

      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Total Playpen ConsumerView Family' 
         , COUNT(DISTINCT exp_cb_key_family)
         , getdate()
         , @run_id
      FROM play_cv_data
      
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Total Playpen ConsumerView HouseHold' 
         , COUNT(DISTINCT cb_key_household)
         , getdate()
         , @run_id
      FROM play_cv_data 
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Total Playpen ConsumerView Postcodes' 
         , count(DISTINCT postcode_fixed_vintage)
         , getdate()
         , @run_id
      FROM play_cv_data

      ------------Get Sky Match Rate at Individual Level
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Match Rate Individual Level - SkyBase vs Playpen_consumerview' 
         , count(DISTINCT cb_key_individual)
         , getdate()
         , @run_id
      FROM skybase AS s
        INNER JOIN play_cv_data AS e ON e.cb_key_individual = s.individual_key
        WHERE postcode_fixed_vintage is not null                                    -- Checking for Empty rows

      ------------Get Sky Match Rate at family Level
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Match Rate Family Level - SkyBase vs Playpen_consumerview' 
         , count(DISTINCT exp_cb_key_family)
         , getdate()
         , @run_id
      FROM skybase AS s
        INNER JOIN play_cv_data AS e ON e.exp_cb_key_family = s.family_key
        WHERE postcode_fixed_vintage is not null                                    -- Checking for Empty rows

        
      ------------Get Sky Match Rate at Household Level
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Match Rate HouseHold Level - SkyBase vs Playpen_consumerview' 
         , count(DISTINCT household_key)
         , getdate()
         , @run_id
      FROM skybase AS s
        INNER JOIN play_cv_data AS e ON e.cb_key_household = s.household_key
        WHERE postcode_fixed_vintage is not null

      ------------Get Sky Match Rate at Postal Code  Level vs Playpen_postcode
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Match Rate Postcode Level - SkyBase vs Playpen_postcode' 
          , COUNT(DISTINCT sky.postcode)
          , GETDATE()       
        , @run_id
      FROM Experian_Refresh_Sky_postcode AS sky 
      INNER JOIN sk_prod.Playpen_consumerview_postcode AS pc
                  ON sky.postcode = TRIM(REPLACE(pc.cb_address_postcode,' ',''))
      WHERE pc.cb_address_postcode  IS NOT NULL

      
        INSERT INTO Experian_Refresh_log (mValue, Description, Date_log)
            VALUES (  @run_id  , 'PLAYPEN CV vs Sky Matching rates Done'        , getdate()) 
    commit
    END

    ----------------------PLAYPEN_EXPERIAN_LIFESTYLE Matching rates
    ----------------TOTAL cb_keys PLAYPEN_Experian_lifestyle
    IF @c10 =1 
    BEGIN 
      ------Rows
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Total Playpen Lifestyle Rows' 
         , count(*)
         , getdate()
         , @run_id
      FROM sk_prod.PLAYPEN_Experian_lifestyle
      ------Individuals
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Total Playpen Lifestyle Individuals' 
         , count(DISTINCT cb_key_individual)
         , getdate()
         , @run_id
      FROM play_lifestyle

      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Total Playpen Lifestyle Family' 
         , count(DISTINCT cb_key_family)
         , getdate()
         , @run_id
      FROM play_lifestyle  
      ------Households
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Total Playpen Lifestyle Households' 
         , count(DISTINCT cb_key_household)
         , getdate()
         , @run_id
      FROM play_lifestyle

      ------------Get Sky Match Rate at Individual Level
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Match Rate Individual Level - SkyBase vs Playpen_Experian_Lifestyle' 
         , COUNT(DISTINCT cb_key_individual)
         , getdate()
         , @run_id
      FROM skybase AS s
        INNER JOIN play_lifestyle AS e ON e.cb_key_individual = s.individual_key
        WHERE cb_row_id is not null                          -- Checking for Empty rows

      ------------Get Sky Match Rate at Family Level
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Match Rate family  Level - SkyBase vs Playpen_Experian_Lifestyle' 
         , COUNT(DISTINCT cb_key_family)
         , getdate()
         , @run_id
      FROM skybase AS s
        INNER JOIN play_lifestyle AS e ON e.cb_key_family = s.family_key
        WHERE cb_row_id is not null                          -- Checking for Empty rows
            
      ------------Get Sky Match Rate at Household Level
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Match Rate Household Level - SkyBase vs Playpen_Experian_Lifestyle' 
         , count(DISTINCT cb_key_household)
         , getdate()
         , @run_id
      FROM skybase AS s
        INNER JOIN play_lifestyle AS e ON e.cb_key_household = s.household_key
        WHERE cb_row_id is not null                          -- Checking for Empty rows
        
        
        INSERT INTO Experian_Refresh_log (mValue, Description, Date_log)
            VALUES (  @run_id , 'PLAYPEN Lifestyle vs Sky Matching rates Done'      , getdate()) 
    commit
    END

    -----------------------------------------------------------------------------
    --------------------------------VESPA matches--------------------------------
      -------Total VESPA Accounts
    IF @c11 = 1
    BEGIN
        INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
        SELECT 'Total VESPA Accounts' 
       , COUNT(DISTINCT account_number)
       , getdate()
       , @run_id
      FROM vespa

      ------------Get VESPA Match Rate at Individual Level
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Match Rate Individual Level - VESPA vs experian_consumerview' 
         , count(DISTINCT cb_key_individual)
         , getdate()
         , @run_id
      FROM vespa AS s
        INNER JOIN exp_cv_data AS e ON e.cb_key_individual = s.individual_key
        WHERE h_fss_v3_group is not null                                 -- Checking for Empty rows

      ------------Get VESPA Match Rate at family Level
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Match Rate family Level - VESPA vs experian_consumerview' 
         , count(DISTINCT cb_key_family)
         , getdate()
         , @run_id
      FROM vespa AS s
        INNER JOIN exp_cv_data AS e ON e.cb_key_family = s.family_key
        WHERE h_fss_v3_group is not null                                 -- Checking for Empty rows
            
      ------------Get VESPA Match Rate at Household Level
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Match Rate HouseHold Level - VESPA vs experian_consumerview' 
         , count(DISTINCT s.household_key)
         , getdate()
         , @run_id
      FROM vespa AS s
        INNER JOIN exp_cv_data AS e ON e.cb_key_household = s.household_key
        WHERE h_fss_v3_group is not null

      ------------Get VESPA Match Rate at Postal Code  Level vs consumerview_postcode
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Total VESPA Postcode' 
          , COUNT(*)
          , GETDATE()       
        , @run_id
      FROM Experian_Refresh_VESPA_postcode

      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Match Rate Postcode Level - VESPA vs consumerview_postcode'
          , COUNT(DISTINCT sky.postcode)
          , GETDATE()       
        , @run_id
      FROM Experian_Refresh_VESPA_postcode AS sky 
      INNER JOIN consumerview_postcode AS pc
                  ON sky.postcode = TRIM(REPLACE(pc.cb_address_postcode,' ',''))
      WHERE pc.cb_address_postcode  IS NOT NULL


      ----------------------EXPERIAN LIFESTYLE Matching rates vs VESPA
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Match Rate Individual Level - VESPA vs Experian_Lifestyle' 
         , count(DISTINCT cb_key_individual)
         , getdate()
         , @run_id
      FROM vespa AS s
        INNER JOIN exp_lifestyle AS e ON e.cb_key_individual = s.individual_key
        WHERE cb_row_id is not null                          -- Checking for Empty rows

      ----------------------EXPERIAN LIFESTYLE Matching rates vs VESPA
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Match Rate family Level - VESPA vs Experian_Lifestyle' 
         , count(DISTINCT cb_key_family)
         , getdate()
         , @run_id
      FROM vespa AS s
        INNER JOIN exp_lifestyle AS e ON e.cb_key_family = s.family_key
        WHERE cb_row_id is not null                          -- Checking for Empty rows


      ------------Get VESPA Match Rate at Household Level

      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Match Rate Household Level - VESPA vs Experian_Lifestyle' 
         , COUNT(DISTINCT cb_key_household)
         , getdate()
         , @run_id
      FROM vespa AS s
        INNER JOIN exp_lifestyle AS e ON e.cb_key_household = s.household_key
        WHERE cb_row_id is not null                          -- Checking for Empty rows


      --------------------------------------------PERSON_PROPENSITIES_GRID_[CUR/NEW] vs VESPA
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Match Rate - VESPA vs PERSON_PROPENSITIES_GRID_CUR - Individual' 
         , COUNT(*)
             , GETDATE()
           , @run_id
      FROM EXPERIAN_CONSUMERVIEW AS a
      INNER JOIN PERSON_PROPENSITIES_GRID_CUR AS b ON a.p_pixel_v2 = b.ppixel AND a.Pc_mosaic_uk_type = b.mosaicuk
      INNER JOIN vespa AS s ON a.cb_key_individual = s.individual_key

      --------------Extracting PERSON_PROPENSITIES_GRID_NEW Data
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Match Rate - VESPA vs PERSON_PROPENSITIES_GRID_NEW - Individual' 
         , COUNT(*)
             , GETDATE()
           , @run_id
      FROM EXPERIAN_CONSUMERVIEW AS a
      INNER JOIN PERSON_PROPENSITIES_GRID_NEW AS b ON a.p_pixel_v2 = b.ppixel2011 AND a.Pc_mosaic_uk_type = b.mosaic_uk_2009_type
      INNER JOIN vespa AS s ON a.cb_key_individual = s.individual_key

      --------------------------------------------HOUSEHOLD_PROPENSITIES_GRID_[CUR/NEW] vs VESPA
      --------------Extracting HOUSEHOLD_PROPENSITIES_GRID_CUR Data
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Match Rate - VESPA vs HOUSEHOLD_PROPENSITIES_GRID_CUR' 
         , Count(distinct household_key)
             , GETDATE()
           , @run_id
      FROM EXPERIAN_CONSUMERVIEW AS a
      INNER JOIN HOUSEHOLD_PROPENSITIES_GRID_CUR AS b ON a.h_pixel_v2 = b.hpixel AND a.Pc_mosaic_uk_type = b.mosaicuk_type
      INNER JOIN vespa AS s ON a.cb_key_household = s.household_key
      --------------Extracting HOUSEHOLD_PROPENSITIES_GRID_NEW Data
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Match Rate - VESPA vs HOUSEHOLD_PROPENSITIES_GRID_NEW' 
        , Count(distinct household_key)
             , GETDATE()
           , @run_id
      FROM EXPERIAN_CONSUMERVIEW AS a
      INNER JOIN HOUSEHOLD_PROPENSITIES_GRID_NEW AS b ON a.h_pixel_v2 = b.hpixel2011 AND a.Pc_mosaic_uk_type = b.mosaic_uk_2009_type
      INNER JOIN vespa AS s ON a.cb_key_household = s.household_key

      
      
      
      
      
      ------------Get VESPA Match Rate at Individual Level
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Match Rate Individual Level - VESPA vs Playpen_consumerview' 
         , COUNT(DISTINCT cb_key_individual)
         , getdate()
         , @run_id
      FROM vespa AS s
        INNER JOIN play_cv_data AS e ON e.cb_key_individual = s.individual_key
        WHERE cb_row_id is not null                                    -- Checking for Empty rows
     
     
     ------------Get VESPA Match Rate at family Level
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Match Rate family Level - VESPA vs Playpen_consumerview' 
         , COUNT(DISTINCT exp_cb_key_family)
         , getdate()
         , @run_id
      FROM vespa AS s
        INNER JOIN play_cv_data AS e ON e.exp_cb_key_family = s.family_key
        WHERE cb_row_id is not null                                    -- Checking for Empty rows

        
      ------------Get VESPA Match Rate at Household Level
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Match Rate HouseHold Level - VESPA vs Playpen_consumerview' 
         , COUNT(DISTINCT household_key)
         , getdate()
         , @run_id
      FROM vespa AS s
        INNER JOIN play_cv_data AS e ON e.cb_key_household = s.household_key
        WHERE cb_row_id is not null
       
      ------------Get VESPA Match Rate at Postal Code  Level vs Playpen_postcode
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Match Rate Postcode Level - VESPA vs Playpen_postcode' 
          , COUNT(DISTINCT sky.postcode)
          , GETDATE()       
        , @run_id
      FROM Experian_Refresh_VESPA_postcode AS sky 
      INNER JOIN sk_prod.Playpen_consumerview_postcode AS pc
                  ON sky.postcode = TRIM(REPLACE(pc.cb_address_postcode,' ',''))
      WHERE pc.cb_address_postcode  IS NOT NULL

      ----------------------PLAYPEN_EXPERIAN_LIFESTYLE Matching rates vs VESPA
      ------------Get VESPA Match Rate at Individual Level
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Match Rate Individual Level - VESPA vs Playpen_Experian_Lifestyle' 
         , COUNT(DISTINCT cb_key_individual)
         , getdate()
         , @run_id
      FROM vespa AS s
        INNER JOIN play_lifestyle AS e ON e.cb_key_individual = s.individual_key
        WHERE cb_row_id is not null                          -- Checking for Empty rows
       
      ------------Get VESPA Match Rate at family Level
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Match Rate family Level - VESPA vs Playpen_Experian_Lifestyle' 
         , COUNT(DISTINCT cb_key_family)
         , getdate()
         , @run_id
      FROM vespa AS s
        INNER JOIN play_lifestyle AS e ON e.cb_key_family = s.family_key
        WHERE cb_row_id is not null                          -- Checking for Empty rows
       
      ------------Get VESPA Match Rate at Household Level
      INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate, run_id)
      SELECT 'Match Rate Household Level - VESPA vs Playpen_Experian_Lifestyle' 
         , COUNT(DISTINCT cb_key_household)
         , getdate()
         , @run_id
      FROM vespa AS s
        INNER JOIN play_lifestyle AS e ON e.cb_key_household = s.household_key
        WHERE cb_row_id is not null                          -- Checking for Empty rows
        
        
        INSERT INTO Experian_Refresh_log (mValue, Description, Date_log)
            VALUES (  @run_id  , 'VESPA Matching rates Done'        , getdate()) 
    commit
    END


END

    /*
    ----------------------------------------DROPPING DUMP TABLES    
    IF object_id('pitteloudj.skybase') IS NOT NULL         DROP TABLE  pitteloudj.skybase
    IF object_id('pitteloudj.vespa') IS NOT NULL           DROP TABLE  pitteloudj.vespa
    IF object_id('pitteloudj.exp_cv_data') IS NOT NULL     DROP TABLE  pitteloudj.exp_cv_data
    IF object_id('pitteloudj.exp_lifestyle') IS NOT NULL   DROP TABLE  pitteloudj.exp_lifestyle
    IF object_id('pitteloudj.play_cv_data') IS NOT NULL    DROP TABLE  pitteloudj.play_cv_data
    IF object_id('pitteloudj.play_lifestyle') IS NOT NULL  DROP TABLE  pitteloudj.play_lifestyle*/


    /**************************************************************************/
    /*      DEBUG                                                             */
    /*                                                                        */
    /*select * from Experian_Refresh_Results
    select * from  
    sp_columns EXPERIAN_CONSUMERVIEW
    SELECT * FROM Experian_Refresh_Columns_Results WHERE Null_Flag=1 
    Select * from Experian_Refresh_Columns_Results where columnname like '%p%'*/
    /**************************************************************************/


    /*DROP TABLE Experian_Refresh_Results
      DROP TABLE EXPERIAN_Refresh_Total_Columns
      DROP TABLE Experian_Refresh_Columns_Results
     */


