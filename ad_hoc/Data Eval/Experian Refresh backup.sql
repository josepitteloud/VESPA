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


------------------------------ Matches Results Table 
CREATE TABLE Experian_Refresh_Results
  (MetricID int IDENTITY
  , MetricDescription varchar(80)
  , MetricValue float
  , Recordate Datetime
  );
  
commit;
---------------------------------Inserting Run ID Info
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)

SELECT 'Run ID' 
    , max(MetricValue)+1
    , getdate() 
FROM Experian_Refresh_Results 
WHERE MetricDescription =  'Run ID';   

---------------------------------Defining all Experian related Tables
SELECT 
    a.uid OwnerID
    , a.name  fOwner
    , b.id    TableID
    , b.name  TableName    
    , 0 Processed
    , 0 Date_processed
    , getdate() DateProcessed
INTO Experian_Refresh_Tables
from dbo.sysusers   AS a
     JOIN dbo.sysobjects AS b ON a.uid = b.uid and (LOWER(b.name) like '%expe%' OR LOWER(b.name) like '%playpen%'
                                      OR LOWER(b.name) like '%consumerview%' OR LOWER(b.name) like '%propensitie%')
WHERE 
  LOWER(a.name) like '%sk_prod';  

ALTER TABLE Experian_Refresh_Tables 
ADD (ID int IDENTITY) 
---------------------------------Flag on selected table / requires improvement
UPDATE Experian_Refresh_Tables SET    Processed = 1
WHERE TableID in (7167519,7167831,10415848,10417456
,10419481,10421089,15852254
,15856405,15858168,15893701);
commit; 
CREATE HG INDEX idx1 on Experian_Refresh_Tables(TableID);
commit;

---------------------------------Defining columns to be Checked
SELECT 
  b.TableID 
  , b.TableName   
  , c.id    ColID1
  , c.name  ColumnName
  , c.colid ColID2
INTO Experian_Refresh_Columns
FROM  pitteloudj.Experian_Refresh_Tables as b 
    JOIN dbo.syscolumns AS c WITH (NOLOCK) ON b.TableId=c.id
WHERE b.Processed = 1;

---------------------------------Summary Table with Total Columns by Table
SELECT TableName
    , count(colID1) Cols 
    , getdate() Record_Date
INTO  EXPERIAN_Refresh_Total_Columns
FROM Experian_Refresh_Columns
GROUP BY TableName;
commit;

---------------------------------Extracting Sky Data
SELECT DISTINCT account_number
    ,currency_code
    ,cb_key_household as household_key
    ,cb_key_individual as individual_key
INTO skybase
FROM sk_prod.cust_subs_hist
 WHERE subscription_sub_type IN ('DTV Primary Viewing')
   AND status_code IN ('AC','AB','PC')
   AND effective_from_dt <= '20130522'
   AND effective_to_dt > '20130522'
   AND EFFECTIVE_FROM_DT IS NOT NULL
   AND cb_key_household > 0             --UK Only
   AND cb_key_household IS NOT NULL
   AND account_number IS NOT NULL
   AND service_instance_id IS NOT NULL;

-- Create index on the sky base data
CREATE HG INDEX idx01 ON skybase(individual_key);
commit;

----------------------------TOTAL Sky Accounts Metric
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Total SkyBase Accounts' 
 , count(DISTINCT account_number)
 , getdate()
FROM skybase; 
commit;
--------------------------------------------------------------------------------------
--------------------------------------------------EXPERIAN CONSUMERVIEW Matching rates
--------------Extracting EXPERIAN_CONSUMERVIEW Data
SELECT cb_key_household
        ,cb_key_individual
        ,cb_key_family
        ,cb_row_id
        ,cb_address_postcode
        ,h_fss_v3_group
        ,h_fss_v3_type
into exp_cv_data
from sk_prod.experian_consumerview;

commit; 

CREATE HG INDEX idx02 ON exp_cv_data(cb_key_individual);
CREATE HG INDEX idx011 ON skybase (account_number);

commit;

----------------------------------------Get Total Rows in Experian Consumerview
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Total Experian ConsumerView individual' 
   , count(DISTINCT cb_key_individual)
   , getdate()
FROM exp_cv_data;   
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Total Experian ConsumerView HouseHold' 
   , count(DISTINCT cb_key_household)
   , getdate()
FROM exp_cv_data; 
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Total Experian ConsumerView Postcodes' 
   , count(DISTINCT cb_address_postcode)
   , getdate()
FROM exp_cv_data;
  
commit;

----------------------------------------Get Sky Match Rate at Individual Level
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Match Rate Individual Level - SkyBase vs experian_consumerview' 
   , count(DISTINCT cb_key_individual)
   , getdate()
FROM skybase AS s
  INNER JOIN exp_cv_data AS e ON e.cb_key_individual = s.individual_key
  WHERE h_fss_v3_group is not null;                                    -- Checking for Empty rows
  
commit;

----------------------------------------Get Sky Match Rate at Household Level

INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Match Rate HouseHold Level - SkyBase vs experian_consumerview' 
   , count(DISTINCT household_key)
   , getdate()
FROM skybase AS s
  INNER JOIN exp_cv_data AS e ON e.cb_key_household = s.household_key
  WHERE h_fss_v3_group is not null;
  
commit; 


------------------------------------Get Sky Match Rate at Postal Code  Level vs consumerview_postcode
CREATE TABLE Experian_Refresh_Sky_postcode
( postcode varchar(10));

commit; 

INSERT INTO Experian_Refresh_Sky_postcode
SELECT distinct 
      trim(replace(SAV.cb_address_postcode,' ',''))
FROM  sk_prod.CUST_SINGLE_ACCOUNT_VIEW as sav
  INNER JOIN skybase    AS sky    ON sky.account_number = sav.account_number
WHERE   sav.cust_active_dtv = 1;

INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Match Rate HouseHold Level - SkyBase vs consumerview_postcode' 
    , COUNT(DISTINCT sky.postcode)
    , GETDATE()       
FROM Experian_Refresh_Sky_postcode AS sky 
INNER JOIN sk_prod.consumerview_postcode AS pc 
            ON sky.postcode = TRIM(REPLACE(pc.cb_address_postcode,' ',''))
WHERE pc.cb_address_postcode  IS NOT NULL
  AND pc.pc_fss_v3_group      IS NOT NULL;

commit;
------------------------------------Get Total Postcodes on consumerview_postcode
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Total Rows Consumerview_Postcode' 
    , COUNT(*)
    , GETDATE()       
FROM sk_prod.consumerview_postcode; 

commit;
----------------------------------EXPERIAN LIEFSTYLE Matching rates vs SKY
----------------------------------Extracting EXPERIAN_LIFESTYLE Data
select cb_row_id
       ,exp_cb_address_mailable_flag
       ,cb_key_household
       ,cb_key_family
       ,cb_key_individual
       ,cb_address_status
       ,cb_source_cd
into exp_lifestyle
from sk_prod.Experian_lifestyle;
commit;

CREATE HG INDEX idx04 ON exp_lifestyle(cb_key_individual);
commit;

------------------------------------Get Total individual, household and rows from Experian_lifestyle
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Total Rows Experian_lifestyle' 
    , COUNT(*)
    , GETDATE()       
FROM exp_lifestyle; 
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Total Experian_lifestyle Individual' 
   , count(DISTINCT cb_key_individual)
   , getdate()
FROM exp_lifestyle;
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Total Experian_lifestyle Household' 
   , count(DISTINCT cb_key_household)
   , getdate()
FROM exp_lifestyle;

------------------------------------Get Sky Match Rate at Individual Level
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Match Rate Individual Level - SkyBase vs Experian_Lifestyle' 
   , count(DISTINCT cb_key_individual)
   , getdate()
FROM skybase AS s
  INNER JOIN exp_lifestyle AS e ON e.cb_key_individual = s.individual_key
  WHERE cb_row_id is not null;                          -- Checking for Empty rows
  
commit;

------------------------------------Get Sky Match Rate at Household Level
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Match Rate Household Level - SkyBase vs Experian_Lifestyle' 
   , count(DISTINCT cb_key_household)
   , getdate()
FROM skybase AS s
  INNER JOIN exp_lifestyle AS e ON e.cb_key_household = s.household_key
  WHERE cb_row_id is not null;                          -- Checking for Empty rows
  
commit;

--------------------------------------------PERSON_PROPENSITIES_GRID_[CUR/NEW]
--------------------------------------Extracting PERSON_PROPENSITIES_GRID_CUR Data Total rows
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Total Rows PERSON_PROPENSITIES_GRID_CUR' 
   , count(*)
   , getdate()
FROM sk_prod.PERSON_PROPENSITIES_GRID_CUR;
------------------------------------Get Sky Match Rate at Individual Level
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Match Rate - Skybase vs PERSON_PROPENSITIES_GRID_CUR' 
   , Count(*)
      , getdate()
FROM sk_prod.EXPERIAN_CONSUMERVIEW AS a
INNER JOIN sk_prod.PERSON_PROPENSITIES_GRID_CUR AS b ON a.p_pixel_v2 = b.ppixel AND a.Pc_mosaic_uk_type = b.mosaicuk
INNER JOIN skybase AS s ON a.cb_key_individual = s.individual_key;

commit;
--------------Extracting PERSON_PROPENSITIES_GRID_NEW Data
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Total Rows PERSON_PROPENSITIES_GRID_NEW' 
   , count(*)
   , getdate()
FROM sk_prod.PERSON_PROPENSITIES_GRID_NEW;
------------------------------------Get Sky Match Rate at Individual Level
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Match Rate - Skybase vs PERSON_PROPENSITIES_GRID_NEW' 
   , Count(*)
      , getdate()
FROM sk_prod.EXPERIAN_CONSUMERVIEW AS a
INNER JOIN sk_prod.PERSON_PROPENSITIES_GRID_NEW AS b ON a.p_pixel_v2 = b.ppixel2011 AND a.Pc_mosaic_uk_type = b.mosaic_uk_2009_type
INNER JOIN skybase AS s ON a.cb_key_individual = s.individual_key;

commit;
--------------------------------------------HOUSEHOLD_PROPENSITIES_GRID_[CUR/NEW]
--------------Extracting HOUSEHOLD_PROPENSITIES_GRID_CUR Data
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Total Rows HOUSEHOLD_PROPENSITIES_GRID_CUR' 
   , count(*)
   , getdate()
FROM sk_prod.HOUSEHOLD_PROPENSITIES_GRID_CUR;
------------------------------------Get Sky Match Rate at Household Level
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Match Rate - Experian_Consumerview vs HOUSEHOLD_PROPENSITIES_GRID_CUR' 
   , Count(*)
      , getdate()
FROM sk_prod.EXPERIAN_CONSUMERVIEW AS a
INNER JOIN sk_prod.HOUSEHOLD_PROPENSITIES_GRID_CUR AS b ON a.h_pixel_v2 = b.hpixel AND a.Pc_mosaic_uk_type = b.mosaicuk_type;
--------------Extracting HOUSEHOLD_PROPENSITIES_GRID_NEW Data
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Total Rows HOUSEHOLD_PROPENSITIES_GRID_NEW' 
   , count(*)
   , getdate()
FROM sk_prod.HOUSEHOLD_PROPENSITIES_GRID_NEW;
------------------------------------Get Sky Match Rate at Household Level
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Match Rate - Experian_Consumerview vs HOUSEHOLD_PROPENSITIES_GRID_NEW' 
   , Count(*)
      , getdate()
FROM sk_prod.EXPERIAN_CONSUMERVIEW AS a
INNER JOIN sk_prod.HOUSEHOLD_PROPENSITIES_GRID_NEW AS b ON a.h_pixel_v2 = b.hpixel2011 AND a.Pc_mosaic_uk_type = b.mosaic_uk_2009_type;

commit;
--------------------------------------------PLAYPEN TABLES
--------------------------------------------PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Total Rows PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD' 
   , count(*)
   , getdate()
FROM sk_prod.PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD;

-------------------------------------Extracting Data
SELECT exp_cb_key_household     cb_key_household
        ,exp_cb_key_individual  cb_key_individual
        ,exp_cb_key_family
        ,cb_row_id
        ,postcode_fixed_vintage
INTO play_cv_data
FROM sk_prod.PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD;

commit;

CREATE HG INDEX idxp02 ON play_cv_data(cb_key_individual);
CREATE HG INDEX idxp03 ON play_cv_data(cb_key_household);

commit;
------------------------------------Get Sky Match Rate at Household Level
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Total Playpen ConsumerView individual' 
   , COUNT(DISTINCT cb_key_individual)
   , getdate()
FROM play_cv_data;

INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Total Playpen ConsumerView HouseHold' 
   , COUNT(DISTINCT cb_key_household)
   , getdate()
FROM play_cv_data; 
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Total Playpen ConsumerView Postcodes' 
   , count(DISTINCT postcode_fixed_vintage)
   , getdate()
FROM play_cv_data;

commit;

------------Get Sky Match Rate at Individual Level
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Match Rate Individual Level - SkyBase vs Playpen_consumerview' 
   , count(DISTINCT cb_key_individual)
   , getdate()
FROM skybase AS s
  INNER JOIN play_cv_data AS e ON e.cb_key_individual = s.individual_key
  WHERE postcode_fixed_vintage is not null;                                    -- Checking for Empty rows
  
commit;

------------Get Sky Match Rate at Household Level

INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Match Rate HouseHold Level - SkyBase vs Playpen_consumerview' 
   , count(DISTINCT household_key)
   , getdate()
FROM skybase AS s
  INNER JOIN play_cv_data AS e ON e.cb_key_household = s.household_key
  WHERE postcode_fixed_vintage is not null;
  
commit; 
------------Get Sky Match Rate at Postal Code  Level vs Playpen_postcode
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Match Rate HouseHold Level - SkyBase vs Playpen_postcode' 
    , COUNT(DISTINCT sky.postcode)
    , GETDATE()       
FROM Experian_Refresh_Sky_postcode AS sky 
INNER JOIN sk_prod.Playpen_consumerview_postcode AS pc 
            ON sky.postcode = TRIM(REPLACE(pc.cb_address_postcode,' ',''))
WHERE pc.cb_address_postcode  IS NOT NULL;
commit;

----------------------PLAYPEN_EXPERIAN_LIFESTYLE Matching rates
--------------Extracting EXPERIAN_LIFESTYLE Data

SELECT cb_row_id
       ,cb_data_date
       ,cb_key_household
       ,cb_key_family
       ,cb_key_individual
       ,cb_address_status
       ,cb_source_cd
INTO play_lifestyle
FROM sk_prod.PLAYPEN_Experian_lifestyle;

commit;

CREATE HG INDEX idxp04 ON play_lifestyle(cb_key_individual);
commit;

----------------TOTAL cb_keys PLAYPEN_Experian_lifestyle
------Rows
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Total Playpen Lifestyle Rows' 
   , count(*)
   , getdate()
FROM sk_prod.PLAYPEN_Experian_lifestyle;
------Individuals
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Total Playpen Lifestyle Individuals' 
   , count(DISTINCT cb_key_individual)
   , getdate()
FROM play_lifestyle;
------Households
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Total Playpen Lifestyle Households' 
   , count(DISTINCT cb_key_household)
   , getdate()
FROM play_lifestyle;

commit;
------------Get Sky Match Rate at Individual Level
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Match Rate Individual Level - SkyBase vs Playpen_Experian_Lifestyle' 
   , COUNT(DISTINCT cb_key_individual)
   , getdate()
FROM skybase AS s
  INNER JOIN play_lifestyle AS e ON e.cb_key_individual = s.individual_key
  WHERE cb_row_id is not null;                          -- Checking for Empty rows
  
commit;

------------Get Sky Match Rate at Household Level

INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Match Rate Household Level - SkyBase vs Playpen_Experian_Lifestyle' 
   , count(DISTINCT cb_key_household)
   , getdate()
FROM skybase AS s
  INNER JOIN play_lifestyle AS e ON e.cb_key_household = s.household_key
  WHERE cb_row_id is not null;                          -- Checking for Empty rows
  
commit;


-----------------------------------------------------------------------------
-----------------------------------VESPA Match Rates ------------------------
SELECT DISTINCT (ve.account_number)
    ,ve.cb_key_individual     AS individual_key
    ,ve.consumerview_cb_row_id
    ,ve.panel
    ,ve.panel_id_vespa
    , sav.cb_key_household    AS household_key
INTO vespa
FROM Vespa_Analysts.Vespa_Single_Box_View as ve
INNER JOIN sk_prod.CUST_SINGLE_ACCOUNT_VIEW as sav ON ve.account_number = sav.account_number
WHERE panel_id_vespa = 12;

commit;
CREATE HG INDEX idx02 ON vespa (individual_key);
commit;

-------Total VESPA Accounts
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Total VESPA Accounts' 
 , COUNT(DISTINCT account_number)
 , getdate()
FROM vespa;

------------Get VESPA Match Rate at Individual Level
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Match Rate Individual Level - VESPA vs experian_consumerview' 
   , count(DISTINCT cb_key_individual)
   , getdate()
FROM vespa AS s
  INNER JOIN exp_cv_data AS e ON e.cb_key_individual = s.individual_key
  WHERE h_fss_v3_group is not null;                                 -- Checking for Empty rows
------------Get Sky Match Rate at Household Level
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Match Rate HouseHold Level - VESPA vs experian_consumerview' 
   , count(DISTINCT s.household_key)
   , getdate()
FROM vespa AS s
  INNER JOIN exp_cv_data AS e ON e.cb_key_household = s.household_key
  WHERE h_fss_v3_group is not null;

------------Get Sky Match Rate at Postal Code  Level vs consumerview_postcode
CREATE TABLE Experian_Refresh_VESPA_postcode
( postcode varchar(10));
commit; 

INSERT INTO Experian_Refresh_VESPA_postcode
SELECT DISTINCT
      TRIM(REPLACE(SAV.cb_address_postcode,' ',''))
FROM  sk_prod.CUST_SINGLE_ACCOUNT_VIEW as sav
  INNER JOIN vespa  sky    ON sky.account_number = sav.account_number
WHERE   sav.cust_active_dtv = 1;

commit;

INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Total VESPA Postcode' 
    , COUNT(*)
    , GETDATE()       
FROM Experian_Refresh_VESPA_postcode;

INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Match Rate HouseHold Level - VESPA vs consumerview_postcode' 
    , COUNT(DISTINCT sky.postcode)
    , GETDATE()       
FROM Experian_Refresh_VESPA_postcode AS sky 
INNER JOIN sk_prod.consumerview_postcode AS pc 
            ON sky.postcode = TRIM(REPLACE(pc.cb_address_postcode,' ',''))
WHERE pc.cb_address_postcode  IS NOT NULL;

commit;
----------------------EXPERIAN LIFESTYLE Matching rates vs VESPA
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Match Rate Individual Level - VESPA vs Experian_Lifestyle' 
   , count(DISTINCT cb_key_individual)
   , getdate()
FROM vespa AS s
  INNER JOIN exp_lifestyle AS e ON e.cb_key_individual = s.individual_key
  WHERE cb_row_id is not null;                          -- Checking for Empty rows
commit;

------------Get VESPA Match Rate at Household Level

INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Match Rate Household Level - VESPA vs Experian_Lifestyle' 
   , COUNT(DISTINCT cb_key_household)
   , getdate()
FROM vespa AS s
  INNER JOIN exp_lifestyle AS e ON e.cb_key_household = s.household_key
  WHERE cb_row_id is not null                          -- Checking for Empty rows
commit;

--------------------------------------------PERSON_PROPENSITIES_GRID_[CUR/NEW] vs VESPA
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Match Rate - VESPA vs PERSON_PROPENSITIES_GRID_CUR - Individual' 
   , COUNT(*)
       , GETDATE()
FROM sk_prod.EXPERIAN_CONSUMERVIEW AS a
INNER JOIN sk_prod.PERSON_PROPENSITIES_GRID_CUR AS b ON a.p_pixel_v2 = b.ppixel AND a.Pc_mosaic_uk_type = b.mosaicuk
INNER JOIN vespa AS s ON a.cb_key_individual = s.individual_key;
commit;
--------------Extracting PERSON_PROPENSITIES_GRID_NEW Data
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Match Rate - VESPA vs PERSON_PROPENSITIES_GRID_NEW - Individual' 
   , COUNT(*)
       , GETDATE()
FROM sk_prod.EXPERIAN_CONSUMERVIEW AS a
INNER JOIN sk_prod.PERSON_PROPENSITIES_GRID_NEW AS b ON a.p_pixel_v2 = b.ppixel2011 AND a.Pc_mosaic_uk_type = b.mosaic_uk_2009_type
INNER JOIN vespa AS s ON a.cb_key_individual = s.individual_key;
commit;
--------------------------------------------HOUSEHOLD_PROPENSITIES_GRID_[CUR/NEW] vs VESPA
--------------Extracting HOUSEHOLD_PROPENSITIES_GRID_CUR Data
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Match Rate - VESPA vs HOUSEHOLD_PROPENSITIES_GRID_CUR' 
   , COUNT(*)
       , GETDATE()
FROM sk_prod.EXPERIAN_CONSUMERVIEW AS a
INNER JOIN sk_prod.HOUSEHOLD_PROPENSITIES_GRID_CUR AS b ON a.h_pixel_v2 = b.hpixel AND a.Pc_mosaic_uk_type = b.mosaicuk_type
INNER JOIN vespa AS s ON a.cb_key_household = s.household_key;
--------------Extracting HOUSEHOLD_PROPENSITIES_GRID_NEW Data
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Match Rate - VESPA vs HOUSEHOLD_PROPENSITIES_GRID_NEW' 
   , COUNT(*)
       , GETDATE()
FROM sk_prod.EXPERIAN_CONSUMERVIEW AS a
INNER JOIN sk_prod.HOUSEHOLD_PROPENSITIES_GRID_NEW AS b ON a.h_pixel_v2 = b.hpixel2011 AND a.Pc_mosaic_uk_type = b.mosaic_uk_2009_type
INNER JOIN vespa AS s ON a.cb_key_household = s.household_key;

commit;

------------Get VESPA Match Rate at Individual Level
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Match Rate Individual Level - VESPA vs Playpen_consumerview' 
   , COUNT(DISTINCT cb_key_individual)
   , getdate()
FROM vespa AS s
  INNER JOIN play_cv_data AS e ON e.cb_key_individual = s.individual_key
  WHERE cb_row_id is not null;                                    -- Checking for Empty rows
commit;
------------Get Sky Match Rate at Household Level
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Match Rate HouseHold Level - VESPA vs Playpen_consumerview' 
   , COUNT(DISTINCT household_key)
   , getdate()
FROM vespa AS s
  INNER JOIN play_cv_data AS e ON e.cb_key_household = s.household_key
  WHERE cb_row_id is not null;
commit; 
------------Get Sky Match Rate at Postal Code  Level vs Playpen_postcode
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Match Rate HouseHold Level - VESPA vs Playpen_postcode' 
    , COUNT(DISTINCT sky.postcode)
    , GETDATE()       
FROM Experian_Refresh_VESPA_postcode AS sky 
INNER JOIN sk_prod.Playpen_consumerview_postcode AS pc 
            ON sky.postcode = TRIM(REPLACE(pc.cb_address_postcode,' ',''))
WHERE pc.cb_address_postcode  IS NOT NULL;
commit;
----------------------PLAYPEN_EXPERIAN_LIFESTYLE Matching rates vs VESPA
------------Get VESPA Match Rate at Individual Level
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Match Rate Individual Level - VESPA vs Playpen_Experian_Lifestyle' 
   , COUNT(DISTINCT cb_key_individual)
   , getdate()
FROM vespa AS s
  INNER JOIN play_lifestyle AS e ON e.cb_key_individual = s.individual_key
  WHERE cb_row_id is not null;                          -- Checking for Empty rows
commit; 

------------Get VESPA Match Rate at Household Level
INSERT INTO Experian_Refresh_Results (MetricDescription, MetricValue, Recordate)
SELECT 'Match Rate Household Level - VESPA vs Playpen_Experian_Lifestyle' 
   , COUNT(DISTINCT cb_key_household)
   , getdate()
FROM vespa AS s
  INNER JOIN play_lifestyle AS e ON e.cb_key_household = s.household_key
  WHERE cb_row_id is not null;                          -- Checking for Empty rows
commit;

-------------------------------------------------------------------------------------
--------------------TABLE DATES -----------------------------------------------------
-------------------------------------------------------------------------------------
IF object_id('#seginfo') is null CREATE TABLE Experian_Table_Dates  (ID int Identity  , Table_Name varchar(70)  , Load_Dates datetime  , Report_Date datetime);
  
DECLARE @sql1     VARCHAR(1000)
      , @Table    VARCHAR(100)
      , @TableID  BIGINT
commit; 

WHILE EXISTS  (SELECT top 1 *
                FROM Experian_Refresh_Tables 
                WHERE Processed = 1 
                    AND Date_processed = 0) 
BEGIN                     
  SET @TableID =  (SELECT top 1 ID
                  FROM Experian_Refresh_Tables 
                  WHERE Processed = 1 
                    AND Date_processed = 0)
  SET @Table =    (SELECT TableName
                  FROM Experian_Refresh_Tables 
                  WHERE ID = @TableID)
  SET @sql1 = (
  'INSERT INTO   Experian_Table_Dates (Table_name, Load_Dates, Report_Date)
  SELECT DISTINCT 
  @Table TableName
  , cb_data_date LoadDate
  , getdate() Report_Date
  FROM sk_prod.'||@Table )
  
  EXECUTE(@sql1)
  
  UPDATE Experian_Refresh_Tables 
  SET Date_processed = 1 WHERE  ID = @TableID
  
  commit

END
-------------------------------------------------------------------------------------
--------------------Columns Contents-------------------------------------------------
-------------------Check for Null and Quasi-nulls------------------------------------
CREATE TABLE Experian_Refresh_Columns_Results
( ID              int IDENTITY
  , TableName     VARCHAR (100)
  , TableID       int
  , ColumnName    VARCHAR (200)
  , records       int
  , proc_reg      bit default 0
  , Content_Flag  bit default 0
  , Null_Flag     bit default 0
  , Date_load     datetime  
  , Date_proc     datetime
 );
commit;

INSERT INTO Experian_Refresh_Columns_Results
( TableName, TableID, ColumnName, Date_load)
SELECT 
     TableName
    , TableID
    , ColumnName
    , GETDATE()
FROM Experian_Refresh_Columns --Experian_Refresh_Tables 
ORDER BY TableName
commit;

DECLARE @sql2       VARCHAR(1000)
      , @Tablec     VARCHAR(100)
      , @ColID      INT
      , @ColumnN    VARCHAR(200)
      , @cont       INT
WHILE EXISTS (SELECT top 1 *
                FROM Experian_Refresh_Columns_Results 
                WHERE proc_reg = 0)
BEGIN
  WHILE (@cont < 500)
  BEGIN
      SET @ColID = (SELECT top 1 ID
                  FROM Experian_Refresh_Columns_Results 
                  WHERE proc_reg = 0)
      SET @Tablec = (SELECT top 1 TableName FROM Experian_Refresh_Columns_Results WHERE ID = @ColID)
      SET @ColumnN = (SELECT top 1 ColumnName FROM Experian_Refresh_Columns_Results WHERE ID = @ColID)
      SET @sql2 = 
      'UPDATE Experian_Refresh_Columns_Results
        SET records = (SELECT count('||@ColumnN||')
                          FROM sk_prod.'||@Tablec||' ),
            Date_proc = GETDATE()
        WHERE ID = @ColID'
    
    EXECUTE(@sql2)
    
    UPDATE Experian_Refresh_Columns_Results 
    SET proc_reg = 1 WHERE  ID = @ColID
  SET @cont = @cont + 1
  END
SET @cont =0 
END

commit;

UPDATE Experian_Refresh_Columns_Results 
  SET Null_Flag = 1 WHERE  records = 0
commit; 





/******************************************************/
/*      DEBUG                                                     */
/*                                                                */
/*select * from Experian_Refresh_Results 
select * from  
sp_columns EXPERIAN_CONSUMERVIEW    
SELECT * FROM Experian_Refresh_Columns_Results WHERE Null_Flag=1 
Select * from Experian_Refresh_Columns_Results where columnname like '%p%'*/
/******************************************************/


/*DROP TABLE Experian_Refresh_Results
    DROP TABLE  Experian_Refresh_Tables;
    DROP TABLE  Experian_Refresh_Columns;
    DROP TABLE  skybase;
    DROP TABLE  vespa;
    DROP TABLE  exp_cv_data;
    DROP TABLE  exp_lifestyle;
    DROP TABLE  play_cv_data;
    DROP TABLE  play_lifestyle;
    DROP TABLE  Experian_Refresh_Sky_postcode;
    DROP TABLE EXPERIAN_Refresh_Total_Columns
    DROP TABLE Experian_Refresh_Columns_Results;
commit; */


