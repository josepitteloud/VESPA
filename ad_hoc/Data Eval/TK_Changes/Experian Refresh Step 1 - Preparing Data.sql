/******************************************************************************
**
**  Project Vespa: Experian Data Quality Report. Phase 1 Preparing the data
**
**  Intended to produce a repository with basic quality metrics. This will feed a Monthly Quality Report that will 
**  allow us to track any deviation on the data acquired
**
**  Code sections:
**      
******************************************************************************/
DECLARE @run_id int
      , @c2 bit
      , @c3 bit
      , @c4 bit
      , @c5 bit
      , @c7 bit
      , @c8 bit
      , @c9 bit
      , @c10 bit


SET @c2 = 1 --Experian Related Tables 
SET @c3 = 1 --Column to check
SET @c4 = 1 --Sky Data
SET @c5 = 1 --Experian CV Data
SET @c7 = 1 --Experian Lifestyle Data 
SET @c8 = 1 --Playpen Data
SET @c10 = 1 --Playpen Lifestyle
SET @c9 = 1 --VESPA ALL
     
  IF object_id('skybase_expr_eval') IS NOT NULL	       DROP TABLE  skybase_expr_eval
  IF object_id('vespa_expr_eval') IS NOT NULL           DROP TABLE  vespa_expr_eval
  IF object_id('exp_cv_data_expr_eval') IS NOT NULL     DROP TABLE  exp_cv_data_expr_eval
  IF object_id('exp_lifestyle_expr_eval') IS NOT NULL   DROP TABLE  exp_lifestyle_expr_eval
  IF object_id('play_cv_data_expr_eval') IS NOT NULL    DROP TABLE  play_cv_data_expr_eval
  IF object_id('play_lifestyle_expr_eval') IS NOT NULL  DROP TABLE  play_lifestyle_expr_eval
  IF object_id('Experian_Refresh_Sky_postcode_expr_eval') IS NOT NULL   DROP TABLE  Experian_Refresh_Sky_postcode_expr_eval
  IF object_id('Experian_Refresh_Tables_expr_eval') IS NOT NULL	        DROP TABLE  Experian_Refresh_Tables_expr_eval
  IF object_id('Experian_Refresh_Columns_expr_eval') IS NOT NULL        DROP TABLE  Experian_Refresh_Columns_expr_eval
  IF object_id('Experian_Refresh_VESPA_postcode_expr_eval') IS NOT NULL        DROP TABLE  Experian_Refresh_VESPA_postcode_expr_eval
---------------------------------Inserting Run ID Info
IF object_id('Experian_Refresh_log_expr_eval') IS NULL 
BEGIN CREATE TABLE Experian_Refresh_log_expr_eval
	(ID 			int Identity
	, mValue		int
	, Description 	varchar(200)
	, Date_log 		DATETIME
	)
END


SET @run_id = ISNULL ((SELECT max(mValue)+1  FROM Experian_Refresh_log_expr_eval WHERE Description =  'Run ID'), 1)


INSERT INTO Experian_Refresh_log_expr_eval (mValue, Description, Date_log)
VALUES ( @run_id   		, 'Run ID' 		, getdate())

---------------------------------Defining all Experian related Tables
IF @c2 = 1 
BEGIN 
  SELECT 
      a.uid OwnerID
      , a.name  fOwner
      , b.id    TableID
      , b.name  TableName    
      , 0 Processed
      , 0 Date_processed
      , getdate() DateProcessed
	  , @run_id run_id
  INTO Experian_Refresh_Tables_expr_eval
  FROM dbo.sysusers   AS a
  JOIN dbo.sysobjects AS b ON a.uid = b.uid and (LOWER(b.name) like '%expe%' OR LOWER(b.name) like '%playpen%'
                                        OR LOWER(b.name) like '%consumerview%' OR LOWER(b.name) like '%propensitie%')
  WHERE 
    LOWER(a.name) like '%sk_prod'
	
  ALTER TABLE Experian_Refresh_Tables_expr_eval
  ADD (ID int IDENTITY)
  
  ---------------------------------Flag on selected table / requires improvement
  UPDATE Experian_Refresh_Tables_expr_eval
  SET  Processed = 1
  WHERE TableID in (7167519,7167831,10415848,10417456,10419481,10421089,15852254,15856405,15858168,15893701)


  CREATE HG INDEX idx1 on Experian_Refresh_Tables_expr_eval(TableID)

INSERT INTO Experian_Refresh_log_expr_eval (mValue, Description, Date_log)
VALUES ( 4 , 'Experian Refresh Table Created' 		, getdate())  
END



---------------------------------Defining columns to be Checked
IF @c3 = 1
BEGIN
  SELECT 
    b.TableID 
    , b.TableName   
    , c.id    ColID1
    , c.name  ColumnName
    , c.colid ColID2
  INTO Experian_Refresh_Columns_expr_eval
  FROM  Experian_Refresh_Tables_expr_eval as b 
      JOIN dbo.syscolumns AS c WITH (NOLOCK) ON b.TableId=c.id
  WHERE b.Processed = 1
END
  ---------------------------------Summary Table with Total Columns by Table
  IF object_id('EXPERIAN_Refresh_Total_Columns_expr_eval') IS NULL 
  BEGIN 
  CREATE TABLE EXPERIAN_Refresh_Total_Columns_expr_eval (TableName VARCHAR(100), Cols int, Record_Date DATETIME, run_id int)
  INSERT INTO EXPERIAN_Refresh_Total_Columns_expr_eval
  SELECT TableName
      , count(colID1) Cols 
      , getdate() Record_Date
	  , @run_id run_id
  FROM Experian_Refresh_Columns_expr_eval
  GROUP BY TableName
  
INSERT INTO Experian_Refresh_log_expr_eval (mValue, Description, Date_log)
VALUES ( 4 , 'Columns Tables Created' 		, getdate()  )
END


---------------------------------Extracting Sky Data
IF @c4 = 1
BEGIN
  SELECT DISTINCT account_number
      ,currency_code
      ,cb_key_household as household_key
      ,cb_key_individual as individual_key
  INTO skybase_expr_eval
  FROM sk_prod.cust_subs_hist
   WHERE subscription_sub_type IN ('DTV Primary Viewing')
     AND status_code IN ('AC','AB','PC')
     AND effective_from_dt <= '20130522'
     AND effective_to_dt > '20130522'
     AND EFFECTIVE_FROM_DT IS NOT NULL
     AND cb_key_household > 0             --UK Only
     AND cb_key_household IS NOT NULL
     AND account_number IS NOT NULL
     AND service_instance_id IS NOT NULL

  -- Create index on the sky base data
  CREATE HG INDEX idx01 ON skybase_expr_eval(individual_key)

  CREATE TABLE Experian_Refresh_Sky_postcode_expr_eval
  ( postcode varchar(10))

  INSERT INTO Experian_Refresh_Sky_postcode_expr_eval
  SELECT distinct 
        trim(replace(SAV.cb_address_postcode,' ',''))
  FROM  sk_prod.CUST_SINGLE_ACCOUNT_VIEW as sav
    INNER JOIN skybase_expr_eval    AS sky    ON sky.account_number = sav.account_number
  WHERE   sav.cust_active_dtv = 1
  
INSERT INTO Experian_Refresh_log_expr_eval (mValue, Description, Date_log)
VALUES ( 4 , 'Skybase Data Tables Created' 		, getdate())

END


--------------------------------------------------------------------------------------
------------------------------------------------Extracting EXPERIAN_CONSUMERVIEW Data
IF @c5 = 1
BEGIN 
  SELECT cb_key_household
          ,cb_key_individual
          ,cb_key_family
          ,cb_row_id
          ,cb_address_postcode
          ,h_fss_v3_group
          ,h_fss_v3_type
  INTO exp_cv_data_expr_eval
  FROM sk_prod.experian_consumerview

  CREATE HG INDEX idx02 ON exp_cv_data_expr_eval(cb_key_individual)
  CREATE HG INDEX idx011 ON skybase_expr_eval (account_number)
  
  
INSERT INTO Experian_Refresh_log_expr_eval (mValue, Description, Date_log)
VALUES ( 4 , 'Experian CV Data Table Created' 		, getdate())

END


--------------------------------Extracting EXPERIAN_LIFESTYLE Data
IF @c7 = 1
BEGIN 
  SELECT cb_row_id
         ,exp_cb_address_mailable_flag
         ,cb_key_household
         ,cb_key_family
         ,cb_key_individual
         ,cb_address_status
         ,cb_source_cd
  INTO exp_lifestyle_expr_eval
  FROM sk_prod.Experian_lifestyle
	
	CREATE HG INDEX idx04 ON exp_lifestyle_expr_eval(cb_key_individual)
  
INSERT INTO Experian_Refresh_log_expr_eval (mValue, Description, Date_log)
VALUES ( 4 , 'Experian LifeStyle Data Table Created' 		, getdate())   
  
END
  

------------------------------------Extracting PLAYPEN Data
IF @c8 = 1
BEGIN  
 SELECT exp_cb_key_household     cb_key_household
          ,exp_cb_key_individual  cb_key_individual
          ,exp_cb_key_family
          ,cb_row_id
          ,postcode_fixed_vintage
  INTO play_cv_data_expr_eval
  FROM sk_prod.PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD

  CREATE HG INDEX idxp02 ON play_cv_data_expr_eval(cb_key_individual)
  CREATE HG INDEX idxp03 ON play_cv_data_expr_eval(cb_key_household)
  
INSERT INTO Experian_Refresh_log_expr_eval (mValue, Description, Date_log)
VALUES ( 4 , 'PLAYPEN CV Data Table Created' 		, getdate())   
  
 END
 


  --------------Extracting PLAYPEN_EXPERIAN_LIFESTYLE Data
IF @c10 = 1
BEGIN
  SELECT cb_row_id
         ,cb_data_date
         ,cb_key_household
         ,cb_key_family
         ,cb_key_individual
         ,cb_address_status
         ,cb_source_cd
  INTO play_lifestyle_expr_eval
  FROM sk_prod.PLAYPEN_Experian_lifestyle

  CREATE HG INDEX idxp04 ON play_lifestyle_expr_eval(cb_key_individual)
  
 INSERT INTO Experian_Refresh_log_expr_eval (mValue, Description, Date_log)
VALUES ( 4 , 'PLAYPEN LifeStyle Data Table Created' 		, getdate())  
  
END

-----------------------------------VESPA Data Extraction ------------------------
IF @c9 = 1
BEGIN
  SELECT DISTINCT (ve.account_number)
      ,ve.cb_key_individual     AS individual_key
      ,ve.consumerview_cb_row_id
      ,ve.panel
      ,ve.panel_id_vespa
      , sav.cb_key_household    AS household_key
  INTO vespa_expr_eval
  FROM Vespa_Analysts.Vespa_Single_Box_View as ve
  INNER JOIN sk_prod.CUST_SINGLE_ACCOUNT_VIEW as sav ON ve.account_number = sav.account_number
  WHERE panel_id_vespa = 12


  CREATE HG INDEX idx02 ON vespa_expr_eval (individual_key)
 
  CREATE TABLE Experian_Refresh_VESPA_postcode_expr_eval
  ( postcode varchar(10))
   
  INSERT INTO Experian_Refresh_VESPA_postcode_expr_eval
  SELECT DISTINCT
        TRIM(REPLACE(SAV.cb_address_postcode,' ',''))
  FROM  sk_prod.CUST_SINGLE_ACCOUNT_VIEW as sav
    INNER JOIN vespa_expr_eval  sky    ON sky.account_number = sav.account_number
  WHERE   sav.cust_active_dtv = 1

INSERT INTO Experian_Refresh_log_expr_eval (mValue, Description, Date_log)
VALUES ( 4 , 'VESPA Data Tables Created' 		, getdate()) 


END
 