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
--execute Experian_montlhy_report_1 0,0,0,0,0,0,0,1,0
--execute Experian_montlhy_report_2
--execute Experian_montlhy_report_3 1,1,1,1,1,1,1	
--execute Experian_montlhy_report_4 1	--- Set to 0 To resume the loop / 1 if a new run
IF object_id('Experian_montlhy_report_1') IS NOT NULL THEN DROP PROCEDURE Experian_montlhy_report_1 END IF;
CREATE PROCEDURE Experian_montlhy_report_1



       @c2 bit
      , @c3 bit
      , @c31 bit
      , @c4 bit
      , @c5 bit
      , @c7 bit
      , @c8 bit
      , @c9 bit
      , @c10 bit

	  
AS
BEGIN

DECLARE @run_id int

/*SET @c2 = 1 --Experian Related Tables 
SET @c3 = 1 --Column to check
SET @c31 = 1 --Column to check
SET @c4 = 1 --Sky Data
SET @c5 = 1 --Experian CV Data
SET @c7 = 1 --Experian Lifestyle Data 
SET @c8 = 1 --Playpen Data
SET @c10 = 1 --Playpen Lifestyle
SET @c9 = 1 --VESPA ALL
  */   
  
---------------------------------Inserting Run ID Info
IF object_id('pitteloudj.Experian_Refresh_log') IS NULL 
BEGIN CREATE TABLE Experian_Refresh_log
	(ID 			int Identity
	, mValue		int
	, Description 	varchar(200)
	, Date_log 		DATETIME
	)
END

commit

SET @run_id = ISNULL ((SELECT max(mValue)+1  FROM Experian_Refresh_log WHERE Description =  'Run ID'), 1)


INSERT INTO Experian_Refresh_log (mValue, Description, Date_log)
VALUES ( @run_id   		, 'Run ID' 		, getdate())

---------------------------------Defining all Experian related Tables
IF @c2 = 1 
BEGIN 
  IF object_id('pitteloudj.Experian_Refresh_Tables') IS NOT NULL	        DROP TABLE  pitteloudj.Experian_Refresh_Tables
  SELECT 
      a.uid OwnerID
      , a.name  fOwner
      , b.id    TableID
      , b.name  TableName    
      , 0 Processed
      , 0 Date_processed
      , getdate() DateProcessed
	  , @run_id run_id
  INTO Experian_Refresh_Tables
  FROM dbo.sysusers   AS a
  JOIN dbo.sysobjects AS b ON a.uid = b.uid and (LOWER(b.name) like '%expe%' OR LOWER(b.name) like '%playpen%'
                                        OR LOWER(b.name) like '%consumerview%' OR LOWER(b.name) like '%propensitie%')
  WHERE 
    LOWER(a.name) like '%sk_prod'
	
  ALTER TABLE Experian_Refresh_Tables 
  ADD (ID int IDENTITY)
  
  ---------------------------------Flag on selected table / requires improvement
  UPDATE Experian_Refresh_Tables 
  SET  Processed = 1
  WHERE TableID in (7167519,7167831,10415848,10417456,10419481,10421089,15852254,15856405,15858168,15893701,
  17658634,17658913,17659525,17297166) 
 OR upper(TableName) in ('EXPERIAN_CONSUMERVIEW', 'EXPERIAN_LIFESTYLE','PLAYPEN_EXPERIAN_LIFESTYLE','PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD'
 ,'HOUSEHOLD_PROPENSITIES_GRID_NEW', 'HOUSEHOLD_PROPENSITIES_GRID_CUR','CONSUMERVIEW_POSTCODE','PERSON_PROPENSITIES_GRID_CUR','PERSON_PROPENSITIES_GRID_NEW'
 ,'PLAYPEN_CONSUMERVIEW_POSTCODE')


  CREATE HG INDEX idx1 on Experian_Refresh_Tables(TableID)

INSERT INTO Experian_Refresh_log (mValue, Description, Date_log)
VALUES ( @run_id , 'Experian Refresh Table Created: ' || @@rowcount 		, getdate())  
END

commit

---------------------------------Defining columns to be Checked
IF @c3 = 1
BEGIN
  IF object_id('pitteloudj.Experian_Refresh_Columns') IS NOT NULL         DELETE FROM pitteloudj.Experian_Refresh_Columns
  INSERT INTO pitteloudj.Experian_Refresh_Columns
  SELECT 
    b.TableID 
    , b.TableName   
    , c.id    ColID1
    , c.name  ColumnName
    , c.colid ColID2
  FROM  pitteloudj.Experian_Refresh_Tables as b 
      JOIN dbo.syscolumns AS c WITH (NOLOCK) ON b.TableId=c.id
  WHERE b.Processed = 1
END
  ---------------------------------Summary Table with Total Columns by Table
  IF @c31 = 1
BEGIN
    
    IF object_id('pitteloudj.EXPERIAN_Refresh_Total_Columns') IS NULL 
    BEGIN 
    CREATE TABLE EXPERIAN_Refresh_Total_Columns (TableName VARCHAR(100), Cols int, Record_Date DATETIME, run_id int)
  	INSERT INTO Experian_Refresh_log (mValue, Description, Date_log)
  	VALUES ( @run_id , 'EXPERIAN_Refresh_Total_Columns Tables Created' 		, getdate()  )

    END 
    
    INSERT INTO EXPERIAN_Refresh_Total_Columns
    SELECT TableName
        , count(colID1) Cols 
        , getdate() Record_Date
  	  , @run_id run_id
    FROM Experian_Refresh_Columns
    GROUP BY TableName
    
  INSERT INTO Experian_Refresh_log (mValue, Description, Date_log)
  VALUES ( @run_id , 'Total Columns by Tables inserted: ' || @@rowcount 		, getdate()  )
END

commit
---------------------------------Extracting Sky Data
IF @c4 = 1
BEGIN
  IF object_id('pitteloudj.skybase') IS NOT NULL	       TRUNCATE TABLE pitteloudj.skybase
  INSERT INTO pitteloudj.skybase
  SELECT DISTINCT a.account_number
      ,a.cb_key_household as household_key
      ,a.cb_key_individual as individual_key
      ,a.cb_key_family as family_key
  FROM cust_subs_hist as a
  JOIN cust_single_account_view as s ON a.account_number = s.account_number
  WHERE subscription_sub_type IN ('DTV Primary Viewing')
     AND a.status_code IN ('AC','AB','PC')
     AND a.effective_from_dt <= today()
     AND a.effective_to_dt > today()
     AND a.EFFECTIVE_FROM_DT IS NOT NULL
     AND a.cb_key_household > 0             --UK Only
     AND a.cb_key_household IS NOT NULL
     AND a.account_number IS NOT NULL
     AND a.service_instance_id IS NOT NULL
     AND s.cb_address_status = '1'



  IF object_id('pitteloudj.Experian_Refresh_Sky_postcode') IS NOT NULL   DELETE FROM pitteloudj.Experian_Refresh_Sky_postcode
  INSERT INTO Experian_Refresh_Sky_postcode
  
  SELECT distinct 
        trim(replace(SAV.cb_address_postcode,' ','')) AS postcode
  FROM  CUST_SINGLE_ACCOUNT_VIEW as sav
    INNER JOIN skybase    AS sky    ON sky.account_number = sav.account_number
  WHERE   sav.cust_active_dtv = 1

INSERT INTO Experian_Refresh_log (mValue, Description, Date_log)
VALUES ( @run_id , 'Skybase Data Tables Created:' || @@rowcount		, getdate())

END

commit

--------------------------------------------------------------------------------------
------------------------------------------------Extracting EXPERIAN_CONSUMERVIEW Data
IF @c5 = 1
BEGIN 
  IF object_id('pitteloudj.exp_cv_data') IS NOT NULL     DELETE FROM pitteloudj.exp_cv_data
  
  INSERT INTO pitteloudj.exp_cv_data
  SELECT cb_key_household
          ,cb_key_individual
          ,cb_key_family
          ,cb_row_id
          ,cb_address_postcode
          ,h_fss_v3_group
          ,h_fss_v3_type

  FROM EXPERIAN_CONSUMERVIEW


  
INSERT INTO Experian_Refresh_log (mValue, Description, Date_log)
VALUES ( @run_id , 'Experian CV Data Table Created: ' 	|| @@rowcount			, getdate())

END


--------------------------------Extracting EXPERIAN_LIFESTYLE Data
IF @c7 = 1
BEGIN 
  
  IF object_id('pitteloudj.exp_lifestyle') IS NOT NULL   DELETE FROM pitteloudj.exp_lifestyle
  
  INSERT INTO pitteloudj.exp_lifestyle
  SELECT cb_row_id
         ,exp_cb_address_mailable_flag
         ,cb_key_household
         ,cb_key_family
         ,cb_key_individual
         ,cb_address_status
         ,cb_source_cd
  FROM Experian_lifestyle
		  
INSERT INTO Experian_Refresh_log (mValue, Description, Date_log)
VALUES ( @run_id , 'Experian LifeStyle Data Table Created: ' || @@rowcount		, getdate())   
  
END
  

------------------------------------Extracting PLAYPEN Data
IF @c8 = 1
BEGIN  
  IF object_id('pitteloudj.play_cv_data') IS NOT NULL    DELETE FROM  pitteloudj.play_cv_data
  
  INSERT INTO pitteloudj.play_cv_data
  SELECT exp_cb_key_household     cb_key_household
          ,exp_cb_key_individual  cb_key_individual
          ,exp_cb_key_family
          ,cb_row_id
          ,postcode_fixed_vintage
  FROM PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD

INSERT INTO Experian_Refresh_log (mValue, Description, Date_log)
VALUES ( @run_id , 'PLAYPEN CV Data Table Created: ' || @@rowcount		, getdate())   
  
 END

  --------------Extracting PLAYPEN_EXPERIAN_LIFESTYLE Data
IF @c10 = 1
BEGIN
  IF object_id('pitteloudj.play_lifestyle') IS NOT NULL  DELETE FROM pitteloudj.play_lifestyle
  
  INSERT INTO pitteloudj.play_lifestyle
  SELECT cb_row_id
         ,cb_data_date
         ,cb_key_household
         ,cb_key_family
         ,cb_key_individual
         ,cb_address_status
         ,cb_source_cd
  FROM PLAYPEN_Experian_lifestyle
    
 INSERT INTO Experian_Refresh_log (mValue, Description, Date_log)
VALUES ( @run_id , 'PLAYPEN LifeStyle Data Table Created: ' || @@rowcount 		, getdate())  
  
END

-----------------------------------VESPA Data Extraction ------------------------
IF @c9 = 1
BEGIN
  IF object_id('pitteloudj.vespa') IS NOT NULL           DELETE FROM pitteloudj.vespa
  INSERT INTO pitteloudj.vespa
  SELECT DISTINCT (ve.account_number)
      ,ve.cb_key_individual     AS individual_key
	  ,sav.cb_key_family 		AS family_key
      ,ve.consumerview_cb_row_id
      ,ve.panel
      ,ve.panel_id_vespa
      , sav.cb_key_household    AS household_key
  FROM Vespa_Analysts.Vespa_Single_Box_View as ve
  INNER JOIN CUST_SINGLE_ACCOUNT_VIEW as sav ON ve.account_number = sav.account_number
  WHERE panel_id_vespa in (11, 12)

 
INSERT INTO Experian_Refresh_log (mValue, Description, Date_log)
VALUES ( @run_id , 'VESPA Data Table Created: ' || @@rowcount 		, getdate())  
 ----------------------------- Populating Experian_Refresh_VESPA_postcode
 
  IF object_id('pitteloudj.Experian_Refresh_VESPA_postcode') IS NOT NULL        DELETE FROM pitteloudj.Experian_Refresh_VESPA_postcode
   
  INSERT INTO Experian_Refresh_VESPA_postcode

  SELECT DISTINCT
        TRIM(REPLACE(SAV.cb_address_postcode,' ','')) as postcode

  FROM  CUST_SINGLE_ACCOUNT_VIEW as sav
    INNER JOIN vespa  sky    ON sky.account_number = sav.account_number
  WHERE   sav.cust_active_dtv = 1

INSERT INTO Experian_Refresh_log (mValue, Description, Date_log)
VALUES ( @run_id , 'VESPA Postal Data Table Created: ' || @@rowcount 		, getdate()) 


END
 
 
commit



END