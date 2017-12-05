
CREATE VARIABLE @run_id int;
DECLARE  @c2 bit
      , @c3 bit
      , @c31 bit
      , @c4 bit


SET @c2 = 1 --Experian Related Tables 
SET @c3 = 1 --Column to check
SET @c31 = 1 --Column to check
     
  
---------------------------------Inserting Run ID Info
IF object_id('pitteloudj.Experian_November_log') IS NULL 
BEGIN CREATE TABLE Experian_November_log
	(ID 			int Identity
	, mValue		int
	, Description 	varchar(200)
	, Date_log 		DATETIME
	)
END

commit

SET @run_id = ISNULL ((SELECT max(mValue)+1  FROM Experian_November_log WHERE Description =  'Run ID'), 1)


INSERT INTO Experian_November_log (mValue, Description, Date_log)
VALUES ( @run_id   		, 'Run ID' 		, getdate())

---------------------------------Defining all Experian related Tables
IF @c2 = 1 
BEGIN 
  IF object_id('pitteloudj.Experian_Refresh_Tables_nov') IS NOT NULL	        DROP TABLE  pitteloudj.Experian_Refresh_Tables_nov
  SELECT 
      a.uid OwnerID
      , a.name  fOwner
      , b.id    TableID
      , b.name  TableName    
      , 0 Processed
      , 0 Date_processed
      , getdate() DateProcessed
	  , @run_id run_id
  INTO Experian_Refresh_Tables_nov
  FROM dbo.sysusers   AS a
  JOIN dbo.sysobjects AS b ON a.uid = b.uid and (LOWER(b.name) like '%expe%' OR LOWER(b.name) like '%playpen%'
                                        OR LOWER(b.name) like '%consumerview%' OR LOWER(b.name) like '%propensitie%')
  WHERE 
    LOWER(a.name) like '%sk_prod'
	OR LOWER(a.name) like '%sk_prodreg'
	
  ALTER TABLE Experian_Refresh_Tables_nov 
  ADD (ID int IDENTITY)
  
  ---------------------------------Flag on selected table / requires improvement
  UPDATE Experian_Refresh_Tables_nov 
  SET  Processed = 1
  WHERE upper(TableName) in ('EXPERIAN_CONSUMERVIEW', 'EXPERIAN_LIFESTYLE','PLAYPEN_EXPERIAN_LIFESTYLE','PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD'
 ,'HOUSEHOLD_PROPENSITIES_GRID_NEW', 'HOUSEHOLD_PROPENSITIES_GRID_CUR','CONSUMERVIEW_POSTCODE','PERSON_PROPENSITIES_GRID_CUR','PERSON_PROPENSITIES_GRID_NEW'
 ,'PLAYPEN_CONSUMERVIEW_POSTCODE')


  CREATE HG INDEX idx1 on Experian_Refresh_Tables_nov(TableID)

INSERT INTO Experian_November_log (mValue, Description, Date_log)
VALUES ( @run_id , 'Experian Refresh Table Created: ' || @@rowcount 		, getdate())  
END

commit

---------------------------------Defining columns to be Checked
IF @c3 = 1
BEGIN
  SELECT 
	fOwner
	, b.TableID 
    , b.TableName   
    , c.id    ColID1
    , c.name  ColumnName
    , c.colid ColID2
  INTO pitteloudj.Experian_Refresh_Columns_nov
  FROM  pitteloudj.Experian_Refresh_Tables_nov as b 
      JOIN dbo.syscolumns AS c WITH (NOLOCK) ON b.TableId=c.id
  WHERE b.Processed = 1
END
  ---------------------------------Summary Table with Total Columns by Table
  IF @c31 = 1
BEGIN
    
    IF object_id('pitteloudj.EXPERIAN_Refresh_Total_Columns_nov') IS NULL 
    BEGIN 
    CREATE TABLE EXPERIAN_Refresh_Total_Columns_nov 
      (fOwner VARCHAR(100) DEFAULT null
      ,  TableName VARCHAR(100) DEFAULT null
      , Cols int
      , Record_Date DATETIME DEFAULT null
      , run_id int)
  	INSERT INTO Experian_November_log (mValue, Description, Date_log)
  	VALUES ( @run_id , 'EXPERIAN_Refresh_Total_Columns_nov Tables Created' || @@rowcount		, getdate()  )

    END 
    
    INSERT INTO EXPERIAN_Refresh_Total_Columns_nov
    SELECT 
		fOwner
		, TableName
        , count(colID1) Cols 
        , getdate() Record_Date
		, 2--@run_id run_id
    FROM Experian_Refresh_Columns_nov
    GROUP BY TableName, 	fOwner
    
  INSERT INTO Experian_November_log (mValue, Description, Date_log)
  VALUES ( @run_id , 'Total Columns by Tables inserted: ' || @@rowcount 		, getdate()  )
END

 
DROP VARIABLE @run_id;  
commit
