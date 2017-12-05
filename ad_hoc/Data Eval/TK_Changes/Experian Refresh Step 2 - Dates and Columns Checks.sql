------------------------------------------------------------------------------------
--------------------TABLE DATES -----------------------------------------------------
-------------------------------------------------------------------------------------
DECLARE @run_id int
      , @sql2       VARCHAR(1000)
      , @Tablec     VARCHAR(100)
      , @ColID      INT
      , @ColumnN    VARCHAR(200)
      , @cont       INT  
      , @sql1     VARCHAR(1000)
      , @Table    VARCHAR(100)
      , @TableID  BIGINT      

	  
SET @run_id   = ISNULL ((SELECT max(mValue)  FROM Experian_Refresh_log_expr_eval WHERE Description =  'Run ID'), 1)

IF object_id('Experian_Table_Dates_expr_eval') is null 
BEGIN
CREATE TABLE Experian_Table_Dates_expr_eval
	(ID int Identity  , Table_Name varchar(70)  , Load_Dates datetime  , Report_Date datetime, run_id int)

INSERT INTO Experian_Refresh_log_expr_eval (mValue, Description, Date_log)
VALUES ( 4 , 'Experian_Table_Dates Table Created' 		, getdate()) 
END

WHILE EXISTS  (SELECT top 1 *
                FROM Experian_Refresh_Tables_expr_eval
                WHERE Processed = 1 
                    AND Date_processed = 0) 
BEGIN                     
  SET @TableID =  (SELECT top 1 ID
                  FROM Experian_Refresh_Tables_expr_eval
                  WHERE Processed = 1 
                    AND Date_processed = 0)
  SET @Table =    (SELECT TableName
                  FROM Experian_Refresh_Tables_expr_eval
                  WHERE ID = @TableID)
  SET @sql1 = (
  'INSERT INTO   Experian_Table_Dates_expr_eval (Table_name, Load_Dates, Report_Date, run_id)
  SELECT DISTINCT 
  @Table TableName
  , cb_data_date LoadDate
  , getdate() Report_Date
  , @run_id
  FROM sk_prod.'||@Table )
  
  EXECUTE(@sql1)
  
  UPDATE Experian_Refresh_Tables_expr_eval
  SET Date_processed = 1 WHERE  ID = @TableID
    
END

 IF @@error = 0	INSERT INTO Experian_Refresh_log_expr_eval (mValue, Description, Date_log)
		VALUES ( 4 , 'Experian_Table_Dates Table Created' 		, getdate()) 

-------------------------------------------------------------------------------------
--------------------Columns Contents-------------------------------------------------
-------------------Check for Null and Quasi-nulls------------------------------------
IF object_id('Experian_Refresh_Columns_Results_expr_eval') IS NULL
BEGIN
CREATE TABLE Experian_Refresh_Columns_Results_expr_eval
	( ID              int IDENTITY
	  , TableName     VARCHAR (100)
	  , TableID       int
	  , ColumnName    VARCHAR (200)
	  , records       int
	  , proc_reg      bit default 0
	  , Content_Flag  bit 
	  , Null_Flag     bit 
	  , New_column	  bit 
	  , Deleted_col   bit 
	  , Date_load     datetime  
	  , Date_proc     datetime
	  , run_id		  int
	 )
	 
	INSERT INTO Experian_Refresh_log_expr_eval (mValue, Description, Date_log)
		VALUES ( 4 , 'Experian_Refresh_Columns_Results Table Created' 		, getdate())  
END


INSERT INTO Experian_Refresh_Columns_Results_expr_eval
( TableName, TableID, ColumnName, Date_load, run_id)
SELECT 
     TableName
    , TableID
    , ColumnName
    , GETDATE()
	, @run_id
FROM Experian_Refresh_Columns_expr_eval --Experian_Refresh_Tables 
ORDER BY TableName

INSERT INTO Experian_Refresh_log_expr_eval (mValue, Description, Date_log)
		VALUES ( 4 , 'Experian_Refresh_Columns_Results Values Inserted' 		, getdate())  
		
--------------------CHECK Loop----------------------
WHILE EXISTS (SELECT top 1 *
                FROM Experian_Refresh_Columns_Results_expr_eval
                WHERE proc_reg = 0)
BEGIN
  WHILE (@cont < 500)
  BEGIN
      SET @ColID = (SELECT top 1 ID
                  FROM Experian_Refresh_Columns_Results_expr_eval
                  WHERE proc_reg = 0)
      SET @Tablec = (SELECT top 1 TableName FROM Experian_Refresh_Columns_Results_expr_eval WHERE ID = @ColID)
      SET @ColumnN = (SELECT top 1 ColumnName FROM Experian_Refresh_Columns_Results_expr_eval WHERE ID = @ColID)
      SET @sql2 = 
      'UPDATE Experian_Refresh_Columns_Results_expr_eval
        SET records = (SELECT count('||@ColumnN||')
                          FROM sk_prod.'||@Tablec||' ),
            Date_proc = GETDATE()
        WHERE ID = @ColID'
    
    EXECUTE(@sql2)
    
    UPDATE Experian_Refresh_Columns_Results_expr_eval
    SET proc_reg = 1 WHERE  ID = @ColID
  SET @cont = @cont + 1
  END
SET @cont =0 
END

 IF @@error = 0	INSERT INTO Experian_Refresh_log_expr_eval (mValue, Description, Date_log)
		VALUES ( 4 , 'Null Columns checked' 		, getdate()) 


UPDATE Experian_Refresh_Columns_Results_expr_eval
SET Null_Flag = 1 
WHERE  records = 0

IF @@error = 0	INSERT INTO Experian_Refresh_log_expr_eval (mValue, Description, Date_log)
		VALUES ( 4 , 'Update Null Columns' 		, getdate()) 

