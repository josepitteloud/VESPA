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

	  
SET @run_id   = ISNULL ((SELECT max(mValue)  FROM Experian_November_log WHERE Description =  'Run ID'), 1)

WHILE EXISTS  (SELECT top 1 *
                FROM Experian_Refresh_Tables_nov 
                WHERE Processed = 1 
                    AND Date_processed = 0) 
BEGIN                     
  SET @TableID =  (SELECT top 1 ID
                  FROM Experian_Refresh_Tables_nov 
                  WHERE Processed = 1 
                    AND Date_processed = 0
                    )
  SET @Table =    (SELECT TableName
                  FROM Experian_Refresh_Tables_nov 
                  WHERE ID = @TableID)
                  
  SET @sql1 = (  'INSERT INTO   Experian_Table_Dates (Table_name, Load_Dates, Report_Date, run_id)  
  SELECT DISTINCT 
  @Table TableName  , cb_data_date LoadDate  , getdate() Report_Date  , @run_id
  FROM sk_prod.'||@Table 
  )
  
  EXECUTE(@sql1)
  
  UPDATE Experian_Refresh_Tables_nov 
  SET Date_processed = 1 WHERE  ID = @TableID
    commit
END

 INSERT INTO Experian_November_log (mValue, Description, Date_log)
 VALUES ( @run_id, 'Experian_Table_Dates Table Created' 		, getdate()) 
 commit
