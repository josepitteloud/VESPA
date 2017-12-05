------------------------------------------------------------------------------------
--------------------TABLE DATES -----------------------------------------------------
-------------------------------------------------------------------------------------

IF object_id('Experian_montlhy_report_2') IS NOT NULL DROP PROCEDURE Experian_montlhy_report_2
CREATE OR REPLACE PROCEDURE Experian_montlhy_report_2

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

          
    SET @run_id   = ISNULL ((SELECT max(mValue)  FROM Experian_Refresh_log WHERE Description =  'Run ID'), 1)

    WHILE EXISTS  (SELECT top 1 *
                    FROM Experian_Refresh_Tables
                    WHERE Processed = 1
                        AND Date_processed = 0)
    BEGIN
      SET @TableID =  (SELECT top 1 ID
                      FROM Experian_Refresh_Tables 
                      WHERE Processed = 1 
                        AND Date_processed = 0
                        )
      SET @Table =    (SELECT TableName
                      FROM Experian_Refresh_Tables 
                      WHERE ID = @TableID)
                      
      SET @sql1 = (  'INSERT INTO   Experian_Table_Dates (Table_name, Load_Dates, Report_Date, run_id)  
      SELECT DISTINCT 
      @Table TableName  , cb_data_date LoadDate  , getdate() Report_Date  , @run_id
      FROM  '||@Table
      )
      
      EXECUTE(@sql1)

      UPDATE Experian_Refresh_Tables 
      SET Date_processed = 1 WHERE  ID = @TableID
        commit
    END

     INSERT INTO Experian_Refresh_log (mValue, Description, Date_log)
     VALUES ( @run_id , 'Experian_Table_Dates Table Created'        , getdate()) 
     commit
END
COMMIT
