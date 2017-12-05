/******************************************************************************
**
**  Project Vespa: Experian Data Quality Report. Phase 4 Columns checks
**
**  Intended to produce a repository with basic quality metrics. This will feed a Monthly Quality Report that will 
**  allow us to track any deviation on the data acquired
**
**  
**      
******************************************************************************/


IF object_id('Experian_montlhy_report_4') IS NOT NULL THEN DROP PROCEDURE Experian_montlhy_report_4 END IF;
CREATE PROCEDURE Experian_montlhy_report_4
      @c13 bit
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

	-------------------------------------------------------------------------------------
	--------------------Columns Contents-------------------------------------------------
	-------------------Check for Null and Quasi-nulls------------------------------------
	IF @c13 = 1 
	BEGIN
		IF object_id('pitteloudj.Experian_Refresh_Columns_temp') IS NOT NULL    DELETE FROM pitteloudj.Experian_Refresh_Columns_temp
	/*
		CREATE TABLE Experian_Refresh_Columns_temp
			( ID              int IDENTITY
			  , TableName     VARCHAR (100)
			  , TableID       int
			  , ColumnName    VARCHAR (200)
			  , proc_reg      bit default 0
			  , run_id		  int
			 )
			 
			INSERT INTO Experian_Refresh_log (mValue, Description, Date_log)
				VALUES ( @run_id , 'Experian_Refresh_Columns_temp Table Created' 		, getdate())  


	*/
		INSERT INTO Experian_Refresh_Columns_temp
		( TableName, TableID, ColumnName, run_id,)
		SELECT 
			 TableName
			, TableID
			, ColumnName
			, @run_id
		FROM Experian_Refresh_Columns --Experian_Refresh_Tables 
		ORDER BY TableName


		INSERT INTO Experian_Refresh_log (mValue, Description, Date_log)
				VALUES ( @run_id , 'Experian_Refresh_Columns_temp Values Inserted' 		, getdate())  
	commit
	END

	--------------------CHECK Loop----------------------
	WHILE EXISTS (SELECT top 1 *
					FROM Experian_Refresh_Columns_temp 
					WHERE proc_reg = 0)
		BEGIN
			  WHILE (@cont < 100)
				  BEGIN
					  SET @ColID = (SELECT top 1 ID
								  FROM Experian_Refresh_Columns_temp 
								  WHERE proc_reg = 0)
					  SET @Tablec = (SELECT top 1 TableName FROM Experian_Refresh_Columns_temp WHERE ID = @ColID)
					  SET @ColumnN = (SELECT top 1 ColumnName FROM Experian_Refresh_Columns_temp WHERE ID = @ColID)
					  
			EXECUTE DQ_Basic_Checks_TEST2 @run_id, @Tablec, @ColumnN
					  
					UPDATE Experian_Refresh_Columns_temp 
					SET proc_reg = 1 WHERE  ID = @ColID
				  SET @cont = @cont + 1
			
			END
			SET @cont =0 
		COMMIT 
		 UPDATE Experian_Refresh_Columns_Results_TEST
		 SET Content_flag = 1 
		 WHERE ColumnName like '%filler%' OR ColumnName like 'cb_%'
		 
		 
		END

	INSERT INTO Experian_Refresh_log (mValue, Description, Date_log)
			VALUES ( @run_id , 'Null Columns checked' 		, getdate()) 
		
END