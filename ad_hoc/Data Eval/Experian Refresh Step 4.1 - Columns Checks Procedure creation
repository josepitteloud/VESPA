
CREATE PROCEDURE pitteloudj.EXP_CHK_4_COLUMNS( 
	@c13 BIT --- Set to 0 To resume the loop / 1 if a new run	
	) AS
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
		  , @c13 BIT

	SET @run_id   = ISNULL ((SELECT max(mValue)  FROM Experian_Refresh_log WHERE Description =  'Run ID'), 1)

	
	IF @c13 = 1 
	BEGIN
		IF object_id('pitteloudj.Experian_Refresh_Columns_temp') IS NOT NULL    DELETE FROM pitteloudj.Experian_Refresh_Columns_temp
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
