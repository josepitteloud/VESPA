/******************************************************************************
**
**  Project Vespa: PROJECT  V186 - Callcredit Insurance Data Eval 1
**  	Data preparation and Basic Quality Checks
**
**  This script will create the tables an perform the basic checks on
**	the CalCredit Insurance Renewal Data as part of the Analytic 
**	Brief - Analytical Task 1 and 2
**
**	Related Documents:
**		- VESPA_INSURANCE DATA EVALUATION 1.sql
**		- VESPA_INSURANCE DATA EVALUATION 2.sql
**		- VESPA_INSURANCE DATA EVALUATION 1.sql
**
**	Code Sections:

**	Section A - Data Preparation
**		A01	-	CREATING LOG Table
**		A02 - 	SELECTING Columns to Check
**		A03 - 	Metric TABLE Creation
**	Section B - Metric Processing
**		B01	-	Metric Processing
**
**	Written by Jose Pitteloud
******************************************************************************/


DECLARE 
      @n int
    , @Count int
    , @Colname varchar(60)
    , @TotalCol int
    , @max int
    , @min int
    , @difcount int
    , @sql varchar(1000)
    , @sql1 varchar(1000)
    , @sql2 varchar(1000)
    , @sql3 varchar(1000)
    , @c0 INT --Creating LOG Table
    , @c1 INT --COLUMNS TO CHECK
    , @c2 INT --Metric TABLE Creation
    , @c3 INT --Metric Processing
    , @c4 INT --
    , @c5 INT --
    , @c6 INT --
    , @c7 INT -- 
    , @run_id int
    
SET @c0=0
SET @c1=0
SET @c2=0
SET @c3=1
SET @c4=1
SET @n=1

/******************EXPLORING
SELECT top 10 * from sk_prod.VESPA_INSURANCE_DATA
SELECT count(*) from sk_prod.VESPA_INSURANCE_DATA
sp_columns VESPA_INSURANCE_DATA
*****************************/

IF @c0 = 1 ------------------A01		-		CREATING LOG Table
BEGIN
  CREATE TABLE VESPA_INSURANCE_EVAL_LOG
  ( ID INT IDENTITY
    , MValue INT
    , Description VARCHAR (100)
    , Date_proc DATETIME)

  INSERT INTO VESPA_INSURANCE_EVAL_LOG (MValue, Description , Date_proc)
    VALUES (1,'LOG TABLE CREATED', GETDATE())
END

SET @run_id = ISNULL((SELECT MAX(Mvalue) FROM VESPA_INSURANCE_EVAL_LOG WHERE Description like 'Run ID')+1, 1)

INSERT INTO VESPA_INSURANCE_EVAL_LOG (MValue, Description , Date_proc)
  VALUES (@run_id,'Run ID', GETDATE())


IF @c1 =1 --------------------A02	-	SELECTING Columns to Check
BEGIN
  SELECT 
    a.uid SchemaID,
    a.name SchemaName,
    b.id TableID,
    b.name TableName, 
    c.colid ColumnID, 
    c.name ColumnName,
    c.type ColType,
    CASE WHEN ColumnName like 'cb_%' THEN 'CB Key' ELSE 'Source' END ColSource,
    'Undefined' Grouping,
    @run_id Run_ID
    
  INTO VESPA_INSURANCE_COLUMNS
  FROM dbo.sysusers   AS a
    JOIN dbo.sysobjects AS b ON a.uid = b.uid 
    JOIN dbo.syscolumns AS c ON c.id = b.id 
  WHERE
    c.id = 16456388            ---- Table ID  16456388

  INSERT INTO VESPA_INSURANCE_EVAL_LOG (MValue, Description , Date_proc)
    VALUES (1,' VESPA_INSURANCE_COLUMNS Table Created', GETDATE())

END

SET @TotalCol = (SELECT count(ColumnID) FROM VESPA_INSURANCE_COLUMNS)

IF @c2 =1 ------------------- A03	-	Metric TABLE Creation
BEGIN 
  CREATE TABLE VESPA_INSURANCE_EVAL
      ( ID INT IDENTITY
      , ColumnName varchar (200)
      , ColumnID int
      , MetricDesc varchar (60)
      , ValidCount varchar(20)
      , Date_proc DATETIME
      , run_id INT)
      
  INSERT INTO VESPA_INSURANCE_EVAL_LOG (MValue , Description , Date_proc)
    VALUES (1,' VESPA_INSURANCE_EVAL Table Created', GETDATE())
END


IF @c3 = 1 ---------------------B01		-	Metric Processing
BEGIN 
  WHILE @n <=@TotalCol 
  BEGIN
    SET @Colname = (SELECT ColumnName FROM VESPA_INSURANCE_COLUMNS WHERE ColumnID = @n)
    SET @sql = '(SELECT count('||@Colname||')				CCount	                  
      , count(DISTINCT '||@Colname||')	difcount			                  
      , MAX('||@Colname||')             MaxNum                  
      , MAX(Len('||@Colname||'))          MaxChar                  
      , MIN('||@Colname||')             MinNum                  
      , MIN(Len('||@Colname||'))          MinChar                  
    INTO #t1                   
    FROM sk_prod.VESPA_INSURANCE_DATA)'

    EXECUTE (@sql)
    
    INSERT INTO VESPA_INSURANCE_EVAL   (ColumnName,  ColumnID, MetricDesc, ValidCount, Date_proc, run_id)
      SELECT @Colname , @n,   'NON-NULL Values', CAST( CCount as varchar) , getdate(), @run_id FROM #t1
    INSERT INTO VESPA_INSURANCE_EVAL   (ColumnName,  ColumnID, MetricDesc, ValidCount, Date_proc, run_id)
      SELECT @Colname , @n,   'DIFF Values', CAST( difcount as varchar) , getdate(), @run_id FROM #t1
    INSERT INTO VESPA_INSURANCE_EVAL   (ColumnName,  ColumnID, MetricDesc, ValidCount, Date_proc, run_id)
      SELECT @Colname , @n,   'MAX Num Values', CAST(MaxNum as varchar) , getdate(), @run_id FROM #t1
    INSERT INTO VESPA_INSURANCE_EVAL   (ColumnName,  ColumnID, MetricDesc, ValidCount, Date_proc, run_id)
      SELECT @Colname , @n,   'MAX Char Values', CAST(MaxChar as varchar) , getdate(), @run_id FROM #t1
    INSERT INTO VESPA_INSURANCE_EVAL   (ColumnName,  ColumnID, MetricDesc, ValidCount, Date_proc, run_id)
      SELECT @Colname , @n,   'MIN Num Values', CAST(MinNum as varchar) , getdate(), @run_id FROM #t1
    INSERT INTO VESPA_INSURANCE_EVAL   (ColumnName,  ColumnID, MetricDesc, ValidCount, Date_proc, run_id)
      SELECT @Colname , @n,   'MIN Char Values', CAST(MinChar as varchar) , getdate(), @run_id FROM #t1
    
    DROP TABLE #t1
    SET @n = @n + 1
  END

  INSERT INTO VESPA_INSURANCE_EVAL_LOG (MValue, Description , Date_proc)
    VALUES (1,' Metrics Inserted', GETDATE())
END



