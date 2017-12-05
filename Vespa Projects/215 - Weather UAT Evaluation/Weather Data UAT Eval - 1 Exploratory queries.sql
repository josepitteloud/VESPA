/******************************************************************************
**
**                  SSSS   KK   KK  YY    YY   IIII   QQQQQ
**                 SS  SS  KK  KK    YY  YY     II   QQ   QQ  
**                 SS      KK KK      YYYY      II   QQ   QQ    
**                  SSSS   KKKK        YY       II   QQ   QQ      
**                     SS  KKK         YY       II   QQ  QQQ    
**                 SS  SS  KK KK       YY       II   QQ   QQQ  
**                  SSSS   KK   KKK    YY      IIII   QQQQQ QQ       
**
**
**  Project Vespa: PROJECT  V215 - Weather Data Evaluation 1
**  
**	
**
**	Related Documents:
**	
**
**
**	Code Sections:
**
**	Section A - Data Preparation
**		A01	- CREATING LOG Table
**		A02 - SELECTING Columns to Check
**		A03 - Metric TABLE Creation
**		A04 - Metric Processing
**	Section B - Postal Info Gathering
**		B01	- Postal District Reference Table Creation
**		B02 - Postal District Reference Table Creation
**
**	Written by Jose Pitteloud
******************************************************************************/

DECLARE 
      @n int
    , @Count int
    , @Colname varchar(60)
    , @Tblname varchar(60)
    , @TotalCol int
    , @max int
    , @min int
    , @difcount int
    , @sql varchar(1000)
    , @c0 INT --Creating LOG Table
    , @c1 INT --COLUMNS TO CHECK
    , @c2 INT --Metric TABLE Creation
    , @c3 INT --Metric Processing
	, @c4 INT --Postal Table Creation
	, @c5 INT --Postal Table Insert
    , @run_id int
    
SET @c0=0
SET @c1=0
SET @c2=0
SET @c3=0
SET @c4=1
SET @c5=1
SET @n=1

  IF object_id('pitteloudj.WEATHER_POST_DISTRICT_REFERENCE') IS NOT NULL	      DROP TABLE  pitteloudj.WEATHER_POST_DISTRICT_REFERENCE
  IF object_id('sk_uat.WEATHER_DATA_TABLES') IS NOT NULL	                      DROP TABLE  sk_uat.WEATHER_DATA_TABLES
  


IF @c0 = 1  ------------------A01		-		CREATING LOG Table
BEGIN
  CREATE TABLE WEATHER_DATA_EVAL_LOG
  ( ID INT IDENTITY
    , MValue INT
    , Description VARCHAR (150)
    , Qty INT DEFAULT NULL
    , Date_proc DATETIME)
END 
IF @c0 = 1
BEGIN
  INSERT INTO WEATHER_DATA_EVAL_LOG (MValue, Description , Date_proc)
    VALUES (0,'LOG TABLE CREATED', GETDATE())
END

SET @run_id = ISNULL((SELECT MAX(Mvalue) FROM WEATHER_DATA_EVAL_LOG WHERE Description like 'Run ID')+1, 1)

INSERT INTO WEATHER_DATA_EVAL_LOG (MValue, Description , Date_proc)
  VALUES (@run_id,'Run ID', GETDATE())
  
  
IF @c1 =1 --------------------A02	-	SELECTING Columns to Check
BEGIN 
  SELECT 
    a.uid     SchemaID,
    a.name    SchemaName,
    b.id      TableID,
    b.name    TableName, 
    c.colid   ColumnID, 
    c.name    ColumnName,
    c.type    ColType,
    c.length  ColLength,
    CASE WHEN ColumnName like 'cb_%' THEN 'CB Key' ELSE 'Source' END ColSource,
    @run_id   Run_ID
  INTO WEATHER_DATA_TABLES
  FROM dbo.sysusers   AS a
    JOIN dbo.sysobjects AS b ON a.uid = b.uid 
    JOIN dbo.syscolumns AS c ON c.id = b.id 
  WHERE a.name LIKE 'sk_uat'
      AND (UPPER (b.name) like 'WEATHER_DATA' OR UPPER(b.name) like 'WEATHER_DATA_HISTORY')
  
  ALTER TABLE WEATHER_DATA_TABLES
  ADD ID int IDENTITY 
  
  INSERT INTO WEATHER_DATA_EVAL_LOG (MValue, Description ,Qty, Date_proc)
    VALUES (1,' WEATHER_DATA_TABLES Table Created and populated',@@rowcount, GETDATE())
END  
      

SET @TotalCol = (SELECT count(ColumnID) FROM WEATHER_DATA_TABLES)

IF @c2 =1 ------------------- A03	-	Metric TABLE Creation
BEGIN 
  CREATE TABLE WEATHER_DATA_METRICS
      ( ID INT IDENTITY
      , TableName varchar (200)
      , ColumnName varchar (200)
      , ColumnID int
      , MetricDesc varchar (60)
      , ValidCount varchar(20)
      , Date_proc DATETIME
      , run_id INT)
      
  INSERT INTO WEATHER_DATA_EVAL_LOG (MValue , Description , Date_proc)
    VALUES (1,' WEATHER_DATA_METRICS Table Created - Empty', GETDATE())
END      

IF @c3 = 1 -------------------- A04		-	Metric Processing
BEGIN 
  WHILE @n <=@TotalCol 
  BEGIN
    SET @Colname = (SELECT ColumnName FROM WEATHER_DATA_TABLES WHERE ID = @n)
    SET @Tblname = (SELECT TableName  FROM WEATHER_DATA_TABLES WHERE ID = @n)
    SET @sql = '(SELECT count('||@Colname||')				CCount	                  
      , count(DISTINCT '||@Colname||')	difcount			                  
      , MAX('||@Colname||')             MaxNum                  
      , MAX(Len('||@Colname||'))          MaxChar                  
      , MIN('||@Colname||')             MinNum                  
      , MIN(Len('||@Colname||'))          MinChar                  
    INTO #t1                   
    FROM sk_uat.'||@Tblname||')'

    EXECUTE (@sql)
   
  
    INSERT INTO WEATHER_DATA_METRICS   (TableName, ColumnName,  ColumnID, MetricDesc, ValidCount, Date_proc, run_id)
      SELECT @Tblname, @Colname , @n,   'NON-NULL Values', CAST( CCount as varchar) , getdate(), @run_id FROM #t1
    INSERT INTO WEATHER_DATA_METRICS   (TableName, ColumnName,  ColumnID, MetricDesc, ValidCount, Date_proc, run_id)
      SELECT @Tblname, @Colname , @n,   'DIFF Values', CAST( difcount as varchar) , getdate(), @run_id FROM #t1
    INSERT INTO WEATHER_DATA_METRICS   (TableName, ColumnName,  ColumnID, MetricDesc, ValidCount, Date_proc, run_id)
      SELECT @Tblname, @Colname , @n,   'MAX Num Values', CAST(MaxNum as varchar) , getdate(), @run_id FROM #t1
    INSERT INTO WEATHER_DATA_METRICS   (TableName, ColumnName,  ColumnID, MetricDesc, ValidCount, Date_proc, run_id)
      SELECT @Tblname, @Colname , @n,   'MAX Char Values', CAST(MaxChar as varchar) , getdate(), @run_id FROM #t1
    INSERT INTO WEATHER_DATA_METRICS   (TableName, ColumnName,  ColumnID, MetricDesc, ValidCount, Date_proc, run_id)
      SELECT @Tblname, @Colname , @n,   'MIN Num Values', CAST(MinNum as varchar) , getdate(), @run_id FROM #t1
    INSERT INTO WEATHER_DATA_METRICS   (TableName, ColumnName,  ColumnID, MetricDesc, ValidCount, Date_proc, run_id)
      SELECT @Tblname, @Colname , @n,   'MIN Char Values', CAST(MinChar as varchar) , getdate(), @run_id FROM #t1
      
    DROP TABLE #t1
    SET @n = @n + 1
  END
  SET @n = (SELECT count(*) FROM WEATHER_DATA_METRICS WHERE     run_id = @run_id 
      and MetricDesc in ( 'NON-NULL Values', 'DIFF Values', 'MAX Num Values', 'MAX Char Values', 'MIN Num Values', 'MIN Char Values'))
  
  INSERT INTO WEATHER_DATA_EVAL_LOG (MValue, Description , qty,  Date_proc)
    VALUES (1,' Metrics Inserted', @n, GETDATE())
END




IF @c4 = 1 -------------------- B01		-	Postal District Reference Table Creation
BEGIN 

CREATE TABLE WEATHER_POST_DISTRICT_REFERENCE
      ( ID INT IDENTITY,
	  postal_district 	VARCHAR(12),
	  postal_code		VARCHAR(12),
	  fixed_postcode	VARCHAR(12),
	  postal_town			VARCHAR(80),
	  postal_county			VARCHAR(80),
	  Experian_flag		bit DEFAULT 0,
	  VESPA_flag		bit DEFAULT 0,
	  Skybase_flag		bit DEFAULT 0,
	  CACI_flag			bit DEFAULT 0)

	INSERT INTO WEATHER_DATA_EVAL_LOG (MValue, Description , qty,  Date_proc)
    VALUES (1,'WEATHER_POST_DISTRICT_REFERENCE Created - Empty', 0, GETDATE())

END	  

IF @c5 = 1 -------------------- B02		-	Postal District Reference Table Creation
BEGIN 
  INSERT INTO WEATHER_POST_DISTRICT_REFERENCE (postal_district,postal_code,fixed_postcode,postal_town,postal_county)
	SELECT DISTINCT  
		cb_address_postcode_outcode district,
		cb_address_postcode         postcode,
		trim(replace(cb_address_postcode ,'  ', ' ')) fixed_postcode,  
		cb_address_town             town,
		cb_address_county           county		
	FROM sk_prod.EXPERIAN_CONSUMERVIEW
	WHERE cb_address_postcode_outcode IS NOT NULL
	AND cb_address_town	IS NOT NULL
	UNION
	SELECT DISTINCT
		cb_address_postcode_outcode	district,
		cb_address_postcode         postcode,
		trim(replace(cb_address_postcode ,'  ', ' ')) fixed_postcode,  
		cb_address_town             town,
		cb_address_county           county
	FROM  sk_prod.CUST_SINGLE_ACCOUNT_VIEW 
	WHERE cb_address_postcode_outcode IS NOT NULL
	AND cb_address_town	IS NOT NULL
	UNION
	SELECT DISTINCT  
		cb_address_postcode_outcode	district,
		cb_address_postcode         postcode,
		trim(replace(cb_address_postcode ,'  ', ' ')) fixed_postcode,  
		cb_address_town             town,
		cb_address_county           county
	FROM  sk_prod.CACI_SOCIAL_CLASS 
	WHERE cb_address_postcode_outcode IS NOT NULL	
	AND cb_address_town	IS NOT NULL
	
	UPDATE WEATHER_POST_DISTRICT_REFERENCE
	SET Experian_flag = 1 
	FROM WEATHER_POST_DISTRICT_REFERENCE AS a
	INNER JOIN sk_prod.EXPERIAN_CONSUMERVIEW AS b ON a.postal_code = b.cb_address_postcode AND a.postal_town = b.cb_address_town
	
	UPDATE WEATHER_POST_DISTRICT_REFERENCE
	SET Skybase_flag = 1 
	FROM WEATHER_POST_DISTRICT_REFERENCE AS a
	INNER JOIN sk_prod.CUST_SINGLE_ACCOUNT_VIEW AS b ON a.postal_code = b.cb_address_postcode AND a.postal_town = b.cb_address_town
	
	UPDATE WEATHER_POST_DISTRICT_REFERENCE
	SET CACI_flag = 1 
	FROM WEATHER_POST_DISTRICT_REFERENCE AS a
	INNER JOIN sk_prod.CACI_SOCIAL_CLASS AS b ON a.postal_code = b.cb_address_postcode AND a.postal_town = b.cb_address_town
	
	SET @n = (SELECT count(*) from WEATHER_POST_DISTRICT_REFERENCE)
	 
	INSERT INTO WEATHER_DATA_EVAL_LOG (MValue, Description , qty,  Date_proc)
    VALUES (1,'WEATHER_POST_DISTRICT_REFERENCE Populated', @n , GETDATE())

END
	

	
	
	
	
	
/*
SP_columns WEATHER_DATA
SP_columns WEATHER_DATA_HISTORY
SELECT top 20 * FROM WEATHER_DATA;
SELECT top 20 * FROM WEATHER_DATA_History
SELECT count(DISTINCT district)  FROM WEATHER_DATA;
SELECT count(DISTINCT district)  FROM WEATHER_DATA_History
*/


