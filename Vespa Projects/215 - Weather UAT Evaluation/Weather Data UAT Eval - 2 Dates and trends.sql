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
**	Section A - 
**		A01	- CHECKING rows distribution by month
**		A02 - Creating Daily Aggregates By District 	
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
    , @run_id int
    
SET @c0=1
SET @c1=1
SET @n=1
SET @run_id = (SELECT MAX(Mvalue) FROM WEATHER_DATA_EVAL_LOG WHERE Description like 'Run ID')


IF @c0 = 1 ------------------A01		-		CHECKING rows distribution by month
BEGIN
SELECT DISTINCT
  CONVERT(VARCHAR, DATEfloor (mm,date_time),103) Month_1,
  @run_id, 
  COUNT(*) TOTAL
  INTO WEATHER_DATA_Dist_by_month
  FROM  sk_uat.WEATHER_DATA_history
  GROUP BY Month_1
  
	INSERT INTO WEATHER_DATA_EVAL_LOG (MValue, Description ,Qty, Date_proc)
	VALUES (1,' WEATHER_DATA_Dist_by_month Table Created and populated',@@rowcount, GETDATE())
  
END

  IF @c1 = 1 ------------------A02		-		Creating Daily Aggregates By District 	
BEGIN
	SELECT 
	  DATEfloor (dd,date_time) Month_1,
	  MIN(minimum_temperature_daily) min_temp,
	  MAX(maximum_temperature_daily) max_temp,
	  sunshine_duration
	  District,
	  @run_id,
	  SUM(rainfall) rain
	INTO WEATHER_DATA_DAILY_BY_DISTRICT
	FROM  sk_uat.WEATHER_DATA_history
	GROUP BY Month_1
		, District
		, sunshine_duration
	
	INSERT INTO WEATHER_DATA_EVAL_LOG (MValue, Description ,Qty, Date_proc)
	VALUES (1,' WEATHER_DATA_DAILY_BY_DISTRICT Table Created and populated',@@rowcount, GETDATE())
    
END  
      



