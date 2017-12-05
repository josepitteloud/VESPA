/******************************************************************************
**
**  Project Vespa: PROJECT  V186 - Callcredit Insurance Data Eval 3
**  	INSURANCE Data Match Rates Report
**
**  Intended check the match rates between CalCredit Insurance Renewal Data 
**	and Skybase, VESPA panel and EXPERIAN Consumerview data
**  as part of the Analytic Brief - Analytical Task 4
**
**	Related Documents:
**		- VESPA_INSURANCE DATA EVALUATION 1.sql
**		- VESPA_INSURANCE DATA EVALUATION 2.sql
**		- VESPA_INSURANCE DATA EVALUATION 1.sql
**
**	Code Sections:
**		A01	-	CONTROL PANEL & SETUP
**	Section	B	-	SKY BASE Matching rates
**		B01	-	TOTAL Sky Accounts Metric
**		B02	-	Get Totals from Insurance Data
**		B03	-	Get Sky Match Rate at Individual Level
**		B04	-	Get Sky Match Rate at Household Level
**		B05	-	Get Sky Match Rate at Postal Code  Level vs consumerview_postcode
**	Section	C	-	VESPA Matching rates
**		C01	-	Total VESPA Accounts
**		C02	-	Get VESPA Match Rate at Individual Level
**		C03	-	Get VESPA Match Rate at Household Level
**		C04	-	Get VESPA Match Rate at Postal Code  Level vs consumerview_postcode
**	Section	D	-	EXPERIAN matches
**		D01	-	Total EXPERIAN CV Accounts
**		D02	-	Get VESPA Match Rate at Individual Level
**		D03	-	Get VESPA Match Rate at Household Level
**		D04	-	Get VESPA Match Rate at Postal Code  Level vs consumerview_postcode
**
**	Written by Jose Pitteloud
******************************************************************************/


DECLARE @run_id int
      , @sql2       VARCHAR(1000)
      , @Tablec     VARCHAR(100)
      , @ColID      INT
      , @ColumnN    VARCHAR(200)
      , @cont       INT  
      , @sql1     VARCHAR(1000)
      , @Table    VARCHAR(100)
      , @TableID  BIGINT      
      , @c5 bit
      , @c6 bit
      , @c7 bit
      , @c8 bit
      , @c9 bit
      , @c10 bit
      , @c11 bit

	  
------------- A01	-	CONTROL PANEL & SETUP
SET @c5 = 1 --Totals
SET @c6 = 1 --Sky Base Match
SET @c10 = 1 --VESPA Match
SET @c11 = 1 --EXPERIAN Match

SET @run_id   = ISNULL ((SELECT max(mValue)  FROM VESPA_INSURANCE_EVAL_LOG WHERE Description =  'Run ID'), 1)

IF object_id('pitteloudj.VESPA_INSURANCE_MATCHES') is null 
BEGIN
CREATE TABLE  VESPA_INSURANCE_MATCHES
  (MetricID int IDENTITY
  , MetricDescription varchar(80)
  , MetricValue float
  , Recordate Datetime
  , run_id int
  )
  END
  
--------------------------------------------------------------------------------------
---------------------------------------------B 	-	SKY BASE Matching rates
IF @c5 = 1
BEGIN 
    ----------------------------B01 	-	TOTAL Sky Accounts Metric
    INSERT INTO VESPA_INSURANCE_MATCHES (MetricDescription, MetricValue, Recordate, run_id)
  SELECT 'Total SkyBase Accounts' 
   , count(DISTINCT account_number)
   , getdate()
   , @run_id
  FROM skybase 
   
  ------------------------------B02		-	Get Totals from Insurance Data
  INSERT INTO VESPA_INSURANCE_MATCHES (MetricDescription, MetricValue, Recordate, run_id)
  SELECT 'Total Insurance Data individual keys' 
     , count(DISTINCT cb_key_individual)
     , getdate()
     , @run_id run_id
  FROM sk_prod.VESPA_INSURANCE_DATA
  
  INSERT INTO VESPA_INSURANCE_MATCHES (MetricDescription, MetricValue, Recordate, run_id)
  SELECT 'Total Insurance Data HouseHold' 
     , count(DISTINCT cb_key_household)
     , getdate()
     , @run_id 
  FROM sk_prod.VESPA_INSURANCE_DATA 
  
  INSERT INTO VESPA_INSURANCE_MATCHES (MetricDescription, MetricValue, Recordate, run_id)
  SELECT 'Total Insurance Data Postcodes' 
     , count(DISTINCT cb_address_postcode)
     , getdate()
     , @run_id
  FROM  sk_prod.VESPA_INSURANCE_DATA 
  
  INSERT INTO VESPA_INSURANCE_EVAL_LOG (MValue, Description , Date_proc)
		VALUES ( 1 , 'Insurance Data Total Values Done' 		, getdate()) 
END
-------------------------------B03		-	Get Sky Match Rate at Individual Level
IF @c6 = 1
BEGIN 
  INSERT INTO VESPA_INSURANCE_MATCHES (MetricDescription, MetricValue, Recordate, run_id)
  SELECT 'Match Rate Individual Level - SkyBase vsc' 
     , count(DISTINCT cb_key_individual)
     , getdate()
     , @run_id
  FROM skybase AS s
    INNER JOIN sk_prod.VESPA_INSURANCE_DATA  AS e ON e.cb_key_individual = s.individual_key
    WHERE cb_key_individual is not null                                    -- Checking for Empty rows

  -------------------------------B04	-	Get Sky Match Rate at Household Level
  INSERT INTO VESPA_INSURANCE_MATCHES (MetricDescription, MetricValue, Recordate, run_id)
  SELECT 'Match Rate HouseHold Level - SkyBase vs cb_key_individual' 
     , count(DISTINCT household_key)
     , getdate()
     , @run_id
  FROM skybase AS s
    INNER JOIN sk_prod.VESPA_INSURANCE_DATA  AS e ON e.cb_key_household = s.household_key
    WHERE cb_key_household is not null

  ------------------------------B05		-	Get Sky Match Rate at Postal Code  Level vs consumerview_postcode
  INSERT INTO VESPA_INSURANCE_MATCHES (MetricDescription, MetricValue, Recordate, run_id)
  SELECT 'Total SkyBase Postcode' 
      , COUNT(*)
      , GETDATE()       
  	, @run_id
  FROM Experian_Refresh_Sky_postcode

  INSERT INTO VESPA_INSURANCE_MATCHES (MetricDescription, MetricValue, Recordate, run_id)
  SELECT 'Match Rate Postcode Level - SkyBase vs consumerview_postcode' 
      , COUNT(DISTINCT sky.postcode)
      , GETDATE()       
  	, @run_id
  FROM Experian_Refresh_Sky_postcode AS sky 
  INNER JOIN sk_prod.VESPA_INSURANCE_DATA  AS pc 
              ON sky.postcode = replace(cb_address_postcode,' ', '')
  WHERE pc.cb_address_postcode  IS NOT NULL

    INSERT INTO VESPA_INSURANCE_EVAL_LOG (mValue, Description, Date_proc)
		VALUES ( 1 , 'Insurance Data vs Sky Matching rates Done' 		, getdate()) 
END

-----------------------------------------------------------------------------
--------------------------------C		-	VESPA matches
------------------------------C01		-	Total VESPA Accounts
  IF @c10 = 1
  BEGIN
	INSERT INTO VESPA_INSURANCE_MATCHES (MetricDescription, MetricValue, Recordate, run_id)
	SELECT 'Total VESPA Accounts' 
   , COUNT(DISTINCT account_number)
   , getdate()
   , @run_id
  FROM vespa

  ------------------------------C02	-	Get VESPA Match Rate at Individual Level
  INSERT INTO VESPA_INSURANCE_MATCHES (MetricDescription, MetricValue, Recordate, run_id)
  SELECT 'Match Rate Individual Level - VESPA vs Insurance Data ' 
     , count(DISTINCT cb_key_individual)
     , getdate()
     , @run_id
  FROM vespa AS s
    INNER JOIN sk_prod.VESPA_INSURANCE_DATA AS e ON e.cb_key_individual = s.individual_key
    WHERE cb_key_individual is not null                                 -- Checking for Empty rows
	
  ------------------------------C03		-	Get VESPA Match Rate at Household Level
  INSERT INTO VESPA_INSURANCE_MATCHES (MetricDescription, MetricValue, Recordate, run_id)
  SELECT 'Match Rate HouseHold Level - VESPA vs Insurance Data ' 
     , count(DISTINCT s.household_key)
     , getdate()
     , @run_id
  FROM vespa AS s
    INNER JOIN sk_prod.VESPA_INSURANCE_DATA AS e ON e.cb_key_household = s.household_key
    WHERE  e.cb_key_household is not null

  ------------------------------C04		-	Get VESPA Match Rate at Postal Code  Level vs consumerview_postcode
  INSERT INTO VESPA_INSURANCE_MATCHES (MetricDescription, MetricValue, Recordate, run_id)
  SELECT 'Total VESPA Postcode' 
      , COUNT(*)
      , GETDATE()       
  	, @run_id
  FROM Experian_Refresh_VESPA_postcode

  INSERT INTO VESPA_INSURANCE_MATCHES (MetricDescription, MetricValue, Recordate, run_id)
  SELECT 'Match Rate Postcode Level - VESPA vs Insurance Data' 
      , COUNT(DISTINCT sky.postcode)
      , GETDATE()       
  	, @run_id
  FROM Experian_Refresh_VESPA_postcode AS sky 
  INNER JOIN sk_prod.VESPA_INSURANCE_DATA AS pc 
              ON sky.postcode = cb_address_postcode
  WHERE pc.cb_address_postcode  IS NOT NULL

	
    INSERT INTO VESPA_INSURANCE_EVAL_LOG (mValue, Description, Date_proc)
		VALUES ( 1 , 'VESPA Matching rates Done' 		, getdate()) 
END

------------------------------------------------------------
--------------------------------D	-	EXPERIAN matches--------------------------------
------------------------------D01		-	Total EXPERIAN CV Accounts
  IF @c11 = 1
  BEGIN
	INSERT INTO VESPA_INSURANCE_MATCHES (MetricDescription, MetricValue, Recordate, run_id)
	SELECT 'Total EXPERIAN CV Individual Keys' 
   , COUNT(DISTINCT cb_key_individual)
   , getdate()
   ,1-- @run_id
  FROM exp_cv_data
	INSERT INTO VESPA_INSURANCE_MATCHES (MetricDescription, MetricValue, Recordate, run_id)
	SELECT 'Total EXPERIAN CV HH Keys' 
   , COUNT(DISTINCT cb_key_household)
   , getdate()
   ,1 --@run_id
  FROM exp_cv_data
  ------------------------------D02		-	Get VESPA Match Rate at Individual Level
  INSERT INTO VESPA_INSURANCE_MATCHES (MetricDescription, MetricValue, Recordate, run_id)
  SELECT 'Match Rate Individual Level -  Experian_consumerview Vs Insurance Data' 
     , count(DISTINCT s.cb_key_individual)
     , getdate()
     , @run_id
  FROM sk_prod.VESPA_INSURANCE_DATA AS s
    INNER JOIN exp_cv_data AS e ON e.cb_key_individual = s.cb_key_individual
    WHERE  s.cb_key_individual is not null                                 -- Checking for Empty rows
	
  ------------------------------D03		-	Get VESPA Match Rate at Household Level
  INSERT INTO VESPA_INSURANCE_MATCHES (MetricDescription, MetricValue, Recordate, run_id)
  SELECT 'Match Rate HouseHold Level -  Experian_consumerview Vs Insurance Data' 
     , count(DISTINCT s.cb_key_household)
     , getdate()
     , @run_id
  FROM sk_prod.VESPA_INSURANCE_DATA AS s
    INNER JOIN exp_cv_data AS e ON e.cb_key_household = s.cb_key_household
    WHERE s.cb_key_household is not null

  ------------------------------D04		-	Get VESPA Match Rate at Postal Code  Level vs consumerview_postcode
  INSERT INTO VESPA_INSURANCE_MATCHES (MetricDescription, MetricValue, Recordate, run_id)
  SELECT 'Match Rate Postcode Level - Consumerview_postcode Vs Insurance Data' 
      , COUNT(DISTINCT sky.cb_address_postcode)
      , GETDATE()       
  	, @run_id
  FROM sk_prod.VESPA_INSURANCE_DATA  AS sky 
  INNER JOIN sk_prod.consumerview_postcode AS pc 
              ON sky.cb_address_postcode = pc.cb_address_postcode
  WHERE pc.cb_address_postcode  IS NOT NULL

   INSERT INTO VESPA_INSURANCE_EVAL_LOG (mValue, Description, Date_proc)
		VALUES ( 1 , 'EXPERIAN Matching rates Done' 		, getdate()) 
END















 
 
 
  SELECT 'Match Rate Individual Level - SkyBase vsc' 
      , motor_renewal_month_code
      , home_renewal_month_code
     , count(DISTINCT cb_key_individual)
FROM skybase AS s
    INNER JOIN sk_prod.VESPA_INSURANCE_DATA  AS e ON e.cb_key_individual = s.individual_key
    WHERE cb_key_individual is not null  
GROUP BY      
    motor_renewal_month_code
    , home_renewal_month_code
UNION
-------------------------------B04	-	Get Sky Match Rate at Household Level
  
  SELECT 'Match Rate HouseHold Level - SkyBase vs cb_key_individual' 
      , motor_renewal_month_code
      , home_renewal_month_code
     , count(DISTINCT household_key)

FROM skybase AS s
    INNER JOIN sk_prod.VESPA_INSURANCE_DATA  AS e ON e.cb_key_household = s.household_key
    WHERE cb_key_household is not null
GROUP BY      
      motor_renewal_month_code
      , home_renewal_month_code
UNION 
 SELECT 'Match Rate Individual Level -  Experian_consumerview Vs Insurance Data - by groups' 
      , motor_renewal_month_code
      , home_renewal_month_code
      , count(DISTINCT s.cb_key_individual)
  FROM sk_prod.VESPA_INSURANCE_DATA AS s
    INNER JOIN exp_cv_data AS e ON e.cb_key_individual = s.cb_key_individual
    WHERE  s.cb_key_individual is not null                                 
GROUP BY      
      motor_renewal_month_code
      , home_renewal_month_code
 UNION   
 SELECT 'Match Rate Household Level -  Experian_consumerview Vs Insurance Data - by groups' 
      , motor_renewal_month_code
      , home_renewal_month_code
      , count(DISTINCT s.cb_key_household)
  FROM sk_prod.VESPA_INSURANCE_DATA AS s
    INNER JOIN exp_cv_data AS e ON e.cb_key_household = s.cb_key_household
    WHERE  s.cb_key_household is not null                                 
GROUP BY      
      motor_renewal_month_code
      , home_renewal_month_code
 UNION	
 ---------------------VESPA 
  SELECT 'Match Rate Individual Level - VESPA vs Insurance Data - by groups' 
     , motor_renewal_month_code
     , home_renewal_month_code
     , count(DISTINCT cb_key_individual)
  FROM vespa AS s
    INNER JOIN sk_prod.VESPA_INSURANCE_DATA AS e ON e.cb_key_individual = s.individual_key
    WHERE cb_key_individual is not null                                 -- Checking for Empty rows
GROUP BY      
      motor_renewal_month_code
      , home_renewal_month_code
	UNION
  ------------------------------C03		-	Get VESPA Match Rate at Household Level
  
  SELECT 'Match Rate HouseHold Level - VESPA vs Insurance Data - by groups' 
    , motor_renewal_month_code
    , home_renewal_month_code
    , count(DISTINCT s.household_key)
  FROM vespa AS s
    INNER JOIN sk_prod.VESPA_INSURANCE_DATA AS e ON e.cb_key_household = s.household_key
    WHERE  e.cb_key_household is not null
  GROUP BY      
      motor_renewal_month_code
      , home_renewal_month_code
      
      
      
      
SELECT 'Monthly Distribution by groups' 
     , motor_renewal_month_code
     , home_renewal_month_code
     , count(DISTINCT cb_key_individual)
  FROM sk_prod.VESPA_INSURANCE_DATA 
    WHERE cb_key_individual is not null                                 
GROUP BY      
      motor_renewal_month_code
      , home_renewal_month_code
