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

SET @c5 = 1 --EXPERIAN_CONSUMERVIEW Totals
SET @c6 = 1 --Experian Match Rates

SET @run_id   = ISNULL ((SELECT max(mValue)  FROM Experian_November_log WHERE Description =  'Run ID'), 1)

IF object_id('pitteloudj.Experian_November_Results') is null 
BEGIN
CREATE TABLE Experian_November_Results
  (MetricID int IDENTITY
  , MetricDescription varchar(80)
  , MetricValue float
  , Recordate Datetime
  , run_id int
  )
  END
--------------------------------------------------------------------------------------
--------------------------------------------------EXPERIAN CONSUMERVIEW Matching rates
--------------Extracting EXPERIAN_CONSUMERVIEW Data
IF @c5 = 1
BEGIN 
    ----------------------------TOTAL Sky Accounts Metric
    INSERT INTO Experian_November_Results (MetricDescription, MetricValue, Recordate, run_id)
  SELECT 'Total SkyBase Accounts' 
   , count(DISTINCT account_number)
   , getdate()
   , @run_id
  FROM sk_prod.cust_subs_hist
  WHERE subscription_sub_type IN ('DTV Primary Viewing')
     AND status_code IN ('AC','AB','PC')
     AND effective_from_dt <= '20131201'
     AND effective_to_dt > '20131201'
     AND EFFECTIVE_FROM_DT IS NOT NULL
     AND cb_key_household > 0             --UK Only
     AND cb_key_household IS NOT NULL
     AND account_number IS NOT NULL
     AND service_instance_id IS NOT NULL 
   
  ----------------------------------------Get Total Rows in Experian Consumerview
  INSERT INTO Experian_November_Results (MetricDescription, MetricValue, Recordate, run_id)
  SELECT 'Total Experian ConsumerView individual' 
     , count(DISTINCT cb_key_individual)
     , getdate()
     , @run_id run_id
  FROM sk_prodreg.EXPERIAN_CONSUMERVIEW
  INSERT INTO Experian_November_Results (MetricDescription, MetricValue, Recordate, run_id)
  SELECT 'Total Experian ConsumerView HouseHold' 
     , count(DISTINCT cb_key_household)
     , getdate()
     , @run_id 
  FROM sk_prodreg.EXPERIAN_CONSUMERVIEW 
  INSERT INTO Experian_November_Results (MetricDescription, MetricValue, Recordate, run_id)
  SELECT 'Total Experian ConsumerView Postcodes' 
     , count(DISTINCT cb_address_postcode)
     , getdate()
     , @run_id
  FROM sk_prodreg.EXPERIAN_CONSUMERVIEW
  
  INSERT INTO Experian_November_log (mValue, Description, Date_log)
		VALUES (  @run_id  , 'EXPERIAN CONSUMERVIEW Total Values Done' 		, getdate()) 
commit
END

----------------------------------------Get Sky Match Rate at Individual Level
IF @c6 = 1
BEGIN 
  INSERT INTO Experian_November_Results (MetricDescription, MetricValue, Recordate, run_id)
  SELECT 'Match Rate Individual Level - SkyBase vs experian_consumerview' 
     , count(DISTINCT e.cb_key_individual)
     , getdate()
     , @run_id
  FROM sk_prod.cust_subs_hist AS s
  INNER JOIN sk_prodreg.EXPERIAN_CONSUMERVIEW AS e ON e.cb_key_individual = s.cb_key_individual 
  WHERE subscription_sub_type IN ('DTV Primary Viewing')
     AND s.status_code IN ('AC','AB','PC')
     AND s.effective_from_dt <= '20131201'
     AND s.effective_to_dt > '20131201'
     AND s.EFFECTIVE_FROM_DT IS NOT NULL
     AND s.cb_key_household > 0             --UK Only
     AND s.cb_key_household IS NOT NULL
     AND s.account_number IS NOT NULL
     AND s.service_instance_id IS NOT NULL  
	 AND e.h_fss_v3_group is not null                                    -- Checking for Empty rows

  ----------------------------------------Get Sky Match Rate at Household Level
  INSERT INTO Experian_November_Results (MetricDescription, MetricValue, Recordate, run_id)
  SELECT 'Match Rate HouseHold Level - SkyBase vs experian_consumerview' 
     , count(DISTINCT s.cb_key_household)
     , getdate()
     , @run_id
 FROM sk_prod.cust_subs_hist AS s
  INNER JOIN sk_prodreg.EXPERIAN_CONSUMERVIEW AS e ON e.cb_key_household = s.cb_key_household
  WHERE subscription_sub_type IN ('DTV Primary Viewing')
     AND s.status_code IN ('AC','AB','PC')
     AND s.effective_from_dt <= '20131201'
     AND s.effective_to_dt > '20131201'
     AND s.EFFECTIVE_FROM_DT IS NOT NULL
     AND s.cb_key_household > 0             --UK Only
     AND s.cb_key_household IS NOT NULL
     AND s.account_number IS NOT NULL
     AND s.service_instance_id IS NOT NULL  
	 AND e.h_fss_v3_group is not null     
  
END

