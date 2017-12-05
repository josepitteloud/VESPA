DECLARE @run_id int
      , @sql2       VARCHAR(1000)
      , @Tablec     VARCHAR(100)
	  , @Owner		VARCHAR(100)
      , @ColID      INT
      , @ColumnN    VARCHAR(200)
      , @cont       INT  
      , @sql1     VARCHAR(1000)
      , @Table    VARCHAR(100)
      , @TableID  BIGINT   
      , @c13 bit


SET @run_id   = ISNULL ((SELECT max(mValue)  FROM Experian_November_log WHERE Description =  'Run ID'), 1)
SET @c13 = 1 --- Set to 0 To resume the loop / 1 if a new run
SET @cont =0 
-------------------------------------------------------------------------------------
--------------------Columns Contents-------------------------------------------------
-------------------------------------------------
IF @c13 = 1
BEGIN
    IF object_id('pitteloudj.Experian_November_Columns_temp') IS NOT NULL    DROP TABLE pitteloudj.Experian_November_Columns_temp
 WHERE cb_source_file like ''Sky_20131113_ConsumerView-PandH_live.dat.gz''
    CREATE TABLE Experian_November_Columns_temp
    	( ID              int IDENTITY
		  , fOwner		  VARCHAR (100)
    	  , TableName     VARCHAR (100)
          , TableID       int
    	  , ColumnName    VARCHAR (200)
    	  , proc_reg      bit default 0
    	  , run_id		  int
    	 )
    	 
    	INSERT INTO Experian_November_log (mValue, Description, Date_log)
    		VALUES ( @run_id , 'Experian_November_Columns_temp Table Created' 		, getdate())  



    INSERT INTO Experian_November_Columns_temp
    (fOwner, TableName, TableID, ColumnName, run_id,)
    SELECT 
		fOwner
        , TableName
        , TableID
        , ColumnName
    	, @run_id
    FROM Experian_Refresh_Columns_nov --Experian_Refresh_Tables 
    WHERE ColumnName not like '%filler%' AND ColumnName not like '%cb_%'
    AND ColumnName not like '%pixel%' AND ColumnName not like 'cb_%'
    
     
     


    INSERT INTO Experian_November_log (mValue, Description, Date_log)
    		VALUES ( @run_id , 'Experian_November_Columns_temp Values Inserted' 		, getdate())  
commit
END

--------------------CHECK Loop----------------------

WHILE EXISTS (SELECT top 1 *
                FROM Experian_November_Columns_temp 
                WHERE proc_reg = 0)
	BEGIN 
		  WHILE (@cont < 100)
			  BEGIN
				  SET @ColID = (SELECT top 1 ID
							  FROM Experian_November_Columns_temp 
							  WHERE proc_reg = 0)
				  SET @Owner = (SELECT top 1 fOwner FROM Experian_November_Columns_temp WHERE ID = @ColID)
				  SET @Tablec = (SELECT top 1 TableName FROM Experian_November_Columns_temp WHERE ID =  @ColID)
				  SET @ColumnN = (SELECT top 1 ColumnName FROM Experian_November_Columns_temp WHERE ID = @ColID)
				  
        EXECUTE DQ_November_Checks @run_id, @Owner, @Tablec, @ColID ,@ColumnN
				  
				UPDATE Experian_November_Columns_temp 
				SET proc_reg = 1 WHERE  ID = @ColID
			  SET @cont = @cont + 1
        
        END
		SET @cont =0 
    COMMIT 
     UPDATE Experian_November_Columns_Results
     SET Content_flag = 1 
     WHERE ColumnName like '%filler%' OR ColumnName like 'cb_%'
     
     
	END

INSERT INTO Experian_November_log (mValue, Description, Date_log)
		VALUES ( @run_id , 'Null Columns checked' 		, getdate()) 
------------------------------------------------------
------------------------------------------------------
		
		SELECT top 50000
  ,a.Delphi_8_Score_DFM8_AS_NC     AS a_Delphi_8_Score_DFM8_AS_NC
,a.h_affluence_v2     AS a_h_affluence_v2
,a.h_household_composition     AS a_h_household_composition
,a.h_income_band     AS a_h_income_band
,a.h_lifestage     AS a_h_lifestage
,a.h_property_type_v2     AS a_h_property_type_v2
,a.h_tenure_v2     AS a_h_tenure_v2
,a.p_age_fine     AS a_p_age_fine
,a.p_length_of_residency     AS a_p_length_of_residency
,a.p_marital_status     AS a_p_marital_status
,a.p_true_touch_type     AS a_p_true_touch_type
,a.p_prospectable_flag     AS a_p_prospectable_flag
,a.h_income_band_v2     AS a_h_income_band_v2
,a.h_mosaic_uk_group     AS a_h_mosaic_uk_group
,a.h_mosaic_uk_second_best_type     AS a_h_mosaic_uk_second_best_type
,a.h_mosaic_uk_segment     AS a_h_mosaic_uk_segment
,a.h_mosaic_uk_segment_alternative     AS a_h_mosaic_uk_segment_alternative
,a.h_mosaic_uk_type--Mosaicsegmentation     AS a_h_mosaic_uk_type--Mosaicsegmentation
,a.h_fss_group     AS a_h_fss_group
,a.h_fss_type     AS a_h_fss_type
,a.h_fss_v3_group     AS a_h_fss_v3_group
,a.h_fss_v3_type--FSSsegmentation     AS a_h_fss_v3_type--FSSsegmentation
,a.cb_key_household     AS a_cb_key_household
,b.cb_key_household     AS b_cb_key_household
,a.cb_key_individual     AS a_cb_key_individual
,b.cb_key_individual     AS b_cb_key_individual
, a.cb_key_db_person      AS a_.cb_key_db_person 
, b.cb_key_db_person     AS b_.cb_key_db_person
,b.Delphi_8_Score_DFM8_AS_NC     AS b_Delphi_8_Score_DFM8_AS_NC
,b.h_affluence_v2     AS b_h_affluence_v2
,b.h_household_composition     AS b_h_household_composition
,b.h_income_band     AS b_h_income_band
,b.h_lifestage     AS b_h_lifestage
,b.h_property_type_v2     AS b_h_property_type_v2
,b.h_tenure_v2     AS b_h_tenure_v2
,b.p_age_fine     AS b_p_age_fine
,b.p_length_of_residency     AS b_p_length_of_residency
,b.p_marital_status     AS b_p_marital_status
,b.p_true_touch_type     AS b_p_true_touch_type
,b.p_prospectable_flag     AS b_p_prospectable_flag
,b.h_income_band_v2     AS b_h_income_band_v2
,b.h_mosaic_uk_group     AS b_h_mosaic_uk_group
,b.h_mosaic_uk_second_best_type     AS b_h_mosaic_uk_second_best_type
,b.h_mosaic_uk_segment     AS b_h_mosaic_uk_segment
,b.h_mosaic_uk_segment_alternative     AS b_h_mosaic_uk_segment_alternative
,b.h_mosaic_uk_type     AS b_h_mosaic_uk_type
,b.h_fss_group     AS b_h_fss_group
,b.h_fss_type     AS b_h_fss_type
,b.h_fss_v3_group     AS b_h_fss_v3_group
,b.h_fss_v3_type     AS b_h_fss_v3_type
INTO #TEMP
FROM sk_prod.EXPERIAN_CONSUMERVIEW as a
JOIN sk_prodreg.EXPERIAN_CONSUMERVIEW as b ON a.cb_key_household = b.cb_key_household AND a.cb_key_individual = b.cb_key_individual AND a.cb_key_db_person = b.cb_key_db_person




SELECT TOP 50000
	a.cb_data_date adate
	,b.cb_data_date bdate
,  H_number_of_children_in_household_2011 

FROM sk_prod.EXPERIAN_CONSUMERVIEW as a
JOIN sk_prodreg.EXPERIAN_CONSUMERVIEW as b ON a.cb_key_household = b.cb_key_household AND a.cb_key_individual = b.cb_key_individual AND a.cb_key_db_person = b.cb_key_db_person



SELECT 
H_family_lifestage_v2
COUNT( 
FROM sk_prod.EXPERIAN_CONSUMERVIEW as a 

SELECT      'sk_prod'	,'EXPERIAN_CONSUMERVIEW'    , 'H_family_lifestage_v2' ,count(H_family_lifestage_v2) hits    ,1    ,0    ,CASE WHEN hits = 0 THEN 1 ELSE 0 END ,0    ,0    ,getdate()    ,@run_id	, H_family_lifestage_v2 FROM sk_prod.EXPERIAN_CONSUMERVIEW GROUP BY H_family_lifestage_v2

SELECT 'skprod.' , 'EXPERIAN_CONSUMERVIEW', 'H_family_lifestage_v2', count(H_family_lifestage_v2) hits, 1, 0, CASEWHEN hits =0 THEN 1 ELSE 0 END, 0, 0, getdate(), 3, H_family_lifestage_v2 FROM skprod.EXPERIAN_CONSUMERVIEW GROUP BY H_family_lifestage_v2
SELECT 'skprod.' , 'EXPERIAN_CONSUMERVIEW', 'h_mosaic_uk_2009_group', count(h_mosaic_uk_2009_group) hits, 1, 0, CASEWHEN hits =0 THEN 1 ELSE 0 END, 0, 0, getdate(), 3, h_mosaic_uk_2009_group FROM skprod.EXPERIAN_CONSUMERVIEW GROUP BY h_mosaic_uk_2009_group
SELECT 'skprod.' , 'EXPERIAN_CONSUMERVIEW', 'h_mosaic_uk_2009_type', count(h_mosaic_uk_2009_type) hits, 1, 0, CASEWHEN hits =0 THEN 1 ELSE 0 END, 0, 0, getdate(), 3, h_mosaic_uk_2009_type FROM skprod.EXPERIAN_CONSUMERVIEW GROUP BY h_mosaic_uk_2009_type
SELECT 'skprod.' , 'EXPERIAN_CONSUMERVIEW', 'h_affluence', count(h_affluence) hits, 1, 0, CASEWHEN hits =0 THEN 1 ELSE 0 END, 0, 0, getdate(), 3, h_affluence FROM skprod.EXPERIAN_CONSUMERVIEW GROUP BY h_affluence
SELECT 'skprod.' , 'EXPERIAN_CONSUMERVIEW', 'Delphi_8_Score_DFM8_AS_NC', count(Delphi_8_Score_DFM8_AS_NC) hits, 1, 0, CASEWHEN hits =0 THEN 1 ELSE 0 END, 0, 0, getdate(), 3, Delphi_8_Score_DFM8_AS_NC FROM skprod.EXPERIAN_CONSUMERVIEW GROUP BY Delphi_8_Score_DFM8_AS_NC
SELECT 'skprod.' , 'EXPERIAN_CONSUMERVIEW', 'p_actual_age', count(p_actual_age) hits, 1, 0, CASEWHEN hits =0 THEN 1 ELSE 0 END, 0, 0, getdate(), 3, p_actual_age FROM skprod.EXPERIAN_CONSUMERVIEW GROUP BY p_actual_age
SELECT 'skprod.' , 'EXPERIAN_CONSUMERVIEW', 'p_age_course', count(p_age_course) hits, 1, 0, CASEWHEN hits =0 THEN 1 ELSE 0 END, 0, 0, getdate(), 3, p_age_course FROM skprod.EXPERIAN_CONSUMERVIEW GROUP BY p_age_course
SELECT 'skprodreg.' , 'EXPERIAN_CONSUMERVIEW', 'H_family_lifestage_v2', count(H_family_lifestage_v2) hits, 1, 0, CASEWHEN hits =0 THEN 1 ELSE 0 END, 0, 0, getdate(), 3, H_family_lifestage_v2 FROM skprodreg.EXPERIAN_CONSUMERVIEW GROUP BY H_family_lifestage_v2
SELECT 'skprodreg.' , 'EXPERIAN_CONSUMERVIEW', 'h_mosaic_uk_2009_group', count(h_mosaic_uk_2009_group) hits, 1, 0, CASEWHEN hits =0 THEN 1 ELSE 0 END, 0, 0, getdate(), 3, h_mosaic_uk_2009_group FROM skprodreg.EXPERIAN_CONSUMERVIEW GROUP BY h_mosaic_uk_2009_group
SELECT 'skprodreg.' , 'EXPERIAN_CONSUMERVIEW', 'h_mosaic_uk_2009_type', count(h_mosaic_uk_2009_type) hits, 1, 0, CASEWHEN hits =0 THEN 1 ELSE 0 END, 0, 0, getdate(), 3, h_mosaic_uk_2009_type FROM skprodreg.EXPERIAN_CONSUMERVIEW GROUP BY h_mosaic_uk_2009_type
SELECT 'skprodreg.' , 'EXPERIAN_CONSUMERVIEW', 'h_affluence', count(h_affluence) hits, 1, 0, CASEWHEN hits =0 THEN 1 ELSE 0 END, 0, 0, getdate(), 3, h_affluence FROM skprodreg.EXPERIAN_CONSUMERVIEW GROUP BY h_affluence
SELECT 'skprodreg.' , 'EXPERIAN_CONSUMERVIEW', 'Delphi_8_Score_DFM8_AS_NC', count(Delphi_8_Score_DFM8_AS_NC) hits, 1, 0, CASEWHEN hits =0 THEN 1 ELSE 0 END, 0, 0, getdate(), 3, Delphi_8_Score_DFM8_AS_NC FROM skprodreg.EXPERIAN_CONSUMERVIEW GROUP BY Delphi_8_Score_DFM8_AS_NC
SELECT 'skprodreg.' , 'EXPERIAN_CONSUMERVIEW', 'p_actual_age', count(p_actual_age) hits, 1, 0, CASEWHEN hits =0 THEN 1 ELSE 0 END, 0, 0, getdate(), 3, p_actual_age FROM skprodreg.EXPERIAN_CONSUMERVIEW GROUP BY p_actual_age
SELECT 'skprodreg.' , 'EXPERIAN_CONSUMERVIEW', 'p_age_course', count(p_age_course) hits, 1, 0, CASEWHEN hits =0 THEN 1 ELSE 0 END, 0, 0, getdate(), 3, p_age_course FROM skprodreg.EXPERIAN_CONSUMERVIEW GROUP BY p_age_course




h_mosaic_uk_2009_group
h_mosaic_uk_2009_type
h_affluence
Delphi_8_Score_DFM8_AS_NC
p_actual_age
p_age_course

