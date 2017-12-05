SELECT 
      'TOTAL EXPERIAN' tab     
      , affluence = CASE WHEN h_affluence_v2 in ('00','01','02')   THEN 'Very Low'
                                      WHEN h_affluence_v2 in ('03','04','05')   THEN 'Low'
                                      WHEN h_affluence_v2 in ('06','07','08')   THEN 'Mid Low'
                                      WHEN h_affluence_v2 in ('09','10','11')   THEN 'Mid'
                                      WHEN h_affluence_v2 in ('12','13','14')   THEN 'Mid High'
                                      WHEN h_affluence_v2 in ('15','16','17')   THEN 'High'
                                      WHEN h_affluence_v2 in ('18','19')        THEN 'Very High'
                                      ELSE                                              'Unknown'
                                 END
      , Lifestage = CASE              WHEN h_lifestage = '00' THEN 'Young singles/homesharers'
                                      WHEN h_lifestage  = '01' THEN 'Young family no children <18'
                                      WHEN h_lifestage  = '02' THEN 'Young family with children <18'
                                      WHEN h_lifestage  = '03' THEN 'Young household with children <18'
                                      WHEN h_lifestage  = '04' THEN 'Mature singles/homesharers'
                                      WHEN h_lifestage  = '05' THEN 'Mature family no children <18'
                                      WHEN h_lifestage  = '06' THEN 'Mature family with children <18'
                                      WHEN h_lifestage  = '07' THEN 'Mature household with children <18'
                                      WHEN h_lifestage  = '08' THEN 'Older single'
                                      WHEN h_lifestage  = '09' THEN 'Older family no children <18'
                                      WHEN h_lifestage  = '10' THEN 'Older family/household with children <18'
                                      WHEN h_lifestage  = '11' THEN 'Elderly single'
                                      WHEN h_lifestage  = '12' THEN 'Elderly family no children <18'
                                      ELSE 'Unknown' END
      , h_income_band_v2  
      , case h_household_composition
                                when '00' then 'Families'
                                when '01' then 'Extended family'
                                when '02' then 'Extended household'
                                when '03' then 'Pseudo family'
                                when '04' then 'Single male'
                                when '05' then 'Single female'
                                when '06' then 'Male homesharers'
                                when '07' then 'Female homesharers'
                                when '08' then 'Mixed homesharers'
                                when '09' then 'Abbreviated male families'
                                when '10' then 'Abbreviated female families'
                                when '11' then 'Multi-occupancy dwelling'
                                when 'U' then 'Unclassified'
                                else null
                                end       'Household Composition'   
      , count(DISTINCT exp_cb_key_household) total 
FROM sk_prod.EXPERIAN_CONSUMERVIEW
GROUP BY       
     affluence
      , Lifestage
      , h_income_band_v2  
      , [Household Composition]   
UNION
SELECT 
      'TOTAL Sky Base' tab
     , affluence = CASE WHEN h_affluence_v2 in ('00','01','02')   THEN 'Very Low'
                                      WHEN h_affluence_v2 in ('03','04','05')   THEN 'Low'
                                      WHEN h_affluence_v2 in ('06','07','08')   THEN 'Mid Low'
                                      WHEN h_affluence_v2 in ('09','10','11')   THEN 'Mid'
                                      WHEN h_affluence_v2 in ('12','13','14')   THEN 'Mid High'
                                      WHEN h_affluence_v2 in ('15','16','17')   THEN 'High'
                                      WHEN h_affluence_v2 in ('18','19')        THEN 'Very High'
                                      ELSE                                              'Unknown'
                                 END
    , Lifestage = CASE                WHEN h_lifestage  = '00' THEN 'Young singles/homesharers'
                                      WHEN h_lifestage  = '01' THEN 'Young family no children <18'
                                      WHEN h_lifestage  = '02' THEN 'Young family with children <18'
                                      WHEN h_lifestage  = '03' THEN 'Young household with children <18'
                                      WHEN h_lifestage  = '04' THEN 'Mature singles/homesharers'
                                      WHEN h_lifestage  = '05' THEN 'Mature family no children <18'
                                      WHEN h_lifestage  = '06' THEN 'Mature family with children <18'
                                      WHEN h_lifestage  = '07' THEN 'Mature household with children <18'
                                      WHEN h_lifestage  = '08' THEN 'Older single'
                                      WHEN h_lifestage  = '09' THEN 'Older family no children <18'
                                      WHEN h_lifestage  = '10' THEN 'Older family/household with children <18'
                                      WHEN h_lifestage  = '11' THEN 'Elderly single'
                                      WHEN h_lifestage  = '12' THEN 'Elderly family no children <18'
                                      ELSE 'Unknown' END
      , h_income_band_v2  
         , case h_household_composition
                                when '00' then 'Families'
                                when '01' then 'Extended family'
                                when '02' then 'Extended household'
                                when '03' then 'Pseudo family'
                                when '04' then 'Single male'
                                when '05' then 'Single female'
                                when '06' then 'Male homesharers'
                                when '07' then 'Female homesharers'
                                when '08' then 'Mixed homesharers'
                                when '09' then 'Abbreviated male families'
                                when '10' then 'Abbreviated female families'
                                when '11' then 'Multi-occupancy dwelling'
                                when 'U' then 'Unclassified'
                                else null
                                end       [Household Composition]
      , count(DISTINCT exp_cb_key_household) total 
FROM sk_prod.EXPERIAN_CONSUMERVIEW as a
JOIN pitteloudj.skybase as b ON a.exp_cb_key_household = b.household_key
GROUP BY       
     affluence
      , Lifestage
      , h_income_band_v2  
      , [Household Composition]   
UNION
SELECT 
      'TOTAL VESPA' tab  
      , affluence = CASE WHEN h_affluence_v2 in ('00','01','02')   THEN 'Very Low'
                                      WHEN h_affluence_v2 in ('03','04','05')   THEN 'Low'
                                      WHEN h_affluence_v2 in ('06','07','08')   THEN 'Mid Low'
                                      WHEN h_affluence_v2 in ('09','10','11')   THEN 'Mid'
                                      WHEN h_affluence_v2 in ('12','13','14')   THEN 'Mid High'
                                      WHEN h_affluence_v2 in ('15','16','17')   THEN 'High'
                                      WHEN h_affluence_v2 in ('18','19')        THEN 'Very High'
                                      ELSE                                              'Unknown'
                                 END
      , Lifestage = CASE              WHEN h_lifestage= '00' THEN 'Young singles/homesharers'
                                      WHEN h_lifestage  = '01' THEN 'Young family no children <18'
                                      WHEN h_lifestage  = '02' THEN 'Young family with children <18'
                                      WHEN h_lifestage  = '03' THEN 'Young household with children <18'
                                      WHEN h_lifestage  = '04' THEN 'Mature singles/homesharers'
                                      WHEN h_lifestage  = '05' THEN 'Mature family no children <18'
                                      WHEN h_lifestage  = '06' THEN 'Mature family with children <18'
                                      WHEN h_lifestage  = '07' THEN 'Mature household with children <18'
                                      WHEN h_lifestage  = '08' THEN 'Older single'
                                      WHEN h_lifestage  = '09' THEN 'Older family no children <18'
                                      WHEN h_lifestage  = '10' THEN 'Older family/household with children <18'
                                      WHEN h_lifestage  = '11' THEN 'Elderly single'
                                      WHEN h_lifestage  = '12' THEN 'Elderly family no children <18'
                                      ELSE 'Unknown' END
      , h_income_band_v2  
       , case h_household_composition
                                when '00' then 'Families'
                                when '01' then 'Extended family'
                                when '02' then 'Extended household'
                                when '03' then 'Pseudo family'
                                when '04' then 'Single male'
                                when '05' then 'Single female'
                                when '06' then 'Male homesharers'
                                when '07' then 'Female homesharers'
                                when '08' then 'Mixed homesharers'
                                when '09' then 'Abbreviated male families'
                                when '10' then 'Abbreviated female families'
                                when '11' then 'Multi-occupancy dwelling'
                                when 'U' then 'Unclassified'
                                else null
                                end       'Household Composition'  
      , count(DISTINCT exp_cb_key_household) total 
FROM sk_prod.EXPERIAN_CONSUMERVIEW as a
JOIN pitteloudj.VESPA as b ON a.exp_cb_key_household = b.household_key
GROUP BY       
     affluence
      , Lifestage
      , h_income_band_v2  
      , [Household Composition]    
UNION
SELECT 
      'TOTAL INSURANCE' tab    
      , affluence = CASE WHEN h_affluence_v2 in ('00','01','02')   THEN 'Very Low'
                                      WHEN h_affluence_v2 in ('03','04','05')   THEN 'Low'
                                      WHEN h_affluence_v2 in ('06','07','08')   THEN 'Mid Low'
                                      WHEN h_affluence_v2 in ('09','10','11')   THEN 'Mid'
                                      WHEN h_affluence_v2 in ('12','13','14')   THEN 'Mid High'
                                      WHEN h_affluence_v2 in ('15','16','17')   THEN 'High'
                                      WHEN h_affluence_v2 in ('18','19')        THEN 'Very High'
                                      ELSE                                              'Unknown'
                                 END
      , Lifestage = CASE              WHEN h_lifestage = '00' THEN 'Young singles/homesharers'
                                      WHEN h_lifestage  = '01' THEN 'Young family no children <18'
                                      WHEN h_lifestage  = '02' THEN 'Young family with children <18'
                                      WHEN h_lifestage  = '03' THEN 'Young household with children <18'
                                      WHEN h_lifestage  = '04' THEN 'Mature singles/homesharers'
                                      WHEN h_lifestage  = '05' THEN 'Mature family no children <18'
                                      WHEN h_lifestage  = '06' THEN 'Mature family with children <18'
                                      WHEN h_lifestage  = '07' THEN 'Mature household with children <18'
                                      WHEN h_lifestage  = '08' THEN 'Older single'
                                      WHEN h_lifestage  = '09' THEN 'Older family no children <18'
                                      WHEN h_lifestage  = '10' THEN 'Older family/household with children <18'
                                      WHEN h_lifestage  = '11' THEN 'Elderly single'
                                      WHEN h_lifestage  = '12' THEN 'Elderly family no children <18'
                                      ELSE 'Unknown' END
      , h_income_band_v2  
      , case h_household_composition
                                when '00' then 'Families'
                                when '01' then 'Extended family'
                                when '02' then 'Extended household'
                                when '03' then 'Pseudo family'
                                when '04' then 'Single male'
                                when '05' then 'Single female'
                                when '06' then 'Male homesharers'
                                when '07' then 'Female homesharers'
                                when '08' then 'Mixed homesharers'
                                when '09' then 'Abbreviated male families'
                                when '10' then 'Abbreviated female families'
                                when '11' then 'Multi-occupancy dwelling'
                                when 'U' then 'Unclassified'
                                else null
                                end       'Household Composition'  
      , count(DISTINCT exp_cb_key_household) total 
FROM sk_prod.EXPERIAN_CONSUMERVIEW as a
JOIN sk_prod.VESPA_INSURANCE_DATA as b ON a.exp_cb_key_household = b.cb_key_household
GROUP BY       
     affluence
      , Lifestage
      , h_income_band_v2  
      , [Household Composition] 
                                    
select DISTINCT cb_address_county
FROM sk_prod.EXPERIAN_CONSUMERVIEW as a



SELECT DISTINCT 
  	
	 region
   , cb_address_county
   , cb_address_postcode_district
   , cb_address_postcode   
   , count(*) Total
	
FROM sk_prod.CUST_SINGLE_ACCOUNT_VIEW
WHERE region is not null and cb_address_county is not null 
and  cb_address_postcode_district = 'TN15'
GROUP BY 	 region
   , cb_address_county
   , cb_address_postcode_district
, cb_address_postcode
SELECT DISTINCT 
  	
	 left(cb_address_postcode_district,2)
	
FROM sk_prod.CUST_SINGLE_ACCOUNT_VIEW
      where region is not null and cb_address_county is not null




SELECT 
      'TOTAL EXPERIAN' tab     
      , cb_address_county
      , count(DISTINCT exp_cb_key_household) total 
FROM sk_prod.EXPERIAN_CONSUMERVIEW
GROUP BY       
    cb_address_county
UNION
SELECT 
      'TOTAL Sky Base' tab
      , a.cb_address_county
      , count(DISTINCT exp_cb_key_household) total 
FROM sk_prod.EXPERIAN_CONSUMERVIEW as a
JOIN pitteloudj.skybase as b ON a.exp_cb_key_household = b.household_key
GROUP BY       
    a.cb_address_county  
UNION
SELECT 
      'TOTAL VESPA' tab  
      , a.cb_address_county
      , count(DISTINCT exp_cb_key_household) total 
FROM sk_prod.EXPERIAN_CONSUMERVIEW as a
JOIN pitteloudj.VESPA as b ON a.exp_cb_key_household = b.household_key
GROUP BY       
    a.cb_address_county 
UNION
SELECT 
      'TOTAL INSURANCE' tab    
      , a.cb_address_county
      , count(DISTINCT exp_cb_key_household) total 
FROM sk_prod.EXPERIAN_CONSUMERVIEW as a
JOIN sk_prod.VESPA_INSURANCE_DATA as b ON a.exp_cb_key_household = b.cb_key_household
GROUP BY       
    a.cb_address_county
      
