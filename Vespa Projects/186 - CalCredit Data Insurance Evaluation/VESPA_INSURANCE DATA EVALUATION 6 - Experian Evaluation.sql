--------------- EXPERIAN Lifestyle
--------- GENERAL  Count
SELECT
'TOTAL TABLE' Item, 
count(*) TOTAL,
count(Distinct exp_cb_key_individual)     Ind_Count, 
count(Distinct exp_cb_key_household)      HH_Count 
FROM sk_prod.EXPERIAN_LIFESTYLE
UNION
SELECT
'CAR Renew Count' Item, 
count(*) TOTAL,
count(Distinct exp_cb_key_individual)     Ind_Count, 
count(Distinct exp_cb_key_household)      HH_Count 
FROM sk_prod.EXPERIAN_LIFESTYLE
WHERE S2_000175_data_INSU_VEHI_BDWN_RENEWAL_MONTH_BREAKDOWN is not null

SELECT S2_000175_data_INSU_VEHI_BDWN_RENEWAL_MONTH_BREAKDOWN Insu_Car_Month_Renew
, count(Distinct exp_cb_key_household)
FROM sk_prod.EXPERIAN_LIFESTYLE
group by Insu_Car_Month_Renew


----------- Matching Rates
------- CalCredit
SELECT   'Experian Lifestyle vs. Insurance HH Car_renew_Month_Renew' Item
    , count(DISTINCT exp_cb_key_household) Total
FROM sk_prod.EXPERIAN_LIFESTYLE as a
JOIN sk_prod.VESPA_INSURANCE_DATA as b ON a.exp_cb_key_household = b.cb_key_household
AND b.motor_renewal_indicator='Y'
WHERE a.S2_000175_data_INSU_VEHI_BDWN_RENEWAL_MONTH_BREAKDOWN is not null
------- VESPA
UNION
SELECT   'Experian Lifestyle vs. Vespa HH Car_renew_Month_Renew' Item
    , count(DISTINCT exp_cb_key_household) TOtal
FROM sk_prod.EXPERIAN_LIFESTYLE as a
JOIN vespa as b ON a.exp_cb_key_household = b.household_key
WHERE a.S2_000175_data_INSU_VEHI_BDWN_RENEWAL_MONTH_BREAKDOWN is not null
------- Skybase
UNION
SELECT   'Overlap Experian Lifestyle vs. Vespa HH Car_renew_Month_Renew' Item
    , count(DISTINCT exp_cb_key_household) TOtal
FROM sk_prod.EXPERIAN_LIFESTYLE as a
JOIN vespa as b ON a.exp_cb_key_household = b.household_key
JOIN sk_prod.VESPA_INSURANCE_DATA as c ON a.exp_cb_key_household = c.cb_key_household
      AND c.motor_renewal_indicator='Y'
WHERE a.S2_000175_data_INSU_VEHI_BDWN_RENEWAL_MONTH_BREAKDOWN is not null
------- Skybase
UNION
SELECT   'Experian Lifestyle vs. Sky base HH Car_renew_Month_Renew' Item
    , count(DISTINCT exp_cb_key_household) TOtal
FROM sk_prod.EXPERIAN_LIFESTYLE as a
JOIN skybase as b ON a.exp_cb_key_household = b.household_key
WHERE a.S2_000175_data_INSU_VEHI_BDWN_RENEWAL_MONTH_BREAKDOWN is not null
UNION
SELECT   'Overlap Experian Lifestyle vs. Sky base HH Car_renew_Month_Renew' Item
    , count(DISTINCT exp_cb_key_household) TOtal
FROM sk_prod.EXPERIAN_LIFESTYLE as a
JOIN skybase as b ON a.exp_cb_key_household = b.household_key
JOIN sk_prod.VESPA_INSURANCE_DATA as c ON a.exp_cb_key_household = c.cb_key_household
      AND c.motor_renewal_indicator='Y'
WHERE a.S2_000175_data_INSU_VEHI_BDWN_RENEWAL_MONTH_BREAKDOWN is not null


----------------------------PLAYPEN LIFESTYLE
---------GENERAL Count
SELECT
count(*) TOTAL,
count(Distinct exp_cb_key_individual)     Ind_Count, 
count(Distinct exp_cb_key_household)      HH_Count ,
count(S3_012026_data_INSU_HOUS_GENR_HAVE_CONTENTS_INSURANCE) ,
count(S2_000192_data_INSU_VEHI_CARS_RENEWAL_MONTH_CAR),
count(S2_000155_data_INSU_HOUS_GENR_RENEWAL_MONTH_CONTENTS),
count(S2_000154_data_INSU_HOUS_BDGS_RENEWAL_MONTH_BUILDINGS)
From sk_prod.PLAYPEN_EXPERIAN_LIFESTYLE;



----------- CAR Renewal
SELECT
'PLAYPEN TOTAL TABLE' Item, 
count(*) TOTAL,
count(Distinct exp_cb_key_individual)     Ind_Count, 
count(Distinct exp_cb_key_household)      HH_Count 
FROM sk_prod.PLAYPEN_EXPERIAN_LIFESTYLE
UNION
SELECT
'PLAYPEN CAR Renew Count' Item, 
count(*) TOTAL,
count(Distinct exp_cb_key_individual)     Ind_Count, 
count(Distinct exp_cb_key_household)      HH_Count 
FROM sk_prod.PLAYPEN_EXPERIAN_LIFESTYLE
WHERE S2_000192_data_INSU_VEHI_CARS_RENEWAL_MONTH_CAR is not null
UNION
SELECT
'PLAYPEN HOME Building Renew Count' Item, 
count(*) TOTAL,
count(Distinct exp_cb_key_individual)     Ind_Count, 
count(Distinct exp_cb_key_household)      HH_Count 
FROM sk_prod.PLAYPEN_EXPERIAN_LIFESTYLE
WHERE S2_000154_data_INSU_HOUS_BDGS_RENEWAL_MONTH_BUILDINGS is not null
UNION
SELECT
'PLAYPEN HOME Content Renew Count' Item, 
count(*) TOTAL,
count(Distinct exp_cb_key_individual)     Ind_Count, 
count(Distinct exp_cb_key_household)      HH_Count 
FROM sk_prod.PLAYPEN_EXPERIAN_LIFESTYLE
WHERE S2_000155_data_INSU_HOUS_GENR_RENEWAL_MONTH_CONTENTS is not null
UNION
SELECT
'PLAYPEN HOME Unified Renew Count' Item, 
count(*) TOTAL,
count(Distinct exp_cb_key_individual)     Ind_Count, 
count(Distinct exp_cb_key_household)      HH_Count 
FROM sk_prod.PLAYPEN_EXPERIAN_LIFESTYLE
WHERE S2_000155_data_INSU_HOUS_GENR_RENEWAL_MONTH_CONTENTS is not null OR 
      S2_000154_data_INSU_HOUS_BDGS_RENEWAL_MONTH_BUILDINGS is not null

/*
S2_000192_data_INSU_VEHI_CARS_RENEWAL_MONTH_CAR
S2_000155_data_INSU_HOUS_GENR_RENEWAL_MONTH_CONTENTS
S2_000154_data_INSU_HOUS_BDGS_RENEWAL_MONTH_BUILDINGS
*/

------- CalCredit
--------  CAR Match Count
SELECT   'PLAYPEN vs. Insurance HH Car_renew_Month_Renew' Item
    , count(DISTINCT exp_cb_key_household) Total
FROM sk_prod.PLAYPEN_EXPERIAN_LIFESTYLE as a
JOIN sk_prod.VESPA_INSURANCE_DATA as b ON a.exp_cb_key_household = b.cb_key_household
AND b.motor_renewal_indicator='Y'
WHERE a.S2_000192_data_INSU_VEHI_CARS_RENEWAL_MONTH_CAR is not null
------- VESPA
UNION
SELECT   'PLAYPEN vs. Vespa HH Car_renew_Month_Renew' Item
    , count(DISTINCT exp_cb_key_household) TOtal
FROM sk_prod.PLAYPEN_EXPERIAN_LIFESTYLE as a
JOIN vespa as b ON a.exp_cb_key_household = b.household_key
WHERE a.S2_000192_data_INSU_VEHI_CARS_RENEWAL_MONTH_CAR is not null
------- Skybase
UNION
SELECT   'Overlap PLAYPEN vs. Vespa HH Car_renew_Month_Renew' Item
    , count(DISTINCT exp_cb_key_household) TOtal
FROM sk_prod.PLAYPEN_EXPERIAN_LIFESTYLE as a
JOIN vespa as b ON a.exp_cb_key_household = b.household_key
JOIN sk_prod.VESPA_INSURANCE_DATA as c ON a.exp_cb_key_household = c.cb_key_household
      AND c.motor_renewal_indicator='Y'
WHERE a.S2_000192_data_INSU_VEHI_CARS_RENEWAL_MONTH_CAR is not null
------- Skybase
UNION
SELECT   'PLAYPEN vs. Sky base HH Car_renew_Month_Renew' Item
    , count(DISTINCT exp_cb_key_household) TOtal
FROM sk_prod.PLAYPEN_EXPERIAN_LIFESTYLE as a
JOIN skybase as b ON a.exp_cb_key_household = b.household_key
WHERE a.S2_000192_data_INSU_VEHI_CARS_RENEWAL_MONTH_CAR is not null
------- Skybase Overlapping
UNION
SELECT   'Overlap PLAYPEN vs. Vespa HH Car_renew_Month_Renew' Item
    , count(DISTINCT exp_cb_key_household) TOtal
FROM sk_prod.PLAYPEN_EXPERIAN_LIFESTYLE as a
JOIN Skybase  as b ON a.exp_cb_key_household = b.household_key
JOIN sk_prod.VESPA_INSURANCE_DATA as c ON a.exp_cb_key_household = c.cb_key_household
      AND c.motor_renewal_indicator='Y'
WHERE a.S2_000192_data_INSU_VEHI_CARS_RENEWAL_MONTH_CAR is not null




--------  HOME UNIFIED Match Count
SELECT   'PLAYPEN vs. Insurance HH Home Unified Match' Item
    , count(DISTINCT exp_cb_key_household) Total
FROM sk_prod.PLAYPEN_EXPERIAN_LIFESTYLE as a
JOIN sk_prod.VESPA_INSURANCE_DATA as b ON a.exp_cb_key_household = b.cb_key_household
AND b.home_renewal_indicator='Y'
WHERE S2_000155_data_INSU_HOUS_GENR_RENEWAL_MONTH_CONTENTS is not null OR 
      S2_000154_data_INSU_HOUS_BDGS_RENEWAL_MONTH_BUILDINGS is not null
------- VESPA
UNION
SELECT   'PLAYPEN vs. Vespa HH Home Unified Match' Item
    , count(DISTINCT exp_cb_key_household) TOtal
FROM sk_prod.PLAYPEN_EXPERIAN_LIFESTYLE as a
JOIN vespa as b ON a.exp_cb_key_household = b.household_key
WHERE S2_000155_data_INSU_HOUS_GENR_RENEWAL_MONTH_CONTENTS is not null OR 
      S2_000154_data_INSU_HOUS_BDGS_RENEWAL_MONTH_BUILDINGS is not null
------- Skybase
UNION
SELECT   'Overlap PLAYPEN vs. Vespa HH Home Unified Match' Item
    , count(DISTINCT exp_cb_key_household) TOtal
FROM sk_prod.PLAYPEN_EXPERIAN_LIFESTYLE as a
JOIN vespa as b ON a.exp_cb_key_household = b.household_key
JOIN sk_prod.VESPA_INSURANCE_DATA as c ON a.exp_cb_key_household = c.cb_key_household
      AND c.home_renewal_indicator='Y'
WHERE S2_000155_data_INSU_HOUS_GENR_RENEWAL_MONTH_CONTENTS is not null OR 
      S2_000154_data_INSU_HOUS_BDGS_RENEWAL_MONTH_BUILDINGS is not null
------- Skybase
UNION
SELECT   'PLAYPEN vs. Sky base HH Home Unified Match' Item
    , count(DISTINCT exp_cb_key_household) TOtal
FROM sk_prod.PLAYPEN_EXPERIAN_LIFESTYLE as a
JOIN skybase as b ON a.exp_cb_key_household = b.household_key
WHERE S2_000155_data_INSU_HOUS_GENR_RENEWAL_MONTH_CONTENTS is not null OR 
      S2_000154_data_INSU_HOUS_BDGS_RENEWAL_MONTH_BUILDINGS is not null
UNION
SELECT   'Overlap PLAYPEN vs. Sky Base HH Home Unified Match' Item
    , count(DISTINCT exp_cb_key_household) TOtal
FROM sk_prod.PLAYPEN_EXPERIAN_LIFESTYLE as a
JOIN skybase as b ON a.exp_cb_key_household = b.household_key
JOIN sk_prod.VESPA_INSURANCE_DATA as c ON a.exp_cb_key_household = c.cb_key_household
      AND c.home_renewal_indicator='Y'
WHERE S2_000155_data_INSU_HOUS_GENR_RENEWAL_MONTH_CONTENTS is not null OR 
      S2_000154_data_INSU_HOUS_BDGS_RENEWAL_MONTH_BUILDINGS is not null










/*











SELECT S3_012026_data_INSU_HOUS_GENR_HAVE_CONTENTS_INSURANCE HH_Have_Content_Month_Renew
, count(*)                                  Total_Count
, count(Distinct exp_cb_key_individual)     Ind_Count
, count(Distinct exp_cb_key_household)      HH_Count
FROM sk_prod.PLAYPEN_EXPERIAN_LIFESTYLE
group by HH_Have_Content_Month_Renew

SELECT S2_000192_data_INSU_VEHI_CARS_RENEWAL_MONTH_CAR Car_renew_Month_Renew
, count(*)                                  Total_Count
, count(Distinct exp_cb_key_individual)     Ind_Count
, count(Distinct exp_cb_key_household)      HH_Count
FROM sk_prod.PLAYPEN_EXPERIAN_LIFESTYLE
group by Car_renew_Month_Renew


SELECT S2_000155_data_INSU_HOUS_GENR_RENEWAL_MONTH_CONTENTS Home_Content_renew_Month_Renew
, count(*)                                  Total_Count
, count(Distinct exp_cb_key_individual)     Ind_Count
, count(Distinct exp_cb_key_household)      HH_Count
FROM sk_prod.PLAYPEN_EXPERIAN_LIFESTYLE
group by Home_Content_renew_Month_Renew


SELECT S2_000154_data_INSU_HOUS_BDGS_RENEWAL_MONTH_BUILDINGS Home_Builing_renew_Month_Renew
, count(*)                                  Total_Count
, count(Distinct exp_cb_key_individual)     Ind_Count
, count(Distinct exp_cb_key_household)      HH_Count
FROM sk_prod.PLAYPEN_EXPERIAN_LIFESTYLE
group by Home_Builing_renew_Month_Renew

--------------------------- MATCH RATES Car_renew_Month_Renew
SELECT   'PLAYPEN LIFESTYLE vs. Insurance HH Car_renew_Month_Renew' Item
    , count(DISTINCT exp_cb_key_household) TOtal
FROM sk_prod.PLAYPEN_EXPERIAN_LIFESTYLE as a
JOIN sk_prod.VESPA_INSURANCE_DATA as b ON a.exp_cb_key_household = b.cb_key_household
AND b.motor_renewal_indicator='Y'
WHERE a.S2_000192_data_INSU_VEHI_CARS_RENEWAL_MONTH_CAR is not null
UNION
SELECT   'PLAYPEN LIFESTYLE vs. Vespa HH Car_renew_Month_Renew' Item
    , count(DISTINCT exp_cb_key_household) TOtal
FROM sk_prod.PLAYPEN_EXPERIAN_LIFESTYLE as a
JOIN vespa as b ON a.exp_cb_key_household = b.household_key
WHERE a.S2_000192_data_INSU_VEHI_CARS_RENEWAL_MONTH_CAR is not null
UNION
SELECT   'PLAYPEN LIFESTYLE vs. Sky base HH Car_renew_Month_Renew' Item
    , count(DISTINCT exp_cb_key_household) TOtal
FROM sk_prod.PLAYPEN_EXPERIAN_LIFESTYLE as a
JOIN skybase as b ON a.exp_cb_key_household = b.household_key
WHERE a.S2_000192_data_INSU_VEHI_CARS_RENEWAL_MONTH_CAR is not null
UNION
--------------------------- MATCH RATES HOME_Content_renew_Month_Renew
SELECT   'PLAYPEN LIFESTYLE vs. Insurance HH Home_Conent_renew_Month_Renew' Item
    , count(DISTINCT exp_cb_key_household) TOtal
FROM sk_prod.PLAYPEN_EXPERIAN_LIFESTYLE as a
JOIN sk_prod.VESPA_INSURANCE_DATA as b ON a.exp_cb_key_household = b.cb_key_household
AND b.home_renewal_indicator='Y'
WHERE S2_000155_data_INSU_HOUS_GENR_RENEWAL_MONTH_CONTENTS is not null  
UNION
SELECT   'PLAYPEN LIFESTYLE vs. Vespa HH Home_Conent_renew_Month_Renew' Item
    , count(DISTINCT exp_cb_key_household) TOtal
FROM sk_prod.PLAYPEN_EXPERIAN_LIFESTYLE as a
JOIN vespa as b ON a.exp_cb_key_household = b.household_key --0944
WHERE S2_000155_data_INSU_HOUS_GENR_RENEWAL_MONTH_CONTENTS is not null  
UNION
SELECT   'PLAYPEN LIFESTYLE vs. Sky base HH Home_Content_renew_Month_Renew' Item
    , count(DISTINCT exp_cb_key_household) TOtal
FROM sk_prod.PLAYPEN_EXPERIAN_LIFESTYLE as a
JOIN skybase as b ON a.exp_cb_key_household = b.household_key
WHERE S2_000155_data_INSU_HOUS_GENR_RENEWAL_MONTH_CONTENTS is not null  
UNION

--------------------------- MATCH RATES HOME_Building_renew_Month_Renew
SELECT   'PLAYPEN LIFESTYLE vs. Insurance HH Home_Building_renew_Month_Renew ' Item
    , count(DISTINCT exp_cb_key_household) TOtal
FROM sk_prod.PLAYPEN_EXPERIAN_LIFESTYLE as a
JOIN sk_prod.VESPA_INSURANCE_DATA as b ON a.exp_cb_key_household = b.cb_key_household
AND b.home_renewal_indicator='Y'
WHERE S2_000154_data_INSU_HOUS_BDGS_RENEWAL_MONTH_BUILDINGS is not null  
UNION
SELECT   'PLAYPEN LIFESTYLE vs. Vespa HH Home_Building_renew_Month_Renew' Item
    , count(DISTINCT exp_cb_key_household) TOtal
FROM sk_prod.PLAYPEN_EXPERIAN_LIFESTYLE as a
JOIN vespa as b ON a.exp_cb_key_household = b.household_key
WHERE S2_000154_data_INSU_HOUS_BDGS_RENEWAL_MONTH_BUILDINGS is not null  
UNION
SELECT   'PLAYPEN LIFESTYLE vs. Sky base HH Home_Building_renew_Month_Renew' Item
    , count(DISTINCT exp_cb_key_household) TOtal
FROM sk_prod.PLAYPEN_EXPERIAN_LIFESTYLE as a
JOIN skybase as b ON a.exp_cb_key_household = b.household_key
WHERE S2_000154_data_INSU_HOUS_BDGS_RENEWAL_MONTH_BUILDINGS is not null  


------------------------------PROPENSITIES
-------- General Count
SELECT 
count(*) Total_rows,
count(have_no_insurance_percentile)                   No_Insurance,
count(have_home_contents_insurance_percentile)        Home_content,
count(have_home_buildings_insurance_percentile)       Home_building,
count(have_combined_buildings_and_contents_insurance_percentile) Home_Both,
count(changed_insurance_supplier_at_last_renewal_percentile)    Changed_Insurance,
count(arranged_insurance_over_the_internet_percentile)    Internet_Insurance,
count(arranged_insurance_by_visiting_branch_percentile)   Visitng_Branch_Insurance,
count(arranged_insurance_by_post_percentile)              Via_Post_Insurance,
count(arranged_insurance_by_phone_percentile)             Via_Phone_Insurance,
count(arranged_insurance_by_agent_coming_to_home_percentile)    Home_Visit_Insurance
FROM sk_prod.HOUSEHOLD_PROPENSITIES_GRID_CUR

*/

/**************************************
**      sp_columns EXPERIAN_LIFESTYLE
**
**
**
**
****************************************/