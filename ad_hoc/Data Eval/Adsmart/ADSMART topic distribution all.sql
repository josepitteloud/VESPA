/*----------------------------------------------------------------------------------------
Project :     ADSMART Topics 
Coded by:     Jose Pitteloud
Date :        03/12/2013

Description:  Count of Adsmartables accounts by Experian variables
Requesst by:  Tim Dixon
----------------------------------------------------------------------------------------*/

-----------------   Setting the tolerance level
CREATE VARIABLE @tol int;
CREATE VARIABLE @c1 bit;
CREATE VARIABLE @c2 bit;

DROP TABLE adsmart_topic_accounts
SET @tol = 90 
SET @c1=0
-----------------   Create table of Adsmartable accounts
if @c1=1
BEGIN
SELECT 
       SAV.account_number acct
     , sav.cb_key_household HH_key
     , sav.cb_key_individual ind_key
     , sav.CUST_VIEWING_DATA_CAPTURE_ALLOWED
     , 0 adsmart_flag
     , 0 adsmartable_flag
INTO adsmart_topic_accounts
FROM sk_prod.CUST_SINGLE_ACCOUNT_VIEW  	AS sav
JOIN sk_prod.cust_subs_hist				AS csh  ON sav.account_number = csh.account_number
WHERE     
	  csh.subscription_sub_type IN ('DTV Primary Viewing')
     AND csh.status_code IN ('AC','AB','PC')
     AND csh.effective_from_dt <= '20131201'
     AND csh.effective_to_dt > '20131201'
     AND csh.EFFECTIVE_FROM_DT IS NOT NULL
     AND csh.cb_key_household > 0             --UK Only
     AND csh.cb_key_household IS NOT NULL
     AND csh.account_number IS NOT NULL
     AND csh.service_instance_id IS NOT NULL 
GROUP BY 
    acct
  , HH_key
  , ind_key
  , CUST_VIEWING_DATA_CAPTURE_ALLOWED
------------------------------------------------------------------------------------
------------------------------------------------------------------------------------
UPDATE   adsmart_topic_accounts
SET   adsmart_flag      = CASE WHEN ad.cb_key_household is not null THEN 1 ELSE 0 END 
    , adsmartable_flag  = CASE WHEN ST.account_number is not null THEN 1 ELSE 0 END
FROM   adsmart_topic_accounts as sav
LEFT JOIN sk_prod.ADSMART   AS ad   ON sav.acct = ad.account_number 
LEFT JOIN kjdl   						AS ST   ON ST.account_number = ad.account_number AND ST.T_ADMS > 0 AND CUST_VIEWING_DATA_CAPTURE_ALLOWED = 'Y'
------------------------------------------------------------------------------------
------------------------------------------------------------------------------------
END
/*    QA
SELECT count(*)
        , sum( adsmart_flag) adsmart_flag1
        , SUM(adsmartable_flag) adsmartable_flag1
FROM adsmart_topic_accounts

count()	  adsmart_flag1	  adsmartable_flag1
10017120	9427434	        5833470
*/
COMMIT;
------------------------------------------------------------------------------------
-----------------   Populate PERSON_PROPENSITIES_GRID_CUR table
------------------------------------------------------------------------------------
drop  TABLE adsmart_topic_prop_cur;
------------------------------------------------------------------------------------
CREATE TABLE adsmart_topic_prop_cur ( 
  HH_key bigint
, daily_telegraph as INT
, times as INT
, sun as INT
, daily_mai as INT
, mirror as INT
, interests_theatre_arts as INT
, is_your_mobile_phone_contract as INT
, holiday_type_cruise_taken as INT
, sports_golf_perc_p as INT
, sports_skiing as INT
, sports_watersports as INT
, car_replacement_0_6 as INT
, car_replacement_6_12 as INT
, student as INT
, tech_ff_source_of_info as INT
, tech_search_magazines as INT
, tech_like_new_products as INT
, tecg_love_hunting as INT
, tech_like_to_talk as INT
, tech_like_to_use_new_prods as INT
, tech_FF_advice as INT
, tech_info_to_offer as INT
, tech_convince_opinion as INT
, read_daily_telegraph_prop_new as INT
, read_times_prop_new as INT
, read_sun_prop_new as INT
, read_mirror_prop_new as INT
, mobile_expend_10_29 as INT
, sport_golf_prop_new as INT
, pet_cat_h_prop as INT
, pet_dog_h_prop as INT
, hols_USA_h_prop as INT
, hols_UK_h_prop as INT
, hols_EU_no_med_h_prop as INT
, hols_EU_H_prop_new as INT
, hols_UK_H_prop_new as INT
, hols_USA_H_prop_new as INT
, hols_outside_H_prop_new as INT
, hols_far_east_life as INT
, hols_uk_life as INT
, read_daily_tele_life as INT
, read_times_life as INT
, read_sun_life as INT
, read_daily_mail_life as INT
, read_mirror_life as INT
, mosaic AS varchar(4) default NULL 
, pixel AS bigint default 0
, Pc_mosaic as varchar(4) Default NULL)
------------------------------------------------------------------------------------
ALTER TABLE adsmart_topic_prop_cur
ADD (h_pixel AS bigint default 0)

------------------------------------------------------------------------------------
INSERT INTO adsmart_topic_prop_cur (
  HH_key
  , hols_far_east_life 
  , c 
  , read_daily_tele_life 
  , read_times_life 
  , read_sun_life 
  , read_daily_mail_life 
  , read_mirror_life 
      )
SELECT     
    cv.cb_key_household HH_key
  , SUM(CASE WHEN s3_012107_data_trav_hols_loct_far_east_inc_thailand_taken ='Y' THEN 1 ELSE 0 END)   AS hols_far_east_life
  , SUM(CASE WHEN s3_012117_data_trav_hols_loct_uk_taken ='Y' THEN 1 ELSE 0 END)                      AS hols_uk_life
  , SUM(CASE WHEN s3_003694_data_intr_read_news_daily_telegraph ='Y' THEN 1 ELSE 0 END)               AS read_daily_tele_life
  , SUM(CASE WHEN s3_003713_data_intr_read_news_times ='Y' THEN 1 ELSE 0 END)                         AS read_times_life
  , SUM(CASE WHEN s3_003712_data_intr_read_news_sun ='Y' THEN 1 ELSE 0 END)                           As read_sun_life
  , SUM(CASE WHEN s3_003692_data_intr_read_news_daily_mail ='Y' THEN 1 ELSE 0 END)                    AS read_daily_mail_life
  , SUM(CASE WHEN s3_003693_data_intr_read_news_mirror ='Y' THEN 1 ELSE 0 END)                        AS read_mirror_life
FROM sk_prod.EXPERIAN_CONSUMERVIEW  as cv
LEFT JOIN sk_prod.PLAYPEN_EXPERIAN_LIFESTYLE    as lf       ON cv.cb_key_household = lf.cb_key_household
LEFT JOIN sk_prod.EXPERIAN_LIFESTYLE            as ef       ON cv.cb_key_household = ef.cb_key_household
GROUP BY HH_key
------------------------------------------------------------------------------------
hols_far_east_l	
hols_uk_life   	
read_daily_mail	
read_mirror_lif	
read_sun_life  	
read_times_life

------------------------------------------------------------------------------------
SELECT 
  cb_key_household as hh_key
  ,max(h_mosaic_uk_type) mos
  ,max(h_pixel_v2) pix
INTO mos_temp
FROM sk_prod.EXPERIAN_CONSUMERVIEW
GROUP BY hh_key

UPDATE adsmart_topic_prop_cur
SET 
  mosaic    = mos
, h_pixel   = pix
FROM adsmart_topic_prop_cur as a 
JOIN mos_temp as cv ON cv.HH_key = a.HH_key
WHERE cast(pix as int) >=25000


commit
--SELECT max(distinct h_pixel) from  adsmart_topic_prop_cur Where mosaic is null OR mosaic ='0'
------------------------------------------------------------------------------------
------------------------------------------------------------------------------------
UPDATE adsmart_topic_prop_cur
SET
    pet_cat_h_prop    = (CAST(cats_yes_perc_h AS INT))    
  , pet_dog_h_prop = (CAST(dogs_yes_perc_h AS INT))     
  , hols_USA_h_prop =(CAST(holiday_where_usa_perc_h AS INT))     
  , hols_UK_h_prop = (CAST(holiday_where_uk_perc_h AS INT))     
  , hols_EU_no_med_h_prop = (CAST(holiday_where_europe_non_mediterranean_perc_h AS INT))   
  , hols_EU_H_prop_new = (CAST(have_taken_a_holiday_in_europe_in_the_last_year_percentile AS INT))   
  , hols_UK_H_prop_new = (CAST(have_taken_a_holiday_in_the_uk_in_the_last_year_percentile AS INT))   
  , hols_USA_H_prop_new = (CAST(have_taken_a_holiday_in_the_usa_in_the_last_year_percentile AS INT))  
  , hols_outside_H_prop_new = (CAST(have_taken_a_holiday_outside_usa_europe_in_the_last_year_percentile AS INT))   
FROM adsmart_topic_prop_cur as cv
LEFT JOIN sk_prod.HOUSEHOLD_PROPENSITIES_GRID_CUR as hpr    ON cv.h_pixel = cast(hpr.hpixel as int)     and cv.mosaic = hpr.mosaicuk_type
LEFT JOIN sk_prod.HOUSEHOLD_PROPENSITIES_GRID_NEW as hpn    ON cv.h_pixel = cast(hpn.hpixel2011 as int) and cv.mosaic  = hpn.mosaic_uk_2009_type
commit;
------------------------------------------------------------------------------------
------------------------------------------------------------------------------------
UPDATE adsmart_topic_prop_cur
SET 
   a.daily_telegraph = v.daily_telegraph
,  a.times = v.times
,  a.sun = v.sun
,  a.daily_mai = v.daily_mai
,  a.mirror = v.mirror
,  a.interests_theatre_arts = v.interests_theatre_arts
,  a.is_your_mobile_phone_contract = v.is_your_mobile_phone_contract
,  a.holiday_type_cruise_taken = v.holiday_type_cruise_taken
,  a.sports_golf_perc_p = v.sports_golf_perc_p
,  a.sports_skiing = v.sports_skiing
,  a.sports_watersports = v.sports_watersports
,  a.car_replacement_0_6 = v.car_replacement_0_6
,  a.car_replacement_6_12 = v.car_replacement_6_12
,  a.student = v.student
FROM adsmart_topic_prop_cur as a
JOIN (SELECT 
           cv.cb_key_household HH_key
        , MAX(CAST(daily_telegraph_perc_p as int))        		daily_telegraph
        , MAX(CAST(times_perc_p AS int))             times
        , MAX(CAST(sun_perc_p AS INT)) 			sun
        , MAX(CAST(daily_mail_perc_p AS INT))    	daily_mai
        , MAX(CAST(mirror_perc_p AS INT))	mirror
        , MAX(CAST(interests_theatre_arts_perc_p AS INT)) interests_theatre_arts
        , MAX(CAST(is_your_mobile_phone_contract_perc_p AS INT))           is_your_mobile_phone_contract
        , MAX(CAST(holiday_type_cruise_taken_perc_p AS INT))           holiday_type_cruise_taken
        , MAX(CAST(sports_golf_perc_p AS INT))           sports_golf_perc_p
        , MAX(CAST(sports_skiing_perc_p AS INT))           sports_skiing
        , MAX(CAST(sports_watersports_perc_p AS INT))           sports_watersports
        , MAX(CAST(next_car_replacement_0_6_months_perc_p AS INT)) car_replacement_0_6
        , MAX(CAST(next_car_replacement_6_12_months_perc_p AS INT)) car_replacement_6_12
        , MAX(CAST(job_student_perc_p AS INT)) student
      FROM sk_prod.EXPERIAN_CONSUMERVIEW  as cv     
      LEFT JOIN sk_prod.PERSON_PROPENSITIES_GRID_CUR as pr        ON cv.p_pixel_v2 = pr.ppixel and pr.mosaicuk = cv.h_mosaic_uk_type
      GROUP BY HH_key) as v ON v.HH_key = a.HH_key
COMMIT;
------------------------------------------------------------------------------------
------------------------------------------------------------------------------------
UPDATE adsmart_topic_prop_cur
SET 
   a.tech_ff_source_of_info = v.tech_ff_source_of_info
,  a.tech_search_magazines = v.tech_search_magazines
,  a.tech_like_new_products = v.tech_like_new_products
,  a.tecg_love_hunting = v.tecg_love_hunting
,  a.tech_like_to_talk = v.tech_like_to_talk
,  a.tech_like_to_use_new_prods = v.tech_like_to_use_new_prods
,  a.tech_FF_advice = v.tech_FF_advice
,  a.tech_info_to_offer = v.tech_info_to_offer
,  a.tech_convince_opinion = v.tech_convince_opinion
,  a.read_daily_telegraph_prop_new = v.read_daily_telegraph_prop_new
,  a.read_times_prop_new = v.read_times_prop_new
,  a.read_sun_prop_new = v.read_sun_prop_new
,  a.read_mirror_prop_new = v.read_mirror_prop_new
,  a.mobile_expend_10_29 = v.mobile_expend_10_29
,  a.sport_golf_prop_new = v.sport_golf_prop_new
FROM adsmart_topic_prop_cur as a
JOIN (SELECT 
           cv.cb_key_household HH_key
  , MAX(CAST(friends_and_family_consider_me_a_good_source_of_information_about_technology_percentile as int))         AS tech_ff_source_of_info
  , MAX(CAST(i_frequently_search_magazines_and_websites_for_information_about_technology_products_and_services_percentile as int))    AS tech_search_magazines
  , MAX(CAST(i_like_to_get_new_technology_products_after_they_ve_been_out_percentile as int))    AS tech_like_new_products
  , MAX(CAST(i_love_hunting_out_the_latest_technology_products_and_services_before_anyone_else_catches_on_to_them_percentile as int))     AS tecg_love_hunting
  , MAX(CAST(i_tend_to_talk_a_lot_about_technology_with_friends_percentile as int))    AS tech_like_to_talk
  , MAX(CAST(i_m_always_keen_to_use_new_technology_products_as_soon_as_they_are_available_percentile as int))      AS tech_like_to_use_new_prods    
  , MAX(CAST(my_family_and_friends_often_ask_my_advice_on_technology_products_and_services_percentile as int))     AS tech_FF_advice  
  , MAX(CAST(when_i_am_asked_for_advice_about_technology_i_have_a_lot_of_information_to_offer_percentile as int))     AS tech_info_to_offer
  , MAX(CAST(when_i_discuss_technology_with_people_i_usually_convince_them_of_my_opinion_percentile as int))       AS tech_convince_opinion
  , MAX(CAST(read_the_daily_telegraph_regularly_percentile as int))        AS read_daily_telegraph_prop_new
  , MAX(CAST(read_the_the_times_regularly_percentile as int))              AS read_times_prop_new
  , MAX(CAST(read_the_sun_regularly_percentile as int))                    AS read_sun_prop_new
  , MAX(CAST(read_the_mirror_daily_record_regularly_percentile as int))    AS read_mirror_prop_new
  , MAX(CAST(monthly_expenditure_on_mobile_phone_10_29_99_percentile as int))    AS mobile_expend_10_29
  , MAX(CAST(enjoy_playing_golf_percentile as int))                        AS sport_golf_prop_new
FROM sk_prod.EXPERIAN_CONSUMERVIEW  as cv     
LEFT JOIN sk_prod.PERSON_PROPENSITIES_GRID_NEW as prn       ON cv.p_pixel_v2 = prn.ppixel2011 and prn.mosaic_uk_2009_type = cv.h_mosaic_uk_type
GROUP BY HH_key) AS v ON v.HH_key = a.HH_key
------------------------------------------------------------------------------------
------------------------------------------------------------------------------------
SELECT 'daily_telegraph' var, daily_telegraph prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 
INTO adsmart_topic_all
FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY daily_telegraph

INSERT INTO adsmart_topic_all
SELECT 'times' var, times prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY times
UNION
SELECT 'sun' var, sun prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY sun
UNION
SELECT 'daily_mai' var, daily_mai prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY daily_mai
UNION
SELECT 'mirror' var, mirror prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY mirror
UNION
SELECT 'interests_theatre_arts' var, interests_theatre_arts prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY interests_theatre_arts
UNION
SELECT 'is_your_mobile_phone_contract' var, is_your_mobile_phone_contract prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY is_your_mobile_phone_contract
UNION
SELECT 'holiday_type_cruise_taken' var, holiday_type_cruise_taken prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY holiday_type_cruise_taken
UNION
SELECT 'sports_golf_perc_p' var, sports_golf_perc_p prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY sports_golf_perc_p
UNION
SELECT 'sports_skiing' var, sports_skiing prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY sports_skiing
UNION
SELECT 'sports_watersports' var, sports_watersports prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY sports_watersports
UNION
SELECT 'car_replacement_0_6' var, car_replacement_0_6 prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY car_replacement_0_6
UNION
SELECT 'car_replacement_6_12' var, car_replacement_6_12 prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY car_replacement_6_12
UNION
SELECT 'student' var, student prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY student
UNION
SELECT 'tech_ff_source_of_info' var, tech_ff_source_of_info prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY tech_ff_source_of_info
UNION
SELECT 'tech_search_magazines' var, tech_search_magazines prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY tech_search_magazines
UNION
SELECT 'tech_like_new_products' var, tech_like_new_products prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY tech_like_new_products
UNION
SELECT 'tecg_love_hunting' var, tecg_love_hunting prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY tecg_love_hunting
UNION
SELECT 'tech_like_to_talk' var, tech_like_to_talk prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY tech_like_to_talk
UNION
SELECT 'tech_like_to_use_new_prods' var, tech_like_to_use_new_prods prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY tech_like_to_use_new_prods
UNION
SELECT 'tech_FF_advice' var, tech_FF_advice prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY tech_FF_advice
UNION
SELECT 'tech_info_to_offer' var, tech_info_to_offer prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY tech_info_to_offer
UNION
SELECT 'tech_convince_opinion' var, tech_convince_opinion prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY tech_convince_opinion
UNION
SELECT 'read_daily_telegraph_prop_new' var, read_daily_telegraph_prop_new prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY read_daily_telegraph_prop_new
UNION
SELECT 'read_times_prop_new' var, read_times_prop_new prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY read_times_prop_new
UNION
SELECT 'read_sun_prop_new' var, read_sun_prop_new prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY read_sun_prop_new
UNION
SELECT 'read_mirror_prop_new' var, read_mirror_prop_new prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY read_mirror_prop_new
UNION
SELECT 'mobile_expend_10_29' var, mobile_expend_10_29 prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY mobile_expend_10_29
UNION
SELECT 'sport_golf_prop_new' var, sport_golf_prop_new prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY sport_golf_prop_new
UNION
SELECT 'pet_cat_h_prop' var, pet_cat_h_prop prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY pet_cat_h_prop
UNION
SELECT 'pet_dog_h_prop' var, pet_dog_h_prop prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY pet_dog_h_prop
UNION
SELECT 'hols_USA_h_prop' var, hols_USA_h_prop prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY hols_USA_h_prop
UNION
SELECT 'hols_UK_h_prop' var, hols_UK_h_prop prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY hols_UK_h_prop
UNION
SELECT 'hols_EU_no_med_h_prop' var, hols_EU_no_med_h_prop prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY hols_EU_no_med_h_prop
UNION
SELECT 'hols_EU_H_prop_new' var, hols_EU_H_prop_new prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY hols_EU_H_prop_new
UNION
SELECT 'hols_UK_H_prop_new' var, hols_UK_H_prop_new prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY hols_UK_H_prop_new
UNION
SELECT 'hols_USA_H_prop_new' var, hols_USA_H_prop_new prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY hols_USA_H_prop_new
UNION
SELECT 'hols_outside_H_prop_new' var, hols_outside_H_prop_new prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY hols_outside_H_prop_new
UNION
SELECT 'hols_far_east_life' var, hols_far_east_life prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY hols_far_east_life
UNION
SELECT 'hols_uk_life' var, hols_uk_life prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY hols_uk_life
UNION
SELECT 'read_daily_tele_life' var, read_daily_tele_life prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY read_daily_tele_life
UNION
SELECT 'read_times_life' var, read_times_life prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY read_times_life
UNION
SELECT 'read_sun_life' var, read_sun_life prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY read_sun_life
UNION
SELECT 'read_daily_mail_life' var, read_daily_mail_life prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY read_daily_mail_life
UNION
SELECT 'read_mirror_life' var, read_mirror_life prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY read_mirror_life

insert into adsmart_topic_all
SELECT 'hols_USA_H_new' var, hols_USA_H_prop_new prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY hols_USA_H_prop_new
UNION
SELECT 'hols_USA_h_cur' var, hols_USA_h_prop prop, count(a.HH_key) total, count(v.HH_key) sky_total , sum( adsmart_flag) adsmart_flag1, sum(adsmartable_flag) adsmartable_flag1 FROM adsmart_topic_prop_cur as a LEFT JOIN adsmart_topic_accounts AS v ON v.HH_key = a.HH_key GROUP BY hols_USA_h_prop


Select * from adsmart_topic_all where var like 'c%'

delete from adsmart_topic_all where  var like 'hols_USA_H_prop'
hols_USA_H_new
hols_USA_h_cur
commit

/*------------ SELECTING total counts
SELECT 
  COUNT (acct)      AS TOTAL_rows
, sum (tot_daily_telegraph)          AS t_tot_read_daily_telegraph
, sum (tot_times)           AS t_tot_read_times
, sum (tot_sun)             AS t_tot_read_sun
, sum (tot_daily_mai)       AS t_tot_read_daily_mail
, sum (tot_mirror)          AS t_tot_read_mirror
, sum (tot_interests_theatre_arts)              AS t_tot_interests_theatre_arts
, sum (tot_is_your_mobile_phone_contract)       AS t_tot_mob_is_your_mobile_phone_contract
, sum (tot_holiday_type_cruise_taken)           AS t_tot_hols_type_cruise_taken
, sum (tot_sports_golf_perc_p)                  AS t_tot_sports_golf_perc_p
, sum (tot_sports_skiing)               AS t_tot_sports_skiing
, sum (tot_sports_watersports)          AS t_tot_sports_watersports
, sum (tot_tech_ff_source_of_info)      AS t_tot_tech_ff_source_of_info
, sum (tot_tech_search_magazines)       AS t_tot_tech_search_magazines
, sum (tot_tech_like_new_products)      AS t_tot_tech_like_new_products
, sum (tot_tecg_love_hunting)           AS t_tot_tech_love_hunting
, sum (tot_tech_like_to_talk)           AS t_tot_tech_like_to_talk
, sum (tot_tech_like_to_use_new_prods)          AS t_tot_tech_like_to_use_new_prods
, sum (tot_tech_FF_advice)              AS t_tot_tech_FF_advice
, sum (tot_tech_info_to_offer)          AS t_tot_tech_info_to_offer
, sum (tot_tech_convince_opinion)       AS t_tot_tech_convince_opinion
, sum (tot_read_daily_telegraph_prop_new)          AS t_tot_read_daily_telegraph_prop_new
, sum (tot_read_times_prop_new)         AS t_tot_read_times_prop_new
, sum (tot_read_sun_prop_new)           AS t_tot_read_sun_prop_new
, sum (tot_read_mirror_prop_new)        AS t_tot_read_mirror_prop_new
, sum (tot_mobile_expend_10_29)         AS t_tot_mob_expend_10_29
, sum (tot_sport_golf_prop_new)         AS t_tot_sport_golf_prop_new
, sum (tot_pet_cat_h_prop)              AS t_tot_pet_cat_h_prop
, sum (tot_pet_dog_h_prop)              AS t_tot_pet_dog_h_prop
, sum (tot_hols_USA_h_prop)             AS t_tot_hols_USA_h_prop
, sum (tot_hols_UK_h_prop)              AS t_tot_hols_UK_h_prop
, sum (tot_hols_EU_no_med_h_prop)       AS t_tot_hols_EU_no_med_h_prop
, sum (tot_hols_EU_H_prop_new)          AS t_tot_hols_EU_H_prop_new
, sum (tot_hols_UK_H_prop_new)          AS t_tot_hols_UK_H_prop_new
, sum (tot_hols_USA_H_prop_new)         AS t_tot_hols_USA_H_prop_new
, sum (tot_hols_outside_H_prop_new)     AS t_tot_hols_outside_H_prop_new
, sum (tot_hols_far_east_life)          AS t_tot_hols_far_east_life
, sum (tot_hols_uk_life)                AS t_tot_hols_uk_life
, sum (tot_read_daily_tele_life)        AS t_tot_read_daily_tele_life
, sum (tot_read_times_life)             AS t_tot_read_times_life
, sum (tot_read_sun_life)               AS t_tot_read_sun_life
, sum (tot_read_daily_mail_life)        AS t_tot_read_daily_mail_life
, sum (tot_read_mirror_life)            AS t_tot_read_mirror_life
FROM adsmart_topic_consolidated


--------------------------------------------------------------------------
--------------------------------------------------------------------------
SELECT COUNT (DISTINCT cb_key_household) as HHs
FROM sk_prod.PLAYPEN_EXPERIAN_LIFESTYLE as lf
JOIN adsmart_topic_accounts a b ON b.HH_key = lf.cb_key_household
WHERE s3_012117_data_trav_hols_loct_uk_taken ='Y'
*/
/*        ----------------    QA
select count(*) FROM adsmart_topic_hprop_new;
select count(*) FROM adsmart_topic_hprop_cur;
select count(*)FROM adsmart_topic_prop_cur;
select count(*)FROM adsmart_topic_prop_new;
select top 2 *  FROM adsmart_topic_hprop_new;
select top 2 * FROM adsmart_topic_hprop_cur;
select top 2 * FROM adsmart_topic_prop_cur;
select top 2 * FROM adsmart_topic_prop_new
*/