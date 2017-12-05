SELECT Movies_Previous_Target_Upgrade_Type , count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  Movies_Previous_Target_Upgrade_Type 
SELECT skyplayer, count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  skyplayer
SELECT Movies_sp_device_vol_12m_Cap, count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  Movies_sp_device_vol_12m_Cap
SELECT PPV_BAK_Flag, count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  PPV_BAK_Flag
SELECT Flag_Movies_DG_12m, count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  Flag_Movies_DG_12m
SELECT Movies_Num_pat_12m_Cap_3, count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  Movies_Num_pat_12m_Cap_3

--------------
SELECT CL_Current_Age , count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  CL_Current_Age 
SELECT dtv_latest_act_date  , count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  dtv_latest_act_date 
SELECT acct_sam_registered , count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  acct_sam_registered 
SELECT nlp , count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  nlp 
SELECT h_affluence_v2 , count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  h_affluence_v2 
SELECT tv_offer_end_dt , count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  tv_offer_end_dt 
SELECT implied_local_loop , count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  implied_local_loop 
SELECT sp_device_vol_12m , count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  sp_device_vol_12m 
SELECT ppv_count , count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  ppv_count









-- Movies


SELECT max(activity_dt), max(cb_data_datE) FROM  SKY_PLAYER_USAGE_DETAIL
SELECT max(first_event_dt) , max(event_dt) , max(prev_event_dt) from citeam.VIEW_CUST_FREE_PRODUCTS_HIST
--max(VIEW_CUST_FREE_PRODUCTS_HIST.first_event_dt)	max(VIEW_CUST_FREE_PRODUCTS_HIST.event_dt)	max(VIEW_CUST_FREE_PRODUCTS_HIST.prev_event_dt)
--2015-11-06	                                        2015-11-06	                                2015-11-05


tt SKY_STORE_TRANSACTIONS
SELECT top 10 cb_data_date, actual_delivery_dt, count(*)  FROM SKY_STORE_TRANSACTIONS GROUP BY  cb_data_date, actual_delivery_dt order by cb_data_date DESC 
tt CUST_PRODUCT_CHARGES_PPV
SELECT top 10 DATE(created_dt) dt , count(*) hits from cust_change_attempt group by dt order by dt desc 

tt cust_change_attempt


--- BB

tt CUST_NON_SUBSCRIPTIONS
SELECT   top 10 created_dt, count(*) hits        FROM CUST_NON_SUBSCRIPTIONS GROUP BY created_dt Order by created_dt DESC

tt Broadband_postcode_exchange
SELECT max(cb_data_date ) FROM  Broadband_postcode_exchange

select top 10  * from citeam.offer_usage_all
SELECT   top 10 created_dt, count(*) hits        FROM citeam.offer_usage_all GROUP BY created_dt Order by created_dt DESC

----------

SELECT Sports_num_pat_12m_Flag , count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  Sports_num_pat_12m_Flag 
SELECT hd_sub , count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  hd_sub
SELECT gender, count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  gender
SELECT skyplayer, count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  skyplayer
SELECT dtv_latest_act_date, count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  dtv_latest_act_date
SELECT Sports_Previous_Target_Upgrade_Type, count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  Sports_Previous_Target_Upgrade_Type
SELECT Sports_DG_Segment, count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  Sports_DG_Segment

tt cust_product_offers

-----------------
SELECT , count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  
SELECT , count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  
SELECT , count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  
SELECT , count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  
SELECT , count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  
SELECT , count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  
SELECT , count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  

sp_device_vol_12m
ppv_count
implied_local_loop
num_premium_upgrade_ever
x_skyfibre_enabled_date


tt BT_FIBRE_POSTCODE


SELECT num_premium_upgrade_ever, count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  num_premium_upgrade_ever
SELECT x_skyfibre_enabled_date, count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  x_skyfibre_enabled_date
num_premium_upgrade_ever

SELECT cvs_segment, count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  cvs_segment
SELECT num_movies_num_upgrade_24m, count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  num_movies_num_upgrade_24m
SELECT num_cust_calls_in_12m, count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  num_cust_calls_in_12m


tt CUST_CONTACT

SELECT   top 10 DATE (created_dt) dt , count(*) hits        FROM CUST_CONTACT GROUP BY created_dt Order by dt DESC



SELECT wlr, count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  wlr
SELECT last_MU_FP_date, count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  last_MU_FP_date
SELECT last_SU_FP_date, count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  last_SU_FP_date
SELECT num_sports_num_downgrade_24m, count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  num_sports_num_downgrade_24m
SELECT num_sports_events, count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  num_sports_events
SELECT tv_offer_end_dt, count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY tv_offer_end_dt


tt cust_product_offers

SELECT top 10 event_dt, count(*) hits from CITeam.view_cust_package_movements_hist GROUP BY event_dt ORDER BY event_dt desc 
SELECT   top 10 created_dt, count(*) hits        FROM cust_product_offers GROUP BY created_dt Order by created_dt DESC

SELECT TopTier_Mths_Since_Target_Downgrade_Grouped, count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  TopTier_Mths_Since_Target_Downgrade_Grouped
SELECT TopTier_Previous_Target_Upgrade_Type, count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  TopTier_Previous_Target_Upgrade_Type


tt email_event_outcome_summary

SELECT   top 10 cb_data_date, x_email_opened, count(*) hits        FROM email_event_outcome_summary GROUP BY cb_data_date, x_email_opened Order by cb_data_date DESC



SELECT HD_downgrade, count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  HD_downgrade
SELECT HD_upgrade	, count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  HD_upgrade	
SELECT PremMovies2	, count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  PremMovies2	

SELECT od_rf, count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  od_rf
SELECT movies_num_downgrade_24m , count(*) hits     FROM simmonsr.MMS_2017_06 GROUP BY  movies_num_downgrade_24m


tt CUST_ANYTIME_PLUS_DOWNLOADS


SELECT   top 10 cb_data_date, count(*) hits        FROM CUST_ANYTIME_PLUS_DOWNLOADS GROUP BY cb_data_date Order by cb_data_date DESC


---------------########################~~~~~~~~~~~~~~~~~~~~~~~~~~~~########################-------------------------
---------------########################~~~~~~~~~~~~~~~~~~~~~~~~~~~~########################-------------------------

