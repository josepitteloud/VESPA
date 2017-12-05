SELECT 'TA_FLAG'  Variable_, TA_FLAG, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, TA_FLAG UNION
SELECT 'box_Type'  Variable_, box_Type, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, box_Type UNION
SELECT 'affluence'  Variable_, affluence, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, affluence UNION
SELECT 'life_stage'  Variable_, life_stage, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, life_stage UNION
SELECT 'kids'  Variable_, kids, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, kids UNION
SELECT 'h_mosaic_uk_group'  Variable_, h_mosaic_uk_group, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, h_mosaic_uk_group UNION
SELECT 'h_income_band_v2'  Variable_, h_income_band_v2, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, h_income_band_v2 UNION
SELECT 'hh_composition'  Variable_, hh_composition, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, hh_composition UNION
SELECT 'h_fss_v3_group'  Variable_, h_fss_v3_group, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, h_fss_v3_group UNION
SELECT 'h_age_coarse'  Variable_, h_age_coarse, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, h_age_coarse UNION
SELECT 'CQM_Score'  Variable_, CQM_Score, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, CQM_Score UNION
SELECT 'MR'  Variable_, MR, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, MR UNION
SELECT 'HDTV'  Variable_, HDTV, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, HDTV UNION
SELECT 'SkyProtect'  Variable_, SkyProtect, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, SkyProtect UNION
SELECT 'SkyGoExtra'  Variable_, SkyGoExtra, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, SkyGoExtra UNION
SELECT 'SkyPlus'  Variable_, SkyPlus, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, SkyPlus UNION
SELECT 'SkyGo_last_login_date'  Variable_, SkyGo_last_login_date, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, SkyGo_last_login_date UNION
SELECT 'total_skygo_logins'  Variable_, total_skygo_logins, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, total_skygo_logins UNION
SELECT 'BroadBand'  Variable_, BroadBand, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, BroadBand UNION
SELECT 'SkyTalk'  Variable_, SkyTalk, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, SkyTalk UNION
SELECT 'BB_type'  Variable_, BB_type, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, BB_type UNION
SELECT 'SkyTalk_type'  Variable_, SkyTalk_type, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, SkyTalk_type UNION
SELECT 'package_desc'  Variable_, package_desc, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, package_desc UNION
SELECT 'Pending_cancel_30days'  Variable_, Pending_cancel_30days, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, Pending_cancel_30days UNION
SELECT 'Pending_cancel_60days'  Variable_, Pending_cancel_60days, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, Pending_cancel_60days UNION
SELECT 'Pending_cancel_90days'  Variable_, Pending_cancel_90days, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, Pending_cancel_90days UNION
SELECT 'Active_Block_30days'  Variable_, Active_Block_30days, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, Active_Block_30days UNION
SELECT 'Active_Block_60days'  Variable_, Active_Block_60days, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, Active_Block_60days UNION
SELECT 'Active_Block_90days'  Variable_, Active_Block_90days, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, Active_Block_90days UNION
SELECT 'Movies_upgrade_last_30days'  Variable_, Movies_upgrade_last_30days, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, Movies_upgrade_last_30days UNION
SELECT 'Movies_upgrade_last_60days'  Variable_, Movies_upgrade_last_60days, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, Movies_upgrade_last_60days UNION
SELECT 'Movies_upgrade_last_90days'  Variable_, Movies_upgrade_last_90days, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, Movies_upgrade_last_90days UNION
SELECT 'Movies_upgrade_last_180days'  Variable_, Movies_upgrade_last_180days, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, Movies_upgrade_last_180days UNION
SELECT 'Movies_downgrade_last_30days'  Variable_, Movies_downgrade_last_30days, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, Movies_downgrade_last_30days UNION
SELECT 'Movies_downgrade_last_60days'  Variable_, Movies_downgrade_last_60days, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, Movies_downgrade_last_60days UNION
SELECT 'Movies_downgrade_last_90days'  Variable_, Movies_downgrade_last_90days, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, Movies_downgrade_last_90days UNION
SELECT 'Movies_downgrade_last_180days'  Variable_, Movies_downgrade_last_180days, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, Movies_downgrade_last_180days UNION
SELECT 'Sports_upgrade_last_30days'  Variable_, Sports_upgrade_last_30days, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, Sports_upgrade_last_30days UNION
SELECT 'Sports_upgrade_last_60days'  Variable_, Sports_upgrade_last_60days, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, Sports_upgrade_last_60days UNION
SELECT 'Sports_upgrade_last_90days'  Variable_, Sports_upgrade_last_90days, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, Sports_upgrade_last_90days UNION
SELECT 'Sports_upgrade_last_180days'  Variable_, Sports_upgrade_last_180days, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, Sports_upgrade_last_180days UNION
SELECT 'Sports_downgrade_last_30days'  Variable_, Sports_downgrade_last_30days, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, Sports_downgrade_last_30days UNION
SELECT 'Sports_downgrade_last_60days'  Variable_, Sports_downgrade_last_60days, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, Sports_downgrade_last_60days UNION
SELECT 'Sports_downgrade_last_90days'  Variable_, Sports_downgrade_last_90days, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, Sports_downgrade_last_90days UNION
SELECT 'Sports_downgrade_last_180days'  Variable_, Sports_downgrade_last_180days, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, Sports_downgrade_last_180days UNION
SELECT 'Product_Holding'  Variable_, Product_Holding, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, Product_Holding UNION
SELECT 'offer_length_DTV'  Variable_, offer_length_DTV, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, offer_length_DTV UNION
SELECT 'TA_ALL_3M'  Variable_, TA_ALL_3M, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, TA_ALL_3M UNION
SELECT 'TA_ALL_6M'  Variable_, TA_ALL_6M, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, TA_ALL_6M UNION
SELECT 'TA_ALL_9M'  Variable_, TA_ALL_9M, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, TA_ALL_9M UNION
SELECT 'TA_ALL_12M'  Variable_, TA_ALL_12M, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, TA_ALL_12M UNION
SELECT 'DTV_first_tenure_months'  Variable_, DTV_first_tenure_months, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, DTV_first_tenure_months UNION
SELECT 'BB_first_tenure_months'  Variable_, BB_first_tenure_months, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, BB_first_tenure_months UNION
SELECT 'age'  Variable_, age, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, age UNION
SELECT 'DTV_offer_rem_and_end_group'  Variable_, DTV_offer_rem_and_end_group, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, DTV_offer_rem_and_end_group UNION
SELECT 'BB_offer_rem_and_end_group'  Variable_, BB_offer_rem_and_end_group, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, BB_offer_rem_and_end_group UNION
SELECT 'Package_segment'  Variable_, Package_segment, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, Package_segment UNION

SELECT 'my_sky_login_30D'  Variable_, my_sky_login_30D, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, my_sky_login_30D UNION
SELECT 'my_sky_login_60D'  Variable_, my_sky_login_60D, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, my_sky_login_60D UNION
SELECT 'my_sky_login_90D'  Variable_, my_sky_login_90D, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, my_sky_login_90D UNION
SELECT 'my_sky_login_180D'  Variable_, my_sky_login_180D, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, my_sky_login_180D UNION
SELECT 'my_sky_login_360D'  Variable_, my_sky_login_360D, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, my_sky_login_360D UNION
SELECT 'OD_count_30D'  Variable_, OD_count_30D, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, OD_count_30D UNION
SELECT 'OD_count_60D'  Variable_, OD_count_60D, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, OD_count_60D UNION
SELECT 'OD_count_90D'  Variable_, OD_count_90D, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, OD_count_90D UNION
SELECT 'OD_count_180D'  Variable_, OD_count_180D, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, OD_count_180D UNION
SELECT 'OD_count_360D'  Variable_, OD_count_360D, TA_flag, count(*) hits FROM TA_DTV_FEB17_SAMPLE GROUP BY TA_Flag, OD_count_360D 
