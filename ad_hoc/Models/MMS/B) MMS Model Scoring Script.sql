last_MU_FP_date
Movies_Previous_Target_Upgrade_Type
TopTier_Last_Target_Upgrade_Date
TopTier_Mths_Since_Target_Downgrade
Movies_Last_Target_Upgrade_Offer_Date 
TopTier_Previous_Target_Upgrade_Type
TopTier_Mths_Since_Target_Downgrade_Grouped 

/*----------------------------------------------------------------------------------------------------------

      Monthly Model Scoring Script
      ----------------------------

      Author   : R Simmons
      Date     : 2015-08-14
      --------------------------------------
      --------------------------------------

--------------------------------------------------------------------------------
-- PART B - MODEL SCORING
--------------------------------------------------------------------------------
*/

--SUBMIT ALL CODE
-- 1. SPORTS (BASIC -> SPORTS) MODEL

alter table MMS_2017_06
 add sp_fp_sports_num_pat_12m_flag                             decimal(20,10)  default 0.00,
 add sp_fp_hd_sub                                              decimal(20,10)  default 0.00,
 add sp_fp_gender                                              decimal(20,10)  default 0.00,
 add sp_fp_skyplayer                                           decimal(20,10)  default 0.00,
 add sp_fp_sports_dtv_tenure                                   decimal(20,10)  default 0.00,
 add sp_fp_sports_previous_target_upgrade_type                 decimal(20,10)  default 0.00,
 add sp_fp_Sports_DG_Segment                                   decimal(20,10)  default 0.00,
 add sp_op_sports_num_pat_12m_flag                             decimal(20,10)  default 0.00,
 add sp_op_hd_sub                                              decimal(20,10)  default 0.00,
 add sp_op_gender                                              decimal(20,10)  default 0.00,
 add sp_op_skyplayer                                           decimal(20,10)  default 0.00,
 add sp_op_sports_dtv_tenure                                   decimal(20,10)  default 0.00,
 add sp_op_sports_previous_target_upgrade_type                 decimal(20,10)  default 0.00,
 add sp_op_Sports_DG_Segment                                   decimal(20,10)  default 0.00,

 add sp_fp_Score                                               decimal(20,10)  default 0.00,
 add sp_op_Score                                               decimal(20,10)  default 0.00


update MMS_2017_06
 set sp_fp_sports_num_pat_12m_flag              = case when Sports_num_pat_12m_Flag = 0 then 1.163 else 0 end,
     sp_fp_hd_sub                               = case when hd_sub = 0 then 0.954 else 0 end,
     sp_fp_gender                               = case when gender = 'M' then 0 else -0.818 end,
     sp_fp_skyplayer                            = case when skyplayer = 0 then -0.518 else 0 end,
     sp_fp_sports_dtv_tenure                    = case when datediff(mm,dtv_latest_act_date,observation_dt) <= 12  then 1.462
                                                        when datediff(mm,dtv_latest_act_date,observation_dt) <= 24  then 1.442
                                                        when datediff(mm,dtv_latest_act_date,observation_dt) <= 36  then 1.223
                                                        when datediff(mm,dtv_latest_act_date,observation_dt) <= 48  then -0.539
                                                        else 0 end,
     sp_fp_sports_previous_target_upgrade_type  = case when Sports_Previous_Target_Upgrade_Type = 'Never Upgrade' then -0.457
                                                       when Sports_Previous_Target_Upgrade_Type = 'Upgrade on FP' then -0.35
                                                        else 0 end,
     sp_fp_Sports_DG_Segment                    = case when Sports_DG_Segment = '0' then -1.977
                                                        when Sports_DG_Segment = 'L12M' then -0.702
                                                        when Sports_DG_Segment = 'L6M' then 0 end,

     sp_op_sports_num_pat_12m_flag              = case when Sports_num_pat_12m_Flag = 0 then 0.003 else 0 end,
     sp_op_hd_sub                               = case when hd_sub = 0 then 0.23 else 0 end,
     sp_op_gender                               = case when gender = 'M' then 0 else -0.474 end,
     sp_op_skyplayer                            = case when skyplayer = 0 then -0.8 else 0 end,
     sp_op_sports_dtv_tenure                    = case when datediff(mm,dtv_latest_act_date,observation_dt) <= 12  then 1.416
                                                        when datediff(mm,dtv_latest_act_date,observation_dt) <= 24  then 0.191
                                                        when datediff(mm,dtv_latest_act_date,observation_dt) <= 36  then 0.367
                                                        when datediff(mm,dtv_latest_act_date,observation_dt) <= 48  then 0.605
                                                                                                else 0 end,
     sp_op_sports_previous_target_upgrade_type  = case when Sports_Previous_Target_Upgrade_Type = 'Never Upgrade' then -1.139
                                                       when Sports_Previous_Target_Upgrade_Type = 'Upgrade on FP' then -0.729
                                                        else 0 end,
     sp_op_Sports_DG_Segment                    = case when Sports_DG_Segment = '0' then -1.793
                                                       when Sports_DG_Segment = 'L12M' then 0.837
                                                       when Sports_DG_Segment = 'L6M' then 0 end


update MMS_2017_06
 set sp_fp_Score =              -5.855 + sp_fp_sports_num_pat_12m_flag 
										+ sp_fp_hd_sub 
										+ sp_fp_gender 
										+ sp_fp_skyplayer 
										+ sp_fp_sports_dtv_tenure
										+ sp_fp_sports_previous_target_upgrade_type 
										+ sp_fp_Sports_DG_Segment,
     sp_op_score =              -3.081 + sp_op_sports_num_pat_12m_flag 
										+ sp_op_hd_sub 
										+ sp_op_gender 
										+ sp_op_skyplayer 
										+ sp_op_sports_dtv_tenure
                                       + sp_op_sports_previous_target_upgrade_type 
									   + sp_op_Sports_DG_Segment

-- 2. MOVIES (BASIC -> MOVIES) MODEL

alter table MMS_2017_06
 add mo_fp_Movies_Previous_Target_Upgrade_Type        decimal(20,10)  default 0.00,
 add mo_fp_skyplayer                                  decimal(20,10)  default 0.00,
 add mo_fp_Movies_sp_device_vol_12m_Cap               decimal(20,10)  default 0.00,
 add mo_fp_PPV_BAK_Flag                               decimal(20,10)  default 0.00,
 add mo_fp_Flag_Movies_DG_12m                         decimal(20,10)  default 0.00,

 add mo_op_sports_num_pat_12m_flag                    decimal(20,10)  default 0.00,
 add mo_op_Movies_Previous_Target_Upgrade_Type        decimal(20,10)  default 0.00,
 add mo_op_Movies_Num_pat_12m_Cap_3                   decimal(20,10)  default 0.00,
 add mo_op_Movies_sp_device_vol_12m_Cap               decimal(20,10)  default 0.00,
 add mo_op_PPV_BAK_Flag                               decimal(20,10)  default 0.00,
 add mo_op_Flag_Movies_DG_12m                         decimal(20,10)  default 0.00,

 add mo_fp_Score                                      decimal(20,10)  default 0.00,
 add mo_op_Score                                      decimal(20,10)  default 0.00


update MMS_2017_06
  set mo_fp_Movies_Previous_Target_Upgrade_Type         = case when Movies_Previous_Target_Upgrade_Type = 'Never Upgrade' then -0.947
                                                               when Movies_Previous_Target_Upgrade_Type = 'Upgrade on FP' then -0.371
                                                               when Movies_Previous_Target_Upgrade_Type = 'Upgrade on OP' then 0 end,
      mo_fp_skyplayer                                   = case when skyplayer = 0 then -0.529 else 0 end,
      mo_fp_Movies_sp_device_vol_12m_Cap                = case when Movies_sp_device_vol_12m_Cap = 'a) <=1' then 1.163
                                                               when Movies_sp_device_vol_12m_Cap = 'b)  =2' then 0.847
                                                               when Movies_sp_device_vol_12m_Cap = 'c) >2' then 0 end,
      mo_fp_PPV_BAK_Flag                                = case when PPV_BAK_Flag = 'Both'       then 1.237
                                                               when PPV_BAK_Flag = 'Either'     then 1.128
                                                               when PPV_BAK_Flag = 'None'       then 0 end,
      mo_fp_Flag_Movies_DG_12m                          = case when Flag_Movies_DG_12m = 0 then -1.433
                                                               when Flag_Movies_DG_12m = 1 then 0 end,

      mo_op_Movies_Previous_Target_Upgrade_Type         = case when Movies_Previous_Target_Upgrade_Type = 'Never Upgrade' then -0.661
                                                               when Movies_Previous_Target_Upgrade_Type = 'Upgrade on FP' then -0.26
                                                               when Movies_Previous_Target_Upgrade_Type = 'Upgrade on OP' then 0 end,
      mo_op_Movies_Num_pat_12m_Cap_3                    = case when Movies_Num_pat_12m_Cap_3 = 0 then -0.659
                                                               when Movies_Num_pat_12m_Cap_3 = 1 then 0 end,
      mo_op_Movies_sp_device_vol_12m_Cap                = case when Movies_sp_device_vol_12m_Cap = 'a) <=1' then -1.085
                                                               when Movies_sp_device_vol_12m_Cap = 'b)  =2' then -0.23
                                                               when Movies_sp_device_vol_12m_Cap = 'c) >2' then 0 end,
      mo_op_PPV_BAK_Flag                                = case when PPV_BAK_Flag = 'Both'       then 1.276
                                                               when PPV_BAK_Flag = 'Either'     then 0.496
                                                               when PPV_BAK_Flag = 'None'       then 0 end,
      mo_op_Flag_Movies_DG_12m                          = case when Flag_Movies_DG_12m = 0 then -0.833
                                                               when Flag_Movies_DG_12m = 1 then 0 end

update MMS_2017_06
  set mo_fp_Score               = - 5.327 + mo_fp_Movies_Previous_Target_Upgrade_Type 
										+ mo_fp_skyplayer 
										+ mo_fp_Movies_sp_device_vol_12m_Cap
										+ mo_fp_PPV_BAK_Flag + mo_fp_Flag_Movies_DG_12m,
      mo_op_score               = - 2.973 + mo_op_Movies_Previous_Target_Upgrade_Type 
											+ mo_op_Movies_Num_pat_12m_Cap_3 
											+ mo_op_Movies_sp_device_vol_12m_Cap
                                          + mo_op_PPV_BAK_Flag 
										  + mo_op_Flag_Movies_DG_12m


-- 3. BROADBAND UPGRADES (NON-FIBRE) MODEL

alter table MMS_2017_06
 add bb_fp_age                                          decimal(20,10)  default 0.00,
 add bb_fp_tenure                                       decimal(20,10)  default 0.00,
 add bb_fp_acct_sam_reg                                 decimal(20,10)  default 0.00,
 add bb_fp_nlp                                          decimal(20,10)  default 0.00,
 add bb_fp_affluence                                    decimal(20,10)  default 0.00,
 add bb_fp_tv_offer_expiry                              decimal(20,10)  default 0.00,
 add bb_fp_implied_local_loop                           decimal(20,10)  default 0.00,

 add bb_op_age                                          decimal(20,10)  default 0.00,
 add bb_op_tenure                                       decimal(20,10)  default 0.00,
 add bb_op_acct_sam_reg                                 decimal(20,10)  default 0.00,
 add bb_op_nlp                                          decimal(20,10)  default 0.00,
 add bb_op_affluence                                    decimal(20,10)  default 0.00,
 add bb_op_tv_offer_expiry                              decimal(20,10)  default 0.00,
 add bb_op_implied_local_loop                           decimal(20,10)  default 0.00,

 add bb_fp_Score                                        decimal(20,10)  default 0.00,
 add bb_op_Score                                        decimal(20,10)  default 0.00

update MMS_2017_06
 set bb_fp_age                                  = case when CL_Current_Age>=18 and CL_Current_Age<=30 then 1.367
                                                        when CL_Current_Age>30 and CL_Current_Age<=40 then 1.017
                                                        when CL_Current_Age>40 and CL_Current_Age<=50 then 0.514
                                                        when CL_Current_Age>50 and CL_Current_Age<=60 then 0.823
                                                        when CL_Current_Age>60 and CL_Current_Age is not null then 0.61
                                                        else 0 end,

    bb_fp_tenure                                = case when dtv_latest_act_date>=dateadd(mm, -12, observation_dt) then 0.755
                                                        when dtv_latest_act_date>=dateadd(mm, -24, observation_dt) then 0.755
                                                        when dtv_latest_act_date>=dateadd(mm, -36, observation_dt) then 0.181
                                                        when dtv_latest_act_date>=dateadd(mm, -48, observation_dt) then 0.181
                                                        when dtv_latest_act_date>=dateadd(mm, -60, observation_dt) then 0.181
                                                        when dtv_latest_act_date>=dateadd(mm, -72, observation_dt) then 0.28
                                                        when dtv_latest_act_date>=dateadd(mm, -96, observation_dt) then 0.28
                                                        when dtv_latest_act_date>=dateadd(mm, -120, observation_dt) then 0
                                                        else 0 end,
    bb_fp_acct_sam_reg                          = case when acct_sam_registered=1 then 0.123
                                                        else 0 end,
    bb_fp_nlp                                   = case when nlp =1 then 1.064
                                                        else 0 end,
    bb_fp_affluence                             = case when h_affluence_v2 in ('00')           then 0.911
                                                        when h_affluence_v2 in ('01','02','03') then 0.387
                                                        when h_affluence_v2 in ('04','05','06') then 0.815
                                                        when h_affluence_v2 in ('07','08','09') then 0.568
                                                        when h_affluence_v2 in ('10','11','12') then 0.229
                                                        when h_affluence_v2 in ('13','14','15') then 0.078
                                                        when h_affluence_v2 in ('16','17','18','19') then -0.049
                                                        when h_affluence_v2 in ('U') then 0
                                                        when h_affluence_v2 in ('Z') then 0
                                                        else 0 end,
    bb_fp_tv_offer_expiry                       = case when (tv_offer_end_dt-observation_dt)<=100 then 0.756
                                                       when (tv_offer_end_dt-observation_dt)<=1500 then 0.017
                                                       else 0 end,
    bb_fp_implied_local_loop                    = case when implied_local_loop <=2.220 then 0.326
                                                        when implied_local_loop <=2.980 then 0.161
                                                        when implied_local_loop <=3.890 then 0.349
                                                        when implied_local_loop <=4.890 then 0.424
                                                        else 0 end,

    bb_op_age                                  = case when CL_Current_Age>=18 and CL_Current_Age<=30 then 0.236
                                                        when CL_Current_Age>30 and CL_Current_Age<=40 then 0.219
                                                        when CL_Current_Age>40 and CL_Current_Age<=50 then -0.136
                                                        when CL_Current_Age>50 and CL_Current_Age<=60 then -0.018
                                                        when CL_Current_Age>60 and CL_Current_Age is not null then 0.117
                                                        else 0 end,

    bb_op_tenure                                = case when dtv_latest_act_date>=dateadd(mm, -12, observation_dt) then 0.722
                                                        when dtv_latest_act_date>=dateadd(mm, -24, observation_dt) then 0.722
                                                        when dtv_latest_act_date>=dateadd(mm, -36, observation_dt) then 0.571
                                                        when dtv_latest_act_date>=dateadd(mm, -48, observation_dt) then 0.571
                                                        when dtv_latest_act_date>=dateadd(mm, -60, observation_dt) then 0.571
                                                        when dtv_latest_act_date>=dateadd(mm, -72, observation_dt) then 0.119
                                                        when dtv_latest_act_date>=dateadd(mm, -96, observation_dt) then 0.119
                                                        when dtv_latest_act_date>=dateadd(mm, -120, observation_dt) then 0
                                                        else 0 end,
    bb_op_acct_sam_reg                          = case when acct_sam_registered=1 then 0.439
                                                        else 0 end,
    bb_op_nlp                                   = case when nlp =1 then 0.829
                                                        else 0 end,
    bb_op_affluence                             = case when h_affluence_v2 in ('00')           then 1.285
                                                        when h_affluence_v2 in ('01','02','03') then 0.251
                                                        when h_affluence_v2 in ('04','05','06') then 0.516
                                                        when h_affluence_v2 in ('07','08','09') then -0.008
                                                        when h_affluence_v2 in ('10','11','12') then -0.285
                                                        when h_affluence_v2 in ('13','14','15') then 0.103
                                                        when h_affluence_v2 in ('16','17','18','19') then -0.359
                                                        when h_affluence_v2 in ('U') then 0
                                                        when h_affluence_v2 in ('Z') then 0
                                                        else 0 end,
    bb_op_tv_offer_expiry                       = case when (tv_offer_end_dt-observation_dt)<=100 then 0.264
                                                       when (tv_offer_end_dt-observation_dt)<=1500 then -0.001
                                                       else 0 end,
    bb_op_implied_local_loop                    = case when implied_local_loop <=2.220 then 0.542
                                                       when implied_local_loop <=2.980 then 0.333
                                                       when implied_local_loop <=3.890 then 0.268
                                                       when implied_local_loop <=4.890 then 0.168
                                                       else 0 end
update MMS_2017_06
 set bb_fp_Score                = -8.042 + bb_fp_age + bb_fp_tenure + bb_fp_acct_sam_reg + bb_fp_nlp + bb_fp_affluence +
                                           bb_fp_tv_offer_expiry + bb_fp_implied_local_loop,
     bb_op_score                = -7.535 + bb_op_age + bb_op_tenure + bb_op_acct_sam_reg + bb_op_nlp + bb_op_affluence +
                                           bb_op_tv_offer_expiry + bb_op_implied_local_loop


-- 4. BROADBAND UPGRADES (FIBRE) MODEL

alter table MMS_2017_06
 add bf_fp_age                                          decimal(20,10)  default 0.00,
 add bf_fp_tenure                                       decimal(20,10)  default 0.00,
 add bf_fp_affluence                                    decimal(20,10)  default 0.00,
 add bf_fp_tv_offer_expiry                              decimal(20,10)  default 0.00,
 add bf_fp_sp_device_vol_12m                            decimal(20,10)  default 0.00,
 add bf_fp_ppv_count                                    decimal(20,10)  default 0.00,

 add bf_op_age                                          decimal(20,10)  default 0.00,
 add bf_op_tenure                                       decimal(20,10)  default 0.00,
 add bf_op_affluence                                    decimal(20,10)  default 0.00,
 add bf_op_tv_offer_expiry                              decimal(20,10)  default 0.00,
 add bf_op_sp_device_vol_12m                            decimal(20,10)  default 0.00,
 add bf_op_ppv_count                                    decimal(20,10)  default 0.00,

 add bf_fp_Score                                        decimal(20,10)  default 0.00,
 add bf_op_Score                                        decimal(20,10)  default 0.00


update MMS_2017_06
 set bf_fp_age                                  = case when CL_Current_Age>=18 and CL_Current_Age<=30 then 0.734
                                                        when CL_Current_Age>30 and CL_Current_Age<=40 then 0.931
                                                        when CL_Current_Age>40 and CL_Current_Age<=50 then 0.739
                                                        when CL_Current_Age>50 and CL_Current_Age<=60 then -0.151
                                                        when CL_Current_Age>60 and CL_Current_Age is not null then -0.097
                                                        else 0 end,

    bf_fp_tenure                                = case when dtv_latest_act_date>=dateadd(mm, -12, observation_dt) then 1.045
                                                        when dtv_latest_act_date>=dateadd(mm, -24, observation_dt) then 1.045
                                                        when dtv_latest_act_date>=dateadd(mm, -36, observation_dt) then 1.045
                                                        when dtv_latest_act_date>=dateadd(mm, -48, observation_dt) then 0.218
                                                        when dtv_latest_act_date>=dateadd(mm, -60, observation_dt) then 0.218
                                                        when dtv_latest_act_date>=dateadd(mm, -72, observation_dt) then 0.218
                                                        when dtv_latest_act_date>=dateadd(mm, -96, observation_dt) then 0
                                                        when dtv_latest_act_date>=dateadd(mm, -120, observation_dt) then 0
                                                        else 0 end,

    bf_fp_affluence                             = case when h_affluence_v2 in ('00')           then 1.908
                                                        when h_affluence_v2 in ('01','02','03') then 0.896
                                                        when h_affluence_v2 in ('04','05','06') then 0.461
                                                        when h_affluence_v2 in ('07','08','09') then -0.165
                                                        when h_affluence_v2 in ('10','11','12') then 1.059
                                                        when h_affluence_v2 in ('13','14','15') then 0.764
                                                        when h_affluence_v2 in ('16','17','18','19') then -0.053
                                                        when h_affluence_v2 in ('U') then 0
                                                        when h_affluence_v2 in ('Z') then 0
                                                        else 0 end,
    bf_fp_tv_offer_expiry                       = case when (tv_offer_end_dt-observation_dt)<=100 then 0.768
                                                       when (tv_offer_end_dt-observation_dt)<=1500 then 0.033
                                                       else 0 end,
    bf_fp_sp_device_vol_12m                     = case when sp_device_vol_12m=0 then -0.656
                                                        when sp_device_vol_12m=1 then 0
                                                        when sp_device_vol_12m=2 then 0
                                                        when sp_device_vol_12m=3 then 0
                                                        when sp_device_vol_12m>=4 then 0
                                                        else 0 end,
    bf_fp_ppv_count                             = case when ppv_count=0 then -0.903 else 0 end,

    bf_op_age                                  = case when CL_Current_Age>=18 and CL_Current_Age<=30 then 1.136
                                                        when CL_Current_Age>30 and CL_Current_Age<=40 then -0.411
                                                        when CL_Current_Age>40 and CL_Current_Age<=50 then 0.163
                                                        when CL_Current_Age>50 and CL_Current_Age<=60 then -0.385
                                                        when CL_Current_Age>60 and CL_Current_Age is not null then -0.186
                                                        else 0 end,

    bf_op_tenure                                = case when dtv_latest_act_date>=dateadd(mm, -12, observation_dt) then -0.226
                                                        when dtv_latest_act_date>=dateadd(mm, -24, observation_dt) then -0.226
                                                        when dtv_latest_act_date>=dateadd(mm, -36, observation_dt) then -0.226
                                                        when dtv_latest_act_date>=dateadd(mm, -48, observation_dt) then 0.111
                                                        when dtv_latest_act_date>=dateadd(mm, -60, observation_dt) then 0.111
                                                        when dtv_latest_act_date>=dateadd(mm, -72, observation_dt) then 0.111
                                                        when dtv_latest_act_date>=dateadd(mm, -96, observation_dt) then 0
                                                        when dtv_latest_act_date>=dateadd(mm, -120, observation_dt) then 0
                                                        else 0 end,

    bf_op_affluence                             = case when h_affluence_v2 in ('00')           then 1.718
                                                        when h_affluence_v2 in ('01','02','03') then 0.872
                                                        when h_affluence_v2 in ('04','05','06') then 0.392
                                                        when h_affluence_v2 in ('07','08','09') then 0.645
                                                        when h_affluence_v2 in ('10','11','12') then 0.915
                                                        when h_affluence_v2 in ('13','14','15') then 0.841
                                                        when h_affluence_v2 in ('16','17','18','19') then 0.915
                                                        when h_affluence_v2 in ('U') then 0
                                                        when h_affluence_v2 in ('Z') then 0
                                                        else 0 end,
    bf_op_tv_offer_expiry                       = case when (tv_offer_end_dt-observation_dt)<=100 then 0.585
                                                       when (tv_offer_end_dt-observation_dt)<=1500 then 0.215
                                                       else 0 end,
    bf_op_sp_device_vol_12m                     = case when sp_device_vol_12m=0 then -0.855
                                                        when sp_device_vol_12m=1 then 0
                                                        when sp_device_vol_12m=2 then 0
                                                        when sp_device_vol_12m=3 then 0
                                                        when sp_device_vol_12m>=4 then 0
                                                        else 0 end,
    bf_op_ppv_count                             = case when ppv_count=0 then 0.035 else 0 end


update MMS_2017_06
 set bf_fp_Score                = -8.378 + bf_fp_age + bf_fp_tenure + bf_fp_affluence + bf_fp_tv_offer_expiry + bf_fp_sp_device_vol_12m
                                         + bf_fp_ppv_count,
     bf_op_score                = -8.510 + bf_op_age + bf_op_tenure + bf_op_affluence + bf_op_tv_offer_expiry + bf_op_sp_device_vol_12m
                                         + bf_op_ppv_count



-- 5. FIBRE REGRADE MODEL

alter table MMS_2017_06
 add fr_fp_sp_device_vol_12m                             decimal(20,10)  default 0.00,
 add fr_fp_age                                           decimal(20,10)  default 0.00,
 add fr_fp_fibre_rollout                                 decimal(20,10)  default 0.00,
 add fr_fp_num_premium_upgrade_ever                      decimal(20,10)  default 0.00,
 add fr_fp_ppv_ever                                      decimal(20,10)  default 0.00,
 add fr_fp_implied_local_loop_length                     decimal(20,10)  default 0.00,

 add fr_op_sp_device_vol_12m                             decimal(20,10)  default 0.00,
 add fr_op_age                                           decimal(20,10)  default 0.00,
 add fr_op_fibre_rollout                                 decimal(20,10)  default 0.00,
 add fr_op_num_premium_upgrade_ever                      decimal(20,10)  default 0.00,
 add fr_op_ppv_ever                                      decimal(20,10)  default 0.00,
 add fr_op_implied_local_loop_length                     decimal(20,10)  default 0.00,

 add fr_fp_Score                                         decimal(20,10)  default 0.00,
 add fr_op_Score                                         decimal(20,10)  default 0.00


update MMS_2017_06
 set  fr_fp_age                                  = case when CL_Current_Age>=18 and CL_Current_Age<=30 then 0.02
                                                        when CL_Current_Age>30 and CL_Current_Age<=40 then 0.19
                                                        when CL_Current_Age>40 and CL_Current_Age<=50 then 0.239
                                                        when CL_Current_Age>50 and CL_Current_Age<=60 then -0.195
                                                        when CL_Current_Age>60 and CL_Current_Age is not null then -0.616
                                                        else 0 end,
     fr_fp_sp_device_vol_12m                     = case when sp_device_vol_12m=0 then -0.496
                                                        when sp_device_vol_12m=1 then -0.063
                                                        when sp_device_vol_12m=2 then -0.293
                                                        when sp_device_vol_12m=3 then 0
                                                        when sp_device_vol_12m>=4 then 0
                                                        else 0 end,
     fr_fp_ppv_ever                              = case when ppv_count=0 then -0.185
                                                        when ppv_count=1 then -0.068 else 0 end,
     fr_fp_implied_local_loop_length             = case when implied_local_loop<2.31 then -0.942
                                                        when implied_local_loop<3    then -0.474
                                                        when implied_local_loop<3.77 then -0.483
                                                        when implied_local_loop<4.65 then -0.41
                                                        else 0 end,
     fr_fp_num_premium_upgrade_ever              = case when num_premium_upgrade_ever is null then -0.458
                                                        when num_premium_upgrade_ever=0  then -0.458
                                                        when num_premium_upgrade_ever=1  then -0.4
                                                        when num_premium_upgrade_ever=2  then  0.144
                                                        else 0 end,
     fr_fp_fibre_rollout                         = case when datediff(day, x_skyfibre_enabled_date, observation_dt)>=365 then -0.409
                                                        else 0 end,

     fr_op_age                                  = case when CL_Current_Age>=18 and CL_Current_Age<=30 then -0.33
                                                        when CL_Current_Age>30 and CL_Current_Age<=40 then 0.14
                                                        when CL_Current_Age>40 and CL_Current_Age<=50 then 0.45
                                                        when CL_Current_Age>50 and CL_Current_Age<=60 then 0.524
                                                        when CL_Current_Age>60 and CL_Current_Age is not null then 0.566
                                                        else 0 end,
     fr_op_sp_device_vol_12m                     = case when sp_device_vol_12m=0 then -0.38
                                                        when sp_device_vol_12m=1 then -0.076
                                                        when sp_device_vol_12m=2 then -0.194
                                                        when sp_device_vol_12m=3 then 0
                                                        when sp_device_vol_12m>=4 then 0
                                                        else 0 end,
     fr_op_ppv_ever                              = case when ppv_count=0 then -0.39
                                                        when ppv_count=1 then -0.155 else 0 end,
     fr_op_implied_local_loop_length             = case when implied_local_loop<2.31 then -0.515
                                                        when implied_local_loop<3    then -0.425
                                                        when implied_local_loop<3.77 then -0.281
                                                        when implied_local_loop<4.65 then -0.132
                                                        else 0 end,
     fr_op_num_premium_upgrade_ever              = case when num_premium_upgrade_ever is null  then -0.476
                                                        when num_premium_upgrade_ever=0  then -0.476
                                                        when num_premium_upgrade_ever=1  then -0.327
                                                        when num_premium_upgrade_ever=2  then  0.403
                                                        else 0 end,
     fr_op_fibre_rollout                         = case when datediff(day, x_skyfibre_enabled_date, observation_dt)>=365 then -0.417
                                                        else 0 end

update MMS_2017_06
set fr_fp_Score                =  -4.365 
+ fr_fp_age 
+ fr_fp_sp_device_vol_12m 
+ fr_fp_ppv_ever 
+ fr_fp_implied_local_loop_length
+ fr_fp_num_premium_upgrade_ever 
+ fr_fp_fibre_rollout
,     fr_op_Score                =  -4.849 
+ fr_op_age 
+ fr_op_sp_device_vol_12m 
+ fr_op_ppv_ever 
+ fr_op_implied_local_loop_length
+ fr_op_num_premium_upgrade_ever 
+ fr_op_fibre_rollout


-- 6. SKY GO EXTRA MODEL

alter table MMS_2017_06
 add sge_fp_age                                         decimal(20,10)  default 0.00,
 add sge_fp_cvs_Segment                                 decimal(20,10)  default 0.00,
 add sge_fp_sp_device_vol_12m                           decimal(20,10)  default 0.00,
 add sge_fp_movies_num_upgrade_24m                      decimal(20,10)  default 0.00,
 add sge_fp_num_cust_calls_in_12m                       decimal(20,10)  default 0.00,
 add sge_op_age                                         decimal(20,10)  default 0.00,
 add sge_op_cvs_Segment                                 decimal(20,10)  default 0.00,
 add sge_op_sp_device_vol_12m                           decimal(20,10)  default 0.00,
 add sge_op_movies_num_upgrade_24m                      decimal(20,10)  default 0.00,
 add sge_op_num_cust_calls_in_12m                       decimal(20,10)  default 0.00,

 add sge_fp_Score                                       decimal(20,10)  default 0.00,
 add sge_op_Score                                       decimal(20,10)  default 0.00


 update MMS_2017_06
 set  sge_fp_age                                = case when CL_Current_Age>=18 and CL_Current_Age<=30 then 0.89
                                                        when CL_Current_Age>30 and CL_Current_Age<=40 then 0.773
                                                        when CL_Current_Age>40 and CL_Current_Age<=50 then 0.752
                                                        when CL_Current_Age>50 and CL_Current_Age<=60 then 0.518
                                                        when CL_Current_Age>60 and CL_Current_Age is not null then 0.17
                                                        else 0 end,
     sge_fp_sp_device_vol_12m                   = case when sp_device_vol_12m=0 then -1.141
                                                        when sp_device_vol_12m=1 then -0.642
                                                        when sp_device_vol_12m=2 then -0.511
                                                        when sp_device_vol_12m=3 then -0.275
                                                        when sp_device_vol_12m>=4 then 0
                                                        else 0 end,
     sge_fp_cvs_Segment                         = case when cvs_segment = 'BEDDING IN' then 0.046
                                                        when cvs_segment = 'BRONZE'     then -0.243
                                                        when cvs_segment = 'COPPER'     then -0.461
                                                        when cvs_segment = 'GOLD'       then -0.347
                                                        when cvs_segment = 'PLATINUM'   then -0.146
                                                        when cvs_segment = 'SILVER'     then -0.053
                                                        else 0 end,
     sge_fp_movies_num_upgrade_24m              = case when num_movies_num_upgrade_24m is null then -0.51
                                                        when num_movies_num_upgrade_24m =0 then -0.51
                                                        when num_movies_num_upgrade_24m =1 then -0.177
                                                        when num_movies_num_upgrade_24m =2 then -0.011
                                                        else 0 end,
     sge_fp_num_cust_calls_in_12m               = case when num_cust_calls_in_12m = 0 then -0.641
                                                        when num_cust_calls_in_12m = 1 then -0.474
                                                        when num_cust_calls_in_12m = 2 then -0.456
                                                        when num_cust_calls_in_12m = 3 then -0.352
                                                        when num_cust_calls_in_12m = 4 then -0.196
                                                        when num_cust_calls_in_12m = 5 then -0.35
                                                        when num_cust_calls_in_12m >= 6 then 0
                                                        else 0 end,

     sge_op_age                                 = case when CL_Current_Age>=18 and CL_Current_Age<=30 then 1.049
                                                        when CL_Current_Age>30 and CL_Current_Age<=40 then 0.964
                                                        when CL_Current_Age>40 and CL_Current_Age<=50 then 0.894
                                                        when CL_Current_Age>50 and CL_Current_Age<=60 then 0.712
                                                        when CL_Current_Age>60 and CL_Current_Age is not null then 0.588
                                                        else 0 end,
     sge_op_sp_device_vol_12m                   = case when sp_device_vol_12m=0 then -2.454
                                                        when sp_device_vol_12m=1 then -1.496
                                                        when sp_device_vol_12m=2 then -1.064
                                                        when sp_device_vol_12m=3 then -0.519
                                                        when sp_device_vol_12m>=4 then 0
                                                        else 0 end,
     sge_op_cvs_Segment                         = case when cvs_segment = 'BEDDING IN' then 0.377
                                                        when cvs_segment = 'BRONZE'     then -0.141
                                                        when cvs_segment = 'COPPER'     then -0.348
                                                        when cvs_segment = 'GOLD'       then -0.014
                                                        when cvs_segment = 'PLATINUM'   then -0.096
                                                        when cvs_segment = 'SILVER'     then  0.013
                                                        else 0 end,
     sge_op_movies_num_upgrade_24m              = case when num_movies_num_upgrade_24m is null then -0.595
                                                        when num_movies_num_upgrade_24m =0 then -0.595
                                                        when num_movies_num_upgrade_24m =1 then -0.375
                                                        when num_movies_num_upgrade_24m =2 then -0.151
                                                        else 0 end,
     sge_op_num_cust_calls_in_12m               = case when num_cust_calls_in_12m = 0 then -0.463
                                                        when num_cust_calls_in_12m = 1 then -0.299
                                                        when num_cust_calls_in_12m = 2 then -0.256
                                                        when num_cust_calls_in_12m = 3 then -0.189
                                                        when num_cust_calls_in_12m = 4 then -0.179
                                                        when num_cust_calls_in_12m = 5 then -0.272
                                                        when num_cust_calls_in_12m >= 6 then 0
                                                        else 0 end

update MMS_2017_06
set sge_fp_Score                       =  - 4.45 
+ sge_fp_age 
+ sge_fp_sp_device_vol_12m 
+ sge_fp_cvs_Segment 
+ sge_fp_movies_num_upgrade_24m
+ sge_fp_num_cust_calls_in_12m,
sge_op_Score                       = - 4.401 
+ sge_op_age 
+ sge_op_sp_device_vol_12m 
+ sge_op_cvs_Segment 
+ sge_op_movies_num_upgrade_24m
+ sge_op_num_cust_calls_in_12m


-- 7. TOP TIER (DM -> TOP TIER)

alter table MMS_2017_06
 add TTDM_fp_age                                         decimal(20,10)  default 0.00,
 add TTDM_fp_last_MU_FP_grp                              decimal(20,10)  default 0.00,
 add TTDM_fp_last_SU_FP_grp                              decimal(20,10)  default 0.00,
 add TTDM_fp_tenure                                      decimal(20,10)  default 0.00,
 add TTDM_fp_sports_num_downgrade                        decimal(20,10)  default 0.00,
 add TTDM_fp_num_sports_events                           decimal(20,10)  default 0.00,
 add TTDM_fp_rs_tv_offer_end                             decimal(20,10)  default 0.00,
 add TTDM_fp_lr                                          decimal(20,10)  default 0.00,
 add TTDM_op_age                                         decimal(20,10)  default 0.00,
 add TTDM_op_last_MU_FP_grp                              decimal(20,10)  default 0.00,
 add TTDM_op_last_SU_FP_grp                              decimal(20,10)  default 0.00,
 add TTDM_op_tenure                                      decimal(20,10)  default 0.00,
 add TTDM_op_sports_num_downgrade                        decimal(20,10)  default 0.00,
 add TTDM_op_num_sports_events                           decimal(20,10)  default 0.00,
 add TTDM_op_rs_tv_offer_end                             decimal(20,10)  default 0.00,
 add TTDM_op_lr                                          decimal(20,10)  default 0.00,

 add TTDM_fp_Score                                       decimal(20,10)  default 0.00,
 add TTDM_op_Score                                       decimal(20,10)  default 0.00


 update MMS_2017_06
 set  TTDM_fp_age                               = case when CL_Current_Age>=18 and CL_Current_Age<=30 then 0.905
                                                        when CL_Current_Age>30 and CL_Current_Age<=40 then 0.846
                                                        when CL_Current_Age>40 and CL_Current_Age<=50 then 0.809
                                                        when CL_Current_Age>50 and CL_Current_Age<=60 then 0.513
                                                        when CL_Current_Age>60 and CL_Current_Age is not null then 0.222
                                                        else 0 end,
      TTDM_fp_last_MU_FP_grp                    = case when last_MU_FP_date is not null and last_MU_FP_date>=(observation_dt-90) then 0.915
                                                        when last_MU_FP_date is not null and last_MU_FP_date>=(observation_dt-180) then 0.362
                                                        when last_MU_FP_date is not null and last_MU_FP_date>=(observation_dt-360) then 0.275
                                                        when last_MU_FP_date is not null then 0.203
                                                        else 0 end,
      TTDM_fp_last_SU_FP_grp                    = case when last_SU_FP_date is not null and last_SU_FP_date>=(observation_dt-90) then -0.036
                                                        when last_SU_FP_date is not null and last_SU_FP_date>=(observation_dt-180) then 0.157
                                                        when last_SU_FP_date is not null and last_SU_FP_date>=(observation_dt-360) then 0.169
                                                        when last_SU_FP_date is not null then -0.171
                                                        else 0 end,
      TTDM_fp_tenure                            = case when dtv_latest_act_date>=dateadd(mm, -12, observation_dt) then 1.031
                                                        when dtv_latest_act_date>=dateadd(mm, -24, observation_dt) then 0.474
                                                        when dtv_latest_act_date>=dateadd(mm, -36, observation_dt) then 0.316
                                                        when dtv_latest_act_date>=dateadd(mm, -48, observation_dt) then 0.102
                                                        when dtv_latest_act_date>=dateadd(mm, -60, observation_dt) then 0.096
                                                        when dtv_latest_act_date>=dateadd(mm, -72, observation_dt) then 0.06
                                                        when dtv_latest_act_date>=dateadd(mm, -96, observation_dt) then -0.026
                                                        when dtv_latest_act_date>=dateadd(mm, -120, observation_dt) then 0.019
                                                        else 0 end,
      TTDM_fp_sports_num_downgrade              = case when num_sports_num_downgrade_24m is not null then num_sports_num_downgrade_24m * 0.86
                                                        else 0 end,
      TTDM_fp_num_sports_events                 = num_sports_events    * 0.455,
      TTDM_fp_rs_tv_offer_end                   = case when (tv_offer_end_dt-observation_dt)<=100 then 0.017
                                                       when (tv_offer_end_dt-observation_dt)<=300 then 0.081
                                                       when (tv_offer_end_dt-observation_dt)<=600 then 0.235
                                                       else 0 end,
      TTDM_fp_lr                                = wlr *-0.162,

      TTDM_op_age                               = case when CL_Current_Age>=18 and CL_Current_Age<=30 then 0.672
                                                        when CL_Current_Age>30 and CL_Current_Age<=40 then 0.688
                                                        when CL_Current_Age>40 and CL_Current_Age<=50 then 0.718
                                                        when CL_Current_Age>50 and CL_Current_Age<=60 then 0.522
                                                        when CL_Current_Age>60 and CL_Current_Age is not null then 0.033
                                                        else 0 end,
      TTDM_op_last_MU_FP_grp                    = case when last_MU_FP_date is not null and last_MU_FP_date>=(observation_dt-90) then 0.779
                                                        when last_MU_FP_date is not null and last_MU_FP_date>=(observation_dt-180) then 0.03
                                                        when last_MU_FP_date is not null and last_MU_FP_date>=(observation_dt-360) then 0.066
                                                        when last_MU_FP_date is not null then 0.053
                                                        else 0 end,
      TTDM_op_last_SU_FP_grp                    = case when last_SU_FP_date is not null and last_SU_FP_date>=(observation_dt-90) then 0.218
                                                        when last_SU_FP_date is not null and last_SU_FP_date>=(observation_dt-180) then 0.748
                                                        when last_SU_FP_date is not null and last_SU_FP_date>=(observation_dt-360) then 0.597
                                                        when last_SU_FP_date is not null then 0.067
                                                        else 0 end,
      TTDM_op_tenure                            = case when dtv_latest_act_date>=dateadd(mm, -12, observation_dt) then 0.18
                                                        when dtv_latest_act_date>=dateadd(mm, -24, observation_dt) then 0.166
                                                        when dtv_latest_act_date>=dateadd(mm, -36, observation_dt) then 0.08
                                                        when dtv_latest_act_date>=dateadd(mm, -48, observation_dt) then 0.164
                                                        when dtv_latest_act_date>=dateadd(mm, -60, observation_dt) then 0.22
                                                        when dtv_latest_act_date>=dateadd(mm, -72, observation_dt) then 0.184
                                                        when dtv_latest_act_date>=dateadd(mm, -96, observation_dt) then 0.157
                                                        when dtv_latest_act_date>=dateadd(mm, -120, observation_dt) then 0.063
                                                        else 0 end,
      TTDM_op_sports_num_downgrade              = case when num_sports_num_downgrade_24m is not null then num_sports_num_downgrade_24m * 0.759
                                                        else 0 end,
      TTDM_op_num_sports_events                 = num_sports_events    * 0.423,
      TTDM_op_rs_tv_offer_end                   = case when (tv_offer_end_dt-observation_dt)<=100 then -0.272
                                                       when (tv_offer_end_dt-observation_dt)<=300 then -0.759
                                                       when (tv_offer_end_dt-observation_dt)<=600 then -0.860
                                                       else 0 end,
      TTDM_op_lr                                = wlr *0.572


update MMS_2017_06
 set TTDM_fp_Score                       =  -6.68  
+ TTDM_fp_age 
+ TTDM_fp_last_MU_FP_grp 
+ TTDM_fp_last_SU_FP_grp 
+ TTDM_fp_tenure
+ TTDM_fp_sports_num_downgrade 
+ TTDM_fp_num_sports_events 
+ TTDM_fp_rs_tv_offer_end 
+ TTDM_fp_lr ,
TTDM_op_Score                       =  -6.904 
+ TTDM_op_age 
+ TTDM_op_last_MU_FP_grp 
+ TTDM_op_last_SU_FP_grp 
+ TTDM_op_tenure
+ TTDM_op_sports_num_downgrade 
+ TTDM_op_num_sports_events 
+ TTDM_op_rs_tv_offer_end 
+ TTDM_op_lr



-- 8. TOP TIER (GENERAL)

alter table MMS_2017_06
 add TT_fp_TopTier_DTV_Tenure_Grouped                                     decimal(20,10)  default 0.00,
 add TT_fp_TopTier_Mths_Since_Target_Downgrade_Grouped                    decimal(20,10)  default 0.00,
 add TT_fp_TopTier_Previous_Target_Upgrade_Type                           decimal(20,10)  default 0.00,
 add TT_fp_TopTier_num_pat_12m_Cap_3                                      decimal(20,10)  default 0.00,
 add TT_fp_TopTier_sp_device_vol_12m_Cap4                                 decimal(20,10)  default 0.00,

 add TT_op_TopTier_DTV_Tenure_Grouped                                     decimal(20,10)  default 0.00,
 add TT_op_TopTier_Mths_Since_Target_Downgrade_Grouped                    decimal(20,10)  default 0.00,
 add TT_op_TopTier_Previous_Target_Upgrade_Type                           decimal(20,10)  default 0.00,
 add TT_op_TopTier_num_pat_12m_Cap_3                                      decimal(20,10)  default 0.00,
 add TT_op_TopTier_sp_device_vol_12m_Cap4                                 decimal(20,10)  default 0.00,

 add TT_fp_Score                                       decimal(20,10)  default 0.00,
 add TT_op_Score                                       decimal(20,10)  default 0.00

update MMS_2017_06
 set TT_fp_TopTier_DTV_Tenure_Grouped                   = case when dtv_latest_act_date is null then 1.577 -- '1) <= 3 Mths'
                                                                when datediff(mm,dtv_latest_act_date,observation_dt) <= 3 then 1.577  -- then '1) <= 3 Mths'
                                                                when datediff(mm,dtv_latest_act_date,observation_dt) <= 12 then 1.156 -- then '2) 4-12 Mths'
                                                                when datediff(mm,dtv_latest_act_date,observation_dt) <= 24 then 0.601 -- then '4) 13-24 Mths'
                                                                when datediff(mm,dtv_latest_act_date,observation_dt) >  24  then 0 -- '5) > 24 Mths'
                                                                                   else 0 end,
     TT_fp_TopTier_Mths_Since_Target_Downgrade_Grouped = case when TopTier_Mths_Since_Target_Downgrade_Grouped = 'a) DG  <= 2M' then 2.188
                                                              when TopTier_Mths_Since_Target_Downgrade_Grouped = 'b) DG  3-6M' then 1.552
                                                              when TopTier_Mths_Since_Target_Downgrade_Grouped = 'c) DG 7-18M' then 1.42
                                                              when TopTier_Mths_Since_Target_Downgrade_Grouped = 'd) DG 18+ / Never' then 0 end,
     TT_fp_TopTier_Previous_Target_Upgrade_Type         = case when TopTier_Previous_Target_Upgrade_Type = 'Never Upgrade' then 0.364
                                                                when TopTier_Previous_Target_Upgrade_Type = 'Upgrade on FP' then 0.015
                                                                when TopTier_Previous_Target_Upgrade_Type = 'Upgrade on OP' then 0 end,
     TT_fp_TopTier_num_pat_12m_Cap_3                    = case when num_pat_12m is null then -0.836
                                                                when num_pat_12m = 0 then -0.836
                                                                when num_pat_12m = 1 then -0.905
                                                                when num_pat_12m >= 2 then 0 end,
     TT_fp_TopTier_sp_device_vol_12m_Cap4               = case when sp_device_vol_12m = 0 then -0.997
                                                                when sp_device_vol_12m = 1 then -0.074
                                                                when sp_device_vol_12m = 2 then -0.232
                                                                when sp_device_vol_12m = 3 then 0.139
                                                                when sp_device_vol_12m >=4 then 0 end,

     TT_op_TopTier_DTV_Tenure_Grouped                   = case when dtv_latest_act_date is null then -0.407 -- '1) <= 3 Mths'
                                                                when datediff(mm,dtv_latest_act_date,observation_dt) <= 3 then -0.407  -- then '1) <= 3 Mths'
                                                                when datediff(mm,dtv_latest_act_date,observation_dt) <= 12 then -0.176 -- then '2) 4-12 Mths'
                                                                when datediff(mm,dtv_latest_act_date,observation_dt) <= 24 then 0.202 -- then '4) 13-24 Mths'
                                                                when datediff(mm,dtv_latest_act_date,observation_dt) >  24  then 0 -- '5) > 24 Mths'
                                                                                   else 0 end,
     TT_op_TopTier_Mths_Since_Target_Downgrade_Grouped = case when TopTier_Mths_Since_Target_Downgrade_Grouped = 'a) DG  <= 2M' then 1.724
                                                              when TopTier_Mths_Since_Target_Downgrade_Grouped = 'b) DG  3-6M' then 1.411
                                                              when TopTier_Mths_Since_Target_Downgrade_Grouped = 'c) DG 7-18M' then 0.857
                                                              when TopTier_Mths_Since_Target_Downgrade_Grouped = 'd) DG 18+ / Never' then 0 end,
     TT_op_TopTier_Previous_Target_Upgrade_Type         = case when TopTier_Previous_Target_Upgrade_Type = 'Never Upgrade' then -0.398
                                                                when TopTier_Previous_Target_Upgrade_Type = 'Upgrade on FP' then 0.166
                                                                when TopTier_Previous_Target_Upgrade_Type = 'Upgrade on OP' then 0 end,
     TT_op_TopTier_num_pat_12m_Cap_3                    = case when num_pat_12m is null then -0.61
                                                                when num_pat_12m = 0 then -0.61
                                                                when num_pat_12m = 1 then -0.336
                                                                when num_pat_12m >= 2 then 0 end,
     TT_op_TopTier_sp_device_vol_12m_Cap4               = case when sp_device_vol_12m = 0 then -0.571
                                                                when sp_device_vol_12m = 1 then -0.191
                                                                when sp_device_vol_12m = 2 then -0.124
                                                                when sp_device_vol_12m = 3 then -0.091
                                                                when sp_device_vol_12m >=4 then 0 end

update MMS_2017_06
set TT_fp_Score                       =  -6.89  
+ TT_fp_TopTier_DTV_Tenure_Grouped 
+ TT_fp_TopTier_Mths_Since_Target_Downgrade_Grouped 
+ TT_fp_TopTier_Previous_Target_Upgrade_Type
+ TT_fp_TopTier_num_pat_12m_Cap_3 
+ TT_fp_TopTier_sp_device_vol_12m_Cap4
,
TT_op_Score                       =  -5.426 
+ TT_op_TopTier_DTV_Tenure_Grouped 
+ TT_op_TopTier_Mths_Since_Target_Downgrade_Grouped 
+ TT_op_TopTier_Previous_Target_Upgrade_Type
+ TT_op_TopTier_num_pat_12m_Cap_3 
+ TT_op_TopTier_sp_device_vol_12m_Cap4


-- 9.  EM MODEL

select
   a.account_number
  ,Observation_dt
  ,count(*) as n
  ,sum(b.x_email_opened) as opened
into #temp
from MMS_2017_06 as a inner join email_event_outcome_summary as b
    on a.account_number = b.Sky_Account_Number
   and b.timesent <  a.Observation_dt
   and b.timesent >= a.Observation_dt-(7*8)
Group by
   a.account_number
  ,Observation_dt



Alter Table MMS_2017_06
 add N_EMail_L8W Integer default 0,
 add N_Open_Email_L8W Integer default 0,
 Add Ratio_Email_Open Varchar(20),
 Add Email_Open_Propensity Varchar(20),
 Add Modeled_channel_Preference Varchar(20)

Update MMS_2017_06 as b
Set b.N_EMail_L8W = s.n
   ,b.N_Open_Email_L8W = s.opened
from #Temp as s
Where b.account_number=s.account_number



Update MMS_2017_06
Set Ratio_Email_Open = case
      when N_EMail_L8W <= 4      then 'a) <4 EM in L8Wks'
      when N_Open_Email_L8W = 0 then 'b) 00%'
      when cast(cast(N_Open_Email_L8W as decimal(5,2))/cast(N_EMail_L8W as decimal(5,2))*100 as decimal (5,0)) <=  25 then 'c)  1%-25%'
      when cast(cast(N_Open_Email_L8W as decimal(5,2))/cast(N_EMail_L8W as decimal(5,2))*100 as decimal (5,0)) <=  50 then 'd) 25%-50%'
      when cast(cast(N_Open_Email_L8W as decimal(5,2))/cast(N_EMail_L8W as decimal(5,2))*100 as decimal (5,0)) <=  75 then 'e) 50%-75%'
      when cast(cast(N_Open_Email_L8W as decimal(5,2))/cast(N_EMail_L8W as decimal(5,2))*100 as decimal (5,0))   > 75 then 'f) >75%'
   end


Update MMS_2017_06
Set Email_Open_Propensity = case when Ratio_Email_Open = 'b) 00%' then 'Low' else 'High/Med' end



-- 10. MULTISCREEN

alter table MMS_2017_06
 add MS_fp_SkyPlayer_Tenure_Grouped                                             decimal(20,10)  default 0.00,
 add MS_fp_MultiScreen_Age_Band_Grouped                                         decimal(20,10)  default 0.00,
 add MS_fp_MultiScreen_HD_Upg_DG_Upgrade_Type                                   decimal(20,10)  default 0.00,
 add MS_fp_PremMovies2                                                          decimal(20,10)  default 0.00,
 add MS_fp_MultiScreen_num_cust_calls_in_12m_band                               decimal(20,10)  default 0.00,

 add MS_op_SkyPlayer_Tenure_Grouped                                             decimal(20,10)  default 0.00,
 add MS_op_MultiScreen_Age_Band_Grouped                                         decimal(20,10)  default 0.00,
 add MS_op_MultiScreen_HD_Upg_DG_Upgrade_Type                                   decimal(20,10)  default 0.00,
 add MS_op_PremMovies2                                                          decimal(20,10)  default 0.00,
 add MS_op_MultiScreen_num_cust_calls_in_12m_band                               decimal(20,10)  default 0.00,

 add MS_fp_Score                                       decimal(20,10)  default 0.00,
 add MS_op_Score                                       decimal(20,10)  default 0.00


update MMS_2017_06
 set MS_fp_SkyPlayer_Tenure_Grouped                   = case when SkyPlayer = 0 then -0.301
                                                             when SkyPlayer = 1 then 0 end,
     MS_fp_MultiScreen_Age_Band_Grouped               = case when CL_Current_Age is null  then 0
                                                                when CL_Current_Age <= 30    then 1.11
                                                                when CL_Current_Age <= 40    then 0.669
                                                                when CL_Current_Age <= 50    then 0.771
                                                                when CL_Current_Age <= 60    then 0.307
                                                                else 0 end,
     MS_fp_MultiScreen_HD_Upg_DG_Upgrade_Type         = case when HD_upgrade = 1 or HD_downgrade = 1 then 0 else -0.329 end,
     MS_fp_PremMovies2                                = case when PremMovies2 = 0 then -0.445
                                                             when PremMovies2 = 1 then 0 end,
     MS_fp_MultiScreen_num_cust_calls_in_12m_band     = case when num_cust_calls_in_12m =  0 or num_cust_calls_in_12m is null then -1.17
                                                             when num_cust_calls_in_12m <= 3 then -0.758
                                                             when num_cust_calls_in_12m <= 9 then -0.57
                                                             when num_cust_calls_in_12m  > 9 then 0 end,

     MS_op_SkyPlayer_Tenure_Grouped                   = case when SkyPlayer = 0 then -0.661
                                                             when SkyPlayer = 1 then 0 end,
     MS_op_MultiScreen_Age_Band_Grouped               = case when CL_Current_Age is null  then 0
                                                                when CL_Current_Age <= 30    then 1.114
                                                                when CL_Current_Age <= 40    then 1.034
                                                                when CL_Current_Age <= 50    then 0.699
                                                                when CL_Current_Age <= 60    then 0.955
                                                                else 0 end,
     MS_op_MultiScreen_HD_Upg_DG_Upgrade_Type         = case when HD_upgrade = 1 or HD_downgrade = 1 then 0 else -0.18 end,
     MS_op_PremMovies2                                = case when PremMovies2 = 0 then -0.191
                                                             when PremMovies2 = 1 then 0 end,
     MS_op_MultiScreen_num_cust_calls_in_12m_band     = case when num_cust_calls_in_12m =  0 or num_cust_calls_in_12m is null then -0.707
                                                             when num_cust_calls_in_12m <= 3 then -0.581
                                                             when num_cust_calls_in_12m <= 9 then -0.128
                                                             when num_cust_calls_in_12m  > 9 then 0 end


update MMS_2017_06
 set MS_fp_Score =  -5.125  
+ MS_fp_SkyPlayer_Tenure_Grouped 
+ MS_fp_MultiScreen_Age_Band_Grouped 
+ MS_fp_MultiScreen_HD_Upg_DG_Upgrade_Type 
+ MS_fp_PremMovies2
+ MS_fp_MultiScreen_num_cust_calls_in_12m_band
,
MS_op_Score =  -6.875  
+ MS_op_SkyPlayer_Tenure_Grouped 
+ MS_op_MultiScreen_Age_Band_Grouped 
+ MS_op_MultiScreen_HD_Upg_DG_Upgrade_Type 
+ MS_op_PremMovies2
+ MS_op_MultiScreen_num_cust_calls_in_12m_band


-- 11. TOP TIER (DS -> TT)

alter table MMS_2017_06
 add TTDS_fp_AGE_Grouped                                                decimal(20,10)  default 0.00,
 add TTDS_fp_TENURE_Grouped                                             decimal(20,10)  default 0.00,
 add TTDS_fp_NUM_PREMIUM_UPGRADE_EVER_Grouped                           decimal(20,10)  default 0.00,
 add TTDS_fp_OD_RF_Grouped                                              decimal(20,10)  default 0.00,
 add TTDS_fp_MOVIES_NUM_DOWNGRADE_24M_Grouped                           decimal(20,10)  default 0.00,

 add TTDS_op_AGE_Grouped                                                decimal(20,10)  default 0.00,
 add TTDS_op_TENURE_Grouped                                             decimal(20,10)  default 0.00,
 add TTDS_op_NUM_PREMIUM_UPGRADE_EVER_Grouped                           decimal(20,10)  default 0.00,
 add TTDS_op_OD_RF_Grouped                                              decimal(20,10)  default 0.00,
 add TTDS_op_MOVIES_NUM_DOWNGRADE_24M_Grouped                           decimal(20,10)  default 0.00,

 add TTDS_fp_Score                                       decimal(20,10)  default 0.00,
 add TTDS_op_Score                                       decimal(20,10)  default 0.00


update MMS_2017_06
 set TTDS_fp_AGE_Grouped                        = case when CL_Current_Age>=18 and CL_Current_Age<=30             then 1.408
                                                        when CL_Current_Age>30 and CL_Current_Age<=40             then 1.291
                                                        when CL_Current_Age>40 and CL_Current_Age<=50             then 1.137
                                                        when CL_Current_Age>50 and CL_Current_Age<=60             then 0.615
                                                        when CL_Current_Age>60 and CL_Current_Age is not null     then 0.161
                                                        else 0 end,
     TTDS_fp_TENURE_Grouped                     = case when dtv_latest_act_date>=dateadd(mm, -12, observation_dt) then 1.369
                                                        when dtv_latest_act_date>=dateadd(mm, -24, observation_dt) then 0.642
                                                        when dtv_latest_act_date>=dateadd(mm, -36, observation_dt) then 0.417
                                                        when dtv_latest_act_date>=dateadd(mm, -48, observation_dt) then 0.122
                                                        when dtv_latest_act_date>=dateadd(mm, -60, observation_dt) then 0.246
                                                        when dtv_latest_act_date>=dateadd(mm, -72, observation_dt) then 0.084
                                                        when dtv_latest_act_date>=dateadd(mm, -96, observation_dt) then 0.134
                                                        when dtv_latest_act_date>=dateadd(mm, -120, observation_dt) then 0.211
                                                        else 0 end,
     TTDS_fp_NUM_PREMIUM_UPGRADE_EVER_Grouped   = case when num_premium_upgrade_ever is not null then num_premium_upgrade_ever * 0.07 else 0 end,
     TTDS_fp_OD_RF_Grouped                      = case when od_rf = '00-03:1-4' then 0.335
                                                       when od_rf = '00-03:5--' then 0.695
                                                       when od_rf = '03-12'     then 0.134
                                                       when od_rf = '12-99'     then 0.101
                                                       else 0 end,
     TTDS_fp_MOVIES_NUM_DOWNGRADE_24M_Grouped   = case when movies_num_downgrade_24m is null then -1.697
                                                       when movies_num_downgrade_24m =0      then -1.697
                                                       when movies_num_downgrade_24m =1      then -0.754
                                                       when movies_num_downgrade_24m =2      then -0.229
                                                       else 0 end,

      TTDS_op_AGE_Grouped                        = case when CL_Current_Age>=18 and CL_Current_Age<=30             then 0.897
                                                        when CL_Current_Age>30 and CL_Current_Age<=40             then 0.816
                                                        when CL_Current_Age>40 and CL_Current_Age<=50             then 0.693
                                                        when CL_Current_Age>50 and CL_Current_Age<=60             then 0.368
                                                        when CL_Current_Age>60 and CL_Current_Age is not null     then -0.174
                                                        else 0 end,
     TTDS_op_TENURE_Grouped                     = case when dtv_latest_act_date>=dateadd(mm, -12, observation_dt) then -0.25
                                                        when dtv_latest_act_date>=dateadd(mm, -24, observation_dt) then 0.225
                                                        when dtv_latest_act_date>=dateadd(mm, -36, observation_dt) then 0.113
                                                        when dtv_latest_act_date>=dateadd(mm, -48, observation_dt) then 0.189
                                                        when dtv_latest_act_date>=dateadd(mm, -60, observation_dt) then 0.144
                                                        when dtv_latest_act_date>=dateadd(mm, -72, observation_dt) then 0.121
                                                        when dtv_latest_act_date>=dateadd(mm, -96, observation_dt) then 0.108
                                                        when dtv_latest_act_date>=dateadd(mm, -120, observation_dt) then 0.025
                                                        else 0 end,
     TTDS_op_NUM_PREMIUM_UPGRADE_EVER_Grouped   = case when num_premium_upgrade_ever is not null then num_premium_upgrade_ever * 0.07 else 0 end,
     TTDS_op_OD_RF_Grouped                      = case when od_rf = '00-03:1-4' then 0.355
                                                       when od_rf = '00-03:5--' then 0.628
                                                       when od_rf = '03-12'     then 0.327
                                                       when od_rf = '12-99'     then 0.185
                                                       else 0 end,
     TTDS_op_MOVIES_NUM_DOWNGRADE_24M_Grouped   = case when movies_num_downgrade_24m is null then -1.507
                                                       when movies_num_downgrade_24m =0      then -1.507
                                                       when movies_num_downgrade_24m =1      then -0.627
                                                       when movies_num_downgrade_24m =2      then -0.144
                                                       else 0 end

update MMS_2017_06
 set TTDS_fp_Score =  -5.59  
+ TTDS_fp_AGE_Grouped 
+ TTDS_fp_TENURE_Grouped 
+ TTDS_fp_NUM_PREMIUM_UPGRADE_EVER_Grouped 
+ TTDS_fp_OD_RF_Grouped 
+ TTDS_fp_MOVIES_NUM_DOWNGRADE_24M_Grouped
,
TTDS_op_Score =  -4.39  
+ TTDS_op_AGE_Grouped 
+ TTDS_op_TENURE_Grouped 
+ TTDS_op_NUM_PREMIUM_UPGRADE_EVER_Grouped 
+ TTDS_op_OD_RF_Grouped 
+ TTDS_op_MOVIES_NUM_DOWNGRADE_24M_Grouped


-- 12. FAMILY select top 10 * from MMS_2017_06

alter table MMS_2017_06
 add FAMILY_fp_skyplayer                                                decimal(20,10)  default 0.00,
 add FAMILY_fp_premmovies2                                              decimal(20,10)  default 0.00,
 add FAMILY_fp_sp_device_vol_12m_Cap3_grp                               decimal(20,10)  default 0.00,
 add FAMILY_fp_num_ppv_12m_cat                                          decimal(20,10)  default 0.00,

 add FAMILY_op_skyplayer                                                decimal(20,10)  default 0.00,
 add FAMILY_op_SP_LIVE_RECENCY                                          decimal(20,10)  default 0.00,
 add FAMILY_op_premmovies2                                              decimal(20,10)  default 0.00,
 add FAMILY_op_Flag_premium_upgrade_12m                                 decimal(20,10)  default 0.00,
 add FAMILY_op_num_ppv_12m_Cat                                          decimal(20,10)  default 0.00,

 add FAMILY_fp_Score                                                    decimal(20,10)  default 0.00,
 add FAMILY_op_Score                                                    decimal(20,10)  default 0.00


update MMS_2017_06
 set FAMILY_fp_skyplayer                = case when skyplayer = 0   then -0.773 else 0 end,
     FAMILY_fp_premmovies2              = case when PremMovies2 = 0 then -0.551
                                               when PremMovies2 = 1 then 0 end,
     FAMILY_fp_sp_device_vol_12m_Cap3_grp  = case when sp_device_vol_12m = 0 then -0.473
                                               when sp_device_vol_12m = 1 then -1.114
                                               when sp_device_vol_12m = 2 then -0.146
                                               else 0 end,
     FAMILY_fp_num_ppv_12m_cat          = case when num_ppv_12m = 0 then -1.457
                                               when num_ppv_12m <= 2 then -0.726
                                               when num_ppv_12m <= 5 then -0.482
                                               else 0 end,

     FAMILY_op_skyplayer                = case when skyplayer = 0 then -0.99 else 0 end,
     FAMILY_op_sp_live_recency          = case when SP_Live_Recency = 'a)L6M'           then -0.824
                                               when SP_Live_Recency = 'b)7-12M'         then 0.193
                                               when SP_Live_Recency = 'c)12+/Never'     then 0 end,
     FAMILY_op_premmovies2              = case when PremMovies2 = 0 then -0.758
                                               when PremMovies2 = 1 then 0 end,
     FAMILY_op_flag_premium_upgrade_12m = case when Flag_premium_upgrade_12m = 0 then -0.797
                                               when Flag_premium_upgrade_12m = 1 then 0 end,
     FAMILY_op_num_ppv_12m_cat          = case when num_ppv_12m = 0 then -1.281
                                               when num_ppv_12m <= 2 then -0.588
                                               when num_ppv_12m <= 5 then -0.327
                                               else 0 end

update MMS_2017_06
 set FAMILY_fp_Score       = -2.895 + FAMILY_fp_skyplayer + FAMILY_fp_premmovies2
                                    + FAMILY_fp_sp_device_vol_12m_Cap3_grp + FAMILY_fp_num_ppv_12m_cat,
     FAMILY_op_Score       = -3.637 + FAMILY_op_skyplayer + FAMILY_op_sp_live_recency + FAMILY_op_premmovies2
                                    + FAMILY_op_flag_premium_upgrade_12m + FAMILY_op_num_ppv_12m_cat



-- 13. BT MODEL
--select top 10 * from MMS_2017_06 where tenure_yrs_bt is null

alter table MMS_2017_06
 add BT_bb_provider                                                decimal(20,10)  default 0.00,
 add BT_ESPN                                                       decimal(20,10)  default 0.00,
 add BT_SKYGOE                                                     decimal(20,10)  default 0.00,
 add BT_Sports_grp                                                 decimal(20,10)  default 0.00,
 add BT_tenure_yrs                                                 decimal(20,10)  default 0.00,

 add BT_fp_Score                                                   decimal(20,10)  default 0.00

update MMS_2017_06
 set BT_bb_provider = CASE WHEN BB_provider = 'BT' THEN 2.521
                           WHEN BB_provider = 'OTHER' THEN 0.796
                           WHEN BB_provider = 'SKY' THEN 0 ELSE NULL END,
     BT_ESPN        = CASE WHEN ESPN = 0 THEN -1.112
                           WHEN ESPN = 1 THEN 0 ELSE NULL END,
     BT_SKYGOE      = CASE WHEN SKYGOE = 0 THEN -0.097
                           WHEN SKYGOE = 1 THEN 0 ELSE NULL END,
     BT_sports_grp  = CASE WHEN Sports_grp = 'Active' THEN 1.881
                           WHEN Sports_grp = 'Lapsed' THEN 0.534 ELSE 0 END,
     BT_TENURE_YRS  = case when tenure_yrs_BT is not null then tenure_yrs_BT*0.018 else 0 end

select bb_provider, avg(BT_bb_provider), count(*) from MMS_2017_06 group by bb_provider
select ESPN, avg(BT_ESPN), count(*) from MMS_2017_06 group by ESPN
select SKYGOE, avg(BT_SKYGOE), count(*) from MMS_2017_06 group by SKYGOE
select Sports_grp, avg(BT_sports_grp), count(*) from MMS_2017_06 group by Sports_grp
select tenure_yrs_BT, avg(BT_TENURE_YRS), count(*) from MMS_2017_06 group by tenure_yrs_BT

update MMS_2017_06
 set BT_fp_Score = -2.225 + BT_bb_provider + BT_ESPN + BT_SKYGOE + BT_sports_grp + BT_TENURE_YRS

select BT_fp_Score, dtv, count(*) from MMS_2017_06 group by BT_fp_Score, dtv




