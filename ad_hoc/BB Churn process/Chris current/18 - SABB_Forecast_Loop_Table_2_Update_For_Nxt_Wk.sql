text
create procedure spencerc2.SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk
as
begin
  --------------------------------------------------------------------------
  -- Update table for start of next loop -----------------------------------
  --------------------------------------------------------------------------
  message convert(timestamp,now()) || ' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Initializing' to client
  -- set the expected churn date for non-syscan pipeline entries based on previous experience 	
  update Forecast_Loop_Table_2 as base
    set PL_Future_Sub_Effective_Dt = convert(date,base.end_date+dur.Days_To_churn) from
    Forecast_Loop_Table_2 as base
    join DTV_PC_Duration_Dist as dur
    on rand_BB_NotSysCan_Duration between dur.PC_Days_Lower_Prcntl and dur.PC_Days_Upper_Prcntl
    where(BB_3rd_Party > 0
    or BB_CusCan > 0
    or BB_HM > 0)
    and base.BB_Status_Code_EoW = 'PC'
  message convert(timestamp,now()) || ' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Updating churn date for non-syscan PL entries ' || @@rowcount to client
  -- set the expected churn date to be 50 days on for SysCan 	  
  update Forecast_Loop_Table_2 as base
    set PL_Future_Sub_Effective_Dt = convert(date,base.end_date+50) from
    Forecast_Loop_Table_2 as base
    where BB_SysCan > 0
    and base.BB_Status_Code_EoW in( 'AB','BCRQ' ) 
  message convert(timestamp,now()) || ' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Updating churn date for syscan PL entries ' || @@rowcount to client
  --- Setting next curr_offer_start_date_BB
  update Forecast_Loop_Table_2 as base
    set curr_offer_start_date_BB = end_date+3,
    Curr_Offer_end_Date_Intended_BB = dateadd(month,Total_Offer_Duration_Mth,end_date+3) from -- Default 10m offer
    Forecast_Loop_Table_2 as base
    join Offer_Applied_Dur_Dist as offer on base.rand_New_Off_Dur between offer.Dur_Pctl_Lower_Bound and offer.Dur_Pctl_Upper_Bound
    and Offer_Segment = 'Other' ---??? check where we get this table from
    where BB_Offer_Applied = 1
    and not(BB_Status_Code in( 'AB','PC','BCRQ' ) 
    and BB_Status_Code_EoW = 'AC')
  message convert(timestamp,now()) || ' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Active: Updating curr_offer_start_date_BB DONE ' || @@rowcount to client
  --- Setting next curr_offer_start_date_BB
  update Forecast_Loop_Table_2 as base
    set curr_offer_start_date_BB = end_date+3,
    Curr_Offer_end_Date_Intended_BB = dateadd(month,Total_Offer_Duration_Mth,end_date+3) from -- Default 10m offer
    Forecast_Loop_Table_2 as base
    join Offer_Applied_Dur_Dist as offer on base.rand_New_Off_Dur between offer.Dur_Pctl_Lower_Bound and offer.Dur_Pctl_Upper_Bound
    and Offer_Segment = 'Reactivations' ---??? check where we get this table from
    where BB_Offer_Applied = 1
    and BB_Status_Code in( 'AB','PC','BCRQ' ) 
    and BB_Status_Code_EoW = 'AC'
  message convert(timestamp,now()) || ' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Updating curr_offer_start_date_BB DONE' to client
  --- Setting next BB_status code 
  update Forecast_Loop_Table_2
    set BB_Status_Code = Coalesce(BB_Status_Code_EoW,BB_Status_Code)
  --- Clearing not pipeline accounts Future effective dt
  update Forecast_Loop_Table_2 as base
    set PL_Future_Sub_Effective_Dt = null ---?? note changed name here
    where base.BB_Status_Code not in( 'PC','AB','BCRQ' ) 
  --- Clearing the pipeline entry status codes
  update Forecast_Loop_Table_2 as base
    set BB_SysCan = 0,
    BB_CusCan = 0,
    BB_HM = 0,
    BB_3rd_Party = 0
  --- Updating organic growth variables
  update Forecast_Loop_Table_2
    set end_date = end_date+7,
    BB_tenure_raw = BB_tenure_raw+7,
    DTV_Activation_Type = null,
    weekid = weekid+1
  --- Setting offer end date when expiration date happen in the previous week
  update Forecast_Loop_Table_2
    set Prev_offer_end_date_BB = Curr_Offer_end_Date_Intended_BB
    where Curr_Offer_end_Date_Intended_BB <= end_date
  message convert(timestamp,now()) || ' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Checkpoint 1/3' to client
  --- Clearing Offer end date when curr offer ended on the previous week
  update Forecast_Loop_Table_2
    set Curr_Offer_end_Date_Intended_BB = null
    where Curr_Offer_end_Date_Intended_BB <= end_date
  update Forecast_Loop_Table_2
    set BB_offer_applied = 0
    where Curr_Offer_end_Date_Intended_BB <= end_date
  --- 
  update Forecast_Loop_Table_2
    set Prev_offer_end_date_BB = null
    where Prev_offer_end_date_BB < (end_date)-53*7
  message convert(timestamp,now()) || ' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Checkpoint 2/3' to client
  --- Updating Offer Remaining days
  update Forecast_Loop_Table_2
    set BB_offer_rem_and_end_raw = case when BB_Offer_Applied = 1 then DATEDIFF(day,end_date,Curr_Offer_end_Date_Intended_BB)
    else BB_offer_rem_and_end_raw-7
    end
  -----======== PLACEHOLDERS FOR CALLS AND MY SKY LOGIN
  --- Updating my_sky_login_3m_raw 
  message convert(timestamp,now()) || ' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Updating my_sky_login_3m_raw ' to client
  select base.account_number,
    base.end_Date,
    'visit_days'=SUM(visit)
    into #days_visited_3m_2
    from(select account_number,visit_date,'visit'=1
        from vespa_shared.mysky_daily_usage union
      select account_number,end_date,my_sky_login_LW
        from FORECAST_Looped_Sim_Output_Platform) as v
      join Forecast_Loop_Table_2 as base on BASE.account_number = v.account_number
    where visit_date between DATEADD(wk,1,DATEADD(mm,-3,end_date)) and end_date
    group by base.account_number,
    base.end_date
  message convert(timestamp,now()) || ' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - days_visited_3m_2:  ' || @@rowcount to client
  ----------------------------------------------------------------------	
  commit work
  create hg index ID1 on #days_visited_3m_2(account_number)
  create dttm index ID2 on #days_visited_3m_2(end_date)
  create lf index ID3 on #days_visited_3m_2(visit_days)
  commit work
  --- Updating CALLS					
  select base.account_number,
    'call_count'=SUM(calls),
    end_date
    into #BBCalls_Temp_1m_2
    /* Extras */
    from(select account_number,
        call_date,
        'calls'=COUNT(1)
        from cust_inbound_calls as a
          join(select 'min_dt'=DATEADD(month,-3,MIN(end_date)) from Forecast_Loop_Table_2) as b on a.call_date >= b.min_dt
          and contact_activity = 'Inbound'
          and service_call_type in( 'SCT_CUSSER_BBusage','SCT_SALOLY_EOODirect_SABB','SCT_SALRET_BB_Campaign2','SCT_SALRET_BB_Churn','SCT_SALRET_BB_MAC','SCT_SALRET_BB_Online','SCT_SALRET_BB_PIPELINE','SCT_SALRET_BB_TA','SCT_SALRET_BB_TA_Xfer','SCT_SALRET_BB_TVWinback','SCT_SALRET_BB_Value','SCT_SALRET_BB_Value_SA','SCT_SALRET_BB_Value_SA_Xfer','SCT_SALRET_BB_Value_Xfer','SCT_SALRET_BB_Value2','SCT_SALRET_ELP_BB','SCT_SALTRN_BB_TA_Xfer','SCT_SALRET_BB_Campaign1','SCT_SALRET_BB_HighChurn','SCT_SALRET_BB_Value_D&G','SCT_SALRET_BB_HighChurn_Xfer','SCT_CUSSER_BBusage','SCT_SALOLY_EOODirect_SABB','SCT_SALRET_BB_Campaign2','SCT_SALRET_BB_Churn','SCT_SALRET_BB_MAC','SCT_SALRET_BB_Online','SCT_SALRET_BB_PIPELINE','SCT_SALRET_BB_TA','SCT_SALRET_BB_TA_Xfer','SCT_SALRET_BB_TVWinback','SCT_SALRET_BB_Value','SCT_SALRET_BB_Value_SA','SCT_SALRET_BB_Value_SA_Xfer','SCT_SALRET_BB_Value_Xfer','SCT_SALRET_BB_Value2','SCT_SALRET_ELP_BB','SCT_SALTRN_BB_TA_Xfer','SCT_SALRET_BB_Campaign1','SCT_SALRET_BB_HighChurn','SCT_SALRET_BB_Value_D&G','SCT_SALRET_BB_HighChurn_Xfer','SCT_HLPALL_NowTV_Cancel_Xfer','SCT_SALRET_ELP_Xfer','SCT_SALTRN_BB_TA_Xfer','SCT_SALRET_BB_Value_SA_Xfer','SCT_SALVAL_BB_Syscan','SCT_SALRET_BB_Campaign3','SCT_HLPTV__PriceTalk_AVS','SCT_HLPTV__PriceTalk_TO','SCT_OTHCTT_DN1','SCT_SALRET_PriceTalk','Support Broadband and Talk','SCT_WELBBT_Fibre','SCT_WELBBT_Fibre_Engineer','SCT_WELBBT_Fibre_NL','SCT_WELBBT_Fibre_Staff','SCT_WELBBT_Fibre_Staff_Xfer','SCT_WELBBT_Fibre_Xfer','SCT_WELBBT_IncompleteJob','SCT_WELBBT_LinePlant_Xfer','SCT_WELBBT_MoveHome_Xfer','SCT_WELBBT_Nuisance_Xfer','SCT_WELBBT_Order','SCT_WELBBT_OrderRecovery_Direct','SCT_WELBBT_OrderRecovery_Xfer','SCT_WELBBT_Order_Engineer','SCT_WELBBT_Order_NL','SCT_WELBBT_Order_Xfer','SCT_WELBBT_Slamming_Direct','SCT_WELBBT_Staff_Order','SCT_WELBBT_Staff_Order_Xfer','SCT_WELBBT_Support_Xfer','SCT_WELBBT_TalkTechnical','SCT_WELBBT_Technical','SCT_WELBBT_Tech_TO','SCT_SUPBBT_Case_Broadband','SCT_SUPBBT_Case_Broadband_NL','SCT_SUPBBT_Case_Talk','SCT_SUPBBT_Case_Talk_NL','Broadband (One Service)','Broadband Escalation (One Service)','Complaints Broadband','Complaints Broadband (ROI)','Complaints Broadband and Talk (MYSKY)','Escalation Broadband','Escalation Broadband (ROI)','EST Broadband and Talk','Fibre Broadband','General Pool for 16 Olympus Retention','General Pool for 17 Pro Broadband','Help and Troubleshooting (Broadband)','Help and Troubleshooting Broadband / Talk (ROI)','Moving Home Talk / Broadband (ROI)','Pro Broadband','Product Information Broadband / Talk (ROI)','Product Missold Broadband and Talk','SCT_CUSDBT_BBTech','SCT_CUSDBT_Spin_BBTech','SCT_CUSSER_BBusage','SCT_DIALLER_CAM_DIGEXP_BBT','SCT_DIALLER_CAM_ONEEXP_BBT','SCT_DIALLER_CAM_ONEEXP_BBTPlus','SCT_DIALLER_CAM_OSSEXP_BBT_Help','SCT_DIALLER_CAM_OSSEXP_BBT_Welcome','SCT_DIALLER_CAM_OSSEXP_HM_BBT','SCT_DIGEXP_BBT_Fibre_Xfer','SCT_DIGEXP_BBT_Xfer','SCT_ESCCOM_Escalation_BBT_Xfer','SCT_ESCCOM_LeaderSupport_BBT_Xfer','SCT_HLPBBT_Alarm','SCT_HLPBBT_BB_Engineer','SCT_HLPBBT_BB_Engineer_NL','SCT_HLPBBT_BB_Online','SCT_HLPBBT_BB_Online_NL','SCT_HLPBBT_BB_Router','SCT_HLPBBT_BB_Router_NL','SCT_HLPBBT_BB_Technical','SCT_HLPBBT_BB_Technical_HSS','SCT_HLPBBT_BB_Technical_NL','SCT_HLPBBT_BB_Technical_TO','SCT_HLPBBT_BB_Tech_HSS_TO','SCT_HLPBBT_BB_Tech_Xfer','SCT_HLPBBT_ClosedOutage','SCT_HLPBBT_Fibre_D&G','SCT_HLPBBT_Fibre_Xfer','SCT_HLPBBT_Fix_Xfer','SCT_HLPBBT_Main_TO','SCT_HLPBBT_PDS_Xfer','SCT_HLPBBT_Pro_Case','SCT_HLPBBT_Pro_Tech_BB','SCT_HLPBBT_Pro_Tech_Comb','SCT_HLPBBT_Pro_Tech_Talk','SCT_HLPBBT_Pro_Tech_Xfer','SCT_HLPBBT_Pro_Upg_BB','SCT_HLPBBT_Pro_Upg_BB_TO','SCT_HLPBBT_Pro_Upg_Talk','SCT_HLPBBT_Pro_WebHost','SCT_HLPBBT_ST_Tech_Xfer','SCT_HLPBBT_TalkTechnical','SCT_HLPBBT_Talk_Engineer','SCT_HLPBBT_Talk_Tarriff','SCT_HLPBBT_Talk_Tarriff_NL','SCT_HLPBBT_Talk_Technical','SCT_HLPBBT_Talk_Technical_HSS','SCT_HLPBBT_Talk_Technical_NL','SCT_HLPBBT_Talk_Tech_HSS_TO','SCT_HLPBBT_Talk_Tech_TO','SCT_HLPBBT_Technical','SCT_HLPBBT_Tech_Connect','SCT_HLPBBT_Tech_Connect_NL','SCT_HLPBBT_Tech_Fibre','SCT_HLPBBT_Tech_Fibre_NL','SCT_HLPBBT_Tech_NL_FB','SCT_HLPBBT_Tech_TO','SCT_ONEEXP_BBT','SCT_ONEEXP_BBTPlus_Xfer','SCT_ONEEXP_BBT_Xfer','SCT_OSSEXP_BBT','SCT_OSSEXP_BBT_APP','SCT_OSSEXP_BBT_Help','SCT_OSSEXP_BBT_Help_Xfer','SCT_OSSEXP_BBT_Welcome_Xfer','SCT_OSSEXP_HM_BBT_Xfer','SCT_REPEXR_BBST','SCT_REPEXR_BBST_Order','SCT_REPEXR_BBST_Order_TO','SCT_REPEXR_BBST_TO','SCT_REPHLP_BBST','SCT_REPHLP_BBST_Direct','SCT_REPHLP_BBST_TO','SCT_REPHLP_BBST_Xfer','SCT_REPHLP_Fibre','SCT_REPWEL_BBST','SCT_REPWEL_BBST_TO','SCT_REPWEL_Fibre','SCT_SALATT_Olympus_Direct','SCT_SALATT_Olympus_Redirect','SCT_SALATT_Olympus_Xfer   ','SCT_SALEXC_BB','SCT_SALEXC_BBFF','SCT_SALEXC_BBMAC','SCT_SALEXC_BBMAC_Xfer','SCT_SALEXC_BBNLP','SCT_SALEXC_BBNoLR','SCT_SALEXC_BBPreActive','SCT_SALEXC_BB_Xfer','SCT_SALEXC_Fibre','SCT_SALEXC_Olympus','SCT_SALEXC_ROI_BBT_Upgrades','SCT_SALEXC_ROI_SwitcherBB','SCT_SALOLY_EOODDR_CAN_SABB','SCT_SALOLY_EOODDR_DGBT_SABB','SCT_SALOLY_EOODirect_SABB   ','SCT_SALOLY_Olympus_Xfer','SCT_SALPAT_ROI_BB','SCT_SALPAT_ROI_BB_Xfer','SCT_SALPAT_ROI_Fibre_Direct ','SCT_SALRET_BB_Campaign1','SCT_SALRET_BB_Campaign2','SCT_SALRET_BB_Campaign3','SCT_SALRET_BB_Churn','SCT_SALRET_BB_HighChurn','SCT_SALRET_BB_HighChurn_Xfer','SCT_SALRET_BB_MAC','SCT_SALRET_BB_Online','SCT_SALRET_BB_PIPELINE','SCT_SALRET_BB_TA','SCT_SALRET_BB_TA_Xfer','SCT_SALRET_BB_TVWinback','SCT_SALRET_BB_Value','SCT_SALRET_BB_Value2','SCT_SALRET_BB_ValueBill','SCT_SALRET_BB_ValueBill_TO','SCT_SALRET_BB_Value_D&G','SCT_SALRET_BB_Value_SA','SCT_SALRET_BB_Value_SA_Xfer','SCT_SALRET_BB_Value_Xfer','SCT_SALRET_ELP_BB','SCT_SALRTM_BBINFO','SCT_SALRTM_SHMS_Olympus','SCT_SALTRN_BB_HighChurn','SCT_SALTRN_BB_HighChurn_Xfer','SCT_SALTRN_BB_TA_Xfer','SCT_SALVAL_BB_Syscan' ) 
        group by account_number,call_date union
      select account_number,
        end_date,
        calls_LW
        from FORECAST_Looped_Sim_Output_Platform
        where calls_LW > 0) as temp
      join Forecast_Loop_Table_2 as base on base.account_number = temp.account_number
    where call_date between DATEADD(week,1,DATEADD(mm,-1,end_date)) and end_date
    group by base.account_number,end_date
  message convert(timestamp,now()) || ' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - BBCalls_Temp_1m_2:  ' || @@rowcount to client
  commit work
  create hg index ID1 on #BBCalls_Temp_1m_2(account_number)
  create dttm index ID2 on #BBCalls_Temp_1m_2(end_date)
  create lf index ID3 on #BBCalls_Temp_1m_2(call_count)
  commit work
  select b.account_number,
    b.segment_sa,
    'L_12'=MAX(case when a.end_date between DATEADD(month,-12,b.end_date) and DATEADD(week,-1,b.end_date) then a.my_sky_login_3m_raw else 0 end), -- Max Login in the past 12 month
    'L_9'=MAX(case when a.end_date between DATEADD(month,-9,b.end_date) and DATEADD(week,-1,b.end_date) then a.my_sky_login_3m_raw else 0 end), -- Max Login in the past 9 month
    'L_6'=MAX(case when a.end_date between DATEADD(month,-6,b.end_date) and DATEADD(week,-1,b.end_date) then a.my_sky_login_3m_raw else 0 end), -- Max Login in the past 6 month
    'L_3'=MAX(case when a.end_date between DATEADD(month,-3,b.end_date) and DATEADD(week,-1,b.end_date) then a.my_sky_login_3m_raw else 0 end),
    'C_12'=MAX(case when a.end_date between DATEADD(month,-12,b.end_date) and DATEADD(week,-1,b.end_date) then a.BB_all_calls_1m_raw else 0 end), -- Max Login in the past 12 month
    'C_9'=MAX(case when a.end_date between DATEADD(month,-9,b.end_date) and DATEADD(week,-1,b.end_date) then a.BB_all_calls_1m_raw else 0 end), -- Max Login in the past 9 month
    'C_6'=MAX(case when a.end_date between DATEADD(month,-6,b.end_date) and DATEADD(week,-1,b.end_date) then a.BB_all_calls_1m_raw else 0 end), -- Max Login in the past 6 month
    'C_3'=MAX(case when a.end_date between DATEADD(month,-3,b.end_date) and DATEADD(week,-1,b.end_date) then a.BB_all_calls_1m_raw else 0 end),
    'Login_group'=case when L_12 = 0 then 1
    when L_9 = 0 then 2
    when L_6 = 0 then 3
    when L_3 = 0 then 4
    else 5
    end,'Call_group'=case when C_12 = 0 then 1
    when C_9 = 0 then 2
    when C_6 = 0 then 3
    when C_3 = 0 then 4
    else 5
    end,'Rand_Login'=convert(real,null),
    'Rand_call'=convert(real,null)
    into #t_prob
    from Forecast_Loop_Table_2 as b
      join pitteloudj.cust_fcast_weekly_base_2 as a on a.account_number = b.account_number
    group by b.account_number,
    b.segment_sa
  commit work
  update #t_prob
    set Rand_LOGIN = RAND((convert(real,account_number))*DATEPART(ms,GETDATE())),
    Rand_call = RAND((convert(real,account_number)*10)*DATEPART(ms,GETDATE()))
  create hg index ID1 on #t_prob(account_number)
  create lf index ID2 on #t_prob(segment_sa)
  create lf index ID3 on #t_prob(Login_group)
  create lf index ID4 on #t_prob(Call_group)
  message convert(timestamp,now()) || ' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - t_prob:  ' || @@rowcount to client
  update Forecast_Loop_Table_2 as a
    set a.my_sky_login_3m_raw = COALESCE(c.Calls_LW,0)+COALESCE(d.visit_days,0),
    a.my_sky_login_LW = COALESCE(c.Calls_LW,0) from
    Forecast_Loop_Table_2 as a
    join #t_prob as b on a.account_number = b.account_number
    left outer join #days_visited_3m_2 as d on a.account_number = d.account_number
    left outer join SABB_my_sky_login_prob_TABLE as c on b.Login_group = c.Prob_Group
    and b.segment_sa = c.segment_sa
    and Rand_login between Lower_limit and UPPER_LIMIT
  message convert(timestamp,now()) || ' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Updateing Forecast_Loop_Table_2/1:  ' || @@rowcount to client
  update Forecast_Loop_Table_2 as a
    set a.BB_all_calls_1m_raw = COALESCE(c.Calls_LW,0)+COALESCE(d.call_count,0),
    a.Calls_LW = COALESCE(c.Calls_LW,0) from
    Forecast_Loop_Table_2 as a
    join #t_prob as b on a.account_number = b.account_number
    left outer join #BBCalls_Temp_1m_2 as d on a.account_number = d.account_number
    left outer join SABB_BB_Calls_prob_TABLE as c on b.Login_group = c.Prob_Group
    and b.segment_sa = c.segment_sa
    and Rand_call between Lower_limit and UPPER_LIMIT
  message convert(timestamp,now()) || ' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Updateing Forecast_Loop_Table_2/2:  ' || @@rowcount to client
  message convert(timestamp,now()) || ' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Checkpoint 3/3' to client
  --- Refreshing binned variables
  update Forecast_Loop_Table_2
    set my_sky_login_3m = case when my_sky_login_3m_raw > 2 then 3 else my_sky_login_3m_raw end,
    BB_all_calls_1m = case when BB_all_calls_1m_raw = 0 then 0 else 1 end,
    BB_offer_rem_and_end = case when BB_offer_rem_and_end_raw between-9998 and-1015 then-3
    when BB_offer_rem_and_end_raw between-1015 and-215 then-2
    when BB_offer_rem_and_end_raw between-215 and-75 then-1
    when BB_offer_rem_and_end_raw between-74 and-0 then 0
    when BB_offer_rem_and_end_raw between 1 and 62 then 1
    when BB_offer_rem_and_end_raw between 63 and 162 then 2
    when BB_offer_rem_and_end_raw between 163 and 271 then 3
    when BB_offer_rem_and_end_raw > 271 then 4
    else-9999
    end,BB_tenure = case when BB_tenure_raw <= 118 then 1
    when BB_tenure_raw between 119 and 231 then 2
    when BB_tenure_raw between 231 and 329 then 3
    when BB_tenure_raw between 329 and 391 then 4
    when BB_tenure_raw between 392 and 499 then 5
    when BB_tenure_raw between 499 and 641 then 6
    when BB_tenure_raw between 641 and 1593 then 7
    when BB_tenure_raw > 1593 then 8
    else-1
    end
  --- Refreshing nodes and segments
  update Forecast_Loop_Table_2 as a
    set sabb_forecast_segment = convert(varchar(4),node),
    segment_sa = segment from
    Forecast_Loop_Table_2 as a
    join pitteloudj.BB_SABB_Churn_segments_lookup as b on a.BB_offer_rem_and_end = b.BB_offer_rem_and_end
    and a.BB_tenure = b.BB_tenure
    and a.my_sky_login_3m = b.my_sky_login_3m
    and a.talk_type = b.talk_type
    and a.home_owner_status = b.home_owner_status
    and a.BB_all_calls_1m = b.BB_all_calls_1m
  drop table #days_visited_3m_2
  drop table #BBCalls_Temp_1m_2
  drop table #t_prob
end
-- Grant execute rights to the members of CITeam
grant execute on spencerc2.SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk to CITeam