text
create procedure spencerc2.SABB_Forecast_Insert_New_Custs_Into_Loop_Table_2( 
  in Forecast_Start_Wk integer,
  in Forecast_End_Wk integer,
  in True_Sample_Rate real ) 
begin
  declare @new_cust_end_date date;
  declare @new_cust_subs_week_and_year integer;
  declare @new_cust_subs_week_of_year integer;
  declare @new_cust_subs_year integer;
  declare @multiplier bigint;
  message cast(now() as timestamp) || ' | SABB_Forecast_Insert_New_Custs_Into_Loop_Table_2 - Initialization begin ' to client;
  set @multiplier = DATEPART(millisecond,now())+2631;
  select * into #Sky_Calendar from CITeam.subs_Calendar(Forecast_Start_Wk/100,Forecast_End_Wk/100);
  set @new_cust_end_date = (select max(end_date+7) from Forecast_Loop_Table_2);
  set @new_cust_subs_week_and_year = (select max(subs_week_and_year) from #sky_calendar where calendar_date = @new_cust_end_date);
  set @new_cust_subs_week_of_year = (select max(subs_week_of_year) from #sky_calendar where calendar_date = @new_cust_end_date);
  set @new_cust_subs_year = (select max(subs_year) from #sky_calendar where calendar_date = @new_cust_end_date);
  drop table if exists #new_customers_last_2Yrs_2;
  select *,
    rand(number()*@multiplier+163456) as rand_sample2,
    rand(number()*@multiplier+1) as e1,
    rand(number()*@multiplier+2) as e2,
    rand(number()*@multiplier+3) as e3,
    rand(number()*@multiplier+4) as e4,
    rand(number()*@multiplier+5) as e5,
    rand(number()*@multiplier+6) as e6,
    rand(number()*@multiplier+7) as e7
    into #new_customers_last_2Yrs_2
    from Forecast_New_Cust_Sample;
  drop table if exists #new_customers_last_2Yrs_3;
  select *,
    row_number() over(order by rand_sample2 asc) as Rand_Rnk
    into #new_customers_last_2Yrs_3
    from #new_customers_last_2Yrs_2;
  delete from #new_customers_last_2Yrs_3 as new_cust from
    #new_customers_last_2Yrs_3 as new_cust
    join Activation_Vols as act on new_cust.Rand_Rnk > act.New_Customers*true_sample_rate
    and act.subs_week_of_year = @new_cust_subs_week_of_year;
  message cast(now() as timestamp) || ' | SABB_Forecast_Insert_New_Custs_Into_Loop_Table_2 - Table insert begin ' to client;
  insert into Forecast_Loop_Table_2
    ( account_number,
    end_date,
    subs_week_and_year,
    subs_year,
    subs_week_of_year,
    weekid,
    BB_Status_Code,
    churn_type,
    BB_Status_Code_EoW,
    BB_Segment,
    country,
    BB_package,
    BB_offer_rem_and_end_raw,
    BB_offer_rem_and_end,
    BB_tenure_raw,
    BB_tenure,
    my_sky_login_3m_raw,
    my_sky_login_3m,
    talk_type,
    home_owner_status,
    BB_all_calls_1m_raw,
    BB_all_calls_1m,
    Simple_Segments,
    sabb_forecast_segment,
    segment_SA,
    PL_Future_Sub_Effective_Dt,
    DTV_Activation_Type,
    Curr_Offer_start_Date_BB,
    Curr_offer_end_date_Intended_BB,
    Prev_offer_end_date_BB,
    Future_offer_Start_dt,
    Future_end_Start_dt,
    BB_first_act_dt,
    rand_sample,
    sample,
    SABB_flag,
    rand_action_Pipeline,
    rand_BB_Offer_Applied,
    rand_Intrawk_BB_NotSysCan,
    rand_Intrawk_BB_SysCan,
    rand_BB_Pipeline_Status_Change,
    rand_New_Off_Dur,
    rand_BB_NotSysCan_Duration,
    SABB_forecast_segment_COUNT,
    SABB_Group_rank,
    pct_SABB_COUNT,
    SABB_Churn,
    BB_offer_applied,
    DTV_AB,
    cum_BB_Offer_Applied_rate,
    pred_bb_enter_SysCan_rate,
    pred_bb_enter_SysCan_YoY_Trend,
    cum_bb_enter_SysCan_rate,
    pred_bb_enter_CusCan_rate,
    pred_bb_enter_CusCan_YoY_Trend,
    cum_bb_enter_CusCan_rate,
    pred_bb_enter_HM_rate,
    pred_bb_enter_HM_YoY_Trend,
    cum_bb_enter_HM_rate,
    pred_bb_enter_3rd_party_rate,
    pred_bb_enter_3rd_party_YoY_Trend,
    cum_bb_enter_3rd_party_rate,
    pred_BB_Offer_Applied_rate,
    pred_BB_Offer_Applied_YoY_Trend,
    CusCan,
    SysCan,
    HM,
    _3rd_Party,
    calls_LW,
    my_sky_login_LW,
    BB_SysCan,
    BB_CusCan,
    BB_HM,
    BB_3rd_Party ) 
    select replicate(CHAR(65+remainder((counter-1),53)),(counter-1)/53+1) || a.account_number as account_number,
      @new_cust_end_date-7 as end_date,
      @new_cust_subs_week_and_year as subs_week_and_year,
      @new_cust_subs_year,
      @new_cust_subs_week_of_year as subs_week_of_year,
      (year(@new_cust_end_date)-2010)*52+@new_cust_subs_week_of_year as weekid,
      BB_Status_Code,
      churn_type,
      BB_Status_Code_EoW,
      BB_Segment,
      country,
      BB_package,
      BB_offer_rem_and_end_raw,
      BB_offer_rem_and_end,
      BB_tenure_raw,
      BB_tenure,
      my_sky_login_3m_raw,
      my_sky_login_3m,
      talk_type,
      home_owner_status,
      BB_all_calls_1m_raw,
      BB_all_calls_1m,
      Simple_Segments,
      cast(node_SA as varchar),
      segment_SA,
      PL_Future_Sub_Effective_Dt,
      DTV_Activation_Type,
      Curr_Offer_start_Date_BB,
      Curr_offer_end_date_Intended_BB,
      Prev_offer_end_date_BB,
      Future_offer_Start_dt,
      Future_end_Start_dt,
      BB_first_act_dt,
      rand_sample2,
      sample,
      SABB_flag,
      e1,
      e2,
      e3,
      e4,
      e5,
      e6,
      e7,
      COUNT() over(partition by node_SA) as y1,
      cast(row_number() over(partition by node_SA order by e1 asc) as real) as y2,
      y2/y1,
      cast(0 as tinyint),
      0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
      from #new_customers_last_2Yrs_3 as a;
  message cast(now() as timestamp) || ' | SABB_Forecast_Insert_New_Custs_Into_Loop_Table_2 - Forecast_Loop_Table_2 insert done: ' || @@rowcount to client;
  commit work;
  message cast(now() as timestamp) || ' | SABB_Forecast_Insert_New_Custs_Into_Loop_Table_2 - COMPLETED ' to client
end