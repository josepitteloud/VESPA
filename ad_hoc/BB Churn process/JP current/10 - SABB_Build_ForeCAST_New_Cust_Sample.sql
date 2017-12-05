text
create procedure pitteloudj.SABB_Build_ForeCAST_New_Cust_Sample( in LV integer ) 
begin
  declare Obs_Dt date;
  declare @multiplier bigint;
  set @multiplier = DATEPART(millisecond,now())+738;
  message cast(now() as timestamp) || ' | SABB_Build_ForeCAST_New_Cust_Sample - Build_ForeCAST_New_Cust_Sample -  Begin ' to client;
  truncate table pitteloudj.FORECAST_New_Cust_Sample;
  set Obs_Dt = (select max(calendar_date) from citeam.subs_calendar(LV/100-1,LV/100) where Subs_Week_And_Year < LV);
  set temporary option Query_Temp_Space_Limit = 0;
  insert into FORECAST_New_Cust_Sample
    select end_date,
      subs_year as year,
      subs_week_of_year as week,
      subs_week_and_year as year_week,
      account_number,
      BB_status_code,
      case when BB_Active > 0 then 'BB' else 'Non BB' end as BB_Segment,
      case when ROI > 0 then 'ROI' else 'UK' end as country, --
      BB_package,
      case when BB_Enter_SysCan+BB_Enter_CusCan+BB_Enter_HM+BB_Enter_3rd_Party > 1 then 'MULTI' --- UPDATED next
      when BB_Enter_SysCan > 0 then 'SysCan'
      when BB_Enter_CusCan > 0 then 'CusCan'
      when BB_Enter_HM > 0 then 'HM'
      when BB_Enter_3rd_Party > 0 then '3rd Party'
      else null
      end as Churn_type,cast(null as varchar(4)) as BB_Status_Code_EoW,
      --- ========================================================================= 	
      case when trim(simple_segment) in( '1 Secure' ) then '1 Secure'
      when trim(simple_segment) in( '2 Start' ) then '2 Start'
      when trim(simple_segment) in( '3 Stimulate','2 Stimulate' ) then '3 Stimulate'
      when trim(simple_segment) in( '4 Support','3 Support' ) then '4 Support'
      when trim(simple_segment) in( '5 Stabilise','4 Stabilise' ) then '5 Stabilise'
      when trim(simple_segment) in( '6 Suspense','5 Suspense' ) then '6 Suspense'
      else 'Other/Unknown'
      end as Simple_Segments,BB_offer_rem_and_end_raw,
      cast(null as integer) as BB_offer_rem_and_end,
      BB_tenure_raw,
      cast(null as integer) as BB_tenure,
      my_sky_login_3m_raw,
      cast(null as integer) as my_sky_login_3m,
      talk_type,
      home_owner_status,
      BB_all_calls_1m_raw,
      cast(null as integer) as BB_all_calls_1m,
      cast(0 as tinyint) as node_SA,
      cast(null as varchar(20)) as segment_SA,
      --		, Cast(NULL AS DATE) AS PC_Future_Sub_Effective_Dt 					
      --		, Cast(NULL AS DATE) AS AB_Future_Sub_Effective_Dt 					
      --		, Cast(NULL AS DATE) AS BCRQ_Future_Sub_Effective_Dt 				
      cast(null as date) as PL_Future_Sub_Effective_Dt,
      cast(null as varchar(100)) as DTV_Activation_Type,
      Curr_Offer_start_Date_BB,
      curr_offer_end_date_Intended_BB,
      Prev_offer_end_date_BB,
      cast(null as date) as Future_offer_Start_dt,
      cast(null as date) as Future_end_Start_dt,
      BB_latest_act_dt,
      BB_first_act_dt,
      rand(number()*@multiplier) as rand_sample,
      cast(null as varchar(10)) as sample,
      case when bb_active = 1 and dtv_active = 0 then 1 else 0 end as SABB_flag
      from pitteloudj.cust_fcast_weekly_base_2
      where end_date between Obs_Dt-5*7 and Obs_Dt
      and bb_active = 1 and dtv_active = 0
      and BB_latest_act_dt between(end_date-6) and end_date -- New customers
      and BB_latest_act_dt is not null;
  message cast(now() as timestamp) || ' | SABB_Build_ForeCAST_New_Cust_Sample -  Insert Into FORECAST_New_Cust_Sample completed: ' || @@rowcount to client;
  commit work;
  select a.account_number,
    a.end_date,
    B.subs_year,
    B.subs_week_of_year,
    case when b.Enter_SysCan > 0 then 'SysCan'
    when b.Enter_CusCan > 0 then 'CusCan'
    when b.Enter_HM > 0 then 'HM'
    when b.Enter_3rd_Party > 0 then '3rd Party'
    else null
    end as Churn_type,RANK() over(partition by a.account_number,a.end_date order by b.event_dt desc) as week_rnk
    into #t1
    from FORECAST_New_Cust_Sample as a
      join CITEAM.Broadband_Comms_Pipeline as b on a.account_number = b.account_number
      and a.year = b.subs_year
      and a.week = b.subs_week_of_year
    where a.Churn_type = 'MULTI';
  commit work;
  delete from #t1 where week_rnk > 1;
  create hg index IO1 on #t1(account_number);
  create dttm index IO2 on #t1(end_date);
  commit work;
  update FORECAST_New_Cust_Sample as a
    set a.Churn_type = b.Churn_type from
    FORECAST_New_Cust_Sample as a
    join #t1 as b on a.account_number = b.account_number
    and a.end_date = b.end_date;
  message cast(now() as timestamp) || ' | SABB_Build_ForeCAST_New_Cust_Sample -  Churn_type fixed: ' || @@rowcount to client;
  drop table #t1;
  commit work;
  update FORECAST_New_Cust_Sample as a
    set a.BB_offer_rem_and_end = b.BB_offer_rem_and_end,
    a.BB_tenure = b.BB_tenure,
    a.my_sky_login_3m = b.my_sky_login_3m,
    a.BB_all_calls_1m = b.BB_all_calls_1m,
    a.node_SA = b.node_SA,
    a.segment_SA = b.segment_SA from
    FORECAST_New_Cust_Sample as a
    join pitteloudj.DTV_FCAST_WEEKLY_BASE_2 as b on a.account_number = b.account_number
    and a.end_date = b.end_date;
  message cast(now() as timestamp) || ' | SABB_Build_ForeCAST_New_Cust_Sample -  DTV_fcast variables updated: ' || @@rowcount to client;
  update FORECAST_New_Cust_Sample as sample
    set sample.PL_Future_Sub_Effective_Dt = MoR.PC_Future_Sub_Effective_Dt from
    FORECAST_New_Cust_Sample as sample
    join CITeam.Broadband_Comms_Pipeline as MoR on MoR.account_number = sample.account_number
    and MoR.PC_Future_Sub_Effective_Dt > sample.end_date
    and MoR.event_dt <= sample.end_date
    and(MoR.PC_effective_to_dt > sample.end_date
    or MoR.PC_effective_to_dt is null)
    where sample.BB_Status_Code = 'PC';
  update FORECAST_New_Cust_Sample as sample
    set PL_Future_Sub_Effective_Dt = MoR.AB_Future_Sub_Effective_Dt from
    FORECAST_New_Cust_Sample as sample
    join CITeam.Broadband_Comms_Pipeline as MoR on MoR.account_number = sample.account_number
    and MoR.AB_Future_Sub_Effective_Dt > sample.end_date
    and MoR.event_dt <= sample.end_date
    and(MoR.AB_effective_to_dt > sample.end_date or MoR.AB_effective_to_dt is null)
    where sample.BB_Status_Code = 'AB';
  update FORECAST_New_Cust_Sample as sample
    set PL_Future_Sub_Effective_Dt = MoR.BCRQ_Future_Sub_Effective_Dt from
    FORECAST_New_Cust_Sample as sample
    join CITeam.Broadband_Comms_Pipeline as MoR on MoR.account_number = sample.account_number
    and MoR.AB_Future_Sub_Effective_Dt > sample.end_date
    and MoR.event_dt <= sample.end_date
    and(MoR.AB_effective_to_dt > sample.end_date or MoR.AB_effective_to_dt is null)
    where sample.BB_Status_Code = 'BCRQ';
  update FORECAST_New_Cust_Sample as sample
    set BB_Status_Code = 'AC'
    where PL_Future_Sub_Effective_Dt is null;
  message cast(now() as timestamp) || ' | SABB_Build_ForeCAST_New_Cust_Sample -  COMPLETED' to client
end